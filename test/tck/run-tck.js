/**
 * Headless runner for the Eclipse Sparkplug TCK, Edge Node profile.
 *
 * The TCK ships a Nuxt web console, which makes it look like a manual process.
 * It isn't: the console is just an MQTT client, and the whole control plane is
 * plain MQTT topics (see Constants.java in the TCK source). This script speaks
 * that protocol directly and drives the Node-RED nodes under test in-process
 * via node-red-node-test-helper, so the whole thing runs unattended in CI.
 *
 * None of the Edge Node tests call the TCK's prompt() - only the Host profile
 * tests do - so no operator interaction is needed. The TCK also brings its own
 * simulated Host Application online, so there is nothing external to
 * orchestrate.
 *
 * Usage:  node test/tck/run-tck.js [testName ...]
 * Exits non-zero if any test does not report OVERALL: PASS.
 */

const mqtt = require("mqtt");
const path = require("path");
const fs = require("fs");
const helper = require("node-red-node-test-helper");
const sparkplugNode = require("../../mqtt-sparkplug-plus.js");
const { buildFlow } = require("./tck-flow");

helper.init(require.resolve("node-red"));

// ── TCK control plane (tck/.../test/common/Constants.java) ───────────────────
const TCK_TEST_CONTROL = "SPARKPLUG_TCK/TEST_CONTROL";
const TCK_RESULT = "SPARKPLUG_TCK/RESULT";
const TCK_LOG = "SPARKPLUG_TCK/LOG";

const BROKER_URL = process.env.TCK_BROKER || "mqtt://localhost:1883";
const HOST_APP_ID = process.env.TCK_HOST_APP_ID || "SparkplugTCKHost";
const GROUP_ID = process.env.TCK_GROUP_ID || "SparkplugTCKGroup";
const EDGE_NODE_ID = process.env.TCK_EDGE_NODE_ID || "NodeRedEdgeNode";
const DEVICE_ID = process.env.TCK_DEVICE_ID || "NodeRedDevice";

const TEST_TIMEOUT_MS = Number(process.env.TCK_TEST_TIMEOUT_MS || 90000);
const BIRTH_TIMEOUT_MS = Number(process.env.TCK_BIRTH_TIMEOUT_MS || 20000);
const RESULTS_FILE = process.env.TCK_RESULTS_FILE || "tck-results.json";

// Every wait that depends on the broker or the TCK extension needs a bound.
// MQTT.js reconnects forever by default and its QoS 2 publish callback only
// fires after PUBCOMP, so a wedged broker parks a promise that never settles.
const CONNECT_TIMEOUT_MS = Number(process.env.TCK_CONNECT_TIMEOUT_MS || 30000);
// Generous, because this bounds a QoS 2 round trip (PUBREC/PUBREL/PUBCOMP) against
// an extension that does real work in its callback. 15s was enough locally but lost
// on the CI runner often enough that three of five tests needed a retry.
const CONTROL_TIMEOUT_MS = Number(process.env.TCK_CONTROL_TIMEOUT_MS || 45000);
const HELPER_TIMEOUT_MS = Number(process.env.TCK_HELPER_TIMEOUT_MS || 15000);
// Backstop for the whole run. Well under the workflow's step timeout so the
// script gets to write partial results before CI kills it.
const RUN_TIMEOUT_MS = Number(process.env.TCK_RUN_TIMEOUT_MS || 900000);

const brokerUrl = new URL(BROKER_URL.replace(/^mqtt(s?)/, "http$1"));
const BROKER_HOST = brokerUrl.hostname;
const BROKER_PORT = brokerUrl.port || "1883";

// The two metrics the device is born with. Every configured metric must have a
// value before this node publishes DBIRTH (see trySendBirth in
// mqtt-sparkplug-plus.js), so the harness has to feed them.
const DEVICE_METRICS = {
	"test/int": { dataType: "Int32" },
	"test/bool": { dataType: "Boolean" }
};

const initialMetricValues = () => [
	{ name: "test/int", value: 1, type: "Int32", timestamp: Date.now() },
	{ name: "test/bool", value: true, type: "Boolean", timestamp: Date.now() }
];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Reject if `promise` has not settled within `ms`.
 *
 * Note this does not cancel the underlying operation - nothing in the MQTT.js
 * API allows that - it only stops the run from waiting on it forever. Callers
 * that hold a socket open are responsible for ending it on the failure path so
 * the process can still exit.
 */
function withTimeout(promise, ms, label) {
	let timer;
	return Promise.race([
		promise,
		new Promise((_, reject) => {
			timer = setTimeout(() => reject(new Error(`timed out after ${ms}ms: ${label}`)), ms);
		})
	]).finally(() => clearTimeout(timer));
}

/**
 * Connect an MQTT client and subscribe, both bounded.
 *
 * A plain `once("error")` is not enough to catch a broker that is listening but
 * never completes the handshake: MQTT.js treats that as a retryable condition
 * and reconnects on a timer forever without ever emitting a terminal error. The
 * socket is destroyed on failure so a half-open client cannot hold the event
 * loop open and stop the process from exiting.
 */
async function connectAndSubscribe(client, topics, opts, label) {
	try {
		await withTimeout(
			new Promise((resolve, reject) => {
				client.once("connect", resolve);
				client.once("error", reject);
			}),
			CONNECT_TIMEOUT_MS,
			`${label} client connecting to ${BROKER_URL}`
		);
		await withTimeout(
			new Promise((resolve, reject) =>
				client.subscribe(topics, opts, (err) => (err ? reject(err) : resolve()))
			),
			CONTROL_TIMEOUT_MS,
			`${label} client subscribing`
		);
	} catch (err) {
		client.end(true);
		throw err;
	}
}

// ── Node-RED lifecycle ──────────────────────────────────────────────────────

function loadFlow(edgeNodeId, flowOpts) {
	const flow = buildFlow(
		Object.assign(
			{
				groupId: GROUP_ID,
				edgeNodeId: edgeNodeId,
				deviceId: DEVICE_ID,
				brokerHost: BROKER_HOST,
				brokerPort: BROKER_PORT,
				metrics: DEVICE_METRICS,
				// Every edge test brings the simulated Host Application online and
				// asserts tck-id-message-flow-edge-node-birth-publish-phid-wait:
				// the node MUST wait for STATE before NBIRTH. So the node under
				// test is always configured with a Primary Host.
				primaryScada: HOST_APP_ID
			},
			flowOpts
		)
	);
	return withTimeout(
		new Promise((resolve) => helper.load(sparkplugNode, flow, {}, resolve)),
		HELPER_TIMEOUT_MS,
		"Node-RED flow load"
	);
}

async function unloadFlow() {
	// A stuck unload is not worth aborting the run over - the next test uses a
	// fresh edge node id anyway - but it must not block either.
	try {
		await withTimeout(helper.unload(), HELPER_TIMEOUT_MS, "Node-RED flow unload");
	} catch (err) {
		console.log(`  ! ${err.message}`);
	}
	// Give the broker a moment to process the resulting DISCONNECT/NDEATH before
	// the next test starts observing.
	await sleep(500);
}

/** Feed every configured metric so the device reaches its DBIRTH condition. */
function sendInitialMetricValues() {
	helper.getNode("device").receive({ payload: { metrics: initialMetricValues() } });
}

// ── Sparkplug observer ──────────────────────────────────────────────────────

/**
 * A passive MQTT client used to tell when the node under test has birthed, so
 * the harness can sequence its actions instead of guessing with sleeps.
 */
class Observer {
	constructor(client) {
		this.client = client;
		this.seen = new Set();
		this.waiters = [];
		client.on("message", (topic) => {
			this.seen.add(topic);
			this.waiters = this.waiters.filter((w) => {
				if (topic.startsWith(w.prefix)) {
					w.resolve(topic);
					return false;
				}
				return true;
			});
		});
	}

	static async connect() {
		const client = mqtt.connect(BROKER_URL, { clientId: `tck-observer-${Date.now()}` });
		await connectAndSubscribe(client, "spBv1.0/#", { qos: 0 }, "observer");
		return new Observer(client);
	}

	reset() {
		this.seen.clear();
	}

	/** Resolve when a topic starting with `prefix` arrives (or already has). */
	wait(prefix, timeoutMs, label) {
		for (const topic of this.seen) {
			if (topic.startsWith(prefix)) return Promise.resolve(topic);
		}
		return new Promise((resolve, reject) => {
			const waiter = { prefix, resolve };
			this.waiters.push(waiter);
			setTimeout(() => {
				this.waiters = this.waiters.filter((w) => w !== waiter);
				reject(new Error(`timed out after ${timeoutMs}ms waiting for ${label || prefix}`));
			}, timeoutMs);
		});
	}

	waitForNBirth(edgeNodeId) {
		return this.wait(`spBv1.0/${GROUP_ID}/NBIRTH/${edgeNodeId}`, BIRTH_TIMEOUT_MS, "NBIRTH");
	}

	waitForDBirth(edgeNodeId) {
		return this.wait(
			`spBv1.0/${GROUP_ID}/DBIRTH/${edgeNodeId}/${DEVICE_ID}`,
			BIRTH_TIMEOUT_MS,
			"DBIRTH"
		);
	}

	end() {
		this.client.end(true);
	}
}

/** Load the flow and drive it all the way to NBIRTH + DBIRTH. */
async function birth(observer, edgeNodeId, flowOpts) {
	await loadFlow(edgeNodeId, flowOpts);
	await observer.waitForNBirth(edgeNodeId);
	sendInitialMetricValues();
	await observer.waitForDBirth(edgeNodeId);
}

// ── TCK control client ──────────────────────────────────────────────────────

class TckControl {
	constructor(client) {
		this.client = client;
		this.resultWaiter = null;
		this.logs = [];
		client.on("message", (topic, payload) => {
			const text = payload.toString();
			if (topic === TCK_LOG) {
				this.logs.push(text);
				if (process.env.TCK_VERBOSE) console.log(`  [tck] ${text}`);
			} else if (topic === TCK_RESULT) {
				if (this.resultWaiter) {
					const waiter = this.resultWaiter;
					this.resultWaiter = null;
					waiter(text);
				}
			}
		});
	}

	static async connect() {
		const client = mqtt.connect(BROKER_URL, { clientId: `tck-harness-${Date.now()}` });
		await connectAndSubscribe(client, [TCK_RESULT, TCK_LOG], { qos: 2 }, "control");
		return new TckControl(client);
	}

	/** Arm the result listener *before* the test starts, to avoid a race. */
	expectResult() {
		return new Promise((resolve) => {
			this.resultWaiter = resolve;
		});
	}

	publish(topic, message) {
		// QoS 2, so this callback waits on the full PUBREC/PUBREL/PUBCOMP
		// handshake. If the TCK extension stops responding it never fires.
		return withTimeout(
			new Promise((resolve, reject) =>
				this.client.publish(topic, message, { qos: 2 }, (err) => (err ? reject(err) : resolve()))
			),
			CONTROL_TIMEOUT_MS,
			`publish to ${topic}`
		);
	}

	startTest(testName, params) {
		this.logs = [];
		const cmd = `NEW_TEST EDGE ${testName} ${params.join(" ")}`;
		console.log(`  -> ${cmd}`);
		return this.publish(TCK_TEST_CONTROL, cmd);
	}

	endTest() {
		return this.publish(TCK_TEST_CONTROL, "END_TEST");
	}

	end() {
		this.client.end(true);
	}
}

// ── Test definitions ────────────────────────────────────────────────────────

const TESTS = [
	{
		name: "SessionEstablishmentTest",
		// The TCK must observe the CONNECT itself, so the flow is loaded only
		// after the test is armed.
		async run({ observer, edgeNodeId }) {
			await birth(observer, edgeNodeId);
		}
	},
	{
		name: "SessionTerminationTest",
		async run({ observer, edgeNodeId }) {
			await birth(observer, edgeNodeId);
			// Unloading closes the nodes, which disconnects the client and
			// produces the DDEATH/NDEATH the test looks for.
			await unloadFlow();
		}
	},
	{
		name: "SendDataTest",
		async run({ observer, edgeNodeId }) {
			await birth(observer, edgeNodeId);

			// DDATA - a value change on an already-born device.
			helper.getNode("device").receive({
				payload: { metrics: [{ name: "test/int", value: 42, type: "Int32", timestamp: Date.now() }] }
			});

			// NDATA - via the generic out node, which publishes whatever topic and
			// payload it is handed, with no validation and no seq of its own.
			//
			// The metric MUST be one the *edge node* births. NBIRTH carries only
			// "Node Control/Rebirth" and "bdSeq" (plus template definitions) - the
			// broker node has no configuration surface for edge-level data metrics -
			// and test/int lives solely in the device's DBIRTH. Naming it here made
			// the harness itself violate tck-id-topics-nbirth-metric-reqs, which
			// requires every NDATA metric to have appeared in the NBIRTH.
			//
			// seq is taken from the broker's own counter rather than hardcoded, so it
			// stays correct no matter how many messages the birth sequence used.
			await sleep(500);
			helper.getNode("out").receive({
				topic: `spBv1.0/${GROUP_ID}/NDATA/${edgeNodeId}`,
				payload: {
					timestamp: Date.now(),
					metrics: [
						{ name: "Node Control/Rebirth", value: false, type: "Boolean", timestamp: Date.now() }
					],
					seq: helper.getNode("broker").nextSeq()
				}
			});
		}
	},
	{
		name: "ReceiveCommandTest",
		// The TCK sends NCMD rebirth / DCMD and watches the response, so the
		// flow just needs to stay up.
		async run({ observer, edgeNodeId }) {
			await birth(observer, edgeNodeId);
		}
	},
	{
		name: "PrimaryHostTest",
		// This test drives the simulated host online/offline itself and checks
		// the node births only for the correct host being ONLINE. The node must
		// therefore already be connected and waiting on STATE when it starts.
		loadBeforeStart: true,
		async run({ edgeNodeId }) {
			// Values are queued while offline and flushed when the correct host
			// goes ONLINE, so NBIRTH and DBIRTH both follow the STATE message.
			sendInitialMetricValues();
		}
	}
];

// ── Result interpretation ───────────────────────────────────────────────────
//
// The TCK appends "but INCOMPLETE" to OVERALL whenever any non-Monitor
// assertion is NOT EXECUTED. Some assertions cannot apply to this node at all,
// so INCOMPLETE on its own is not a failure - but an assertion going
// unexecuted for any *other* reason is a coverage gap we do want to see. Hence
// the allowlist: anything not in it makes the run fail.
const EXPECTED_NOT_EXECUTED = {
	"principles-persistence-clean-session-50":
		"node connects with MQTT 3.1.1 (protocolVersion 4); the -311 variant is asserted instead",
	"payloads-ndeath-will-message-publisher-disconnect-mqtt50":
		"node connects with MQTT 3.1.1 (protocolVersion 4); the -311 variant is asserted instead",
	"payloads-alias-uniqueness": "metric aliases are an optional feature group, not used by this node",
	// Templates are an optional feature group. The node does support them, but
	// the spec requires that if any assertion in an optional group is tested,
	// all of them must pass - so the TCK fixture deliberately uses plain metrics.
	"payloads-template-definition-members": "templates are an optional group, not used by the TCK fixture",
	"payloads-template-definition-nbirth": "templates are an optional group, not used by the TCK fixture",
	"payloads-template-definition-parameters": "templates are an optional group, not used by the TCK fixture",
	"payloads-template-definition-parameters-default":
		"templates are an optional group, not used by the TCK fixture",
	"payloads-template-definition-ref": "templates are an optional group, not used by the TCK fixture",
	"payloads-template-instance-members": "templates are an optional group, not used by the TCK fixture",
	"payloads-template-instance-members-birth": "templates are an optional group, not used by the TCK fixture",
	"payloads-template-instance-members-data": "templates are an optional group, not used by the TCK fixture",
	"payloads-template-instance-parameters": "templates are an optional group, not used by the TCK fixture",
	"payloads-template-instance-ref": "templates are an optional group, not used by the TCK fixture"
};

// Assertions that fail as a timing race rather than a defect.
//
// Both DBIRTH timestamp assertions come from one check in
// SessionEstablishmentTest.checkDBirth: `payloadTs < receivedTime`, with a strict
// upper bound, where receivedTime is `new Date()` taken as the broker processes
// the packet. Over loopback the DBIRTH is frequently received in the very
// millisecond it was stamped, so the comparison fails on equality. The payload is
// correct - it carries a valid protobuf timestamp - and the identical NBIRTH check
// passes only because that message is stamped fractionally earlier relative to its
// write.
// Exposed so the orchestrator can decide whether a failure is worth retrying
// against a fresh broker.
const FLAKY_ASSERTIONS = ["payloads-dbirth-timestamp", "topics-dbirth-timestamp"];

function parseOverall(resultText) {
	const match = /OVERALL:\s*(.+?);/.exec(resultText);
	return match ? match[1].trim() : "UNKNOWN";
}

/** Parse "assertion-id: RESULT;" lines out of a TCK summary. */
function parseAssertions(resultText) {
	const failed = [];
	const notExecuted = [];
	for (const line of resultText.split(/\r?\n/)) {
		const match = /^([^:]+):\s*(.*);$/.exec(line.trim());
		if (!match) continue;
		const [, id, value] = match;
		if (id === "OVERALL") continue;
		if (value.startsWith("FAIL")) failed.push({ id, value });
		// Monitor-level assertions going unexecuted is normal and the TCK itself
		// ignores them when deciding INCOMPLETE.
		else if (value === "NOT EXECUTED" && !id.startsWith("Monitor:") && !id.startsWith("MQTTListener"))
			notExecuted.push(id);
	}
	return { failed, notExecuted };
}

async function runTest(control, observer, test) {
	console.log(`\n=== ${test.name} ===`);
	observer.reset();

	// The Monitor keeps a per-edge-node bdSeq map for the whole broker session
	// and only clears testResults between tests. Reloading the flow restarts the
	// node under test with bdSeq 0, which would read as a regression. A distinct
	// edge node id per test is the faithful analogue of the TCK's intended usage
	// (each test run against a freshly started edge node).
	const edgeNodeId = `${EDGE_NODE_ID}${test.name.replace("Test", "")}`;
	const params = [HOST_APP_ID, GROUP_ID, edgeNodeId, DEVICE_ID];

	const resultPromise = control.expectResult();

	// Setup runs before the TEST_TIMEOUT_MS race below is armed, so it is not
	// covered by it - each step here carries its own bound instead. A setup
	// failure ends this test only, so the remaining tests still run and the
	// results file still gets written.
	try {
		if (test.loadBeforeStart) {
			await loadFlow(edgeNodeId, {});
			// Let the node connect and subscribe to STATE before the TCK starts
			// toggling the host.
			await sleep(2000);
			await control.startTest(test.name, params);
		} else {
			await control.startTest(test.name, params);
			// Give the TCK a moment to register the test and bring its host online.
			await sleep(1000);
		}
	} catch (err) {
		// A timed-out publish to TEST_CONTROL does NOT mean the TCK missed it. On a
		// loaded runner the QoS 2 handshake can outlast the bound while the broker
		// log still shows "Test requested edge <name>" - that was true of every
		// PrimaryHostTest attempt in CI, and reporting SETUP_FAILED threw away a
		// test that had in fact started. Carry on and let the per-test timeout
		// below be the liveness guard instead.
		const controlPublishTimedOut =
			/timed out/.test(err.message) && err.message.includes(TCK_TEST_CONTROL);
		if (!controlPublishTimedOut) {
			console.log(`  ! setup failed: ${err.message}`);
			await unloadFlow();
			return {
				test: test.name,
				overall: "SETUP_FAILED",
				passed: false,
				timedOut: /timed out/.test(err.message),
				detail: err.message
			};
		}
		console.log(`  ! ${err.message}`);
		console.log(`    (the TCK may still have received it - continuing)`);
	}

	let timedOut = false;
	try {
		await Promise.race([
			(async () => {
				await test.run({ observer, edgeNodeId });
				await resultPromise;
			})(),
			(async () => {
				await sleep(TEST_TIMEOUT_MS);
				timedOut = true;
				throw new Error(`test timed out after ${TEST_TIMEOUT_MS}ms`);
			})()
		]);
	} catch (err) {
		console.log(`  ! ${err.message}`);
		// Ask the TCK to report whatever it has, so a hang still yields detail
		// about which assertions were reached. This is the recovery path, so it
		// must not itself become the thing that hangs.
		try {
			await control.endTest();
		} catch (endErr) {
			console.log(`  ! could not end test cleanly: ${endErr.message}`);
		}
	}

	const resultText = await Promise.race([resultPromise, sleep(10000).then(() => null)]);
	await unloadFlow();

	if (!resultText) {
		console.log("  RESULT: no result reported");
		return { test: test.name, overall: "NO_RESULT", passed: false, timedOut, detail: null };
	}

	const overall = parseOverall(resultText);
	const { failed, notExecuted } = parseAssertions(resultText);
	const unexpectedNotExecuted = notExecuted.filter((id) => !(id in EXPECTED_NOT_EXECUTED));
	const passed = overall.startsWith("PASS") && !failed.length && !unexpectedNotExecuted.length;

	console.log(`  RESULT: ${overall}${passed ? "" : "  <- not accepted"}`);
	for (const f of failed) console.log(`    FAILED    ${f.id}`);
	for (const id of unexpectedNotExecuted) console.log(`    UNTESTED  ${id}`);

	return {
		test: test.name,
		overall,
		passed,
		timedOut,
		failed: failed.map((f) => f.id),
		unexpectedNotExecuted,
		detail: resultText
	};
}

async function main() {
	const requested = process.argv.slice(2);
	const tests = requested.length ? TESTS.filter((t) => requested.includes(t.name)) : TESTS;

	if (!tests.length) {
		console.error(`No matching tests. Available: ${TESTS.map((t) => t.name).join(", ")}`);
		process.exit(2);
	}

	console.log("Sparkplug TCK - Edge Node profile");
	console.log(`  broker : ${BROKER_URL}`);
	console.log(`  host   : ${HOST_APP_ID}`);
	console.log(`  group  : ${GROUP_ID}`);

	const results = [];
	const writeResults = () =>
		fs.writeFileSync(path.resolve(RESULTS_FILE), JSON.stringify(results, null, 2));

	// Last line of defence. Every individual wait above is bounded, but a bound
	// that was missed - or a stuck handle keeping the event loop alive after the
	// work is done - would otherwise burn the whole CI job. Exit 124 to match
	// timeout(1), after saving whatever has been collected so far.
	const watchdog = setTimeout(() => {
		console.error(`\n!! run exceeded ${RUN_TIMEOUT_MS}ms - aborting`);
		try {
			writeResults();
			console.error(`Partial results written to ${RESULTS_FILE}`);
		} catch (err) {
			console.error(`Could not write partial results: ${err.message}`);
		}
		process.exit(124);
	}, RUN_TIMEOUT_MS);

	await withTimeout(
		new Promise((resolve) => helper.startServer(resolve)),
		HELPER_TIMEOUT_MS,
		"Node-RED test server start"
	);

	const control = await TckControl.connect();
	const observer = await Observer.connect();

	try {
		// Sequentially, never in parallel: the TCK holds one active test at a
		// time and its simulated host is shared state.
		for (const test of tests) {
			// Deliberately no in-process retry. The TCK Monitor keeps per-edge-node
			// state for the whole broker session and never clears it, so re-running
			// a test against the same broker is contaminated by the attempt that
			// just failed. Retries must restart the broker - see the orchestrator
			// that drives this script one test at a time.
			results.push(await runTest(control, observer, test));
		}
	} finally {
		control.end();
		observer.end();
		try {
			await withTimeout(
				new Promise((resolve) => helper.stopServer(resolve)),
				HELPER_TIMEOUT_MS,
				"Node-RED test server stop"
			);
		} catch (err) {
			console.log(`  ! ${err.message}`);
		}
		// In the finally block so a mid-run failure still leaves the artifact the
		// workflow uploads with `if: always()`.
		writeResults();
		clearTimeout(watchdog);
	}

	console.log("\n=== Summary ===");
	for (const r of results) {
		console.log(`  ${(r.passed ? "PASS" : "FAIL").padEnd(6)} ${r.test.padEnd(26)} ${r.overall}`);
	}

	const failed = results.filter((r) => !r.passed);
	console.log(`\n${results.length - failed.length}/${results.length} passed`);
	console.log(`Full detail written to ${RESULTS_FILE}`);
	process.exit(failed.length ? 1 : 0);
}

main().catch((err) => {
	console.error(err);
	process.exit(1);
});
