#!/usr/bin/env node
/**
 * Generate test/tck/CONFORMANCE.md - which Sparkplug B 3.0 requirements this package
 * meets, and where each one is demonstrated.
 *
 * Everything is derived rather than hand-maintained, so the matrix can be regenerated
 * after a TCK run instead of drifting away from reality:
 *
 *   Requirements.java    the id -> text map for all requirements
 *   edge|host|broker/    which profile asserts which requirement
 *   Monitor.java         assertions checked continuously during any profile run
 *   tck-results/         PASS / FAIL / NOT EXECUTED from the most recent run
 *   our own sources      tck-id-* citations, so a requirement covered by a mocha test
 *                        or implemented deliberately is credited even when the TCK
 *                        never asserts it
 *
 * The requirement text comes from the TCK's Requirements.java, which is a
 * transcription of the specification - it is the only machine-readable list there is,
 * and the generated document says so rather than implying the spec was parsed.
 *
 * Usage:  node test/tck/coverage.js [path-to-sparkplug-checkout]
 * Env:    TCK_RESULTS_DIR   where per-test result JSON lives (default tck-results)
 */

const fs = require("fs");
const path = require("path");

const REPO = path.resolve(__dirname, "..", "..");
const SPARKPLUG_DIR = process.argv[2] || process.env.TCK_SPARKPLUG_DIR || path.join(REPO, "ref/sparkplug");
const RESULTS_DIR = process.env.TCK_RESULTS_DIR || path.join(REPO, "tck-results");
const OUT_FILE = path.join(__dirname, "CONFORMANCE.md");

const TCK_SRC = path.join(SPARKPLUG_DIR, "tck/src/main/java/org/eclipse/sparkplug/tck/test");

// Requirements the fixture cannot reach, and whether that is worth changing. Anything
// not listed falls back to a generic explanation derived from the asserting class.
const GAP_NOTES = {
	"topics-nbirth-metric-reqs": "Closable: the Monitor wants an NDATA metric that was not in the NBIRTH, which the fixture never sends.",
	"topics-nbirth-templates": "Closable: needs a template *instance* in the NBIRTH; the fixture puts definitions in NBIRTH and instances in DBIRTH.",
	"payloads-ndata-seq-inc": "Closable: needs two or more NDATA in one session so the Monitor can compare sequence numbers.",
	"operational-behavior-data-publish-nbirth-change": "Closable: needs the edge node's metric set to change mid-session, forcing a new NBIRTH.",
	"operational-behavior-data-publish-nbirth": "Closable: as above - the Monitor only records this when it sees a second NBIRTH.",
	"operational-behavior-data-publish-nbirth-order": "Closable: as above.",
	"operational-behavior-data-publish-dbirth-order": "Closable: needs more than one device under the edge node.",
	"message-flow-device-birth-publish-dbirth-payload-seq": "Closable: needs more than one device under the edge node.",
	"topic-structure-namespace-unique-device-id": "Closable: needs two devices, to show their ids are distinct.",
	"topic-structure-namespace-unique-edge-node-descriptor": "Closable: needs two edge nodes on one broker.",
	"payloads-nbirth-edge-node-descriptor": "Closable: needs two edge nodes on one broker.",
	"payloads-state-birth-payload": "Host Application behaviour - the TCK's simulated host publishes STATE, not this package.",
	"host-topic-phid-death-payload-timestamp-disconnect-clean": "Host Application behaviour, checked against the TCK's simulated host."
};

const KNOWN_TCK_DEFECTS = [
	["payloads-template-instance-members-data",
		"Text says a DDATA instance MAY carry a subset of members; SendDataTest.checkInstance requires the full set. The fixture updates every member at once."],
	["payloads-propertyset-quality-value-type",
		"Text mandates type 3 (Int32), which the node emits; the check compares against ValueCase.LONG_VALUE.getNumber(), i.e. 4. The optional Quality key is omitted."],
	["payloads-metric-propertyvalue-type-type",
		"Property datatypes are filtered through ValueCase.forNumber(), non-null only for 3..10, so String (12) and Boolean (11) properties are rejected. The fixture uses numeric property types."]
];

function die(message) {
	console.error(message);
	process.exit(1);
}

function readOrDie(file, hint) {
	if (!fs.existsSync(file)) { die(`Not found: ${file}\n${hint}`); }
	return fs.readFileSync(file, "utf8");
}

/** Requirement definitions: constant name -> id, and id -> specification text. */
function readRequirements() {
	const src = readOrDie(
		path.join(TCK_SRC, "common/Requirements.java"),
		`Pass the path to a Sparkplug checkout, e.g.\n  node test/tck/coverage.js ref/sparkplug\nSee test/tck/README.md for how to obtain one (ref/ is not committed).`
	);
	const constToId = new Map();
	for (const m of src.matchAll(/ID_([A-Z0-9_]+)\s*=\s*"([a-z0-9-]+)"/g)) {
		constToId.set(m[1], m[2]);
	}
	const text = new Map();
	for (const m of src.matchAll(/"\[tck-id-([a-z0-9-]+)\]\s*([^"]*)"/g)) {
		if (!text.has(m[1])) { text.set(m[1], m[2].trim()); }
	}
	return { constToId, text };
}

/** Map each requirement id to the TCK classes that assert it. */
function assertedBy(globDir, constToId) {
	const found = new Map();
	if (!fs.existsSync(globDir)) { return found; }
	for (const file of fs.readdirSync(globDir).filter(f => f.endsWith(".java"))) {
		const cls = file.replace(/\.java$/, "");
		const src = fs.readFileSync(path.join(globDir, file), "utf8");
		for (const m of src.matchAll(/ID_([A-Z0-9_]+)/g)) {
			const id = constToId.get(m[1]);
			if (!id) { continue; }
			if (!found.has(id)) { found.set(id, new Set()); }
			found.get(id).add(cls);
		}
	}
	return found;
}

function assertedByFile(file, constToId, label) {
	const found = new Map();
	if (!fs.existsSync(file)) { return found; }
	const src = fs.readFileSync(file, "utf8");
	for (const m of src.matchAll(/ID_([A-Z0-9_]+)/g)) {
		const id = constToId.get(m[1]);
		if (!id) { continue; }
		if (!found.has(id)) { found.set(id, new Set()); }
		found.get(id).add(label);
	}
	return found;
}

/**
 * Results of the most recent run. A requirement asserted by several tests takes the
 * most informative outcome: a failure anywhere matters more than a pass elsewhere,
 * and a pass anywhere beats being unexecuted in another test.
 */
const RANK = { "FAIL": 3, "PASS": 2, "MAYBE": 1, "NOT EXECUTED": 0 };

/** Every .json under the results directory, including one run per subdirectory. */
function resultFiles(dir) {
	if (!fs.existsSync(dir)) { return []; }
	const out = [];
	for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
		const full = path.join(dir, entry.name);
		if (entry.isDirectory()) { out.push(...resultFiles(full)); }
		else if (entry.name.endsWith(".json")) { out.push(full); }
	}
	return out;
}

function readResults() {
	const results = new Map();
	const tests = [];
	for (const file of resultFiles(RESULTS_DIR)) {
		let parsed;
		try {
			parsed = JSON.parse(fs.readFileSync(file, "utf8"));
		} catch (e) {
			continue; // a truncated file from an aborted run is not a result
		}
		for (const entry of [].concat(parsed)) {
			if (!entry || !entry.detail) { continue; }
			// Never guess: a result file written before the runner recorded the version
			// says so, rather than being reported as 3.1.1 on the strength of a default.
			const mqtt = entry.protocolVersion === 5 ? "5.0"
				: entry.protocolVersion === 4 ? "3.1.1"
					: "unspecified";
			tests.push({ test: entry.test, overall: entry.overall, mqtt });
			for (const line of entry.detail.split(/\r?\n/)) {
				const m = /^([^:]+(?::[^:]+)*):\s*(.*);$/.exec(line.trim());
				if (!m) { continue; }
				let id = m[1].trim();
				if (id === "OVERALL") { continue; }
				// Monitor:foo and MQTTListener:foo are the same requirement.
				id = id.replace(/^(Monitor|MQTTListener[^:]*):/, "");
				const status = (m[2].trim().split(/\s+/)[0] === "NOT" ? "NOT EXECUTED" : m[2].trim().split(/\s+/)[0]);
				const prev = results.get(id);
				if (prev === undefined || (RANK[status] ?? -1) > (RANK[prev] ?? -1)) {
					results.set(id, status);
				}
			}
		}
	}
	return { results, tests };
}

/** tck-id-* citations in our own runtime and specs. */
function ourCitations() {
	const cited = new Map();
	const add = (file, label) => {
		if (!fs.existsSync(file)) { return; }
		for (const m of fs.readFileSync(file, "utf8").matchAll(/tck-id-([a-z0-9-]+)/g)) {
			if (!cited.has(m[1])) { cited.set(m[1], new Set()); }
			cited.get(m[1]).add(label);
		}
	};
	add(path.join(REPO, "mqtt-sparkplug-plus.js"), "runtime");
	const testDir = path.join(REPO, "test");
	for (const f of fs.readdirSync(testDir).filter(f => f.endsWith("_spec.js"))) {
		add(path.join(testDir, f), f.replace(/_spec\.js$/, ""));
	}
	return cited;
}

const area = id => id.split("-").slice(0, 2).join("-");
const shorten = (s, n) => (s && s.length > n ? s.slice(0, n - 1).trimEnd() + "…" : (s || ""));
const list = set => (set ? [...set].sort().join(", ") : "");

function build() {
	const { constToId, text } = readRequirements();
	const edge = assertedBy(path.join(TCK_SRC, "edge"), constToId);
	const host = assertedBy(path.join(TCK_SRC, "host"), constToId);
	const broker = assertedBy(path.join(TCK_SRC, "broker"), constToId);
	const monitor = assertedByFile(path.join(TCK_SRC, "Monitor.java"), constToId, "Monitor");
	const { results, tests } = readResults();
	const cited = ourCitations();

	const allIds = [...new Set(constToId.values())].sort();

	// What an Edge Node run can assert: the edge profile plus the always-on Monitor.
	const applicable = new Map();
	for (const [id, classes] of [...edge, ...monitor]) {
		if (!applicable.has(id)) { applicable.set(id, new Set()); }
		classes.forEach(c => applicable.get(id).add(c));
	}

	const exercised = [...applicable.keys()].filter(id => results.has(id));
	const gaps = [...applicable.keys()].filter(id => !results.has(id)).sort();
	const statusCount = {};
	for (const id of exercised) { statusCount[results.get(id)] = (statusCount[results.get(id)] || 0) + 1; }

	const out = [];
	const w = line => out.push(line);

	w("# Sparkplug B 3.0 conformance");
	w("");
	w("<!-- Generated by test/tck/coverage.js - do not edit by hand. -->");
	w("");
	w("Which specification requirements this package meets, and where each one is");
	w("demonstrated. Requirement ids and text come from the Eclipse Sparkplug TCK's");
	w("`Requirements.java`, a transcription of the specification and the only");
	w("machine-readable list of it available; the specification document itself is the");
	w("authority in any disagreement.");
	w("");
	w("Regenerate after a TCK run:");
	w("");
	w("```bash");
	w("test/tck/run-tck-isolated.sh ref/sparkplug   # writes tck-results/");
	w("node test/tck/coverage.js ref/sparkplug");
	w("```");
	w("");
	if (!tests.length) {
		w("> **No TCK results were available when this was generated**, so every status");
		w("> below reads `not run`. Run the harness and regenerate.");
		w("");
	} else {
		const byVersion = {};
		for (const t of tests) { (byVersion[t.mqtt] = byVersion[t.mqtt] || []).push(t); }
		for (const [version, runs] of Object.entries(byVersion).sort()) {
			const names = [...new Set(runs.map(r => `\`${r.test}\` (${r.overall})`))].sort();
			w(`Results from MQTT ${version}, ${names.length} test(s): ` + names.join(", ") + ".");
			w("");
		}
		if (Object.keys(byVersion).length < 2) {
			w("> Only one MQTT version was run. Two paired requirements can only be asserted");
			w("> one version at a time, so the other pair will read `NOT EXECUTED` below - see");
			w("> the MQTT 5.0 section and `README.md` for running both.");
			w("");
		}
	}

	w("## Scope");
	w("");
	w("This package implements a Sparkplug **Edge Node**: `mqtt sparkplug device` plus");
	w("the `mqtt-sparkplug-broker` configuration node. That is the profile assessed here.");
	w("");
	w("| Profile | Requirements | In scope |");
	w("|---|---:|---|");
	w(`| Edge Node (incl. Monitor) | ${applicable.size} | Yes - this is what the package is |`);
	w(`| Host Application | ${host.size} | No - see below |`);
	w(`| Broker | ${broker.size} | No - this package is not a broker |`);
	w(`| **Defined in total** | **${allIds.length}** | |`);
	w("");

	w("## Summary");
	w("");
	w("| | Count |");
	w("|---|---:|");
	w(`| Edge Node requirements exercised by the harness | ${exercised.length} of ${applicable.size} |`);
	for (const s of ["PASS", "FAIL", "MAYBE", "NOT EXECUTED"]) {
		if (statusCount[s]) { w(`| — ${s} | ${statusCount[s]} |`); }
	}
	w(`| Edge Node requirements not exercised | ${gaps.length} |`);
	w(`| Requirements cited directly in our runtime or specs | ${cited.size} |`);
	w("");
	w("`MAYBE` is the TCK's outcome for a SHOULD-level requirement, not a failure.");
	w("`PASS but INCOMPLETE` on a test means nothing failed and something was never");
	w("exercised - see \"What counts as a pass\" in `README.md`.");
	w("");

	w("## Edge Node requirements");
	w("");
	w("Grouped by area. **Where tested** is the TCK test that asserts it; **Cited** marks");
	w("requirements this repository refers to directly, in the runtime or in a mocha spec.");
	w("");
	let currentArea = null;
	for (const id of [...applicable.keys()].sort()) {
		if (area(id) !== currentArea) {
			currentArea = area(id);
			w("");
			w(`### ${currentArea}`);
			w("");
			w("| Requirement | Status | Where tested | Cited | Text |");
			w("|---|---|---|---|---|");
		}
		const status = results.has(id) ? results.get(id) : "not run";
		w(`| \`${id}\` | ${status} | ${list(applicable.get(id))} | ${list(cited.get(id)) || "—"} | ${shorten(text.get(id), 110)} |`);
	}
	w("");

	w("## Requirements not exercised");
	w("");
	w(`${gaps.length} Edge Node requirements are never reached by the current harness.`);
	w("None is known to be violated - they are simply not asserted, which is why they are");
	w("listed rather than counted as passes.");
	w("");
	w("| Requirement | Asserted by | Why not reached |");
	w("|---|---|---|");
	for (const id of gaps) {
		const classes = applicable.get(id);
		let note = GAP_NOTES[id];
		if (!note) {
			note = classes.has("MultipleBrokerTest")
				? "`MultipleBrokerTest` is not run - it needs several brokers."
				: "The Monitor records this only under conditions the fixture does not produce.";
		}
		w(`| \`${id}\` | ${list(classes)} | ${note} |`);
	}
	w("");

	w("## Host Application requirements");
	w("");
	w(`**This package ships no Host Application**, so none of the ${host.size} Host profile`);
	w("requirements is claimed.");
	w("");
	w("`mqtt sparkplug in` subscribes, decodes a Sparkplug payload and emits it as a");
	w("Node-RED message - roughly 50 lines that never publish anything. `mqtt sparkplug");
	w("out` publishes whatever topic and payload it is handed, with no session semantics");
	w("of its own. A Host Application additionally has to publish its STATE birth and");
	w("death, track every edge node's session, and originate rebirth and command");
	w("messages; none of that exists here.");
	w("");
	w("A flow can of course *act* as a host by combining these nodes with its own logic,");
	w("but conformance would then be a property of that flow, not of this package.");
	w("");
	w("| Area | Requirements | Provided |");
	w("|---|---:|---|");
	{
		const groups = {};
		for (const id of host.keys()) { groups[area(id)] = (groups[area(id)] || 0) + 1; }
		const PROVIDED = {
			"host-topic": "No - STATE is never published",
			"payloads-state": "No - STATE is never published",
			"topics-ncmd": "Partly - `mqtt sparkplug out` can publish an NCMD, but nothing orchestrates it",
			"payloads-ncmd": "Partly - as above",
			"topics-dcmd": "Partly - as above",
			"payloads-dcmd": "Partly - as above",
			"operational-behavior": "No - no session or rebirth orchestration",
			"message-flow": "No - no host session lifecycle"
		};
		for (const [g, n] of Object.entries(groups).sort((a, b) => b[1] - a[1])) {
			w(`| ${g} | ${n} | ${PROVIDED[g] || "No"} |`);
		}
	}
	w("");

	w("## Where the TCK is wrong");
	w("");
	w("Three assertions read a Sparkplug datatype code as though it were a protobuf field");
	w("number. In each the node follows the specification text and the TCK rejects it, so");
	w("the fixture works around them rather than bending the node - which would misreport");
	w("conformance. Detail in `README.md`.");
	w("");
	w("| Requirement | Issue |");
	w("|---|---|");
	for (const [id, note] of KNOWN_TCK_DEFECTS) { w(`| \`${id}\` | ${note} |`); }
	w("");

	const mqtt5 = [...applicable.keys()].filter(id => /-(50|mqtt50)$/.test(id)).sort();
	if (mqtt5.length) {
		const ran5 = tests.some(t => t.mqtt === "5.0");
		w("## MQTT 5.0");
		w("");
		w("The TCK branches on the version in the CONNECT packet: for each of these pairs it");
		w("asserts either the `-311` or the `-50` variant, never both. A run at one version");
		w("therefore always leaves the other pair `NOT EXECUTED`.");
		w("");
		if (ran5) {
			w("Both versions were run, so all four are covered - the node connects as 5.0 with");
			w("Clean Start set and a Session Expiry Interval of 0, and its intentional");
			w("DISCONNECT carries the 'Disconnect with Will Message' reason code.");
		} else {
			w("Only MQTT 3.1.1 was run here. MQTT 5.0 is selectable in the broker");
			w("configuration, so these can be covered by also running with");
			w("`TCK_PROTOCOL_VERSION=5` - not done for this report, so nothing is claimed.");
		}
		w("");
		w("| Requirement | Status |");
		w("|---|---|");
		for (const id of mqtt5) { w(`| \`${id}\` | ${results.has(id) ? results.get(id) : "not run"} |`); }
		w("");
	}

	return out.join("\n") + "\n";
}

fs.writeFileSync(OUT_FILE, build());
console.log(`Wrote ${path.relative(REPO, OUT_FILE)}`);
