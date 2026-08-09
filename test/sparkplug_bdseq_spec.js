var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");

var spPayload = require('sparkplug-payload').get("spBv1.0");
helper.init(require.resolve('node-red'));
let testBroker = process.env.TEST_BROKER || 'mqtt://localhost';
const _brokerUrl = new URL(testBroker.replace(/^mqtt(s?)/, 'http$1'));
let brokerHost = _brokerUrl.hostname;
let brokerPort = _brokerUrl.port || '1883';
let brokerUsername = _brokerUrl.username || '';
let brokerPassword = _brokerUrl.password || '';
var client = null;

// Deliberately NOT the "MY SCADA" used by the other specs. This spec is the only
// one that publishes genuinely retained STATE, and a retained message outlives
// the run - sharing the id would let it dictate the Primary Host state of every
// spec that follows.
const PHID = "MY SCADA BDSEQ";
const STATE_TOPIC_V3 = `spBv1.0/STATE/${PHID}`;
const STATE_TOPIC_V2 = `STATE/${PHID}`;
const NBIRTH_TOPIC = "spBv1.0/My Devices/NBIRTH/Node-Red";
const NDEATH_TOPIC = "spBv1.0/My Devices/NDEATH/Node-Red";

/**
 * [tck-id-topics-nbirth-bdseq-increment] The bdSeq number MUST start at zero and
 * increment by one on every new MQTT CONNECT packet.
 *
 * The Primary Host OFFLINE handler publishes NDEATH, ends the client and then
 * reconnects. bdSeq is allocated in connect() rather than on a successful
 * CONNECT, so a reconnect that involves more than one connect() call burns
 * extra bdSeq values and the sequence skips.
 */
describe('mqtt sparkplug broker node - bdSeq', function () {

	// Handle for the pending STATE publish below. It must be cancellable from
	// afterEach: a publish that outlives its test re-retains ONLINE *after*
	// cleanup has run, and retained messages persist on the broker forever.
	let stateTimer = null;

	beforeEach(function (done) {
		helper.startServer(done);
	});

	afterEach(async function () {
		clearTimeout(stateTimer);
		stateTimer = null;

		await helper.unload();
		helper.stopServer();
		if (client) {
			client.end(true);
			client = null;
		}

		// This spec publishes *retained* STATE messages, so clear them on the way
		// out. Awaited rather than fire-and-forget - the previous version raced
		// helper.unload() and could return before the clear had landed.
		await new Promise(function (resolve) {
			let cleanup = mqtt.connect(testBroker);
			let finish = function () {
				// end(false), not end(true): a forced end tears the socket down
				// immediately and can drop the clear before it reaches the broker,
				// which is what left a retained message behind previously.
				cleanup.end(false, {}, resolve);
			};
			cleanup.on('connect', function () {
				// qos 1 so the callback means "broker acknowledged", not merely
				// "written to the socket".
				cleanup.publish(STATE_TOPIC_V2, "", { retain: true, qos: 1 });
				cleanup.publish(STATE_TOPIC_V3, "", { retain: true, qos: 1 }, finish);
			});
			cleanup.on('error', function () { cleanup.end(true); resolve(); });
		});
	});

	var simpleFlow = [
		{
			"id": "n1",
			"type": "mqtt sparkplug device",
			"name": "TEST2",
			"metrics": {
				"test": { "dataType": "Int32" }
			},
			"broker": "b1"
		},
		{
			"id": "b1",
			"type": "mqtt-sparkplug-broker",
			"name": "Local Host",
			"deviceGroup": "My Devices",
			"eonName": "Node-Red",
			"broker": brokerHost,
			"port": brokerPort,
			"clientid": "",
			"usetls": false,
			"protocolVersion": "4",
			"keepalive": "60",
			"cleansession": true,
			"enableStoreForward": true,
			"primaryScada": PHID,
			"username": brokerUsername,
			"password": brokerPassword
		}
	];

	/** Publish a retained SPb 3.0 STATE message for the Primary Host. */
	function publishState(online, timestamp, cb) {
		client.publish(
			STATE_TOPIC_V3,
			JSON.stringify({ online: online, timestamp: timestamp }),
			{ retain: true, qos: 0 },
			cb || function () {}
		);
	}

	function bdSeqOf(message) {
		let payload = spPayload.decodePayload(Buffer.from(message));
		let metric = payload.metrics.find(m => m.name === "bdSeq");
		should.exist(metric, "NBIRTH must contain a bdSeq metric");
		return metric.value.toInt();
	}

	// One offline/online cycle is not enough to expose the skip - PrimaryHostTest
	// toggles the host repeatedly, so drive several cycles here too.
	const CYCLES = 4;

	it('should increment bdSeq by exactly one per reconnect when the Primary Host toggles', function (done) {
		this.timeout(60000);

		let bdSeqs = [];
		let trace = [];
		let ts = Date.now();
		let finished = false;

		function finish(err) {
			if (finished) return;
			finished = true;
			clearTimeout(stateTimer);
			stateTimer = null;
			done(err);
		}

		client = mqtt.connect(testBroker);

		client.on('connect', function () {
			// Clear any retained STATE left behind by other specs, on both the
			// 2.0 and 3.0 topics, so this test controls the host state entirely.
			client.publish(STATE_TOPIC_V2, "", { retain: true, qos: 0 });
			client.publish(STATE_TOPIC_V3, "", { retain: true, qos: 0 }, function () {
				publishState(true, ++ts, function () {
					client.subscribe('spBv1.0/#', function (err) {
						if (err) return finish(err);
						helper.load(sparkplugNode, simpleFlow, {}, function () {});
					});
				});
			});
		});

		client.on('message', function (topic, message) {
			if (topic === NDEATH_TOPIC) {
				try {
					trace.push(`NDEATH(bdSeq=${bdSeqOf(message)})`);
				} catch (e) {
					trace.push("NDEATH(unparsed)");
				}
				return;
			}
			if (topic !== NBIRTH_TOPIC) return;

			let bd;
			try {
				bd = bdSeqOf(message);
			} catch (e) {
				return finish(e);
			}
			bdSeqs.push(bd);
			trace.push(`NBIRTH(bdSeq=${bd})`);

			// Check the increment as soon as we have a pair, so the failure
			// message names the exact cycle that went wrong.
			if (bdSeqs.length > 1) {
				let prev = bdSeqs[bdSeqs.length - 2];
				if (bd !== prev + 1) {
					return finish(new Error(
						`bdSeq must increment by exactly one on every new MQTT CONNECT ` +
						`[tck-id-topics-nbirth-bdseq-increment], but the sequence was ` +
						`${bdSeqs.join(" -> ")}\n    trace: ${trace.join(" | ")}`
					));
				}
			}

			if (bdSeqs.length > CYCLES) {
				return finish();
			}

			// Knock the host offline: the node should publish NDEATH, disconnect
			// and reconnect - consuming exactly one bdSeq. Then bring it back so
			// it births again and reveals the new value.
			trace.push("STATE(offline)");
			publishState(false, ++ts);
			stateTimer = setTimeout(function () {
				stateTimer = null;
				if (finished) return;
				trace.push("STATE(online)");
				publishState(true, ++ts);
			}, 1500);
		});
	});
});
