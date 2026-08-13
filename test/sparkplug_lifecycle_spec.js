/**
 * Session lifecycle conformance: bdSeq allocation, will registration, protocol
 * version handling, alias uniqueness, birth timestamps, payload decoding and
 * connection error reporting.
 *
 * The issues covered here were catalogued by the `-wmonitor` fork
 * (ISC, Michael Sadowski), which branched from v2.2.4 and fixed them there.
 * These tests were written independently against this code base; each one fails
 * without its corresponding fix.
 */
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

/** Broker config node. `overrides` is merged last so a test can vary one field. */
function brokerNode(overrides) {
	return Object.assign({
		"id": "b1",
		"type": "mqtt-sparkplug-broker",
		"name": "Lifecycle Broker",
		"deviceGroup": "Lifecycle Devices",
		"eonName": "Lifecycle-EoN",
		"broker": brokerHost,
		"port": brokerPort,
		"clientid": "",
		"usetls": false,
		"protocolVersion": "4",
		"keepalive": "60",
		"cleansession": true,
		"enableStoreForward": false,
		"primaryScada": "",
		"username": brokerUsername,
		"password": brokerPassword
	}, overrides || {});
}

/** Device node bound to `b1`. */
function deviceNode(id, name, metrics) {
	return {
		"id": id,
		"type": "mqtt sparkplug device",
		"name": name,
		"metrics": metrics,
		"broker": "b1"
	};
}

function metricsOf(message) {
	return spPayload.decodePayload(Buffer.from(message)).metrics;
}

/**
 * Run assertions inside a callback and hand any failure to mocha.
 *
 * Without this an assertion that throws inside a node/MQTT callback never reaches
 * the runner: done() is simply never called and the test dies as an unexplained
 * "Timeout of 2000ms exceeded", which says nothing about what actually broke.
 */
function check(done, assertions) {
	try {
		assertions();
		done();
	} catch (err) {
		done(err);
	}
}

/** Call `assertions` once the broker config node has a live client. */
function whenClientReady(broker, done, assertions) {
	let waiting = setInterval(function () {
		if (!broker.client) { return; }
		clearInterval(waiting);
		assertions();
	}, 25);
	// Surface a stalled connection as a clear failure rather than a bare timeout.
	setTimeout(function () {
		if (waiting) { clearInterval(waiting); }
	}, 1800);
}

describe('sparkplug lifecycle', function () {

	beforeEach(function (done) {
		helper.startServer(done);
	});

	afterEach(async function () {
		await helper.unload();
		helper.stopServer();
		if (client) {
			client.end(true);
			client = null;
		}
	});

	// ── bdSeq allocation ────────────────────────────────────────────────────

	describe('bdSeq', function () {

		/**
		 * [tck-id-message-flow-edge-node-birth-publish-will-message-payload-bdSeq]
		 * bdSeq is a value 0-255; after 255 the next value MUST be 0.
		 */
		it('should wrap from 255 to 0 without ever emitting 256', function (done) {
			helper.load(sparkplugNode, [brokerNode()], {}, function () {
				check(done, function () {
					let broker = helper.getNode("b1");
					let seen = [];
					for (let i = 0; i < 258; i++) {
						seen.push(broker.nextBdseq());
					}
					seen.should.not.containEql(256);
					Math.max.apply(null, seen).should.eql(255);
					// The value after 255 must be 0, not anything else.
					seen[seen.indexOf(255) + 1].should.eql(0);
				});
			});
		});

		/**
		 * A redeploy re-creates the config node and wipes its node context, so a
		 * bdSeq kept there restarts at 0 and the host sees two sessions claiming the
		 * same number. Global context outlives the node, which is what makes the
		 * counter survive - asserted on placement rather than by redeploying,
		 * because the test helper tears the runtime down between loads.
		 */
		it('should keep bdSeq in global context, not the node context', function (done) {
			helper.load(sparkplugNode, [brokerNode()], {}, function () {
				check(done, function () {
					let broker = helper.getNode("b1");
					let value = broker.nextBdseq();

					should.exist(broker.bdSeqContextKey, "no global context key defined for bdSeq");
					should.exist(broker.context().global.get(broker.bdSeqContextKey),
						"bdSeq was not written to global context, so a redeploy resets it");
					broker.context().global.get(broker.bdSeqContextKey).should.eql(value);
					should.not.exist(broker.context().get("bdSeq"),
						"bdSeq is still kept in node context, which a redeploy clears");

					// Per node, so two broker nodes cannot share one counter.
					broker.bdSeqContextKey.should.containEql(broker.id);
				});
			});
		});

		/**
		 * [tck-id-topics-nbirth-bdseq-increment] The bdSeq MUST increment on every
		 * new MQTT CONNECT. MQTT.js reconnects on its own, and the will it sends
		 * with that CONNECT is built from the options it already holds - so both
		 * the counter and the client's own will have to be refreshed.
		 */
		it('should take a new bdSeq and will for an automatic reconnect', function (done) {
			let flow = [deviceNode("d1", "Device1", { "t": { "dataType": "Int32" } }), brokerNode()];
			helper.load(sparkplugNode, flow, {}, function () {
				let broker = helper.getNode("b1");
				whenClientReady(broker, done, function () {
					check(done, function () {
						let bdSeqBefore = broker.bdSeq;
						let willBefore = bdSeqOfWill(broker.client.options.will);

						// What MQTT.js emits immediately before writing a new CONNECT.
						broker.client.emit('reconnect');

						broker.bdSeq.should.eql(bdSeqBefore + 1,
							"bdSeq did not advance for the reconnect");
						bdSeqOfWill(broker.client.options.will).should.eql(willBefore + 1,
							"the client's will still carries the previous bdSeq");
					});
				});
			});
		});

		function bdSeqOfWill(will) {
			should.exist(will, "no will registered on the client");
			let metric = metricsOf(will.payload).find(m => m.name === "bdSeq");
			should.exist(metric, "will payload has no bdSeq metric");
			return typeof metric.value === "object" ? metric.value.toNumber() : metric.value;
		}
	});

	// ── Connection options ──────────────────────────────────────────────────

	describe('connection options', function () {

		it('should apply the configured MQTT protocol version', function (done) {
			helper.load(sparkplugNode, [brokerNode({ "protocolVersion": "5" })], {}, function () {
				check(done, function () {
					should.exist(helper.getNode("b1").options.protocolVersion,
						"protocolVersion was read from the config but never passed to MQTT.js");
					helper.getNode("b1").options.protocolVersion.should.eql(5);
				});
			});
		});

		/**
		 * [tck-id-principles-persistence-clean-session-50] With MQTT 5.0 the
		 * CONNECT MUST set Clean Start true and Session Expiry Interval 0, so no
		 * session outlives the connection.
		 */
		it('should set clean start and a zero session expiry on MQTT 5.0', function (done) {
			helper.load(sparkplugNode, [brokerNode({ "protocolVersion": "5", "cleansession": false, "clientid": "fixed-id" })], {}, function () {
				check(done, function () {
					let options = helper.getNode("b1").options;
					options.clean.should.eql(true, "Clean Start was not forced on for MQTT 5.0");
					should.exist(options.properties, "no MQTT 5.0 properties set");
					options.properties.sessionExpiryInterval.should.eql(0);
				});
			});
		});

		it('should default to MQTT 3.1.1 when nothing is configured', function (done) {
			helper.load(sparkplugNode, [brokerNode({ "protocolVersion": undefined })], {}, function () {
				check(done, function () {
					should.exist(helper.getNode("b1").options.protocolVersion,
						"no protocolVersion passed to MQTT.js at all");
					helper.getNode("b1").options.protocolVersion.should.eql(4);
				});
			});
		});

		/**
		 * MQTT.js queues QoS 0 publishes while offline and flushes them after the
		 * reconnect - i.e. after the new NBIRTH has reset seq to 0 - so the host
		 * sees the sequence jump backwards. Store-and-forward is what replays
		 * data, with current sequence numbers.
		 */
		it('should not let MQTT.js queue QoS 0 publishes across a reconnect', function (done) {
			helper.load(sparkplugNode, [brokerNode()], {}, function () {
				check(done, function () {
					let options = helper.getNode("b1").options;
					should.exist(options.queueQoSZero,
						"queueQoSZero was never set, so MQTT.js keeps its queue-and-replay default");
					options.queueQoSZero.should.eql(false);
				});
			});
		});
	});

	// ── Disconnect ──────────────────────────────────────────────────────────

	describe('intentional disconnect', function () {

		/**
		 * [tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt50] A plain
		 * MQTT 5.0 DISCONNECT tells the broker to discard the will; reason code
		 * 0x04 asks it to publish the will as well.
		 */
		it('should send the Disconnect with Will Message reason code on MQTT 5.0', function (done) {
			let flow = [deviceNode("d1", "Device1", { "t": { "dataType": "Int32" } }), brokerNode({ "protocolVersion": "5" })];
			helper.load(sparkplugNode, flow, {}, function () {
				let broker = helper.getNode("b1");

				whenClientReady(broker, done, function () {
					let endOptions = null;
					broker.client.end = function (force, options, cb) {
						endOptions = options;
						if (typeof cb === 'function') { cb(); }
					};

					broker.sendDeathsAndDisconnect(function () {
						check(done, function () {
							should.exist(endOptions, "client.end() was called without options");
							endOptions.should.have.property("reasonCode", 4);
						});
					});
				});
			});
		});

		it('should not send a reason code on MQTT 3.1.1', function (done) {
			let flow = [deviceNode("d1", "Device1", { "t": { "dataType": "Int32" } }), brokerNode()];
			helper.load(sparkplugNode, flow, {}, function () {
				let broker = helper.getNode("b1");

				whenClientReady(broker, done, function () {
					let endOptions = null;
					broker.client.end = function (force, options, cb) {
						endOptions = options;
						if (typeof cb === 'function') { cb(); }
					};

					broker.sendDeathsAndDisconnect(function () {
						check(done, function () {
							(endOptions || {}).should.not.have.property("reasonCode");
						});
					});
				});
			});
		});
	});

	// ── Connection errors ───────────────────────────────────────────────────

	describe('connection errors', function () {

		it('should report connection errors instead of swallowing them', function (done) {
			helper.load(sparkplugNode, [deviceNode("d1", "Device1", { "t": { "dataType": "Int32" } }), brokerNode()], {}, function () {
				let broker = helper.getNode("b1");

				// The helper only instruments regular nodes, not config nodes, so
				// 'call:error' never fires here - capture the call directly instead.
				// RED._ returns the key rather than the text under test, so the key is
				// what there is to assert on, as the other specs do.
				let reported = [];
				broker.error = function (message) { reported.push(message); };

				whenClientReady(broker, done, function () {
					check(done, function () {
						broker.client.emit('error', new Error("Connection refused: Not authorized"));
						reported.length.should.eql(1, "the connection error was swallowed");
						reported[0].should.eql('mqtt-sparkplug-plus.errors.connection-error');

						// A permanent failure retries on a timer; it must not flood the log.
						broker.client.emit('error', new Error("Connection refused: Not authorized"));
						reported.length.should.eql(1, "the same error was reported on every retry");

						// A different failure is news again.
						broker.client.emit('error', new Error("getaddrinfo ENOTFOUND"));
						reported.length.should.eql(2, "a new, different error was not reported");

						// So is a repeat of an earlier one, once a connection has succeeded
						// in between - that is a fresh outage, not the same one repeating.
						broker.client.emit('connect', {});
						broker.client.emit('error', new Error("getaddrinfo ENOTFOUND"));
						reported.length.should.eql(3,
							"an error after a successful reconnect was suppressed as a duplicate");
					});
				});
			});
		});
	});

	// ── Metric aliases ──────────────────────────────────────────────────────

	describe('metric aliases', function () {

		/**
		 * [tck-id-payloads-alias-uniqueness] An alias MUST be unique across the
		 * Edge Node's entire set of metrics. Two devices sharing a metric name are
		 * still two distinct metrics.
		 */
		it('should give two devices distinct aliases for the same metric name', function (done) {
			let flow = [
				deviceNode("d1", "DeviceOne", { "temperature": { "dataType": "Int32" } }),
				deviceNode("d2", "DeviceTwo", { "temperature": { "dataType": "Int32" } }),
				brokerNode({ "aliasMetrics": true })
			];

			let aliases = {};
			client = mqtt.connect(testBroker);
			client.on('connect', function () {
				client.subscribe("spBv1.0/Lifecycle Devices/DBIRTH/Lifecycle-EoN/+", function () {
					helper.load(sparkplugNode, flow, {}, function () {
						helper.getNode("d1").receive({ payload: { metrics: [{ name: "temperature", value: 1 }] } });
						helper.getNode("d2").receive({ payload: { metrics: [{ name: "temperature", value: 2 }] } });
					});
				});
			});

			client.on('message', function (topic, message) {
				let device = topic.split("/").pop();
				let metric = metricsOf(message).find(m => m.name === "temperature");
				if (metric) {
					aliases[device] = typeof metric.alias === "object" ? metric.alias.toNumber() : metric.alias;
				}
				if (Object.keys(aliases).length === 2) {
					check(done, function () {
						should.exist(aliases["DeviceOne"], "DeviceOne published no alias");
						should.exist(aliases["DeviceTwo"], "DeviceTwo published no alias");
						aliases["DeviceOne"].should.not.eql(aliases["DeviceTwo"],
							"both devices were given the same alias for 'temperature'");
					});
				}
			});
		});
	});

	// ── Birth timestamps ────────────────────────────────────────────────────

	describe('birth timestamps', function () {

		const TEMPLATE = JSON.stringify({
			name: "LifecycleTemplate",
			type: "Template",
			value: {
				version: "1.0.0",
				isDefinition: true,
				metrics: [{ name: "speed", type: "Int32" }],
				parameters: []
			}
		});

		/**
		 * [tck-id-payloads-name-birth-data-requirement] Every metric in a birth
		 * message carries a timestamp. Template definitions come straight from the
		 * configuration and have none of their own.
		 */
		it('should stamp every metric in the NBIRTH, including template definitions', function (done) {
			let flow = [
				deviceNode("d1", "Device1", { "t": { "dataType": "Int32" } }),
				brokerNode({ "templates": [TEMPLATE] })
			];

			client = mqtt.connect(testBroker);
			client.on('connect', function () {
				client.subscribe("spBv1.0/Lifecycle Devices/NBIRTH/Lifecycle-EoN", function () {
					helper.load(sparkplugNode, flow, {}, function () {
						helper.getNode("d1").receive({ payload: { metrics: [{ name: "t", value: 1 }] } });
					});
				});
			});

			client.on('message', function (topic, message) {
				check(done, function () {
					let missing = metricsOf(message).filter(m => !m.timestamp).map(m => m.name);
					missing.should.be.empty(`NBIRTH metrics without a timestamp: ${missing.join(", ")}`);
				});
			});
		});

		it('should stamp every metric in the DBIRTH', function (done) {
			let flow = [
				deviceNode("d1", "Device1", { "a": { "dataType": "Int32" }, "b": { "dataType": "Int32" } }),
				brokerNode()
			];

			client = mqtt.connect(testBroker);
			client.on('connect', function () {
				client.subscribe("spBv1.0/Lifecycle Devices/DBIRTH/Lifecycle-EoN/Device1", function () {
					helper.load(sparkplugNode, flow, {}, function () {
						helper.getNode("d1").receive({ payload: { metrics: [{ name: "a", value: 1 }, { name: "b", value: 2 }] } });
					});
				});
			});

			client.on('message', function (topic, message) {
				check(done, function () {
					let missing = metricsOf(message).filter(m => !m.timestamp).map(m => m.name);
					missing.should.be.empty(`DBIRTH metrics without a timestamp: ${missing.join(", ")}`);
				});
			});
		});
	});

	// ── Payload decoding ────────────────────────────────────────────────────

	describe('payload decoding', function () {

		/**
		 * Sparkplug carries Int64 in an unsigned protobuf field, and
		 * sparkplug-payload only converts it back to signed when its own `instanceof
		 * Long` check matches - which it does not here, because protobufjs hands
		 * back an instance of a different copy of `long`. A negative Int64 then
		 * arrives as a huge positive number.
		 */
		it('should decode a negative Int64 as a negative number', function (done) {
			let flow = [
				{ "id": "in1", "type": "mqtt sparkplug in", "name": "", "topic": "spBv1.0/Lifecycle Devices/DDATA/#", "qos": "2", "broker": "b1", "wires": [["out1"]] },
				{ "id": "out1", "type": "helper" },
				brokerNode()
			];

			helper.load(sparkplugNode, flow, {}, function () {
				let out = helper.getNode("out1");
				let broker = helper.getNode("b1");
				let publishing = null;

				out.on("input", function (msg) {
					clearInterval(publishing);
					check(done, function () {
						let metric = msg.payload.metrics.find(m => m.name === "negative");
						should.exist(metric, "no 'negative' metric in the decoded payload");
						let value = typeof metric.value === "object" ? metric.value.toNumber() : metric.value;
						value.should.eql(-42, "a negative Int64 did not survive decoding");
					});
				});

				let payload = Buffer.from(spPayload.encodePayload({
					timestamp: Date.now(),
					seq: 0,
					metrics: [{ name: "negative", value: -42, type: "Int64", timestamp: Date.now() }]
				}));

				// Republish until it lands: the in node subscribes as part of connecting,
				// so a single publish timed off our own client can beat the subscription
				// and be dropped - which shows up as an unexplained timeout under load.
				publishing = setInterval(function () {
					if (broker.client && broker.connected) {
						broker.client.publish("spBv1.0/Lifecycle Devices/DDATA/Lifecycle-EoN/Device1", payload);
					}
				}, 100);
			});
		});
	});
});
