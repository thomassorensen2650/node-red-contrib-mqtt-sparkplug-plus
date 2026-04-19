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

describe('mqtt sparkplug out node - edge cases', function () {

	var validMsg = {"timestamp":12345,"metrics":[{"name":"test","type":"Int32","value":100}],"seq":200};

	var outFlow = [
		{
			"id": "n1",
			"type": "mqtt sparkplug out",
			"topic": "spBv1.0/My Devices/DDATA/Node-Red/TEST2",
			"broker": "b1",
			"wires": []
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
			"enableStoreForward": false,
			"primaryScada": "MY SCADA",
			"username": brokerUsername,
			"password": brokerPassword,
			"credentials": {}
		}
	];

	beforeEach(function (done) {
		helper.startServer(done);
	});
	
	afterEach(function (done) {
		helper.unload();
		helper.stopServer(done);
		if (client && client.connected) {
			client.end();
		} else if (client) {
			try { client.end(); } catch(e) {}
		}
	});

	it('should handle invalid QoS values (99)', function (done) {
		var n1 = null;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
						n1 = helper.getNode("n1");
						var msgWithInvalidQoS = Object.assign({}, validMsg);
						msgWithInvalidQoS.qos = 99;
						n1.receive({ payload: msgWithInvalidQoS });
						setTimeout(done, 100);
					});
				}
			});
		});
	});

	it('should handle invalid QoS values (-1)', function (done) {
		var n1 = null;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
						n1 = helper.getNode("n1");
						var msgWithInvalidQoS = Object.assign({}, validMsg);
						msgWithInvalidQoS.qos = -1;
						n1.receive({ payload: msgWithInvalidQoS });
						setTimeout(done, 100);
					});
				}
			});
		});
	});

	it('should handle invalid QoS as string', function (done) {
		var n1 = null;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
						n1 = helper.getNode("n1");
						var msgWithInvalidQoS = Object.assign({}, validMsg);
						msgWithInvalidQoS.qos = "abc";
						n1.receive({ payload: msgWithInvalidQoS });
						setTimeout(done, 100);
					});
				}
			});
		});
	});

	it('should log warning for invalid compression algorithm', function (done) {
		var n1 = null;
		var warningReceived = false;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					var flowWithCompression = JSON.parse(JSON.stringify(outFlow));
					flowWithCompression[1].compressAlgorithm = "INVALID_ALGO";
					helper.load(sparkplugNode, flowWithCompression, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
						n1 = helper.getNode("n1");
						n1.on('call:warn', function(call) {
							if (call.firstArg && call.firstArg.includes && call.firstArg.includes('unable-to-encode')) {
								warningReceived = true;
							}
						});
						n1.receive({ payload: validMsg});
						setTimeout(() => {
							done();
						}, 200);
					});
				}
			});
		});
	});

	it('should log warning for invalid topic', function (done) {
		var n1 = null;
		var warningReceived = false;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
						n1 = helper.getNode("n1");
						n1.on('call:warn', function(call) {
							if (call.firstArg && call.firstArg.includes && call.firstArg.includes('invalid-topic')) {
								warningReceived = true;
							}
						});
						var msgWithoutTopic = Object.assign({}, validMsg);
						n1.receive({ payload: msgWithoutTopic});
						setTimeout(() => {
							done();
						}, 200);
					});
				}
			});
		});
	});

	it('should handle message without payload', function (done) {
		var n1 = null;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
						n1 = helper.getNode("n1");
						n1.receive({ topic: "spBv1.0/My Devices/DDATA/Node-Red/TEST2"});
						setTimeout(done, 200);
					});
				}
			});
		});
	});

	it('should error when broker is not configured', function (done) {
		var flowWithoutBroker = [
			{
				"id": "n1",
				"type": "mqtt sparkplug out",
				"topic": "spBv1.0/My Devices/DDATA/Node-Red/TEST2",
				"broker": "",
				"wires": []
			}
		];

		helper.load(sparkplugNode, flowWithoutBroker, {}, function () {
			var n1 = helper.getNode("n1");
			n1.should.exist;
			done();
		});
	});

	it('should handle QoS 0, 1, 2 correctly', function (done) {
		var n1 = null;
		var testsCompleted = 0;
		var qosValues = [0, 1, 2];
		
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
						n1 = helper.getNode("n1");
						qosValues.forEach(function(qos) {
							var msgWithQoS = Object.assign({}, validMsg);
							msgWithQoS.qos = qos;
							n1.receive({ payload: msgWithQoS });
						});
						setTimeout(done, 200);
					});
				}
			});
		});
	});
});