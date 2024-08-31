var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");
var pako = require('pako');

var spPayload = require('sparkplug-payload').get("spBv1.0");
helper.init(require.resolve('node-red'));
let testBroker = 'mqtt://localhost';
var client = null;

/**  
 * mqtt sparkplug out testing
 */
describe('mqtt sparkplug out node', function () {

	beforeEach(function (done) {
		helper.startServer(done);
	});
	
	afterEach(function (done) {
		helper.unload();
		helper.stopServer(done);
		if (client) {
			client.end();
		}
	});
	var validMsg = {"timestamp":12345,"metrics":[{"name":"test","type":"Int32","value":100}],"seq":200}
	
	outFlow = [
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
			"broker": "localhost",
			"port": "1883",
			"clientid": "",
			"usetls": false,
			"protocolVersion": "4",
			"keepalive": "60",
			"cleansession": true,
			"enableStoreForward": true,
			"primaryScada": "MY SCADA",
			"credentials": {}
		}
	]

	outFlow2 = [
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
			"broker": "localhost",
			"port": "1883",
			"clientid": "",
			"usetls": false,
			"protocolVersion": "4",
			"keepalive": "60",
			"cleansession": true,
			"enableStoreForward": false,
			"primaryScada": "MY SCADA",
			"credentials": {}
		}
	]
	/**
	 * Verify that we outout a topic even though the primary SCADA is offline
	 */
	it('should ouput a publish topic', function (done) {

		var n1 = null;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, function () {
						n1 = helper.getNode("n1");
						n1.brokerConn.enableStoreForward = false; // Force enable to buffer
						
						setTimeout(() => n1.receive({ payload: validMsg}), 500);
					});
				}
			});
		});

		client.on('message', function (topic, message) {
			var buffer = Buffer.from(message);
			var payload = spPayload.decodePayload(buffer);
			n1.brokerConn.primaryScadaStatus.should.eql("OFFLINE");
			payload.timestamp = payload.timestamp.toNumber()
			payload.seq = payload.seq.toNumber()
			payload.should.deepEqual(validMsg);
			done();
		});
	});


	/** 
	
	 * Verify that we'll buffer if forced enabled,
	
	it('should buffer if publish topic', function (done) {

		outFlow[0].shouldBuffer = true;
	
		var n1 = null;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.publish("STATE/MY SCADA", "OFFLINE", true)

			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, function () {
						n1 = helper.getNode("n1");
						n1.shouldBuffer = true; // Force enable to buffer
						setTimeout(() => n1.receive({ payload: validMsg}), 500);
						setTimeout(() => client.publish("STATE/MY SCADA", "ONLINE", true), 1000);						
					});
				}
			});
		});

		client.on('message', function (topic, message) {
			console.log(topic);
			var buffer = Buffer.from(message);
			var payload = spPayload.decodePayload(buffer);
			n1.brokerConn.primaryScadaStatus.should.eql("ONLINE");
			payload.timestamp = payload.timestamp.toNumber()
			payload.seq = payload.seq.toNumber()
			payload.should.deepEqual(validMsg);
			done();
		});
	});

		/**
	 * Verify that we'll buffer if forced enabled,
	
	it('should buffer on Sparkplug B 3.0.0 style State variable', function (done) {

		var n1 = null;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, function () {
						n1 = helper.getNode("n1");
						n1.shouldBuffer = true; // Force enable to buffer
						setTimeout(() => n1.receive({ payload: validMsg}), 100);
						setTimeout(() => client.publish("spBv1.0/STATE/MY SCADA", JSON.stringify({ online : true, timestamp : new Date() }), true), 700);						});
				}
				
			});
		});

		client.on('message', function (topic, message) {
			var buffer = Buffer.from(message);
			var payload = spPayload.decodePayload(buffer);
			n1.brokerConn.primaryScadaStatus.should.eql("ONLINE");
			payload.timestamp = payload.timestamp.toNumber()
			payload.seq = payload.seq.toNumber()
			payload.should.deepEqual(validMsg);
			done();
		});
	});
*/
});