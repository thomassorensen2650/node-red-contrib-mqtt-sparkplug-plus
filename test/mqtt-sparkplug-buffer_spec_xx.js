var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");
var mqttCb = null;

var spPayload = require('sparkplug-payload').get("spBv1.0");

helper.init(require.resolve('node-red'));
let testBroker = 'mqtt://localhost';//'mqtt://test.mosquitto.org';
var simpleFlow = [
	{
		"id": "n1",
		"type": "mqtt sparkplug device",
		"name": "TEST2",
		"metrics": {
			"test": {
				"dataType": "Int32"
			},
			"test2": {
				"dataType": "Int32"
			}
		},
		"broker": "b1",
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
		"credentials": {}
	}
];
var client = null;

beforeEach(function (done) {
	helper.startServer(done);
});

afterEach(function (done) {
	helper.unload();
	helper.stopServer(done);
});

describe('mqtt sparkplug device node', function () {

	it('should be loaded', function (done) {
		
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  done(err);
			});
		});

		/*var flow = [{ id: "n1", type: "mqtt sparkplug device", name: "device" }];
		helper.load(sparkplugNode, flow, function () {
			var n1 = helper.getNode("n1");
			n1.should.have.property('name', 'device');
			//done();
		});*/
	  });

	  it('should be disconnect MQTT Broker', function (done) {
		
		client.end();
		client = null;

		done();
	  });


});