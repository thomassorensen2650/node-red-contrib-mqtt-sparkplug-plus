var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");
var client = null;
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

beforeEach(function (done) {
	helper.startServer(done);
});

afterEach(function (done) {

	if (client) {
		client.end();
	}
	helper.unload();
	helper.stopServer(done);
});

describe('mqtt sparkplug device node', function () {

	it('should be loaded', function (done) {
		var flow = [{ id: "n1", type: "mqtt sparkplug device", name: "device" }];
		helper.load(sparkplugNode, flow, function () {
			var n1 = helper.getNode("n1");
			n1.should.have.property('name', 'device');
			done();
		});
	  });

	  /**
	   * Verify NBirth is send when starting up Node-Red with a Device loaded.
	   */
	  it('should send NBirth message', function (done) {
		client = mqtt.connect(testBroker);
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			if (topic === "spBv1.0/My Devices/NBIRTH/Node-Red"){
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics.should.containDeep([
					{ name: 'Node Control/Rebirth', type: 'Boolean', value: false },
					{ name: 'bdSeq', type: 'Int8', value: 0 }
				 ]);
				payload.should.have.property("seq").which.is.eql(0);
				done();
				client.end();
			}
		});

	}); // it end 

	// FIXME add unit testing:
	//   Test metric with no name
	//   Test metrics as object (should be array)
	//   Test unknown metric data type
	//   Test metric date as Data Object (should be converted to EPOC)
	//   Test DBIRTH
	//   Test NDEATH
	//   
});

