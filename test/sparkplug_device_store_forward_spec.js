var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");
var pako = require('pako');

var spPayload = require('sparkplug-payload').get("spBv1.0");
helper.init(require.resolve('node-red'));
let testBroker = 'mqtt://localhost';
var client = null;

describe('mqtt sparkplug device node - Store Forward', function () {
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
			"broker": "b1"
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
			"primaryScada": "MY SCADA"
		}
	];
    	// STORE FORWARD TESTING
	it('should buffer when primary SCADA IS OFFLINE', function (done) {
		client = mqtt.connect(testBroker);

		// WARN! We'll enable buffering for all tests
		simpleFlow[1].enableStoreForward = true;

		// SET OFFLINE
		// Send Birth
		// SET SCADA ONLINE
		// VERIFY BIRTH is send when ONLINE

		var initBirthDone = false;
		let n1;
		let b1;
		client.on('connect', function () {
			client.publish("STATE/MY SCADA", "OFFLINE", true);
			// Set Online after 250ms 
			setTimeout(() => client.publish("STATE/MY SCADA", "ONLINE", true), 500);
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;
						n1.on('call:error', call => {
							console.log("ERROR", call.firstArg);
							call.firstArg.should.eql("mqtt-sparkplug-plus.errors.payload-type-object")
							done();
						});
						b1.on('call:error', call => {
							console.log("ERROR1", call.firstArg);
							call.firstArg.should.eql("mqtt-sparkplug-plus.errors.payload-type-object")
							done();
						});
			
						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
						});
						
					}catch (e) {
						done(e);
					}
				});
			  }
			})
			
			
		  });

		  client.on('message', function (topic, message) {
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("seq");
                payload.seq.toInt().should.eql(0);
				n1.brokerConn.primaryScadaStatus.should.eql("ONLINE");

			} else if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("seq");
                payload.seq.toInt().should.eql(1);
				n1.brokerConn.primaryScadaStatus.should.eql("ONLINE");
				simpleFlow[1].enableStoreForward = false;
				done();
			}
		});
	}); // it end 
});