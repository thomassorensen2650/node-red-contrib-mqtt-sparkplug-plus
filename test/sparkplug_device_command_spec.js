var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");
var pako = require('pako');

var spPayload = require('sparkplug-payload').get("spBv1.0");
helper.init(require.resolve('node-red'));
let testBroker = 'mqtt://localhost';
var client = null;

describe('mqtt sparkplug device commands', function () {
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

    it('should support device rename', function (done) {

        // TODO: Implement
		client = mqtt.connect(testBroker);

		// 1. Send a rename command and metrics.. verify that metrics are send using the new name 

		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;
						n1.receive({
							"command" : {
								"device" : {
									"set_name" : "NEW_NAME"
								}
							}
						})
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
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/NEW_NAME") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("seq").which.is.eql(1);
				done();
			}	
		});
	}); // it end 

    it('should rebirth on device rename', function (done) {
            // TODO: Implement
		client = mqtt.connect(testBroker);

		// 1. Send a rename command and metrics.. verify that metrics are send using the new name 

		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {


					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						b1.client.on('connect',function (connack) {
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
							n1.receive({
								"command" : {
									"device" : {
										"set_name" : "NEW_NAME"
									}
								}
							})
						});
						// Send all metrics to trigger DBIRTH
					
						
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		var stateId = 0;
		client.on('message', function (topic, message) {

			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2") {
				stateId.should.eql(0);
				stateId++
			}
			if (topic === "spBv1.0/My Devices/DDEATH/Node-Red/TEST2") {
				stateId.should.eql(1);
				stateId++
			}
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/NEW_NAME") {
				stateId.should.eql(2);
				done();
			}	
		});
    });

});