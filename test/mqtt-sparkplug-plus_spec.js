var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");
var pako = require('pako');


var spPayload = require('sparkplug-payload').get("spBv1.0");

helper.init(require.resolve('node-red'));

let testBroker = 'mqtt://localhost';
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
var client = null;

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

	it('should send DBirth message', function (done) {
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

						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);

				//console.log(payload);
				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics.should.containDeep([{
					name: 'test',
					type: 'Int32',
					value: 11,
					//timestamp: 1630716767231
					},
					{
					name: 'test2',
					type: 'Int32',
					value: 11,
					//timestamp: 1630716767232
					}
				]);
				payload.should.have.property("seq").which.is.eql(1);
				done();
				//client.end();
			}
			
		});

	}); // it end 

	it('should send REBIRTH messages', function (done) {
		client = mqtt.connect(testBroker);
		var initBirthDone = false;
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

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
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			  
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red") {
				if (initBirthDone === true) {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					// Verify that we reset the seq to 0
					payload.should.have.property("seq").which.is.eql(1);
				}
			} else if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
					// Ready to issue rebirth
					if (initBirthDone === true) {
						var buffer = Buffer.from(message);
						var payload = spPayload.decodePayload(buffer);
						payload.should.have.property("seq").which.is.eql(1);
						done();
	
					} else {
						var rebirth = {
							metrics : [
							{
								"name" : "Node Control/Rebirth",
								"type" : "Boolean",
								"value": true
							},
						]
					}	
					var payload = spPayload.encodePayload(rebirth);
	
					client.publish("spBv1.0/My Devices/NCMD/Node-Red",payload);
					initBirthDone = true;
					}
				}
		});

	}); // it end 

	it('should send valid NData in input', function (done) {
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
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			//console.log("TOPIC:", topic);
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				n1.receive({
					"payload" : {
						"metrics": [
							{
								"name": "test",
								"value": 100,
								//"timestamp": new Date()
							},
						]}
					}
				);
			} else if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);

				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics[0].should.have.property("name").which.is.eql("test");
				payload.metrics[0].should.have.property("value").which.is.eql(100);
				payload.metrics[0].should.have.property("type").which.is.eql("Int32");
				//payload.metrics[0].should.have.property("timestamp").which.is.a.Number();
				payload.metrics.length.should.eql(1);
				Object.keys(payload.metrics[0]).length.should.eql(3);
				payload.should.have.property("seq").which.is.eql(2); // 0 is NBIRTH, 1 is DBIRTH
				done();
				//client.end();
			}
			
		});

	}); // it end 

	it('should convert NData timestamp to EPOC', function (done) {
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
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			//console.log("TOPIC:", topic);
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				n1.receive({
					"payload" : {
						"metrics": [
							{
								"name": "test",
								"value": 100,
								"timestamp": new Date()
							},
						]}
					}
				);
			} else if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics[0].should.have.property("name").which.is.eql("test");
				payload.metrics[0].should.have.property("value").which.is.eql(100);
				payload.metrics[0].should.have.property("type").which.is.eql("Int32");
				payload.metrics[0].should.have.property("timestamp").which.is.a.Number();
				payload.metrics.length.should.eql(1);
				Object.keys(payload.metrics[0]).length.should.eql(4);
				payload.should.have.property("seq").which.is.eql(2); // 0 is NBIRTH, 1 is DBIRTH
				done();
				client.end();
			}
			
		});

	}); // it end 

	it('should warn when passing unknown NData metric', function (done) {
		helper.load(sparkplugNode, simpleFlow, function () {
		
			const n1 = helper.getNode("n1");
			n1.on('input', () => {
				n1.warn.should.be.calledWithExactly("mqtt-sparkplug-plus.errors.device-unknown-metric");
				done();
			}); 
			  n1.receive({
				"payload" : {
					"metrics": [
						{
							"name": "does_not_exits",
							"value": 100
						},
					]}
				}
			);			

		}); // end helper
	}); // it end 

	it('should warn when passing NData metric without name', function (done) {
		helper.load(sparkplugNode, simpleFlow, function () {
		
			let n1 = helper.getNode("n1");
			n1.on('input', () => {
				n1.warn.should.be.calledWithExactly("mqtt-sparkplug-plus.errors.missing-attribute-name");
				done();
			  });
			n1.receive({
				"payload" : {
					"metrics": [
						{
							"value": 100
						},
					]}
				}
			);

		}); // end helper
	}); // it end 

	it('should error when passing NData metric that is not array', function (done) {
		helper.load(sparkplugNode, simpleFlow, function () {
		
			let n1 = helper.getNode("n1");
			n1.receive({
				"payload" : {
					"metrics": {"A": "B"} }
				}
			);

			n1.on('call:error', call => {
				// XXX
				call.firstArg.should.eql("mqtt-sparkplug-plus.errors.device-no-metrics")
				done();
			  });
		}); // end helper
	}); // it end 

	it('should error when passing NData payload that is not object', function (done) {
		helper.load(sparkplugNode, simpleFlow, function () {
		
			let n1 = helper.getNode("n1");
			/*n1.on('input', () => {
				n1.error.should.be.calledWithExactly("Metrics should be an Array");
				done();
			  });*/
			n1.receive({
				"payload" : ["A", "B"]
			});

			n1.on('call:error', call => {
				// XXX
				call.firstArg.should.eql("mqtt-sparkplug-plus.errors.payload-type-object")
				done();
			  });


		}); // end helper
	}); // it end 


it('should add null_value on DData without value', function (done) {
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

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			//console.log("TOPIC:", topic);
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				n1.receive({
					"payload" : {
						"metrics": [
							{
								"name": "test",
								"value": null
								//"timestamp": new Date()
							},
						]}
					}
				);
			} else if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
				try {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					payload.should.have.property("timestamp").which.is.a.Number();
					payload.metrics[0].should.have.property("name").which.is.eql("test");
					payload.metrics[0].should.have.property("value").which.is.eql(null);
					payload.metrics[0].should.have.property("type").which.is.eql("Int32");
					done();
				} catch (e) {
					done(e);
				}

			}
			
		});

	}); // it end */



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
							}
						);
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
				payload.should.have.property("seq").which.is.eql(0);
				n1.brokerConn.primaryScadaStatus.should.eql("ONLINE");

			} else if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("seq").which.is.eql(1);
				n1.brokerConn.primaryScadaStatus.should.eql("ONLINE");
				done();
			}
		});
	}); // it end 


	// FIXME add unit testing:
	//   Test unknown metric data type
	//   Test NDEATH
	//   Test Null Value
	//   Test Invalid DCMD


	// MQTT IN
	// Test that it works
	// Test That filtering works
	// That that it errors on invalid data
	
});


var inExample = [
    {
        "id": "n2",
        "type": "helper",
    },
    {
        "id": "n1",
        "type": "mqtt sparkplug in",
        "name": "",
        "topic": "#", //"spBv1.0/+/DDATA/+/+",
        "qos": "2",
        "broker": "b1",
        "wires": [["n2"]]
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
]

/**
 * MQTT Sparkplug B in testing
 */
describe('mqtt sparkplug in node', function () {

	var validMsg = {"timestamp":12345,"metrics":[{"name":"test","type":"Int32","value":100}],"seq":200}

	// Connect to 
	it('should ouput a subscribed topic', function (done) {

		var testMsg = {
			topic : "spBv1.0/My Devices/DDATA/Node-Red/TEST2",
			payload : spPayload.encodePayload(validMsg)
		}


		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			helper.load(sparkplugNode, inExample, function () {
				var n2 = helper.getNode("n2");
				n2.on("input", function (msg) {
					try {
					msg.should.have.property('payload');
					if (msg.payload.seq === 200) {
						msg.payload.should.deepEqual(validMsg);
						done();
					}else {
						// Nasty hack, to make sure we publish after node is online. 
						client.publish(testMsg.topic, testMsg.payload);
					}
					} catch(err) {
						console.log("Error");
					  done(err);
					}
				  });
			});
		});
	});

		// Connect to 
	it('should decompress a DEFLATE encoded topic', function (done) {

		var testMsg = {
			topic : "spBv1.0/My Devices/DDATA/Node-Red/TEST2",
			payload : spPayload.encodePayload(validMsg)
		}

		var compressedPayload = {
            "uuid" : "SPBV1.0_COMPRESSED",
            body : pako.deflate(testMsg.payload),
            metrics : [ {
                "name" : "algorithm", 
                "value" : "DEFLATE", 
                "type" : "string"
            } ]
        };
		compressedPayload = spPayload.encodePayload(compressedPayload);
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			helper.load(sparkplugNode, inExample, function () {
				var n2 = helper.getNode("n2");
				n2.on("input", function (msg) {
					try {
					msg.should.have.property('payload');
					if (msg.payload.seq === 200) {
						msg.payload.should.deepEqual(validMsg);
						done();
					}else {
						// Nasty hack, to make sure we publish after node is online. 
						client.publish(testMsg.topic, compressedPayload);
					}
					} catch(err) {
						console.log("Error");
					  done(err);
					}
				  });
			});
		});
	});
	it('should decompress a GZIP encoded topic', function (done) {

		var testMsg = {
			topic : "spBv1.0/My Devices/DDATA/Node-Red/TEST2",
			payload : spPayload.encodePayload(validMsg)
		}

		var compressedPayload = {
            "uuid" : "SPBV1.0_COMPRESSED",
            body : pako.gzip(testMsg.payload),
            metrics : [ {
                "name" : "algorithm", 
                "value" : "DEFLATE", 
                "type" : "string"
            } ]
        };
		compressedPayload = spPayload.encodePayload(compressedPayload);
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			helper.load(sparkplugNode, inExample, function () {
				var n2 = helper.getNode("n2");
				n2.on("input", function (msg) {
					try {
					msg.should.have.property('payload');
					if (msg.payload.seq === 200) {
						msg.payload.should.deepEqual(validMsg);
						done();
					}else {
						// Nasty hack, to make sure we publish after node is online. 
						client.publish(testMsg.topic, compressedPayload);
					}
					} catch(err) {
						console.log("Error");
					  done(err);
					}
				  });
			});
		});
	});

});


/**  
 * mqtt sparkplug out testing
 */
describe('mqtt sparkplug out node', function () {

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
						setTimeout(() => n1.receive({ payload: validMsg}), 500);
					});
				}
			});
		});

		client.on('message', function (topic, message) {
			var buffer = Buffer.from(message);
			var payload = spPayload.decodePayload(buffer);
			n1.brokerConn.primaryScadaStatus.should.eql("OFFLINE");
			payload.should.deepEqual(validMsg);
			done();
		});
	});

	/**
	 * Verify that we'll buffer if forced enabled,
	 */
	it('should buffer if publish topic', function (done) {

		var n1 = null;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, function () {
						n1 = helper.getNode("n1");
						n1.shouldBuffer = true; // Force enable to buffer
						setTimeout(() => n1.receive({ payload: validMsg}), 500);
						setTimeout(() => client.publish("STATE/MY SCADA", "ONLINE", true), 1000);						});
				}
			});
		});

		client.on('message', function (topic, message) {
			var buffer = Buffer.from(message);
			var payload = spPayload.decodePayload(buffer);
			n1.brokerConn.primaryScadaStatus.should.eql("ONLINE");
			payload.should.deepEqual(validMsg);
			done();
		});
	});

});
