var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");
var pako = require('pako');

var spPayload = require('sparkplug-payload').get("spBv1.0");
helper.init(require.resolve('node-red'));
let testBroker = 'mqtt://localhost';
var client = null;

describe('mqtt sparkplug EoN - Commands', function () {
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
			"enableStoreForward": false,
			"primaryScada": "MY SCADA"
		},
		{
			"id": "o1",
			"type": "mqtt sparkplug out",
			"broker": "b1",
			"wires": []
		},
		{ id: "n2", type: "helper" }
	];

	it('should not birth until connect', function (done) {
	
	flow = simpleFlow;
	flow[1].manualEoNBirth = true;
	client = mqtt.connect(testBroker);

	let n1;
	let b1;
	var waitOver = false;
	client.on('connect', function () {
		client.subscribe('#', function (err) {
		  if (!err) {
			helper.load(sparkplugNode, simpleFlow, function () {
				

				try {
					n1 = helper.getNode("n1");
					b1 = n1.brokerConn;

					setTimeout(() => {
						waitOver = true;
						n1.receive({
							"command" : {
								"node" : {
									"connect" : true
								}
							}
						})	
					}, 500);
				}catch (e) {
					done(e);
				}
			});
		  }
		})
	  });

	

	client.on('message', function (topic, message) {
		if (topic === "spBv1.0/My Devices/NBIRTH/Node-Red") {
			waitOver.should.be.true();
			done();
		}
	});
});

	it('should rebirth on new name', function (done) {
	
	simpleFlow = simpleFlow;
	simpleFlow[1].manualEoNBirth = false;
    simpleFlow[0].birthImmediately = true;
	client = mqtt.connect(testBroker);
	let n1;
	let b1;
	var waitOver = false;
	client.on('connect', function () {
		client.subscribe('#', function (err) {
		  if (!err) {
			helper.load(sparkplugNode, simpleFlow, function () {
				

				try {
					n1 = helper.getNode("n1");
					b1 = n1.brokerConn;
                  

					setTimeout(() => {
						waitOver = true;
						n1.receive({
							"command" : {
								"node" : {
									"set_name" : "NEW_NAME",
                                    "set_group" : "NEW_GROUP"
								}
							}
						})	
					}, 500);
				}catch (e) {
					done(e);
				}
			});
		  }
		})
	  });

	

    var stateId = 0;
    client.on('message', function (topic, message) {

        if (topic === "spBv1.0/My Devices/NBIRTH/Node-Red") {
            stateId.should.eql(0);

            var buffer = Buffer.from(message);
            var payload = spPayload.decodePayload(buffer);
   
            let bd = payload.metrics.find(x=>x.name == "bdSeq");
            bd.value.low.should.eql(0);
        
            stateId++
        }
        if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2") {
            stateId.should.eql(1);
            stateId++
        }
        if (topic === "spBv1.0/My Devices/NDEATH/Node-Red") {
            stateId.should.eql(2);

            var buffer = Buffer.from(message);
            var payload = spPayload.decodePayload(buffer);
            let bd = payload.metrics.find(x=>x.name == "bdSeq");
            bd.value.low.should.eql(0);

            stateId++
        }
        if (topic === "spBv1.0/NEW_GROUP/NBIRTH/NEW_NAME") {
            stateId.should.eql(3);

            var buffer = Buffer.from(message);
            var payload = spPayload.decodePayload(buffer);
            let bd = payload.metrics.find(x=>x.name == "bdSeq");
            bd.value.low.should.eql(0); // BDSeq should not increase on REBIRTH

            stateId++
        }	

        if (topic === "spBv1.0/NEW_GROUP/DBIRTH/NEW_NAME/TEST2") {
            stateId.should.eql(4);
            done();
        }	

      });
	});

	it('should subscribe on new node topic (Node name change)', function (done) {

		simpleFlow[1].manualEoNBirth = true;
		simpleFlow[0].birthImmediately = true;

		helper.load(sparkplugNode, simpleFlow, function () {
			
			var n1 = helper.getNode("n1");
			var o1 = helper.getNode("o1");

			var n2 = helper.getNode("n2");
			n2.on("input", function (msg) {
				msg.topic.should.eql("spBv1.0/My Devices/DCMD/NEW_NAME/TEST2")
				done();
	
			});


			n1.receive({
				"command" : {
					"node" : {
						"set_name" : "NEW_NAME",
						"connect" : true
					}
				},
				"payload" : [
					{
						"name": "test",
						"value": 11,
					},
					{
						"name": "test2",
						"value": 11
					}
				]	
			})	;
			setTimeout(() => {

				var c1 = n1.brokerConn.client;
				// Send on old topic and new topic to make sure it only subscribes to new topic
				c1.connected.should.be.true();
				o1.receive({
					topic : "spBv1.0/My Devices/DCMD/Node-Red/TEST2",
					payload : {
						"metrics" : [
						{
							"name": "test",
							"value": 500,
							"type" : "Int32"
						}
					]
					}
						
				});

				o1.receive({
					"topic" : "spBv1.0/My Devices/DCMD/RANDOM/TEST2",
					payload : {
						"metrics" : [
						{
							"name": "test",
							"value": 500,
							"type" : "Int32"
						}
					]
					}
				});

				o1.receive({
					"topic" : "spBv1.0/My Devices/DCMD/NEW_NAME/TEST2",
					payload : {
						"metrics" : [
						{
							"name": "test",
							"value": 500,
							"type" : "Int32"
						}
					]
					}
				});
				}, 200);
			});
	});

	it('should subscribe on new node topic (Device name change)', function (done) {

		simpleFlow[1].manualEoNBirth = true;
		simpleFlow[0].birthImmediately = true;

		helper.load(sparkplugNode, simpleFlow, function () {
			
			var n1 = helper.getNode("n1");
			var o1 = helper.getNode("o1");

			var n2 = helper.getNode("n2");
			n2.on("input", function (msg) {
				msg.topic.should.eql("spBv1.0/My Devices/DCMD/Node-Red/NEW_NAME")
				done();
	
			});
			n1.receive({
				"command" : {
					"node" : {
						"connect" : true
					},
					"device" : {
						"set_name" : "NEW_NAME"
					}
				},
				"payload" : [
					{
						"name": "test",
						"value": 11,
					},
					{
						"name": "test2",
						"value": 11
					}
				]	
			})	;
			setTimeout(() => {

				var c1 = n1.brokerConn.client;
				// Send on old topic and new topic to make sure it only subscribes to new topic
				c1.connected.should.be.true();
				o1.receive({
					topic : "spBv1.0/My Devices/DCMD/Node-Red/TEST2",
					payload : {
						"metrics" : [
						{
							"name": "test",
							"value": 500,
							"type" : "Int32"
						}
					]
					}
						
				});

				o1.receive({
					"topic" : "spBv1.0/My Devices/DCMD/RANDOM/TEST2",
					payload : {
						"metrics" : [
						{
							"name": "test",
							"value": 500,
							"type" : "Int32"
						}
					]
					}
				});

				o1.receive({
					"topic" : "spBv1.0/My Devices/DCMD/Node-Red/NEW_NAME",
					payload : {
						"metrics" : [
						{
							"name": "test",
							"value": 500,
							"type" : "Int32"
						}
					]
					}
				});
				}, 200);
			});
	});


	it('should NBIRTH ON REBIRTH CMD', function (done) {


		/**
		 * 1. 
		 */
		client = mqtt.connect(testBroker);
		expectedMessageId = 0;

		// WARN! We'll enable buffering for all tests
		simpleFlow[1].enableStoreForward = false;
		simpleFlow[1].manualEoNBirth = false;
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
						});
						
					}catch (e) {
						done(e);
					}
				});
			  }
			})
			
			
		  });

		  client.on('message', function (topic, message) {
			switch (expectedMessageId++)
			{
				case 0:
					topic.should.equal("spBv1.0/My Devices/NBIRTH/Node-Red")
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					payload.should.have.property("seq");
					payload.seq.toInt().should.eql(0);
					break;
				case 1:
					topic.should.equal("spBv1.0/My Devices/DBIRTH/Node-Red/TEST2")
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					payload.should.have.property("seq");
					payload.seq.toInt().should.eql(1);

					// Send a Rebirth Command
					var cmdPayload = spPayload.encodePayload({
						metrics : [
							{
								name: "Node Control/Rebirth",
								type: "Boolean",
								value: true
							}
						]
					});
					client.publish("spBv1.0/My Devices/NCMD/Node-Red", cmdPayload);
					break;
				case 2: // We Issue a rebirth command
					topic.should.equal("spBv1.0/My Devices/NCMD/Node-Red")
					break;
				case 3: // EON will NDEATH Before new NBIRTH
					topic.should.equal("spBv1.0/My Devices/NDEATH/Node-Red")
					break;
				case 4: // EON will NBIRTH
					topic.should.equal("spBv1.0/My Devices/NBIRTH/Node-Red")
					break;
				case 5: // EON will DBIRTH
					topic.should.equal("spBv1.0/My Devices/DBIRTH/Node-Red/TEST2")
					done();
					break;
			}
		});
	}); // it end 
});

