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
								"EoN" : {
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
								"EoN" : {
									"set_name" : "NEW_NAME"
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
        if (topic === "spBv1.0/My Devices/NBIRTH/NEW_NAME") {
            stateId.should.eql(3);

            var buffer = Buffer.from(message);
            var payload = spPayload.decodePayload(buffer);
            let bd = payload.metrics.find(x=>x.name == "bdSeq");
            bd.value.low.should.eql(1);

            stateId++
        }	

        if (topic === "spBv1.0/My Devices/DBIRTH/NEW_NAME/TEST2") {
            stateId.should.eql(4);
            done();
        }	

      });
});
});