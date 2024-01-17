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
 * MQTT Sparkplug B in testing
 */
describe('mqtt sparkplug in node', function () {

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

	var inExample = [
		{
			"id": "n2",
			"type": "helper",
		},
		{
			"id": "in",
			"type": "mqtt sparkplug in",
			"name": "",
			"topic": "#", //"spBv1.0/+/DDATA/+/+",
			"qos": "2",
			"broker": "b1",
			"wires": [["n2"]]
		},
		{
			"id": "out",
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
			"credentials": {}
		}
	]
	var validMsg = {"timestamp":new Date(),"metrics":[{"name":"test","type":"Int32","value":100}],"seq":200}

	it('should ouput a subscribed topic', function (done) {

		var testMsg = {
			topic : "",
			payload : spPayload.encodePayload(validMsg)
		}

		helper.load(sparkplugNode, inExample, function () {

			var n2 = helper.getNode("n2");
			var out = helper.getNode("out");
			b1 = out.brokerConn;

			b1.client.on('connect',function (connack) {
				out.receive({
					"payload" : validMsg
				})
			});
			n2.on("input", function (msg) {
				//  Output event from MQTT Sparkplug In
				try {
					if (msg.topic == "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
						msg.should.have.property('payload');
						msg.payload.metrics.should.deepEqual(validMsg.metrics);
						msg.payload.timestamp.getTime().should.eql(validMsg.timestamp);

						//msg.payload.should.deepEqual(validMsg);
						done();
					}

				} catch(err) {
				  done(err);
				}
			});	
		});

/*

b1 = n1.brokerConn;

						b1.client.on('connect',function (connack) {
							n1.receive({

		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			helper.load(sparkplugNode, inExample, function () {

				// Output event from MQTT Sparkplug In
				var n2 = helper.getNode("n2");
				n2.on("input", function (msg) {
					
					
				  });
			});
		});*/
	});

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
							msg.payload.metrics.should.deepEqual(validMsg.metrics);
							done();
						}else {
							// Nasty hack, to make sure we publish after node is online. 
							client.publish(testMsg.topic, compressedPayload);
						}
					} catch(err) {
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
                "value" : "GZIP", 
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
							msg.payload.metrics.should.deepEqual(validMsg.metrics);
							done();
						}else {
							// Nasty hack, to make sure we publish after node is online. 
							client.publish(testMsg.topic, compressedPayload);
						}
					} catch(err) {
					  done(err);
					}
				  });
			});
		});
	});

	it('should error on invalid compression topic', function (done) {

		var compressedPayload = {
            "uuid" : "SPBV1.0_COMPRESSED",
            body : "Hello World!",
            metrics : [ {
                "name" : "algorithm", 
                "value" : "DEFLATE", 
                "type" : "string"
            } ],
			seq : 200
        };
		compressedPayload = spPayload.encodePayload(compressedPayload);
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			helper.load(sparkplugNode, inExample, function () {
				var n2 = helper.getNode("n2");
				var n1 = helper.getNode("in");
				n1.on('call:error', call => {
					call.should.be.calledWithExactly('mqtt-sparkplug-plus.errors.unable-to-decode-message');
					done();
				  });

				n2.on("input", function (msg) {
					try {
						msg.should.have.property('payload');
						
						
						if (msg.payload.seq === 200) {
							done();
						}else {
							// Nasty hack, to make sure we publish after node is online. 
							client.publish("spBv1.0/My Devices/DDATA/Node-Red/TEST2", compressedPayload);
						}
					} catch(err) {
					  done(err);
					}
				  });
			});
		});
	});

});
