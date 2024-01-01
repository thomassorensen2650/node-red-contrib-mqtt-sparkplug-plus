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
	var validMsg = {"timestamp":12345,"metrics":[{"name":"test","type":"Int32","value":100}],"seq":200}

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
				var n1 = helper.getNode("n1");
				n1.on('call:error', call => {
					call.should.be.calledWithExactly('mqtt-sparkplug-plus.errors.unable-to-decode-message');
					done();
				  });

				n2.on("input", function (msg) {
					try {
					msg.should.have.property('payload');
					if (msg.payload.seq === 200) {
						console.log(payload);
						done();
					}else {
						// Nasty hack, to make sure we publish after node is online. 
						client.publish("spBv1.0/My Devices/DDATA/Node-Red/TEST2", compressedPayload);
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
