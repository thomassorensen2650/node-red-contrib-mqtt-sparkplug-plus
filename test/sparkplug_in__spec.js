var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");
var pako = require('pako');

var spPayload = require('sparkplug-payload').get("spBv1.0");
helper.init(require.resolve('node-red'));

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
	});

	it('should only decode spB namespace', function (done) {

		helper.load(sparkplugNode, inExample, function () {

			var n2 = helper.getNode("n2");
			var out = helper.getNode("out");
			b1 = out.brokerConn;

			b1.client.on('connect',function (connack) {
				b1.client.publish("MyTopic", "Hello there");
			});
			n2.on("input", function (msg) {
				//  Output event from MQTT Sparkplug In
				try {
					if (msg.topic == "MyTopic") {
						let p = msg.payload.toString();
						p.should.eql("Hello there");
						//msg.payload.should.deepEqual(validMsg);
						done();
					}
				} catch(err) {
				  done(err);
				}
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
		helper.load(sparkplugNode, inExample, function () {
			var n2 = helper.getNode("n2");
			var b1 = helper.getNode("b1");
			b1.client.on('connect',function (connack) {
				b1.client.publish(testMsg.topic, compressedPayload);
			});
			n2.on("input", function (msg) {
				try {
					if (msg.topic == testMsg.topic) {
						msg.should.have.property('payload');
						msg.payload.seq.should.eql(200) 
						msg.payload.metrics.should.deepEqual(validMsg.metrics);
						done();
					}
				} catch(err) {
					done(err);
				}
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
	
		helper.load(sparkplugNode, inExample, function () {
			var n2 = helper.getNode("n2");

			var b1 = helper.getNode("b1");
			b1.client.on('connect',function (connack) {
				b1.client.publish(testMsg.topic, compressedPayload);
			});

			n2.on("input", function (msg) {
				if (msg.topic == testMsg.topic) {
					try {
						msg.should.have.property('payload');
						msg.payload.seq.should.eql(200)
						msg.payload.metrics.should.deepEqual(validMsg.metrics);
						done();
					} catch(err) {
					done(err);
					}
				}
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
	

		helper.load(sparkplugNode, inExample, function () {
			var n2 = helper.getNode("n2");
			var n1 = helper.getNode("in");
			var b1 = helper.getNode("b1");
			b1.client.on('connect',function (connack) {
				b1.client.publish("spBv1.0/My Devices/DDATA/Node-Red/TEST2", compressedPayload);
			});
			n1.on('call:error', call => {
				call.should.be.calledWithExactly('mqtt-sparkplug-plus.errors.unable-to-decode-message');
				done();
			});

			n2.on("input", function (msg) {
				try {
					msg.should.have.property('payload');
				} catch(err) {
				done(err);
				}
			});
		});
	
	});

});
