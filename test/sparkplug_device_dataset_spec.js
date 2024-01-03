var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");
var pako = require('pako');

var spPayload = require('sparkplug-payload').get("spBv1.0");
helper.init(require.resolve('node-red'));
let testBroker = 'mqtt://localhost';
var client = null;

describe('mqtt sparkplug device node - DataSet Support', function () {
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
			"name": "TEST100",
			"metrics": {
				"a": {
					"dataType": "DataSet"
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

	it('Should send valid dataset', function (done) { 
		let msg = {
			"payload" : {
				"metrics": [
					{
						"name": "a",
						"value": {
							"numOfColumns": 2,
							
							"types": [
								"String",
								"String"
							],
							"columns": [
								"Col1",
								"OtherCol"
							],
							"rows": [
								[
									"a",
									"A"
								],
								[
									"v",
									"B"
								]
							]
						}
					}
				]
			}
		};

		client = mqtt.connect(testBroker);
		let n1;
		let b1;
		client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker

			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST100"){
				
				
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				let val = payload.metrics[0].value;
				let metric = payload.metrics[0];

				val.types.should.containDeep(msg.payload.metrics[0].value.types)
				val.columns.should.containDeep(msg.payload.metrics[0].value.columns)
				val.rows.should.containDeep(msg.payload.metrics[0].value.rows)
				val.numOfColumns.low.should.eql(2);
				//.should.containDeep(msg.payload.metrics);
				done();
			}
		});
		client.on('connect', function () {
			client.subscribe('#', function (err) {
				if (!err) {
					helper.load(sparkplugNode, simpleFlow, function () {
						try {
							n1 = helper.getNode("n1");
							b1 = n1.brokerConn;
							n1.receive(msg);
						}catch (e) {
							done(e);
						}
					});
				}else {
					done(err);
				}
			})
		});
	});
    
	it('Should warn of invalid dataset', function (done) {
		let msg = {
			"payload" : {
				"metrics": [
					{
						"name": "a",
						"value": {
							"rows": [
								[
									"a",
									"A"
								],
								[
									"v",
									"B"
								]
							]
						}
					}
				]
			}
		};

		let n1;
		let b1;
		helper.load(sparkplugNode, simpleFlow, function () {
			try {
				n1 = helper.getNode("n1");
				n1.on('input', () => {
					n1.warn.should.be.calledWithExactly("mqtt-sparkplug-plus.errors.invalid-metric-data");
					done();
				}); 
				b1 = n1.brokerConn;
				n1.receive(msg);
			}catch (e) {
				done(e);
			}
		});
	});

	it('Should warn if rowCount != actual array size', function (done) {
		let msg = {
			"payload" : {
				"metrics": [
					{
						"name": "a",
						"value": {
							"numOfColumns": 3,
							
							"types": [
								"String",
								"String"
							],
							"columns": [
								"Col1",
								"OtherCol"
							],
							"rows": [
								[
									"a",
									"A"
								],
								[
									"v",
									"B"
								]
							]
						}
					}
				]
			}
		};
		let n1;
		let b1;


		helper.load(sparkplugNode, simpleFlow, function () {
			try {
				n1 = helper.getNode("n1");
				n1.on('input', () => {
					n1.warn.should.be.calledWithExactly("mqtt-sparkplug-plus.errors.invalid-metric-data");
					done();
				}); 
				b1 = n1.brokerConn;
				n1.receive(msg);
			}catch (e) {
				done(e);
			}
		});
	})

	it('Should warn if arrays are not the same size', function (done) {
		let msg = {
			"payload" : {
				"metrics": [
					{
						"name": "a",
						"value": {
							"numOfColumns": 2,
							
							"types": [
								"String",
								"String"
							],
							"columns": [
								"Col1",
								//"OtherCol"
							],
							"rows": [
								[
									"a",
									"A"
								],
								[
									"v",
									"B"
								]
							]
						}
					}
				]
			}
		};

		let n1;
		let b1;
		
		helper.load(sparkplugNode, simpleFlow, function () {
			n1 = helper.getNode("n1");
			n1.on('input', () => {
				n1.warn.should.be.calledWithExactly("mqtt-sparkplug-plus.errors.invalid-metric-data");
				done();
			}); 

			try {
			
				b1 = n1.brokerConn;
				n1.receive(msg);
			}catch (e) {
				done(e);
			}
		});
	});
});

