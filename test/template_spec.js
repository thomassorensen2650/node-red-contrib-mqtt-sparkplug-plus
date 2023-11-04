var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");
var pako = require('pako');

//let validnbirth = {"timestamp":1692110985865,"metrics":[{"name":"MyType","type":"Template","value":{"version":"","templateRef":"Type-IS-A","isDefinition":true,"metrics":[{"name":"D","type":"Template","value":{"version":"","templateRef":"Type-Has-A","isDefinition":false,"metrics":[],"parameters":[]},"timestamp":1692110984675,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"B","type":"Int32","value":33,"timestamp":1691437351927,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"C","type":"Int32","value":22,"timestamp":1691437351927,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"A","type":"Int32","value":null,"timestamp":1692110984676,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110984676,"properties":{}},{"name":"Node Control/Next Server","type":"Boolean","value":false,"timestamp":1692110985890},{"name":"Type-Has-A","type":"Template","value":{"version":"","templateRef":"","isDefinition":true,"metrics":[{"name":"C","type":"Int32","value":null,"timestamp":1692110984677,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110984677,"properties":{}},{"name":"Type-IS-A","type":"Template","value":{"version":"","templateRef":"","isDefinition":true,"metrics":[{"name":"A","type":"Int32","value":null,"timestamp":1692110984675,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110984675,"properties":{}},{"name":"MyTypeInstance","type":"Template","value":{"version":"","templateRef":"MyType","isDefinition":false,"metrics":[{"name":"A","type":"Int32","value":null,"timestamp":1692110985865,"isHistorical":false,"isTransient":false,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"B","type":"Int32","value":33,"timestamp":1692110985865,"isHistorical":false,"isTransient":false,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"C","type":"Int32","value":22,"timestamp":1692110985865,"isHistorical":false,"isTransient":false,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"D","type":"Template","value":{"version":"","templateRef":"Type-Has-A","isDefinition":false,"metrics":[{"name":"C","type":"Int32","value":null,"timestamp":1692110985865,"isHistorical":false,"isTransient":false,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110985865,"isHistorical":false,"isTransient":false,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110985890,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"Type-Has-A-P","type":"Template","value":{"version":"","templateRef":"","isDefinition":true,"metrics":[{"name":"D","type":"Int32","value":null,"timestamp":1692110984676,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110984677,"properties":{}},{"name":"Node Info/Transmission Version","type":"String","value":"4.0.13 (b2022092123)","timestamp":1692110985890},{"name":"bdSeq","type":"Int64","value":{"low":1,"high":0,"unsigned":true},"timestamp":1692110985865},{"name":"Node Control/Rebirth","type":"Boolean","value":false,"timestamp":1692110985890}],"seq":0}
let validnbirth = {"timestamp":1692110985865,"metrics":[{"name":"MyType","type":"Template","value":{"version":"","isDefinition":true,"metrics":[{"name":"D","type":"Template","value":{"version":"","templateRef":"Type-Has-A","isDefinition":false,"metrics":[],"parameters":[]},"timestamp":1692110984675,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"B","type":"Int32","value":33,"timestamp":1691437351927,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"C","type":"Int32","value":22,"timestamp":1691437351927,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"A","type":"Int32","value":null,"timestamp":1692110984676,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110984676,"properties":{}},{"name":"Node Control/Next Server","type":"Boolean","value":false,"timestamp":1692110985890},{"name":"Type-Has-A","type":"Template","value":{"version":"","isDefinition":true,"metrics":[{"name":"C","type":"Int32","value":null,"timestamp":1692110984677,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110984677,"properties":{}},{"name":"Type-IS-A","type":"Template","value":{"version":"","isDefinition":true,"metrics":[{"name":"A","type":"Int32","value":null,"timestamp":1692110984675,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110984675,"properties":{}},{"name":"MyTypeInstance","type":"Template","value":{"version":"","templateRef":"MyType","isDefinition":false,"metrics":[{"name":"A","type":"Int32","value":null,"timestamp":1692110985865,"isHistorical":false,"isTransient":false,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"B","type":"Int32","value":33,"timestamp":1692110985865,"isHistorical":false,"isTransient":false,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"C","type":"Int32","value":22,"timestamp":1692110985865,"isHistorical":false,"isTransient":false,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"D","type":"Template","value":{"version":"","templateRef":"Type-Has-A","isDefinition":false,"metrics":[{"name":"C","type":"Int32","value":null,"timestamp":1692110985865,"isHistorical":false,"isTransient":false,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110985865,"isHistorical":false,"isTransient":false,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110985890,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}},{"name":"Type-Has-A-P","type":"Template","value":{"version":"","isDefinition":true,"metrics":[{"name":"D","type":"Int32","value":null,"timestamp":1692110984676,"metadata":{"isMultiPart":false,"contentType":"","size":{"low":0,"high":0,"unsigned":true},"seq":{"low":0,"high":0,"unsigned":true},"fileName":"","fileType":"","md5":"","description":""},"properties":{}}],"parameters":[]},"timestamp":1692110984677,"properties":{}},{"name":"Node Info/Transmission Version","type":"String","value":"4.0.13 (b2022092123)","timestamp":1692110985890},{"name":"bdSeq","type":"Int64","value":{"low":1,"high":0,"unsigned":true},"timestamp":1692110985865},{"name":"Node Control/Rebirth","type":"Boolean","value":false,"timestamp":1692110985890}],"seq":0}


helper.init(require.resolve('node-red'));

let testBroker = 'mqtt://localhost';
var client = null;

beforeEach(function (done) {
	//helper.startServer(done);
    done();
});

afterEach(function (done) {

	//if (client) {
	//	client.end();
	//}
	//helper.unload();
	//helper.stopServer(done);
    done();
});


describe('mqtt sparkplug device template support', function () {
		/**
		 * Template Testing...
		 * 
		 * 1. OK - Test that template defintions are send correctly on NBIRTH
		 * 2. Test that template instance is send correctly on DBIRTH
		 *   a. Test Valid
		 *   b. Test that invalid instance names throws error.
		 *   c. Test with birth immidiatly
		 * 3. Test that templates metrics are send correctly on DDATA (Test with one and more)
		 */

		templateFlow = [
			{
				"id": "n1",
				"type": "mqtt sparkplug device",
				"metrics": {
					"a": {
						"dataType": "MyTemplate"
					},
					"b": {
						"dataType": "Int32"
					}
				},
				"name" : "TheDevice",
				"broker": "b1",
				"birthImmediately": false,
			},
			{
				"id": "b1",
				"type": "mqtt-sparkplug-broker",
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
				"compressAlgorithm": "",
				"aliasMetrics": false,
				"templates": [
					"{\"name\":\"MyTemplate\",\"type\":\"Template\",\"value\":{\"version\":\"1.0.0\",\"isDefinition\":true,\"metrics\":[{\"name\":\"FirstTag\",\"type\":\"Int32\"},{\"name\":\"SecondTag\",\"type\":\"Int32\"}],\"parameters\":[]}}"
				],
				"primaryScada": "",
				"credentials": {}
			}
		]

        validateTemplate = function(root, template) {

			let shouldBeDefinition = root === "";

            if (!template.hasOwnProperty("name")) {
                throw "Template ust have a name"; // Can't find requirement for this in the spec, but i guess is obvius?
            }
            let name = `${root}$>>{template.name}`;

            let isTemplate = template.hasOwnProperty("type") && template.type === "Template";
            if (!(isTemplate)) {
                throw `${name} : A Template Definition MUST type and type must be template`;
            }

            let hasValue = template.hasOwnProperty("value") && typeof template.value === 'object';
            if (!hasValue) {
                throw `${name} : A Template Definition MUST have a value`; // Can't find a requirement for this one
            }
            let v = template.value;

            // [tck-id-payloads-template-definition-is-definition] A Template Definition MUST have is_definition set to true.
            if (!v.hasOwnProperty("isDefinition")) {
                throw `${name} : A Template Definition MUST have isDefinition`;
            }
			
            if (v.isDefinition !== true && shouldBeDefinition) {
                throw `${name} : A Template Definition MUST have isDefinition set to true`;
            }

			if (v.isDefinition === true && !shouldBeDefinition) {
                throw `${name} : A Template Instance MUST have isDefinition set to false`;
            }

            // [tck-id-payloads-template-definition-ref] A Template Definition MUST omit the template_ref field.
            if (v.hasOwnProperty("templateRef") && v.isDefinition) {
                throw `${name} : A Template Definition MUST omit the templateRef field`;
            }

            // Check Template Ref. 
            v.metrics.forEach(m => {

				if (!m.hasOwnProperty("name")) {
					throw `${name}: Metric must have a name`; // Can't find requirement for this in the spec, but i guess is obvius?
				}
				let mName = `${name}>>${m.name}`;

				if (!m.hasOwnProperty("type")) {
					throw `${mName}: metrics must have a type `;
				}

                // if metrics is 
                if (m.type === "Template") {
                    validateTemplate(name, m);
                }else {
                    // Validate Metric. 
                }
            })
            // for each metrics if template then call otherwise validate
        }

		getTemplateMetrics = function(templateDef) {
			metrics = [];
			templateDef.value.metrics.forEach(m => {
				if (m.type == Template) {
					metrics = metrics.concat(getTemplateMetrics(m));
				}
				else {
					metrics.push(m);
				}
			})
		}

		templateToMetrics = function(templates, root, metrics) {
			console.log("Inflating,", metrics);
			result = [];
			Object.keys(metrics).forEach(m => { 

				metric = metrics[m];
				if (metric.type === "Template") {
					// Get Metrics for 
					let dt = m.dataType;
					let t = templates.find((e) => e.hasOwnProperty("name") && e.name == dt);

					if (t == undefined) {
						throw "Template not found";
					}

                    result = result.concat(getTemplateMetrics(metric));
					
                }else {
                    result.push(m);
                }

			});
			return result;
		}

        // [tck-id-topics-nbirth-templates] If Template instances will be published by this Edge Node or any devices, all Template definitions MUST be published in the NBIRTH.
        // [tck-id-payloads-template-definition-nbirth-only] Template Definitions MUST only be included in NBIRTH messages.
        // [tck-id-payloads-template-definition-members] A Template Definition MUST include all member metrics that will ever be included in corresponding template instances.
        // [tck-id-payloads-template-definition-nbirth] A Template Definition MUST be included in the NBIRTH for all Template Instances that are included in the NBIRTH and DBIRTH messages.
        // [tck-id-payloads-template-definition-parameters] A Template Definition MUST include all parameters that will be included in the corresponding Template Instances.
        // • [tck-id-payloads-template-definition-parameters-default] A Template Definition MAY include values for parameters in the Template Definition parameters.

        /*

         [tck-id-payloads-template-instance-is-definition] A Template Instance MUST have is_definition set to false.
• [tck-id-payloads-template-instance-ref] A Template Instance MUST have template_ref set to the type of template definition it is.
• [tck-id-payloads-template-instance-members] A Template Instance MUST include only members that were included in the corresponding template definition.
• [tck-id-payloads-template-instance-members-birth] A Template Instance in a NBIRTH or DBIRTH message MUST include all members that were included in the corresponding Template Definition.
• [tck-id-payloads-template-instance-members-data] A Template Instance in a NDATA or DDATA message MAY include only a subset of the members that were included in the corresponding template definition.
• [tck-id-payloads-template-instance-parameters] A Template Instance MAY include parameter values for any parameters that were included in the corresponding Template Definition.
*/

        it('TestTemplate', function (done) {
            // Create a list of all templates (to make sure references are correct)
            validnbirth.metrics.forEach(x=> {
               

                if (x.type === "Template" && x.value.isDefinition === true) {
                    console.log(`validating ${x.name}`)
                    validateTemplate("", x);
                    console.log("Complate");
                }else if (x.type === "Template" && x.value.isDefinition === false) {
                    console.log(`instance ${x.name}`)
                }else {
                    console.log(`skipping ${x.name}`)
                }
            })

            done();
        });

		it('TestTemplateInflate', function (done) {
			let metrics = templateFlow[0].metrics;
			let templates = JSON.parse(templateFlow[1].templates);
		
			result =  templateToMetrics(templates, "", metrics);
			console.log(result);
			/*console.log(templateFlow);
			Object.keys(metrics).forEach(x=> {
				xx = metrics[x];
				console.log("working", xx);
				// templateToMetrics = function(templates, root, metrics) {
				
				console.log(x);
			})*/
			
		});
		/***
		 * 
		it('Should send template on NBIRTH', function (done) {

			var n1 = null;
			client = mqtt.connect(testBroker);

			client.on('connect', function () {
				client.subscribe("spBv1.0/My Devices/#", function (err) {
					if (!err) {
						helper.load(sparkplugNode, templateFlow, function () {
							n1 = helper.getNode("n1");
						});
					}
				});
			});
	
			client.on('message', function (topic, message) {
				topic.should.eql("spBv1.0/My Devices/NBIRTH/Node-Red");
				
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.metrics.should.deepEqual([
					  { name: 'MyTemplate', type: 'Template', value: {
						"version": "1.0.0",
						"isDefinition": true,
						"metrics": [
							{
								"name": "FirstTag",
								"type": "Int32",
								value : 0
							},
							{
								"name": "SecondTag",
								"type": "Int32",
								value : 0
							}
						],
						"parameters": [],
						"templateRef": ""
					  } },
					  { name: 'Node Control/Rebirth', type: 'Boolean', value: false },
					  { name: 'bdSeq', type: 'Int8', value: 0 }
					]);
				done();
			});
		});

		it('Should send template instance on DBIRTH (birthImmediately)', function (done) {

			var n1 = null;
			client = mqtt.connect(testBroker);

			templateFlow[0].birthImmediately = true;
			client.on('connect', function () {
				client.subscribe("spBv1.0/My Devices/#", function (err) {
					if (!err) {
						helper.load(sparkplugNode, templateFlow, function () {
							n1 = helper.getNode("n1");
						});
					}
				});
			});
	
			client.on('message', function (topic, message) {
				if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TheDevice") {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					done();
				}				
			});
		});


	
		it('Should send template instance on DBIRTH)', function (done) {

			var n1 = null;
			client = mqtt.connect(testBroker);

			templateFlow[0].birthImmediately = false;
			client.on('connect', function () {
				client.subscribe("spBv1.0/My Devices/#", function (err) {
					if (!err) {
						helper.load(sparkplugNode, templateFlow, function () {
							n1 = helper.getNode("n1");
							n1.receive({
								"payload" : {
									"metrics": [
										{
											"name": "b",
											"value": 11,
										},
										{
											"name": "a/FirstTag",
											"type": "Int32",
											value : 2
										},
										{
											"name": "a/SecondTag",
											"type": "Int32",
											value : 3
										}
									]}
								}
							);
						});
					}
				});
			});
	
			client.on('message', function (topic, message) {
				if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TheDevice") {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					console.log(payload);
	
						"1".should.eql("2");
					//done();
				}				
			});
		});	 */

})




