var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-plus.js");
var should = require("should");
var mqtt = require("mqtt");

var spPayload = require('sparkplug-payload').get("spBv1.0");
helper.init(require.resolve('node-red'));
let testBroker = process.env.TEST_BROKER || 'mqtt://localhost';
const _brokerUrl = new URL(testBroker.replace(/^mqtt(s?)/, 'http$1'));
let brokerHost = _brokerUrl.hostname;
let brokerPort = _brokerUrl.port || '1883';
let brokerUsername = _brokerUrl.username || '';
let brokerPassword = _brokerUrl.password || '';
var client = null;

// ---------------------------------------------------------------------------
// Shared test flow — one device with a template-typed metric "a" (MyTemplate)
// and a regular Int32 metric "b".
// ---------------------------------------------------------------------------
var templateFlow = [
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
        "name": "TheDevice",
        "broker": "b1",
        "birthImmediately": false,
    },
    {
        "id": "b1",
        "type": "mqtt-sparkplug-broker",
        "deviceGroup": "My Devices",
        "eonName": "Node-Red",
        "broker": brokerHost,
        "port": brokerPort,
        "clientid": "",
        "usetls": false,
        "protocolVersion": "4",
        "keepalive": "60",
        "cleansession": true,
        "enableStoreForward": false,
        "compressAlgorithm": "",
        "aliasMetrics": false,
        "templates": [
            JSON.stringify({
                "name": "MyTemplate",
                "type": "Template",
                "value": {
                    "version": "1.0.0",
                    "isDefinition": true,
                    "metrics": [
                        { "name": "FirstTag",  "type": "Int32" },
                        { "name": "SecondTag", "type": "Int32" }
                    ],
                    "parameters": []
                }
            })
        ],
        "primaryScada": "",
			"username": brokerUsername,
			"password": brokerPassword,
        "credentials": {}
    }
];

// ---------------------------------------------------------------------------
// Helper: decode a raw MQTT message buffer into a Sparkplug payload object
// ---------------------------------------------------------------------------
function decode(message) {
    return spPayload.decodePayload(Buffer.from(message));
}

// ---------------------------------------------------------------------------
// Helper: find a metric by name in a decoded payload
// ---------------------------------------------------------------------------
function findMetric(payload, name) {
    return payload.metrics.find(m => m.name === name);
}

describe('mqtt sparkplug device template support', function () {

    beforeEach(function (done) {
        helper.startServer(done);
    });

    afterEach(function (done) {
	var finished = false;
	
	var timeout = setTimeout(function() {
		if (!finished) {
			finished = true;
			done();
		}
	}, 500);
	
	helper.unload();
	helper.stopServer();
	
	if (client) {
		client.end(true);  // Force immediate disconnect
		client = null;
	}
	
	setTimeout(function() {
		if (!finished) {
			finished = true;
			done();
		}
	}, 100);
});

    // -----------------------------------------------------------------------
    // 1. Template Definitions MUST be sent in the NBIRTH message
    // -----------------------------------------------------------------------
    it('Should send template definition in NBIRTH', function (done) {
        this.timeout(5000);
        client = mqtt.connect(testBroker);

        client.on('connect', function () {
            client.subscribe("spBv1.0/My Devices/#", function (err) {
                if (err) return done(err);
                helper.load(sparkplugNode, templateFlow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {});
            });
        });

        client.on('message', function (topic, message) {
            if (topic !== "spBv1.0/My Devices/NBIRTH/Node-Red") return;

            var payload = decode(message);
            var tplMetric = findMetric(payload, "MyTemplate");

            should(tplMetric).be.ok();
            tplMetric.type.should.eql("Template");
            tplMetric.value.isDefinition.should.eql(true);
            should(tplMetric.value.templateRef === undefined || tplMetric.value.templateRef === "").be.ok();

            var firstTag  = tplMetric.value.metrics.find(m => m.name === "FirstTag");
            var secondTag = tplMetric.value.metrics.find(m => m.name === "SecondTag");
            should(firstTag).be.ok();
            should(secondTag).be.ok();
            firstTag.type.should.eql("Int32");
            secondTag.type.should.eql("Int32");

            done();
        });
    });

    // -----------------------------------------------------------------------
    // 2a. Template instance in DBIRTH — triggered via birthImmediately
    //     DBIRTH must contain ALL member metrics (with null values OK)
    // -----------------------------------------------------------------------
    it('Should send complete template instance in DBIRTH (birthImmediately)', function (done) {
        this.timeout(5000);

        // Deep clone flow and enable birthImmediately
        var flow = JSON.parse(JSON.stringify(templateFlow));
        flow[0].birthImmediately = true;

        client = mqtt.connect(testBroker);

        client.on('connect', function () {
            client.subscribe("spBv1.0/My Devices/#", function (err) {
                if (err) return done(err);
                helper.load(sparkplugNode, flow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {});
            });
        });

        client.on('message', function (topic, message) {
            if (topic !== "spBv1.0/My Devices/DBIRTH/Node-Red/TheDevice") return;

            var payload = decode(message);
            var aMetric = findMetric(payload, "a");

            should(aMetric).be.ok();
            aMetric.type.should.eql("Template");
            aMetric.value.isDefinition.should.eql(false);
            aMetric.value.templateRef.should.eql("MyTemplate");

            // DBIRTH MUST include ALL members
            var firstTag  = aMetric.value.metrics.find(m => m.name === "FirstTag");
            var secondTag = aMetric.value.metrics.find(m => m.name === "SecondTag");
            should(firstTag).be.ok();
            should(secondTag).be.ok();

            done();
        });
    });

    // -----------------------------------------------------------------------
    // 2b. Template instance in DBIRTH — triggered via input message using
    //     flat-path notation (a/FirstTag, a/SecondTag)
    // -----------------------------------------------------------------------
    it('Should send complete template instance in DBIRTH (input message, flat-path)', function (done) {
        this.timeout(5000);

        var flow = JSON.parse(JSON.stringify(templateFlow));
        flow[0].birthImmediately = false;

        client = mqtt.connect(testBroker);

        client.on('connect', function () {
            client.subscribe("spBv1.0/My Devices/#", function (err) {
                if (err) return done(err);
                helper.load(sparkplugNode, flow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
                    var n1 = helper.getNode("n1");
                    n1.receive({
                        payload: {
                            metrics: [
                                { name: "b",            value: 11  },
                                { name: "a/FirstTag",   value: 2   },
                                { name: "a/SecondTag",  value: 3   }
                            ]
                        }
                    });
                });
            });
        });

        client.on('message', function (topic, message) {
            if (topic !== "spBv1.0/My Devices/DBIRTH/Node-Red/TheDevice") return;

            var payload = decode(message);
            var aMetric = findMetric(payload, "a");
            var bMetric = findMetric(payload, "b");

            should(aMetric).be.ok();
            aMetric.type.should.eql("Template");
            aMetric.value.isDefinition.should.eql(false);
            aMetric.value.templateRef.should.eql("MyTemplate");

            // DBIRTH MUST include ALL members
            var firstTag  = aMetric.value.metrics.find(m => m.name === "FirstTag");
            var secondTag = aMetric.value.metrics.find(m => m.name === "SecondTag");
            should(firstTag).be.ok();
            should(secondTag).be.ok();
            firstTag.type.should.eql("Int32");
            secondTag.type.should.eql("Int32");

            // Values should be populated from the input
            should(firstTag.value).not.be.null();
            should(secondTag.value).not.be.null();

            should(bMetric).be.ok();

            done();
        });
    });

    // -----------------------------------------------------------------------
    // 3. Partial template instance in DDATA — only changed sub-metrics sent
    // -----------------------------------------------------------------------
    it('Should send partial template instance in DDATA', function (done) {
        this.timeout(5000);

        var flow = JSON.parse(JSON.stringify(templateFlow));
        flow[0].birthImmediately = false;

        client = mqtt.connect(testBroker);
        var ddataReceived = false;

        client.on('connect', function () {
            client.subscribe("spBv1.0/My Devices/#", function (err) {
                if (err) return done(err);
                helper.load(sparkplugNode, flow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
                    var n1 = helper.getNode("n1");

                    // First message: trigger DBIRTH
                    n1.receive({
                        payload: {
                            metrics: [
                                { name: "b",           value: 1 },
                                { name: "a/FirstTag",  value: 10 },
                                { name: "a/SecondTag", value: 20 }
                            ]
                        }
                    });
                });
            });
        });

        client.on('message', function (topic, message) {
            if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TheDevice" && !ddataReceived) {
                // After DBIRTH, send a second message with only one sub-metric changed
                var n1 = helper.getNode("n1");
                n1.receive({
                    payload: {
                        metrics: [
                            { name: "a/FirstTag", value: 99 }
                        ]
                    }
                });
            }

            if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TheDevice" && !ddataReceived) {
                ddataReceived = true;
                var payload = decode(message);
                var aMetric = findMetric(payload, "a");

                should(aMetric).be.ok();
                aMetric.type.should.eql("Template");
                aMetric.value.isDefinition.should.eql(false);
                aMetric.value.templateRef.should.eql("MyTemplate");

                // DDATA MAY include only a subset — only FirstTag was sent
                var firstTag  = aMetric.value.metrics.find(m => m.name === "FirstTag");
                var secondTag = aMetric.value.metrics.find(m => m.name === "SecondTag");

                should(firstTag).be.ok();
                firstTag.value.should.eql(99);

                // SecondTag was NOT in the DDATA input, so it MUST NOT appear in the partial instance
                should(secondTag).be.undefined();

                done();
            }
        });
    });

    // -----------------------------------------------------------------------
    // 4. Flat-path sub-metrics must be recognised when the definition key
    //    itself contains slashes (e.g. "site/area/mytemplate").
    //    Bug: the resolution logic splits on the first "/" and looks for
    //    "site" in this.metrics, which does not exist, so sub-metrics are
    //    silently dropped and no DBIRTH is ever sent.
    // -----------------------------------------------------------------------
    it('should recognise flat-path sub-metrics when definition key contains slashes', function (done) {
        this.timeout(5000);

        var flow = JSON.parse(JSON.stringify(templateFlow));
        flow[0].metrics = {};           // no static metrics — definition arrives dynamically
        flow[0].birthImmediately = false;

        // Extend MyTemplate to also include a member whose own name contains a slash
        flow[1].templates = [
            JSON.stringify({
                "name": "MyTemplate",
                "type": "Template",
                "value": {
                    "version": "1.0.0",
                    "isDefinition": true,
                    "metrics": [
                        { "name": "FirstTag",      "type": "Int32"   },
                        { "name": "SecondTag",     "type": "Int32"   },
                        { "name": "status/running","type": "Boolean" }
                    ],
                    "parameters": []
                }
            })
        ];

        client = mqtt.connect(testBroker);

        client.on('connect', function () {
            client.subscribe("spBv1.0/My Devices/#", function (err) {
                if (err) return done(err);

                helper.load(sparkplugNode, flow, { b1: { user: brokerUsername, password: brokerPassword } }, function () {
                    var n1 = helper.getNode("n1");

                    // Fail immediately if any metric is not recognised
                    n1.on('input', () => {
                        if (n1.warn.called) {
                            done(new Error("device-unknown-metric fired: slashed definition key was not resolved correctly"));
                        }
                    });

                    n1.receive({
                        definition: {
                            "site/area/mytemplate": { dataType: "MyTemplate" }
                        },
                        payload: {
                            metrics: [
                                { name: "site/area/mytemplate/FirstTag",       value: 1    },
                                { name: "site/area/mytemplate/SecondTag",      value: 2    },
                                { name: "site/area/mytemplate/status/running", value: true }
                            ]
                        }
                    });
                });
            });
        });

        client.on('message', function (topic, message) {
            if (topic !== "spBv1.0/My Devices/DBIRTH/Node-Red/TheDevice") return;

            var payload = decode(message);
            var tplMetric = findMetric(payload, "site/area/mytemplate");

            should(tplMetric).be.ok();
            tplMetric.type.should.eql("Template");
            tplMetric.value.isDefinition.should.eql(false);
            tplMetric.value.templateRef.should.eql("MyTemplate");

            var firstTag      = tplMetric.value.metrics.find(m => m.name === "FirstTag");
            var secondTag     = tplMetric.value.metrics.find(m => m.name === "SecondTag");
            var statusRunning = tplMetric.value.metrics.find(m => m.name === "status/running");
            should(firstTag).be.ok();
            should(secondTag).be.ok();
            should(statusRunning).be.ok();
            firstTag.value.should.eql(1);
            secondTag.value.should.eql(2);
            statusRunning.value.should.eql(true);

            done();
        });
    });

    // -----------------------------------------------------------------------
    // 5. Template definition containing a DataSet metric must encode in NBIRTH
    //    without crashing.  Bug: when no value is provided for a DataSet metric
    //    in the template definition, the sparkplug-payload encoder crashes with
    //    "Cannot read properties of null (reading 'numOfColumns')".
    // -----------------------------------------------------------------------
    it('Should encode NBIRTH when template definition contains a DataSet metric', function (done) {
        this.timeout(8000);

        var flow = JSON.parse(JSON.stringify(templateFlow));
        flow[1].templates = [
            JSON.stringify({
                "name": "MyTemplate",
                "type": "Template",
                "value": {
                    "version": "1.0.0",
                    "isDefinition": true,
                    "metrics": [
                        { "name": "FirstTag",  "type": "Int32" },
                        { "name": "MyDataSet", "type": "DataSet" }
                    ],
                    "parameters": []
                }
            })
        ];

        var nbirthReceived = false;
        client = mqtt.connect(testBroker);

        var failTimer = setTimeout(function () {
            if (!nbirthReceived) {
                done(new Error("NBIRTH was never sent — DataSet metric in template definition caused encoding failure"));
            }
        }, 6000);

        client.on('connect', function () {
            client.subscribe("spBv1.0/My Devices/#", function (err) {
                if (err) return done(err);
                helper.load(sparkplugNode, flow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {});
            });
        });

        client.on('message', function (topic, message) {
            if (topic !== "spBv1.0/My Devices/NBIRTH/Node-Red") return;
            nbirthReceived = true;
            clearTimeout(failTimer);

            var payload = decode(message);
            var tplMetric = findMetric(payload, "MyTemplate");

            should(tplMetric).be.ok();
            tplMetric.type.should.eql("Template");
            tplMetric.value.isDefinition.should.eql(true);

            var dataSetMetric = tplMetric.value.metrics.find(m => m.name === "MyDataSet");
            should(dataSetMetric).be.ok();
            dataSetMetric.type.should.eql("DataSet");

            done();
        });
    });

    // -----------------------------------------------------------------------
    // 6. After partial updates and a REBIRTH, DBIRTH must contain all
    //    previously-seen sub-metric values — not just the last partial update.
    //    (Regression test for latestMetrics cache overwrite bug.)
    // -----------------------------------------------------------------------
    it('Should preserve all sub-metric values in DBIRTH after partial updates and REBIRTH', function (done) {
        this.timeout(8000);

        var flow = JSON.parse(JSON.stringify(templateFlow));
        flow[0].birthImmediately = false;

        client = mqtt.connect(testBroker);
        var dbirth1Received = false;

        client.on('connect', function () {
            client.subscribe("spBv1.0/My Devices/#", function (err) {
                if (err) return done(err);
                helper.load(sparkplugNode, flow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
                    var n1 = helper.getNode("n1");

                    // First message: populate both sub-metrics → triggers DBIRTH
                    n1.receive({
                        payload: {
                            metrics: [
                                { name: "b",           value: 1  },
                                { name: "a/FirstTag",  value: 10 },
                                { name: "a/SecondTag", value: 20 }
                            ]
                        }
                    });
                });
            });
        });

        client.on('message', function (topic, message) {
            if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TheDevice" && !dbirth1Received) {
                dbirth1Received = true;
                var n1 = helper.getNode("n1");

                // Second message: update ONLY FirstTag (partial update — SecondTag untouched)
                n1.receive({
                    payload: {
                        metrics: [
                            { name: "a/FirstTag", value: 99 }
                        ]
                    }
                });

                // Third message: trigger REBIRTH after the partial update is processed
                setImmediate(function () {
                    n1.receive({
                        command: {
                            device: { rebirth: true }
                        }
                    });
                });
                return;
            }

            // Second DBIRTH (after REBIRTH) — this is what we are testing
            if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TheDevice" && dbirth1Received) {
                var payload = decode(message);
                var aMetric = findMetric(payload, "a");

                should(aMetric).be.ok();
                var firstTag  = aMetric.value.metrics.find(m => m.name === "FirstTag");
                var secondTag = aMetric.value.metrics.find(m => m.name === "SecondTag");

                should(firstTag).be.ok();
                should(secondTag).be.ok();

                // FirstTag was updated to 99 in the partial DDATA
                firstTag.value.should.eql(99);
                // SecondTag was NOT in the last partial update — must still carry value 20
                should(secondTag.value).not.be.null();
                secondTag.value.should.eql(20);

                done();
            }
        });
    });


    // -----------------------------------------------------------------------
    // 8. Nested Template instances (a Template-typed member inside a Template)
    //    must keep their sub-metric values across a partial update and a
    //    REBIRTH. The cached-instance flatten used by trySendBirth and by the
    //    DDATA merge must recurse into nested instances, otherwise every
    //    nested member is rebuilt as null.
    // -----------------------------------------------------------------------
    it('Should preserve nested template sub-metric values in DBIRTH after partial updates and REBIRTH', function (done) {
        this.timeout(8000);

        var flow = JSON.parse(JSON.stringify(templateFlow));
        flow[0].birthImmediately = false;

        // Item template instantiated once per array slot.
        var itemTemplate = {
            "name": "MyItemTemplate",
            "type": "Template",
            "value": {
                "version": "1.0.0",
                "isDefinition": true,
                "metrics": [
                    { "name": "id",      "type": "Int32"  },
                    { "name": "message", "type": "String" }
                ],
                "parameters": []
            }
        };
        flow[1].templates.push(JSON.stringify(itemTemplate));

        // Give MyTemplate an "items" array: a scalar size plus one nested slot.
        var myTemplate = JSON.parse(flow[1].templates[0]);
        myTemplate.value.metrics.push({ "name": "items/item_size", "type": "Int32" });
        myTemplate.value.metrics.push({
            "name": "items/0_item",
            "type": "Template",
            "templateRef": "MyItemTemplate"
        });
        flow[1].templates[0] = JSON.stringify(myTemplate);

        client = mqtt.connect(testBroker);
        var dbirth1Received = false;

        client.on('connect', function () {
            client.subscribe("spBv1.0/My Devices/#", function (err) {
                if (err) return done(err);
                helper.load(sparkplugNode, flow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
                    var n1 = helper.getNode("n1");

                    // Populate every member, including both nested ones -> DBIRTH
                    n1.receive({
                        payload: {
                            metrics: [
                                { name: "b",                     value: 1    },
                                { name: "a/FirstTag",            value: 10   },
                                { name: "a/SecondTag",           value: 20   },
                                { name: "a/items/item_size",     value: 1    },
                                { name: "a/items/0_item/id",     value: 7    },
                                { name: "a/items/0_item/message", value: "hi" }
                            ]
                        }
                    });
                });
            });
        });

        // Assertions run inside the MQTT handler, where a throw would otherwise be
        // swallowed and surface only as a mocha timeout. Route it to done() instead.
        client.on('message', function (topic, message) {
          try {
            if (topic !== "spBv1.0/My Devices/DBIRTH/Node-Red/TheDevice") {
                return;
            }

            if (!dbirth1Received) {
                dbirth1Received = true;

                // Sanity: the first DBIRTH must already carry the nested values
                var payload = decode(message);
                var aMetric = findMetric(payload, "a");
                should(aMetric).be.ok();
                var item = aMetric.value.metrics.find(m => m.name === "items/0_item");
                should(item).be.ok();
                item.value.metrics.find(m => m.name === "id").value.should.eql(7);
                item.value.metrics.find(m => m.name === "message").value.should.eql("hi");

                var n1 = helper.getNode("n1");

                // Partial update: only the nested "id" changes. "message" is untouched
                // and must survive in the cached instance.
                n1.receive({
                    payload: {
                        metrics: [
                            { name: "a/items/0_item/id", value: 8 }
                        ]
                    }
                });

                setImmediate(function () {
                    n1.receive({
                        command: {
                            device: { rebirth: true }
                        }
                    });
                });
                return;
            }

            // Second DBIRTH (after REBIRTH) — this is what we are testing
            var payload2 = decode(message);
            var a2 = findMetric(payload2, "a");
            should(a2).be.ok();

            // Direct (non-nested) members still work today
            a2.value.metrics.find(m => m.name === "FirstTag").value.should.eql(10);
            a2.value.metrics.find(m => m.name === "items/item_size").value.should.eql(1);

            var item2 = a2.value.metrics.find(m => m.name === "items/0_item");
            should(item2).be.ok();
            should(item2.value).be.ok();
            should(item2.value.metrics).be.an.Array();

            var idM  = item2.value.metrics.find(m => m.name === "id");
            var msgM = item2.value.metrics.find(m => m.name === "message");
            should(idM).be.ok();
            should(msgM).be.ok();

            // "id" was updated to 8 in the partial DDATA
            should(idM.value).not.be.null();
            idM.value.should.eql(8);
            // "message" was NOT in the partial update — must still carry "hi"
            should(msgM.value).not.be.null();
            msgM.value.should.eql("hi");

            done();
          } catch (e) {
            done(e);
          }
        });
    });



    // -----------------------------------------------------------------------
    // 9. The config UI stores a nested template member as { type: "<TemplateName>" }
    //    rather than { type: "Template", templateRef: "..." }. getTemplates() must
    //    normalise that shape, otherwise the encoder drops the member's datatype
    //    and buildTemplateInstance treats the member as a scalar.
    // -----------------------------------------------------------------------
    it('Should accept a nested template member typed with the definition name (config UI shape)', function (done) {
        this.timeout(8000);

        var flow = JSON.parse(JSON.stringify(templateFlow));
        flow[0].birthImmediately = false;

        flow[1].templates.push(JSON.stringify({
            "name": "MyItemTemplate",
            "type": "Template",
            "value": {
                "version": "1.0.0",
                "isDefinition": true,
                "metrics": [
                    { "name": "id",      "type": "Int32"  },
                    { "name": "message", "type": "String" }
                ],
                "parameters": []
            }
        }));

        var myTemplate = JSON.parse(flow[1].templates[0]);
        // The UI shape: the definition name IS the type, with no templateRef.
        myTemplate.value.metrics.push({ "name": "items/0_item", "type": "MyItemTemplate" });
        flow[1].templates[0] = JSON.stringify(myTemplate);

        client = mqtt.connect(testBroker);
        var nbirthChecked = false;

        client.on('connect', function () {
            client.subscribe("spBv1.0/My Devices/#", function (err) {
                if (err) return done(err);
                helper.load(sparkplugNode, flow, {b1: {user: brokerUsername, password: brokerPassword}}, function () {
                    var n1 = helper.getNode("n1");
                    n1.receive({
                        payload: {
                            metrics: [
                                { name: "b",                      value: 1    },
                                { name: "a/FirstTag",             value: 10   },
                                { name: "a/SecondTag",            value: 20   },
                                { name: "a/items/0_item/id",      value: 7    },
                                { name: "a/items/0_item/message", value: "hi" }
                            ]
                        }
                    });
                });
            });
        });

        client.on('message', function (topic, message) {
          try {
            // The NBIRTH must describe the member as a Template, not drop its datatype
            if (topic === "spBv1.0/My Devices/NBIRTH/Node-Red" && !nbirthChecked) {
                nbirthChecked = true;
                var def = findMetric(decode(message), "MyTemplate");
                should(def).be.ok();
                var member = def.value.metrics.find(m => m.name === "items/0_item");
                should(member).be.ok();
                member.type.should.eql("Template");
                member.value.templateRef.should.eql("MyItemTemplate");
                return;
            }

            if (topic !== "spBv1.0/My Devices/DBIRTH/Node-Red/TheDevice") {
                return;
            }

            // ...and the DBIRTH must carry a real nested instance with the values
            var aMetric = findMetric(decode(message), "a");
            should(aMetric).be.ok();
            var item = aMetric.value.metrics.find(m => m.name === "items/0_item");
            should(item).be.ok();
            item.type.should.eql("Template");
            item.value.templateRef.should.eql("MyItemTemplate");
            item.value.metrics.find(m => m.name === "id").value.should.eql(7);
            item.value.metrics.find(m => m.name === "message").value.should.eql("hi");

            done();
          } catch (e) {
            done(e);
          }
        });
    });


});

