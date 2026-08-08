/**
 * Copyright JS Foundation and other contributors, http://js.foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

const { encodePayload } = require("sparkplug-payload/lib/sparkplugbpayload");

module.exports = function(RED) {
    "use strict";
    var mqtt = require("mqtt");
    var spPayload = require('sparkplug-payload').get("spBv1.0");
    var HttpsProxyAgent = require('https-proxy-agent');
    var url = require('url');
    var long = require("long");
    var pako = require('pako');
    var compressed = "SPBV1.0_COMPRESSED";
    // [tck-id-operational-behavior-data-commands-rebirth-name-aliases] When aliases are being used by an Edge Node an NBIRTH message MUST NOT include an alias for the Node Control/Rebirth metric.
    // We do not add alias to bdSeq as there has been reported issues with Ignition MQTT Engine. 
    var aliasIgnoreMetrics = ["Node Control/Rebirth", "bdSeq"];

    /**
     * Try to decompress the payload if if compressed uuid is set on the payload
     * @param {object} payload 
     * @returns {object} payload
     */
    function maybeDecompressPayload(payload) {
        return payload.uuid === compressed ? sparkplugDecode(decompressPayload(payload)) : payload;
    };

    /**
     * Function will compress the payload and return the compressed payload as a new object.
     * @param {object} payload The payload that should be compressed
     * @param {object} options options for the compressPayload (algorithm)
     * @throws Will throw an error if options['algorithm'] is not [DEFLATE|GZIP]
     * @returns compressed payload (payload still needs to be protobuf encoded)
     */
    function compressPayload(payload, options) {
        var algorithm = options && options['algorithm'] ? options['algorithm'].toUpperCase() : "DEFLATE";
        var resultPayload = {
            "uuid" : compressed,
            body : null,
            metrics : [ {
                "name" : "algorithm", 
                "value" : algorithm.toUpperCase(), 
                "type" : "string"
            } ]
        };
     
        switch(algorithm) {
            case "DEFLATE":
                resultPayload.body = pako.deflate(encodePayload(payload));
                break;
            case "GZIP":
                resultPayload.body = pako.gzip(encodePayload(payload));
                break;
            default:
                throw new Error("Unknown or unsupported compression algorithm " + algorithm);
        }
        return resultPayload;
    };

    /**
     * 
     * @param {object} payload the compressed payload (payload should NOT be protobuf encoded)
     * @throws Will throw an error unable to decompress
     * @returns {object} the decoded payload
     * 
     */
    function decompressPayload(payload) {
         // Inflate will auto detect compression algorithm via the header.
        return pako.inflate(payload.body);
    };

    /**
     * Sparkplug Encode Payload
     * @param {object} payload object to encode 
     * @returns a sparkplug B encoded Buffer
     */
    function sparkplugEncode(payload) {
        // return JSON.stringify(payload); // for debugging

        if (typeof payload.timestamp  === "string") {
                        
            let ts = Date.parse(payload.timestamp);
            if (!ts) {
                throw RED._("mqtt-sparkplug-plus.errors.unable-to-encode-message", { type : "", error :  "Unable to encode message, unable to encode metric timestamp for payload"});
            }
            payload.timestamp = ts;
        }
        // Sparkplug dates are always send a Unix Time
        if (payload.timestamp instanceof Date && !isNaN(payload.timestamp)) {
            payload.timestamp = payload.timestamp.getTime();
        }

        // Verify that all metrics have a type (if people copy message from e.g. MQTT.FX, then the variable is not called type)
        if (payload.hasOwnProperty("metrics")) {
            if (!Array.isArray(payload.metrics)) {
                throw RED._("mqtt-sparkplug-plus.errors.metrics-not-array");
            } else {
                payload.metrics.forEach(met => {
                    if (!met.hasOwnProperty("type")) {
                        throw RED._("mqtt-sparkplug-plus.errors.unable-to-encode-message", { type : "", error :  "Unable to encode message, all metrics must have a 'type' Attribute" });
                    }
                    // Sparkplug dates are always send a Unix Time
                    if (met.timestamp instanceof Date && !isNaN(met.timestamp)) {
                        met.timestamp = met.timestamp.getTime();
                    }

                    if (typeof met.timestamp  === "string") {
                        
                        let ts = Date.parse(met.timestamp);
                        if (!ts) {
                            throw RED._("mqtt-sparkplug-plus.errors.unable-to-encode-message", { type : "", error :  "Unable to encode message, unable to encode metric timestamp for metric " + met.name });
                        }
                        met.timestamp = ts;
                    }

                });
                
            }
        }
        return spPayload.encodePayload(payload);
    }
    
    /**
     * 
     * @param {Number[]} payload Sparkplug B encoded Payload
     * @returns {Object} decoded JSON object
     */
    function sparkplugDecode(payload_) {
        let buffer = Buffer.from(payload_);
        let payload = spPayload.decodePayload(buffer);
        if (long.isLong(payload.seq)) {
            payload.seq = payload.seq.toNumber();
        }
        if (long.isLong(payload.timestamp)) {
            payload.timestamp = new Date(payload.timestamp.toNumber());
        }
        if (payload.hasOwnProperty("metrics") && Array.isArray(payload.metrics)) {
            payload.metrics.forEach(m=> {
                if (long.isLong(m.timestamp)) {
                    m.timestamp = new Date(m.timestamp.toNumber());
                }
            })
        }
        return payload;
    }

    function matchTopic(ts,t) {
        if (ts == "#") {
            return true;
        }
        var re = new RegExp("^"+ts.replace(/([\[\]\?\(\)\\\\$\^\*\.|])/g,"\\$1").replace(/\+/g,"[^/]+").replace(/\/#$/,"(\/.*)?")+"$");
        return re.test(t);
    }

    function MQTTSparkplugDeviceNode(n) {
        RED.nodes.createNode(this,n);
        this.dataTypes = ["Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float", "Double", "Boolean", "DateTime", "UUID", "DataSet", "Bytes", "String", "Unknown", "Template"]

        this.bufferDevice = n.bufferDevice
        this.broker = n.broker;
        this.name = n.name||"Sparkplug Device";
        this.latestMetrics = {};
        this.metrics = n.metrics || {};
        this.birthMessageSend = false;
        this.birthImmediately = n.birthImmediately || false;
        this.dcmdTopic = ""; // Used to store the topic for DCMD
        this.inflatedMetrics = {};
        this.shouldBuffer = true; // hardcoded / Devices always buffers

        /**
         * Build a Sparkplug B compliant Template Instance value object.
         *
         * @param {string}  templateName  Name of the Template Definition to instantiate
         * @param {object}  flatValues    Flat map of relative metric paths to values,
         *                               e.g. { "speed": 42, "nested/rpm": 5 }
         * @param {Array}   templates     Full list of Template Definition objects from the broker
         * @param {number}  timestamp     Unix ms timestamp to attach to each metric
         * @param {boolean} forBirth      true → include ALL definition members (nulls for missing);
         *                               false → include only members present in flatValues (partial DDATA)
         * @returns {object|null} Template instance value, or null if definition not found
         */
        this.buildTemplateInstance = function buildTemplateInstance(templateName, flatValues, templates, timestamp, forBirth) {
            var def = templates.find(function(t) { return t.name === templateName; });
            if (!def || !def.value) {
                node.error("Template definition '" + templateName + "' not found in broker templates");
                return null;
            }

            var defMetrics = def.value.metrics || [];
            var instanceMetrics = [];

            defMetrics.forEach(function(defMetric) {
                var mName = defMetric.name;
                var mType = defMetric.type;

                if (mType === "Template") {
                    // Nested template — collect sub-paths and recurse
                    var nestedTemplateRef = defMetric.templateRef || mName;
                    // filter flatValues to entries starting with "mName/"
                    var prefix = mName + "/";
                    var nestedFlat = {};
                    Object.keys(flatValues).forEach(function(k) {
                        if (k === mName || k.indexOf(prefix) === 0) {
                            var subKey = k === mName ? "" : k.slice(prefix.length);
                            nestedFlat[subKey] = flatValues[k];
                        }
                    });

                    // Only include this nested template in DDATA if at least one sub-value is present
                    if (forBirth || Object.keys(nestedFlat).length > 0) {
                        var nestedInstance = buildTemplateInstance(nestedTemplateRef, nestedFlat, templates, timestamp, forBirth);
                        if (nestedInstance) {
                            instanceMetrics.push({
                                name: mName,
                                type: "Template",
                                value: nestedInstance,
                                timestamp: timestamp
                            });
                        }
                    }
                } else {
                    var hasValue = flatValues.hasOwnProperty(mName);
                    if (forBirth || hasValue) {
                        var mValue = hasValue ? flatValues[mName] : null;
                        if (mValue == null && mType === "DataSet") {
                            mValue = { numOfColumns: 0, columns: [], types: [], rows: [] };
                        }
                        instanceMetrics.push({
                            name: mName,
                            type: mType,
                            value: mValue,
                            timestamp: timestamp
                        });
                    }
                }
            });

            // Copy parameters from definition (instances MAY include parameter values)
            var instanceParams = (def.value.parameters || []).map(function(p) {
                return Object.assign({}, p);
            });

            return {
                templateRef: templateName,
                isDefinition: false,
                version: def.value.version || "",
                metrics: instanceMetrics,
                parameters: instanceParams
            };
        };

        if (typeof this.birthImmediately === 'undefined') {
            this.birthImmediately = false;
        }
        if (typeof this.bufferDevice === 'undefined') {
            this.bufferDevice = false;
        }
        var node = this;

        this.emptyBuffer = async function() {
            let x = this.brokerConn.getItemFromQueue(this.name);
            while(x) { 
                x.forEach(s=> s.isHistorical = true);
                let dMsg = this.brokerConn.createMsg(this.name, "DDATA", x, f => {});
                if (dMsg) {
                    await new Promise((resolve) => this.brokerConn.publish(dMsg, !this.shouldBuffer, resolve)); 
                }
                x = this.brokerConn.getItemFromQueue(this.name);
            }            
        }
        /**
         * 
         * @returns the DCMD (Device command) topic for this Devive 
         */
        this.getDCMDTopic = () => `spBv1.0/${this.brokerConn.deviceGroup}/DCMD/${this.brokerConn.eonName}/${this.name}`;
        this.subscribe_dcmd = function() {
             // Handle DCMD Messages
             let options = { qos: 0 };
    

             node.dcmdTopic = this.getDCMDTopic();
             node.brokerConn.subscribe(this.dcmdTopic,options,function(topic_,payload_,packet) {
                 try {
                    // This should never happen.
                    // Extra check to make sure that we are not handing command from old topic name. 
                    // This was a issue before V2.1.1.. Hopefully this has been fully resolved now, but keep this extra check...
                    let expectedTopic = node.getDCMDTopic()
                    if (topic_ != expectedTopic) {
                        node.error(`Topic ${payload_} does not match expected topic ${expectedTopic}.. Pleae create a Gibhub issue with description on how to reproduce`)
                        return;
                    }
                     var msg = {
                         topic : topic_,
                         payload : maybeDecompressPayload(sparkplugDecode(payload_))
                     };
 
                     if (node.brokerConn.aliasMetrics && msg.payload.hasOwnProperty("metrics")) {
                         let lookup = Object.entries(node.brokerConn.metricsAliasMap).reduce((acc, [key, value]) => (acc[value] = key, acc), {})
                         msg.payload.metrics.forEach(m=> {
                             if (long.isLong(m.alias)) {
                                 m.alias = m.alias;
                             }
                             if (lookup.hasOwnProperty(m.alias)) {
                                 m.name = lookup[m.alias]
                                 delete m.alias;
                             }
                         })                       
                     }
                     node.send(msg);
                 } catch (e) {
                     node.error(RED._("mqtt-sparkplug-plus.errors.unable-to-decode-message", {type : "DCMD", error: e.toString()}));
                 }
             }, node.id);
        }

        this.unsubscribe_dcmd = function() {
            node.brokerConn.unsubscribe(node.dcmdTopic,node.id, {});
        }

        /**
         * try to send Sparkplug DBirth Messages
         * @param {function} done Node-Red Done Function 
         */
        this.trySendBirth = function(done) {

            var isOnline = (node.brokerConn.enableStoreForward && node.brokerConn.primaryScadaStatus === "ONLINE" && node.brokerConn.connected) ||
            (!node.brokerConn.enableStoreForward && node.brokerConn.connected);

            if (!isOnline) {
                return;
            }

            // A DBIRTH may never precede the NBIRTH of its Edge Node. With a Primary
            // Host configured, NBIRTH is withheld until that host reports ONLINE, and
            // isOnline above does not account for that - it only tracks buffering. The
            // device would otherwise announce itself under an edge node the host has
            // never seen born.
            if (!node.brokerConn.nbirthSent) {
                return;
            }
            let readyToSend = Object.keys(this.metrics).every(m => this.latestMetrics.hasOwnProperty(m));

            // Don't send birth if no metrics. we can assume that a dynamic defintion will be send if on metrics are defined.
            let hasMetrics = Object.keys(this.metrics).length > 0;
            if (readyToSend && hasMetrics) {
                let birthMetrics = [];
                let templates = [];
                try { templates = node.brokerConn.getTemplates(); } catch(e) {}
                let ts = Date.now();

                for (const [key, value] of Object.entries(this.metrics)) {
                    const lv = Object.assign({}, this.latestMetrics[key]);
                    lv.name = key;  // Ensure name is always set for DBIRTH
                    let dataType = value.dataType;
                    let isNamedTemplate = (dataType !== "Template" && !node.dataTypes.includes(dataType));

                    if (isNamedTemplate) {
                        // Rebuild a FULL template instance (DBIRTH requires all members)
                        // Gather existing sub-values from the cached partial instance (if any)
                        let existingFlatVals = {};
                        if (lv.value && lv.value.metrics) {
                            lv.value.metrics.forEach(function(im) {
                                existingFlatVals[im.name] = im.value;
                            });
                        }
                        let fullInstance = node.buildTemplateInstance(dataType, existingFlatVals, templates, ts, true);
                        if (fullInstance) {
                            lv.value = fullInstance;
                            lv.type = "Template";
                            lv.timestamp = ts;
                        }
                    }

                    if (value.hasOwnProperty("properties")) {
                        lv.properties = value.properties;
                    }
                    birthMetrics.push(lv);
                }
                let bMsg = node.brokerConn.createMsg(this.name, "DBIRTH", birthMetrics, f => {});
                if(bMsg) {
                    this.brokerConn.publish(bMsg, true, done);  // send the message
                    this.birthMessageSend = true;
                    this.emptyBuffer();
                }
            }
        }

        /**
         * Send DDeath message
         * @param {function} done Node-Red Done Function 
         */
        this.sendDDeath = function(done) {
            let dMsg = node.brokerConn.createMsg(this.name, "DDEATH", [], x=>{});
            if(dMsg) {
                this.brokerConn.publish(dMsg, !this.shouldBuffer, done);  // send the message 
                this.birthMessageSend = false;
            }
        }

        this.brokerConn = RED.nodes.getNode(this.broker);
     
        if (this.brokerConn) {
            this.on("input",function(msg,send,done) {
                if (msg.hasOwnProperty("command")) {

                    // Lets always refresh subscriptions for now.
                    let resubscribeRequired = true;
                    let u = [];
                    if (resubscribeRequired) {
                        // Unsubscribe for all topic from Devices
                        for (const [key, n] of Object.entries(this.brokerConn.users)) {
                            if (typeof n.unsubscribe_dcmd === 'function') {
                                u.push(n);
                                n.unsubscribe_dcmd();
                            }
                        }
                    }

                    if (msg.command.hasOwnProperty("device")) {
                        if (msg.command.device.set_name) {
                            if (this.birthMessageSend) {
                                this.sendDDeath();
                            }
                            node.name = msg.command.device.set_name;
                            this.trySendBirth();
                        }
                        if (msg.command.device.rebirth) {
                            
                            // I Assume no-one is using SparkplugB V2.2 anymore,
                            //if (this.birthMessageSend) {
                            //    this.sendDDeath();    
                            //}
                            this.trySendBirth();
                        }
                        if (msg.command.device.death) {
                            if (this.birthMessageSend) {
                                this.sendDDeath();
                            }
                        }

                    };
                    if (msg.command.hasOwnProperty("node")) {
                        let rebirthRequired = (msg.command.node.set_name || msg.command.node.set_group) && this.brokerConn.connected;
   
                        if (rebirthRequired) {
                            let msg = this.brokerConn.getDeathPayload();
                            this.brokerConn.publish(msg, false);
                        }
                        if (msg.command.node.set_name) {
                            this.brokerConn.eonName = msg.command.node.set_name;
                        }
                        if (msg.command.node.set_group) {
                            this.brokerConn.deviceGroup = msg.command.node.set_group;
                        }
                        if (msg.command.node.set_server) {
                            // FIXME: Should we Disconnect here?
                            this.brokerConn.broker = msg.command.node.set_server;
                        }
                        if (rebirthRequired) {
                            // Forced: a set_name/set_group changes the node's Sparkplug
                            // identity, and an NDEATH for the old one has just gone out
                            // above. The NBIRTH already sent was for a different edge
                            // node, so the nbirthSent guard must not suppress this one.
                            this.brokerConn.sendBirth(true);
                        }

                        if (msg.command.node.connect) {
                            if (!this.brokerConn.connected) {
                                this.brokerConn.manualEoNBirth = false;
                                this.brokerConn.connect();
                            }
                        }
                    }
                    // Resubscribe to MQTT topics (DCMD topic might have changed)
                    u.forEach(n => {
                        n.subscribe_dcmd();
                    });
                }

                let validPayload = msg.hasOwnProperty("payload") && typeof msg.payload === 'object' && msg.payload !== null && !Array.isArray(msg.payload);
                
                if (msg.hasOwnProperty("definition")) {
                
                    // Verify that all metric definitions are correct
                    let definitionValid = typeof msg.definition === 'object' && msg.definition !== null && !Array.isArray(msg.definition);
                    if (definitionValid) {
                        for (const [key, value] of Object.entries(msg.definition)) {
                            // Check name
                            if (false) { // TODO: Is there any requirements for the metric name?
                                this.error(`${key} is not a valid definition !!!`);
                                definitionValid = false;
                            }

                            if (!value.hasOwnProperty("dataType")) {
                                this.error(RED._("mqtt-sparkplug-plus.errors.invalid-metric-definition", { name : key, error: `datatype required` }));
                                definitionValid = false;
                            }else if (!node.dataTypes.includes(value.dataType) &&
                                      !node.brokerConn.resolveTemplateDefinition(value.dataType)) {
                                this.error(RED._("mqtt-sparkplug-plus.errors.invalid-metric-definition", { name : key, error: `Invalid datatype ${value.dataType}` }));
                                definitionValid = false;
                            }

                        }
                    }

                    if (definitionValid) {
                        this.metrics = msg.definition;

                        // Filter metrics cache to only include metrics from new definition
                        var newMetric = {}
                        for (const [key, value] of Object.entries(this.latestMetrics)) {
                            if (msg.definition.hasOwnProperty(key)) {
                                newMetric[key] = value;
                            }
                        }
                        this.latestMetrics = newMetric;

                        if (this.birthMessageSend) {
                            
                            this.sendDDeath();
                            
                            // if there are no payload, then see if we can send a new birth message with the latest 
                            // data, otherwise we'll try to send after the values have been updated
                            if (!validPayload) {
                                this.trySendBirth();
                            }
                        } 
                    }
                }
                    
                if (validPayload) {

                    if (msg.payload.hasOwnProperty("metrics") && Array.isArray(msg.payload.metrics)) {
                        let _metrics = [];

                        // ── Flat-path accumulator for template metrics ──────────────────
                        // Maps: templateMetricName → { subPath → value, ... }
                        // e.g. "motor" → { "speed": 42, "nested/rpm": 5 }
                        let templateFlatValues = {};

                        msg.payload.metrics.forEach(m => {

                            if (!m.hasOwnProperty("name")){
                                this.warn(RED._("mqtt-sparkplug-plus.errors.missing-attribute-name"));
                                return;
                            }

                            // ── Check for exact metric name match ───────────────────────
                            if (this.metrics.hasOwnProperty(m.name)) {
                                let metricDef = this.metrics[m.name];
                                let dataType = metricDef.dataType;
                                let isTemplateDef = (dataType !== "Template" && !node.dataTypes.includes(dataType));

                                if (isTemplateDef) {
                                    // This is a named-template metric received with its root name.
                                    // A caller may pass the full Template instance value directly.
                                    m = JSON.parse(JSON.stringify(m));
                                    if (!m.hasOwnProperty("timestamp")) { m.timestamp = new Date(); }
                                    if (m.timestamp instanceof Date && !isNaN(m.timestamp)) { m.timestamp = m.timestamp.getTime(); }
                                    m.type = "Template";
                                    this.latestMetrics[m.name] = m;
                                    _metrics.push(m);
                                    return;
                                }

                                // Clone every metric to make we dont modify the input metric
                                m = JSON.parse(JSON.stringify(m));

                                if (!m.hasOwnProperty("value")) {
                                    m.value = null; // the Sparkplug-payload module will create the isNull property
                                }

                                // [tck-id-payloads-name-birth-data-requirement] The timestamp MUST be included with every metric in all NBIRTH, DBIRTH, NDATA, and DDATA messages
                                if (!m.hasOwnProperty("timestamp")) {
                                    m.timestamp = new Date();
                                }
                                // Sparkplug dates are always send a Unix Time
                                if (m.timestamp instanceof Date && !isNaN(m.timestamp)) {
                                    m.timestamp = m.timestamp.getTime();
                                }

                                // Type must be send on every message per the specicications (not sure why)
                                // We already know then type, so lets append it if it not already there
                                if (!m.hasOwnProperty("type")) {
                                    m.type = dataType;
                                }

                                // Extra validation of DataSet Types
                                if (m.type == "DataSet" && m.value != null) {
                                    if (false == (m.value.hasOwnProperty("types") && Array.isArray(m.value.types))) {
                                        node.warn(RED._("mqtt-sparkplug-plus.errors.invalid-metric-data", { "name" : m.name, "error" : "Value does not contain a types array"}));
                                    }
                                    else if (false === m.value.hasOwnProperty("columns") && Array.isArray(m.value.columns)) {
                                        node.warn(RED._("mqtt-sparkplug-plus.errors.invalid-metric-data", { "name" : m.name, "error" : "Value does not contain a columns array"}));
                                    }
                                    else if (m.value.columns.length !== m.value.types.length) {
                                        node.warn(RED._("mqtt-sparkplug-plus.errors.invalid-metric-data", { "name" : m.name, "error" : "size of types and columns array does not match"}));
                                    }
                                    else if (m.value.hasOwnProperty("numOfColumns") && m.value.numOfColumns !== m.value.columns.length) {
                                        node.warn(RED._("mqtt-sparkplug-plus.errors.invalid-metric-data", { "name" : m.name, "error" : "numOfColumns does not match the size of the columns"}));
                                    }
                                    if (!m.value.hasOwnProperty("numOfColumns")) {
                                        m.value.numOfColumns = m.value.columns.length;
                                    }
                                }

                                // We dont know how long it will take or when REBIRTH will be send
                                // so always include timestamp in DBIRTH messages
                                this.latestMetrics[m.name] = m;
                                _metrics.push(m);

                            } else {
                                // ── Flat-path template sub-metric: "templateMetric/subPath" ──
                                // e.g. name = "motor/speed" where "motor" is a template-typed metric.
                                // The definition key may itself contain slashes (e.g. "site/area/dev"),
                                // so find the longest matching key in this.metrics that is a prefix of
                                // m.name followed by "/".
                                let matched = false;
                                let prefix = Object.keys(this.metrics)
                                    .filter(k => m.name.startsWith(k + "/"))
                                    .sort((a, b) => b.length - a.length)[0];
                                if (prefix !== undefined) {
                                    let subPath = m.name.slice(prefix.length + 1);
                                    let prefixDataType = this.metrics[prefix].dataType;
                                    let isPrefixTemplate = (prefixDataType !== "Template" && !node.dataTypes.includes(prefixDataType));
                                    if (isPrefixTemplate || prefixDataType === "Template") {
                                        if (!templateFlatValues.hasOwnProperty(prefix)) {
                                            templateFlatValues[prefix] = {};
                                        }
                                        templateFlatValues[prefix][subPath] = m.hasOwnProperty("value") ? m.value : null;
                                        matched = true;
                                    }
                                }
                                if (!matched) {
                                    node.warn(RED._("mqtt-sparkplug-plus.errors.device-unknown-metric", m));
                                }
                            }
                        });

                        // ── Build Template Instance metrics from accumulated flat values ──
                        let ts = Date.now();
                        let templates = [];
                        try { templates = node.brokerConn.getTemplates(); } catch(e) {}

                        Object.keys(templateFlatValues).forEach(tMetricName => {
                            let dataType = this.metrics[tMetricName].dataType;
                            let incomingFlatVals = templateFlatValues[tMetricName];

                            // Build the partial instance for DDATA (only the changed sub-metrics)
                            let partialInstance = node.buildTemplateInstance(dataType, incomingFlatVals, templates, ts, false);
                            if (!partialInstance) return;

                            // Merge incoming values on top of any previously cached sub-metric values
                            // so that trySendBirth can reconstruct a complete DBIRTH after a REBIRTH.
                            let mergedFlatVals = Object.assign({}, incomingFlatVals);
                            const cached = this.latestMetrics[tMetricName];
                            if (cached && cached.value && Array.isArray(cached.value.metrics)) {
                                cached.value.metrics.forEach(function(im) {
                                    if (!mergedFlatVals.hasOwnProperty(im.name)) {
                                        mergedFlatVals[im.name] = im.value;
                                    }
                                });
                            }
                            let mergedInstance = node.buildTemplateInstance(dataType, mergedFlatVals, templates, ts, false);

                            // Cache the merged state so REBIRTH produces a complete DBIRTH
                            this.latestMetrics[tMetricName] = {
                                name: tMetricName,
                                type: "Template",
                                value: mergedInstance || partialInstance,
                                timestamp: ts
                            };
                            // Publish only the partial instance (DDATA = changed sub-metrics only)
                            _metrics.push({
                                name: tMetricName,
                                type: "Template",
                                value: partialInstance,
                                timestamp: ts
                            });
                        });

                        var shouldBuffer = (this.brokerConn.enableStoreForward && this.brokerConn.primaryScadaStatus !== "ONLINE") ||
                                                (!this.brokerConn.connected && this.bufferDevice);

                        if (shouldBuffer) {    
                                               
                            // Timestamps are required on historical metrics
                            _metrics.forEach(m=>{
                                if (!m.timestamp) {
                                    m.timestamp = Date.now();
                                }
                            })
                            this.brokerConn.addItemToQueue(this.name, _metrics);
                        }
                        else if (!this.birthMessageSend) {    // Send DBIRTH
                            this.trySendBirth(done);
                        }else if (_metrics.length > 0) { // SEND DDATA
                            let dMsg = this.brokerConn.createMsg(this.name, "DDATA", _metrics, f => {});
                            if (dMsg) {
                                this.brokerConn.publish(dMsg, !this.shouldBuffer, done); 
                            }
                        }
                    }else 
                    {
                        node.error(RED._("mqtt-sparkplug-plus.errors.device-no-metrics"));
                        done();
                    }
                } else {
                    if (!msg.hasOwnProperty("definition") && !msg.hasOwnProperty("command")) { // Its ok there are no payload if we set the metric definition 
                        node.error(RED._("mqtt-sparkplug-plus.errors.payload-type-object"));
                    }
                    done();
                }
            }); // end input

            
            //  Create "NULL" metrics if metrics should be sendt immediately
            if (this.birthImmediately) {
                this.latestMetrics = {};
                var templates = [];
                try { templates = node.brokerConn.getTemplates(); } catch(e) {}
                var ts = Date.now();
                Object.keys(this.metrics).forEach(m => {
                    var dataType = this.metrics[m].dataType;
                    if (dataType !== "Template" && !node.dataTypes.includes(dataType)) {
                        // Template name used as dataType
                        var instance = node.buildTemplateInstance(dataType, {}, templates, ts, true);
                        node.latestMetrics[m] = { value: instance, name: m, type: "Template" };
                    } else if (dataType === "Template") {
                        // Generic "Template" without a named definition — leave value null
                        node.latestMetrics[m] = { value: null, name: m, type: "Template" };
                    } else {
                        node.latestMetrics[m] = { value: null, name: m, type: dataType, timestamp: ts };
                    }
                });
            }
            node.brokerConn.register(node);
            
           this.subscribe_dcmd();
            this.on('close', function(done) {
                node.brokerConn.deregister(node, done);
            });
        } else {
            this.error(RED._("mqtt-sparkplug-plus.errors.missing-config"));
        }
    }
    RED.nodes.registerType("mqtt sparkplug device",MQTTSparkplugDeviceNode);

    function MQTTBrokerNode(n) {
        RED.nodes.createNode(this,n);

        this.name = n.name||"Sparkplug Node";
        this.deviceGroup = n.deviceGroup||"Sparkplug Devices";
        this.eonName = n.eonName||RED._("mqtt-sparkplug-plus.placeholder.eonname");
        // Configuration options passed by Node Red
        this.broker = n.broker;
        this.port = n.port;
        this.clientid = n.clientid;
        this.usetls = n.usetls;
        this.verifyservercert = n.verifyservercert;
        this.protocolVersion = n.protocolVersion;
        this.keepalive = n.keepalive;
        this.cleansession = n.cleansession;
        
        this.compressAlgorithm = n.compressAlgorithm;
        this.aliasMetrics = n.aliasMetrics;
        this.useTemplates = true;
        this.templates = n.templates||[];
        // Config node state
        this.brokerurl = "";
        this.connected = false;
        this.connecting = false;
        this.closing = false;
        this.options = {};
        this.subscriptions = {};
        this.bdSeq = -1;
        this.nbirthSent = false; // has NBIRTH been sent for the current MQTT session?
        this.seq = 0;

        this.manualEoNBirth = n.manualEoNBirth||false,

        this.maxQueueSize = 100000;

        // Get information about store forward
        this.enableStoreForward = n.enableStoreForward || false;
        this.primaryScada = n.primaryScada || "";

        // This will be set by primary SCADA and written via MQTT (OFFLINE or ONLINE)
        this.primaryScadaStatus = "OFFLINE";

        // Queue to store events while primary scada offline
        this.queue = this.context().get("queue");
        if (!this.queue){
            this.queue = {};
            this.context().set("queue", this.queue);
        }

        this.addItemToQueue = function(queueName, item) {
            if (!this.queue.hasOwnProperty(queueName)){
                this.queue[queueName] = [];
            }
            let q = this.queue[queueName];
            if (q.length === node.maxQueueSize) {
                q.shift();
            }else if (q.length  === node.maxQueueSize-1) {
                node.warn(RED._("mqtt-sparkplug-plus.errors.buffer-full"));
            }
            q.push(item);
            this.context().set("queue", this.queue);
        }

        this.getItemFromQueue = function(queueName) {
            let item = this.queue.hasOwnProperty(queueName) ? this.queue[queueName].shift() : undefined;
            this.context().set("queue", this.queue);
            return item;
        }

        this.emptyDDataBuffer = async function() {
            let x = this.getItemFromQueue("ddata");
            while(x) { 
                await new Promise((resolve) => this.publish(x, !this.shouldBuffer, resolve)); 
                x = this.getItemFromQueue(this.name);
            }
        }

        this.setConnectionState = function(node, state) {
        
            switch(state) {
                case "CONNECTED":
                    node.status({fill:"green",shape:"dot",text:"node-red:common.status.connected"});
                    break;
                case "DISCONNECTED":
                    node.status({fill:"red",shape:"ring",text:"node-red:common.status.disconnected"});
                    break;
                case "RECONNECTING":
                    node.status({fill:"yellow",shape:"ring",text:"node-red:common.status.connecting"});
                    break;
                case "WAITING_PRIMARY_HOST": // Broker connected, host not ONLINE yet
                    node.status({fill:"blue",shape:"ring",text:"waiting for primary host"});
                    break;
                case "BUFFERING": // As above, and queueing data meanwhile
                    node.status({fill:"blue",shape:"dot",text:"buffering - primary host offline"});
                    break;
                case "WAITING_CONNECT": // Online´
                    node.status({fill:"gray",shape:"dot",text:"awaiting connect command"});
                    break;
                default:
                    node.status({fill:"gray",shape:"dot",text:state}); // Unknown State
            }
        };

        /**
         * Connection state for a registered node.
         *
         * The Primary Host condition drives this on its own: a configured host
         * withholds NBIRTH whether or not store-and-forward is enabled, so the wait
         * has to be visible either way. Buffering only decides *which* waiting state
         * is shown.
         *
         * @param {object} user a node registered against this broker
         * @returns {string} state for setConnectionState
         */
        this.userConnectionState = function(user) {
            if (node.primaryScada && node.primaryScadaStatus === "OFFLINE") {
                return (node.enableStoreForward && user.shouldBuffer === true)
                    ? "BUFFERING"
                    : "WAITING_PRIMARY_HOST";
            }
            return "CONNECTED";
        };

        /**
         * @returns the next sequence number for the payload
         */
        this.nextSeq = function() {
            if (this.seq > 255) {
                this.seq = 0;
            }
            return this.seq++;
        };

        /**
         * We Store bdSeq in context, as a redeployment of the node can cause 
         * 
         * FIXME: context for config nodes are reset on redeploy (node-red bug) We still have the logic to store bdSeq in Context, if it get fixed on day
         * redeploy should always be gracefull shutdown, so it should not cause any issues (I think) 
         * @returns the next birth sequence number
         */
        this.nextBdseq = function() {
            let bdSeq = this.context().get("bdSeq");
            if (bdSeq === undefined) { // we can't || here because it will also filter out 0
                bdSeq = -1;
            }
            if (bdSeq > 255) {
                bdSeq = 0;
            } else {
                bdSeq += 1;
            }
            this.context().set("bdSeq", bdSeq);
            this.bdSeq = bdSeq;
            return bdSeq;
        };


        /**
         * Create a sparkplug b complient message
         * @param {string} deviceName the name of the device (leave blank for EoN messages)
         * @param {string} msgType the message type (DBIRTH, DDATA) 
         * @param {*} metrics The metrics to include in the payload
         * @returns a encoded sparkplug B message
         */
        this.createMsg = function(deviceName, msgType, metrics, done) {
            let that = this;
            let topic = deviceName ? `spBv1.0/${this.deviceGroup}/${msgType}/${this.eonName}/${deviceName}` :
                                     `spBv1.0/${this.deviceGroup}/${msgType}/${this.eonName}`;
            let msg = {
                topic : topic,
                payload : {
                    timestamp : new Date().getTime(),
                    metrics : metrics
                }
            };

            //[tck-id-topics-ndeath-seq] The NDEATH message MUST NOT include a sequence number.
            if (msgType != "NDEATH") {
                msg.payload.seq = that.nextSeq(); 
            }

            if (node.aliasMetrics) {
                msg.payload.metrics = node.addAliasMetrics(msgType, msg.payload.metrics);
            }
            try {
                if (node.compressAlgorithm) {
                    msg.payload =  compressPayload(msg.payload, { algorithm : node.compressAlgorithm});
                }
            }catch (e) {
                that.warn(RED._("mqtt-sparkplug-plus.errors.unable-to-encode-message", {type : msgType, error: e.toString()}));
                done(e);
            }

            try {
                msg.payload = sparkplugEncode(msg.payload); 
            }catch (e) {
                that.error(RED._("mqtt-sparkplug-plus.errors.unable-to-encode-message", {type : msgType, error: e.toString()}));
                done(e);
                return null;
            }
            return msg;   
        };
        this.nextMetricAlias = 0;
        this.metricsAliasMap = {};
        /**
         * Convert metric names to metric aliases. 
         * This method expect that the metrics attribute name exists
         */
        this.addAliasMetrics = function(msgType, metrics) {
            return metrics.map(metric => {

                // [tck-id-operational-behavior-data-commands-rebirth-name-aliases] When aliases are being used by an Edge Node an NBIRTH message MUST NOT include an alias for the Node Control/Rebirth metric.
                if (aliasIgnoreMetrics.includes(metric.name)) {
                    return metric;
                }

                // Update the alias map if necessary
                if (!node.metricsAliasMap.hasOwnProperty(metric.name)) {
                    node.metricsAliasMap[metric.name] = ++node.nextMetricAlias;
                }

                metric.alias = node.metricsAliasMap[metric.name];
                // Remove the name property if the message type is not NBIRTH or DBIRTH
                if (msgType != "NBIRTH" && msgType != "DBIRTH") {
                    delete metric.name;
                }
                return metric;
            });
        }

        this.getTemplates = function() {
            var _result = this.templates.map(m => typeof m === "string" ? JSON.parse(m) : m);
            _result.forEach(function(tpl) {
                if (tpl.value && Array.isArray(tpl.value.metrics)) {
                    tpl.value.metrics.forEach(function(met) {
                        if (met.type === "DataSet" && met.value == null) {
                            met.value = { numOfColumns: 0, columns: [], types: [], rows: [] };
                        }
                    });
                }
            });
            return _result;
        }

        /**
         * Find a Template Definition by name.
         * @param {string} name Template definition name
         * @returns {object|null} The template definition object or null if not found
         */
        this.resolveTemplateDefinition = function(name) {
            try {
                return this.getTemplates().find(t => t.name === name) || null;
            } catch(e) {
                return null;
            }
        }

        /**
         * 
         * @returns node death payload and topic
         */
        this.getDeathPayload = function() {
            let metric = [ {
                    name : "bdSeq", 
                    value : this.bdSeq, 
                    type : "int64"
                }];
            return node.createMsg("", "NDEATH", metric,  x=>{});
        };

        /**
         * Send NBirth Message
         *
         * [tck-id-topics-nbirth-bdseq-increment] The bdSeq number MUST increment by
         * one on every new MQTT CONNECT. bdSeq only advances when we connect, so
         * sending more than one NBIRTH per session would repeat a bdSeq value. A
         * repeat STATE ONLINE (a retained replay after re-subscribing, say) used to
         * do exactly that. Birth once per session unless explicitly forced.
         *
         * @param {boolean} force send even if this session has already birthed.
         *                        Required for a Rebirth NCMD, which by definition
         *                        re-births an established session.
         */
        this.sendBirth = function(force) {

            if (this.nbirthSent === true && force !== true) {
                return;
            }
            this.nbirthSent = true;

            this.seq = 0;
            var birthMessageMetrics = []
            
            if (node.useTemplates) {
                try {
                    birthMessageMetrics = this.getTemplates();
                } catch (e) {
                    node.error(RED._("mqtt-sparkplug-plus.errors.unable-to-deserialize-templates", {error: e.toString()}));
                }
            }            
            let ts = Date.now();
            birthMessageMetrics = birthMessageMetrics.concat([
                {
                    "name" : "Node Control/Rebirth",
                    "type" : "Boolean",
                    "value": false,
                    "timestamp": ts
                },
                {
                    "name" : "bdSeq",
                    "type" : "int64",
                    "value": this.bdSeq,
                    "timestamp": ts
                }]);
            var nbirth = node.createMsg("", "NBIRTH", birthMessageMetrics, x=>{});
            if (nbirth) {
                node.publish(nbirth);
                for (var id in node.users) {
                    if (node.users.hasOwnProperty(id) && node.users[id].trySendBirth) {
                        node.users[id].birthMessageSend = false;
                        node.users[id].trySendBirth(x=>{});
                    }
                }
            }
           
        }

        if (this.credentials) {
            this.username = this.credentials.user;
            this.password = this.credentials.password;
        }

        // If the config node is missing certain options (it was probably deployed prior to an update to the node code),
        // select/generate sensible options for the new fields
        if (typeof this.usetls === 'undefined') {
            this.usetls = false;
        }
        if (typeof this.verifyservercert === 'undefined') {
            this.verifyservercert = false;
        }
        if (typeof this.keepalive === 'undefined') {
            this.keepalive = 60;
        } else if (typeof this.keepalive === 'string') {
            this.keepalive = Number(this.keepalive);
        }
        if (typeof this.cleansession === 'undefined') {
            this.cleansession = true;
        }

        var prox, noprox;
        if (process.env.http_proxy) { prox = process.env.http_proxy; }
        if (process.env.HTTP_PROXY) { prox = process.env.HTTP_PROXY; }
        if (process.env.no_proxy) { noprox = process.env.no_proxy.split(","); }
        if (process.env.NO_PROXY) { noprox = process.env.NO_PROXY.split(","); }

        // Create the URL to pass in to the MQTT.js library
        if (this.brokerurl === "") {
            // if the broker may be ws:// or wss:// or even tcp://
            if (this.broker.indexOf("://") > -1) {
                this.brokerurl = this.broker;
                // Only for ws or wss, check if proxy env var for additional configuration
                if (this.brokerurl.indexOf("wss://") > -1 || this.brokerurl.indexOf("ws://") > -1 ) {
                    // check if proxy is set in env
                    var noproxy;
                    if (noprox) {
                        for (var i = 0; i < noprox.length; i += 1) {
                            if (this.brokerurl.indexOf(noprox[i].trim()) !== -1) { noproxy=true; }
                        }
                    }
                    if (prox && !noproxy) {
                        var parsedUrl = url.parse(this.brokerurl);
                        var proxyOpts = url.parse(prox);
                        // true for wss
                        proxyOpts.secureEndpoint = parsedUrl.protocol ? parsedUrl.protocol === 'wss:' : true;
                        // Set Agent for wsOption in MQTT
                        var agent = new HttpsProxyAgent(proxyOpts);
                        this.options.wsOptions = {
                            agent: agent
                        }
                    }
                }
            } else {
                // construct the std mqtt:// url
                if (this.usetls) {
                    this.brokerurl="mqtts://";
                } else {
                    this.brokerurl="mqtt://";
                }
                if (this.broker !== "") {
                    //Check for an IPv6 address
                    if (/(?:^|(?<=\s))(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))(?=\s|$)/.test(this.broker)) {
                        this.brokerurl = this.brokerurl+"["+this.broker+"]:";
                    } else {
                        this.brokerurl = this.brokerurl+this.broker+":";
                    }
                    // port now defaults to 1883 if unset.
                    if (!this.port){
                        this.brokerurl = this.brokerurl+"1883";
                    } else {
                        this.brokerurl = this.brokerurl+this.port;
                    }
                } else {
                    this.brokerurl = this.brokerurl+"localhost:1883";
                }
            }
        }

        if (!this.cleansession && !this.clientid) {
            this.cleansession = true;
            this.warn(RED._("mqtt-sparkplug-plus.errors.nonclean-missingclientid"));
        }

        // Build options for passing to the MQTT.js API
        this.options.clientId = this.clientid || 'mqtt_' + RED.util.generateId();
        this.options.username = this.username;
        this.options.password = this.password;
        this.options.keepalive = this.keepalive;
        this.options.clean = this.cleansession;
        this.options.reconnectPeriod = RED.settings.mqttReconnectTime||5000;
     
        if (this.usetls && n.tls) {
            var tlsNode = RED.nodes.getNode(n.tls);
            if (tlsNode) {
                tlsNode.addTLSOptions(this.options);
            }
        }

        // If there's no rejectUnauthorized already, then this could be an
        // old config where this option was provided on the broker node and
        // not the tls node
        if (typeof this.options.rejectUnauthorized === 'undefined') {
            this.options.rejectUnauthorized = (this.verifyservercert == "true" || this.verifyservercert === true);
        }
        
        
        // Define functions called by MQTT Devices
        var node = this;
        this.users = {};

        /**
         * Register a mqttNode to. This will ensure that this object can communcate with
         * clients (e.g for REBIRTH commands)
         * @param {object} mqttNode 
         */
        this.register = function(mqttNode) {
            
            node.users[mqttNode.id] = mqttNode;
            // Via userConnectionState, so a node registering while the Primary Host
            // is still OFFLINE reports the wait rather than a bare "connected".
            let state = node.manualEoNBirth ? "WAITING_CONNECT"
                      : node.connected ? node.userConnectionState(mqttNode)
                      : "DISCONNECTED";
            
            node.setConnectionState(mqttNode, state);
            if (Object.keys(node.users).length === 1) {
                node.connect();
            }
        };

        /**
         * Deregister a client
         * @param {object} mqttNode 
         * @param {function} done 
         * @returns void
         */
        this.deregister = function(mqttNode,done) {
            // done is optional - callers such as the test helper and the node's own
            // close handler may omit it, so never call it unguarded.
            let finished = function() {
                if (typeof done === 'function') {
                    done();
                }
            };
            delete node.users[mqttNode.id];
            if (node.closing) {
                return finished();
            }
            if (Object.keys(node.users).length === 0) {
                if (node.client && node.client.connected) {
                    // Send close message
                    let msg = this.getDeathPayload();
                    node.publish(msg, false, function(err) {
                        // [tck-id-operational-behavior-edge-node-intentional-disconnect-packet]
                        // A DISCONNECT packet MUST follow the NDEATH publish. end(true)
                        // would drop the socket without sending one, so force must be
                        // false here.
                        node.client.end(false, {}, function() {
                            // end(false) sends DISCONNECT and only then defers
                            // stream.end() via setImmediate, so 'close' - and the
                            // node.connected = false it performs - lands several turns
                            // after the NDEATH has reached other clients. A register()
                            // arriving in that window would call connect() and hit its
                            // `!connected && !connecting` early-return, leaving the node
                            // down for good. Clear the flags here and re-drive if
                            // someone re-registered while we were closing.
                            node.connected = false;
                            node.connecting = false;
                            if (Object.keys(node.users).length > 0) {
                                node.connect();
                            }
                            finished();
                        });
                    });
                    return;
                } else {
                    node.client.end();
                    return finished();
                }
            }
            finished();
        };

        /**
         * Connect to the MQTT Broker
         */
        this.connect = function () {
            if (node.manualEoNBirth === true) {
                return;
            }
            if (!node.connected && !node.connecting) {
                node.connecting = true;
                try {
                    this.nextBdseq(); // Next connect will use next bdSeq
                    node.nbirthSent = false; // new session -> NBIRTH is due again
                    node.options.will = this.getDeathPayload();
                    if (node.options.will) node.options.will.qos = 1;
                    node.serverProperties = {};
                    node.client = mqtt.connect(node.brokerurl ,node.options);
                    node.client.setMaxListeners(0);
                    // Register successful connect or reconnect handler
                    node.client.on('connect', function (connack) {
                        node.connecting = false;
                        node.connected = true;
                        node.log(RED._("mqtt-sparkplug-plus.state.connected",{broker:(node.clientid?node.clientid+"@":"")+node.brokerurl}));
                        for (var id in node.users) {
                            if (node.users.hasOwnProperty(id)) {
                                node.setConnectionState(node.users[id], node.userConnectionState(node.users[id]));
                            }
                        }

                        // Not sure if connect will be called after a reconnect?? Need to check and delete if not needed
                        // Remove any existing listeners before resubscribing to avoid duplicates in the event of a re-connection
                        node.client.removeAllListeners('message');

                        // Re-subscribe to stored topics
                        for (var s in node.subscriptions) {
                            if (node.subscriptions.hasOwnProperty(s)) {
                                let topic = s;
                                let qos = 0;
                                let _options = {};
                                for (var r in node.subscriptions[s]) {
                                    if (node.subscriptions[s].hasOwnProperty(r)) {
                                        qos = Math.max(qos,node.subscriptions[s][r].qos);
                                        _options = node.subscriptions[s][r].options;
                                        node.client.on('message',node.subscriptions[s][r].handler);
                                    }
                                }
                                _options.qos = _options.qos || qos;
                                node.client.subscribe(topic, _options);
                            }
                        }

                        // Subscribe to NCMDs
                        let options = { qos: 0 };
                        let subscribeTopic = `spBv1.0/${node.deviceGroup}/NCMD/${node.eonName}`;
                        node.subscribe(subscribeTopic,options,function(topic_,payload_,packet) {
                            node.handleNCMD(payload_);
                        });
 
                        // Subscribe to Primary SCADA status if primaryScada is configured.
                        // [tck-id-message-flow-edge-node-birth-publish-phid-wait] A
                        // configured Primary Host means this node MUST withhold NBIRTH
                        // until that host publishes ONLINE - independently of
                        // store-and-forward, which only governs buffering.
                        if (node.primaryScada) {
                            node.log(`Primary Host "${node.primaryScada}" configured: withholding NBIRTH until it publishes an ONLINE STATE message.`);
                            let options = { qos: 0 };

                            // SPb 2.0 Support
                            let primaryScadaTopic = `STATE/${node.primaryScada}`;
                            node.subscribe(primaryScadaTopic,options,function(topic_,payload_,packet) {
                                let status = payload_.toString();
                                node.primaryScadaStatus = status;

                                if (node.primaryScadaStatus === "ONLINE") {
                                   node.sendBirth();
                                   node.emptyDDataBuffer();
                                }
                                for (var id in node.users) {
                                    if (node.users.hasOwnProperty(id)) {
                                        node.setConnectionState(node.users[id], node.userConnectionState(node.users[id]));
                                        //if (node.primaryScadaStatus == "ONLINE" && typeof node.users[id].trySendBirth === 'function') {
                                         //   node.users[id].trySendBirth();
                                        //}
                                    }
                                }
                            });

                            // SPb 3.0 Support
                            let primaryScadaTopicv3 = `spBv1.0/STATE/${node.primaryScada}`;
                            node.subscribe(primaryScadaTopicv3,options,function(topic_,payload_,packet) {
                                let payload = payload_.toString();

                                try {
                                    var pss = JSON.parse(payload);
                                    var wasOnline = node.primaryScadaStatus === "ONLINE";
                                    var incomingStatus = pss.hasOwnProperty("online") ? (pss.online ? "ONLINE" : "OFFLINE") : "OFFLINE";
                                    var hasTs = pss.hasOwnProperty("timestamp") && typeof pss.timestamp === 'number';
                                    // Only update status/timestamp for non-stale messages
                                    var isStale = hasTs && typeof node.lastPhidTimestamp !== 'undefined' && pss.timestamp < node.lastPhidTimestamp;
                                    if (!isStale) {
                                        node.primaryScadaStatus = incomingStatus;
                                        if (hasTs) node.lastPhidTimestamp = pss.timestamp;
                                    }
                                } catch{
                                    node.warn("Invalid Primary SCADA State:" + payload)
                                    node.primaryScadaStatus = "OFFLINE";
                                }
                                if (node.primaryScadaStatus === "ONLINE") {
                                    node.sendBirth();
                                    node.emptyDDataBuffer();
                                 } else if (wasOnline && node.primaryScadaStatus === "OFFLINE" && typeof pss !== 'undefined') {
                                    // Patch: publish NDEATH when Primary Host goes offline
                                    // Status was only updated if !isStale (handled above), so no need to re-check timestamp here.
                                    var deathPayload = node.getDeathPayload();
                                    if (deathPayload && node.client && node.client.connected) {
                                        node.publish(deathPayload, false, function(err) {
                                            node.client.end(false, {}, function() {
                                                node.connected = false;
                                                if (typeof node.connect === 'function') {
                                                    node.connect();
                                                }
                                            });
                                        });
                                    }
                                 }
                                for (var id in node.users) {
                                    if (node.users.hasOwnProperty(id)) {
                                        node.setConnectionState(node.users[id], node.userConnectionState(node.users[id]));
                                        //if (node.primaryScadaStatus == "ONLINE" && typeof node.users[id].trySendBirth === 'function') {
                                        //    node.users[id].trySendBirth();
                                        //}
                                    }
                                }
                            });

                            if (node.primaryScadaStatus === "ONLINE") {
                                node.emptyDDataBuffer()
                            }

                        } else {
                        // Send Node Birth right away if no primaryScada is configured
                            node.sendBirth();
                            node.emptyDDataBuffer();
                        }

                    });

                    node.client.on("reconnect", function() {
                        for (var id in node.users) {
                            if (node.users.hasOwnProperty(id)) {
                                node.setConnectionState(node.users[id], "RECONNECTING");
                            }
                        }
                    });
                    //TODO: what to do with this event? Anything? Necessary?
                    node.client.on("disconnect", function(packet) {
                        //Emitted after receiving disconnect packet from broker. MQTT 5.0 feature.
                        //var rc = packet && packet.properties && packet.properties.reasonString;
                        //var rc = packet && packet.properties && packet.reasonCode;
                        //TODO: If keeping this event, do we use these? log these?
                    });
                    // Register disconnect handlers
                    node.client.on('close', function () {
                        // The session is gone; the next one must birth again.
                        node.nbirthSent = false;
                        if (node.connected) {
                            node.connected = false;
                            node.log(RED._("mqtt-sparkplug-plus.state.disconnected",{broker:(node.clientid?node.clientid+"@":"")+node.brokerurl}));
                            for (var id in node.users) {
                                if (node.users.hasOwnProperty(id)) {
                                    node.setConnectionState(node.users[id], "DISCONNECTED");
                                }
                            }
                        } else if (node.connecting) {
                            node.log(RED._("mqtt-sparkplug-plus.state.connect-failed",{broker:(node.clientid?node.clientid+"@":"")+node.brokerurl}));
                        }
                    });

                    // Register connect error handler
                    // The client's own reconnect logic will take care of errors
                    node.client.on('error', function (error) {
                    });
                }catch(err) {
                    console.log(err);
                }
            }
        };

        /**
         * Handle NCMD commands from Broker
         * @param {object} payload sparkplug encoded payload
         */
        this.handleNCMD = function(payload) {
            try {
                payload = maybeDecompressPayload(sparkplugDecode(payload));
                if (payload.hasOwnProperty("metrics") && Array.isArray(payload.metrics)){
                    payload.metrics.forEach(m => {
                        if (typeof m === 'object' && m.hasOwnProperty("name") && m.name) {
                            if (m.name.toLowerCase() === "node control/rebirth") {
                                
                                //let bMsg = this.getDeathPayload();
                                //if(bMsg) {
                                //    node.publish(bMsg, !this.shouldBuffer, f => {});  // send the message 
                                //}

                                // A Rebirth re-births an established session, so it
                                // must bypass the once-per-session guard.
                                node.sendBirth(true);
                            }else
                            {
                                node.warn(`NCMD command ${m.name} is not supported`);
                            }
                        }else {
                            node.warn(`invalid NCMD received`);
                        }
                    })
                }else {
                    node.warn(RED._("mqtt-sparkplug-plus.errors.unable-to-decode-message", {type : "NCMD", error: "Metrics is not an Array"}));
                }

            }catch (e) {
                node.error(RED._("mqtt-sparkplug-plus.errors.unable-to-decode-message", {type : "NCMD", error: e.toString()}));
            }
        };

        this.subscriptionIds = {};
        this.subid = 1;
        /**
         * Subscribe to a MQTT Topic
         * @param {string} topic the topic to subscribe to 
         * @param {object} options objects for the subsribtion
         * @param {function} callback a function that will be called when new data comes in 
         * @param {*} ref 
         */
        this.subscribe = function (topic,options,callback,ref) {
            ref = ref||0;
            var qos;
            if(typeof options == "object") {
                qos = options.qos;
            } else {
                qos = options;
                options = {};
            }
            options.qos = qos;
            if (!node.subscriptionIds[topic]) {
                node.subscriptionIds[topic] = node.subid++;
            }
            options.properties = options.properties || {};
            options.properties.subscriptionIdentifier = node.subscriptionIds[topic];

            node.subscriptions[topic] = node.subscriptions[topic]||{};
            var sub = {
                topic:topic,
                qos:qos,
                options:options,
                handler:function(mtopic,mpayload, mpacket) {
                    if(mpacket.properties && options.properties && mpacket.properties.subscriptionIdentifier && options.properties.subscriptionIdentifier && (mpacket.properties.subscriptionIdentifier !== options.properties.subscriptionIdentifier) ) {
                        //do nothing as subscriptionIdentifier does not match
                        // node.debug(`> no match - this nodes subID (${options.properties.subscriptionIdentifier}) !== packet subID (${mpacket.properties.subscriptionIdentifier})`); //TODO: remove
                    } else if (matchTopic(topic,mtopic)) {
                        // node.debug(`>  MATCHED '${topic}' to '${mtopic}' - performing callback`); //TODO: remove
                        callback(mtopic,mpayload, mpacket);
                    } else {
                        // node.debug(`> no match / no callback`); //TODO: remove
                    }
                },
                ref: ref
            };
            node.subscriptions[topic][ref] = sub;
            if (node.connected) {
                // node.debug(`this.subscribe - registering handler ref ${ref} for ${topic} and subscribing `+JSON.stringify(options)); //TODO: remove
                node.client.on('message',sub.handler);
                node.client.subscribe(topic, options);
            }
        };

        /**
         * Unsubscribe from topic
         * @param {string} topic 
         * @param {object} ref 
         * @param {*} removed not used ()
         */
        this.unsubscribe = function (topic, ref, removed) {
            ref = ref||0;
            var sub = node.subscriptions[topic];
            if (sub) {
                if (sub[ref]) {
                    if (node.client) {
                        node.client.removeListener('message',sub[ref].handler);
                    }
                    
                    delete sub[ref];
                }
                //TODO: Review. The `if(removed)` was commented out to always delete and remove subscriptions.
                // if we dont then property changes dont get applied and old subs still trigger
                //if (removed) {

                if (Object.keys(sub).length === 0) {
                    delete node.subscriptions[topic];
                    delete node.subscriptionIds[topic];
                    if (node.connected) {
                        node.client.unsubscribe(topic);
                    }
                }
                //}
            } else {
                // _debug += "sub not found! "; //TODO: remove
            }
            // node.debug(_debug); //TODO: remove
            
        };

        /**
         * 
         * @param {object} msg 
         * @param {function} done 
         * @param {boolean} bypassQueue 
         */
        this.publish = function (msg, bypassQueue, done) {

            if (true)  { // (node.connected && (!node.enableStoreForward || (node.primaryScadaStatus === "ONLINE") || bypassQueue)) {
                if (msg.payload === null || msg.payload === undefined) {
                    msg.payload = "";
                } else if (!Buffer.isBuffer(msg.payload)) {
                    if (typeof msg.payload === "object") {
                        msg.payload = JSON.stringify(msg.payload);
                    } else if (typeof msg.payload !== "string") {
                        msg.payload = "" + msg.payload;
                    }
                }
                var options = {
                    qos: msg.qos || 0,
                    retain: msg.retain || false
                };
    
                node.client.publish(msg.topic, msg.payload, options, function(err) {
                    done && done(err);
                    return;
                });
            } else {
               console.log("This should not happen");
                done && done();
            }
        };

        this.on('close', function(done) {
            this.closing = true;
            if (this.connected) {
                this.client.once('close', function() {
                    done();
                });
                this.client.end();
            } else if (this.connecting || node.client.reconnecting) {
                node.client.end();
                done();
            } else {
                done();
            }
        });
    }

    RED.nodes.registerType("mqtt-sparkplug-broker", MQTTBrokerNode,{
        credentials: {
            user: {type:"text"},
            password: {type: "password"}
        }
    });

    /**
     * MQTT In node subscribes to MQTT Topics and output them to Node-Red as messages
     * @param {object} n node 
     * @returns 
     */
    function MQTTInNode(n) {
        RED.nodes.createNode(this,n);
        this.topic = n.topic;
        this.qos = parseInt(n.qos);
        this.name = n.name;
        
        this.shouldBuffer = false; // hardcoded as in node will never write

        if (isNaN(this.qos) || this.qos < 0 || this.qos > 2) {
            this.qos = 2;
        }
        this.broker = n.broker;
        this.brokerConn = RED.nodes.getNode(this.broker);
        if (!/^(#$|(\+|[^+#]*)(\/(\+|[^+#]*))*(\/(\+|#|[^+#]*))?$)/.test(this.topic)) {
            return this.warn(RED._("mqtt-sparkplug-plus.errors.invalid-topic"));
        }

        var node = this;
        if (this.brokerConn) {
            if (this.topic) {
                node.brokerConn.register(this);
                let options = { qos: this.qos };

                this.brokerConn.subscribe(this.topic,options, function(topic,payload,packet) {
                    
                    // Decode Payload
                    try {
                        if (topic.startsWith("spBv1.0/")) {
                            payload = maybeDecompressPayload(sparkplugDecode(payload));
                        }
                        var msg = {topic:topic, payload:payload, qos:packet.qos, retain:packet.retain};
                        node.send(msg);
                    } catch (e) {
                    node.error(RED._("mqtt-sparkplug-plus.errors.unable-to-decode-message", {type : "Unknown", error: e.toString()}));
                    }
                    
                }, this.id);
            }
            else {
                this.error(RED._("mqtt-sparkplug-plus.errors.not-defined"));
            }
            this.on('close', function(removed, done) {
                if (node.brokerConn) {
                    node.brokerConn.unsubscribe(node.topic,node.id, removed);
                    node.brokerConn.deregister(node,done);
                }
            });
        } else {
            this.error(RED._("mqtt-sparkplug-plus.errors.missing-config"));
        }
    }
    RED.nodes.registerType("mqtt sparkplug in", MQTTInNode);

    function MQTTOutNode(n) {
        RED.nodes.createNode(this,n);
        this.topic = n.topic;
        this.qos = n.qos || null;
        this.retain = n.retain;
        this.broker = n.broker;
        this.shouldBuffer = false; // hardcoded - buffering NCMD/DCMD is a bad idea... if we enable, then it shnould come with a big warning.
        
        this.brokerConn = RED.nodes.getNode(this.broker);
        var node = this;

        if (this.brokerConn) {            
            this.on("input",function(msg,send,done) {

                // abort if not connected and node is not configured to buffer
                if (!node.brokerConn.connected && this.shouldBuffer !== true) {
                    return;
                }
                if (msg.qos) {
                    msg.qos = parseInt(msg.qos);
                    if ((msg.qos !== 0) && (msg.qos !== 1) && (msg.qos !== 2)) {
                        msg.qos = null;
                    }
                }
                msg.qos = Number(node.qos || msg.qos || 0);
                msg.retain = node.retain || msg.retain || false;
                msg.retain = ((msg.retain === true) || (msg.retain === "true")) || false;
                /** If node property exists, override/set that to property in msg  */
                let msgPropOverride = function(propName) { if(node[propName]) { msg[propName] = node[propName]; } }
                msgPropOverride("topic");

                if (msg.hasOwnProperty("payload")) {
                    let topicOK = msg.hasOwnProperty("topic") && (typeof msg.topic === "string") && (msg.topic !== "");
                    if (topicOK) { // topic must exist
                            // Sparkplug dates are always send a Unix Time
                        if (msg.payload.timestamp instanceof Date && !isNaN(msg.payload.timestamp)) {
                            msg.payload.timestamp = msg.payload.timestamp.getTime();
                        }
                        try{
                            if (this.brokerConn.compressAlgorithm) {
                                msg.payload =  compressPayload(msg.payload, { algorithm : this.brokerConn.compressAlgorithm});
                            }
                        }
                        catch (e) {
                            this.warn(RED._("mqtt-sparkplug-plus.errors.unable-to-encode-message", { error: e.toString()}));
                        }

                        try {
                            var isOnline = (node.brokerConn.enableStoreForward && node.brokerConn.primaryScadaStatus === "ONLINE" && node.brokerConn.connected) ||
                                            (!node.brokerConn.enableStoreForward && node.brokerConn.connected);
                            msg.payload =  sparkplugEncode(msg.payload); 

                            if (isOnline) {
                                this.brokerConn.publish(msg, !this.shouldBuffer, done);  // send the message
                            } else {
                                 // ddata queue will be flushed as soon as the conncetion is back online.
                                 this.brokerConn.addItemToQueue("ddata", msg);
                            }
                        } catch (e) {
                            done(e);
                        }
                    } else {
                        node.warn(RED._("mqtt-sparkplug-plus.errors.invalid-topic"));
                        done();
                    }
                } else {
                    done();
                }
            });
            node.brokerConn.register(node);
            this.on('close', function(done) {
                node.brokerConn.deregister(node,done);
            });
        } else {
            this.error(RED._("mqtt-sparkplug-plus.errors.missing-config"));
        }
    }
    RED.nodes.registerType("mqtt sparkplug out",MQTTOutNode);
};
