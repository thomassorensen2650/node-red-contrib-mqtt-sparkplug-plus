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

    var pako = require('pako');
    var compressed = "SPBV1.0_COMPRESSED";

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
        var metrics = payload.metrics;
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

        // Verify that all metrics have a type (if people copy message from e.g. MQTT.FX, then the variable is not called type)
        if (payload.hasOwnProperty("metrics")) {
            if (!Array.isArray(payload.metrics)) {
                throw RED._("mqtt-sparkplug-plus.errors.metrics-not-array");
            } else {
                payload.metrics.forEach(met => {
                    if (!met.hasOwnProperty("type")) {
                        throw RED._("mqtt-sparkplug-plus.errors.unable-to-encode-message", { type : "", error :  "Unable to encode message, all metrics must have a 'type' Attribute" });
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
        var buffer = Buffer.from(payload_);
        return spPayload.decodePayload(buffer);
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
        this.dataTypes = ["Int8", "Int16", "Int32", "Int64", "Float", "Double", "Boolean" , "String", "Unknown"],


        this.broker = n.broker;
        this.name = n.name||"Sparkplug Device";
        this.latestMetrics = {};
        this.metrics = n.metrics || {};
        this.birthMessageSend = false;
        this.birthImmediately = n.birthImmediately || false;

        this.shouldBuffer = true; // hardcoded / Devices always buffers

        if (typeof this.birthImmediately === 'undefined') {
            this.birthImmediately = false;
        }
        /**
         * try to send Sparkplug DBirth Messages
         * @param {function} done Node-Red Done Function
         */
        this.trySendBirth = function(done) {
            let readyToSend = Object.keys(this.metrics).every(m => this.latestMetrics.hasOwnProperty(m));

            // Don't send birth if no metrics. we can assume that a dynamic defintion will be send if on metrics are defined.
            let hasMetrics = Object.keys(this.metrics).length > 0;
            if (readyToSend && hasMetrics) {
                let birthMetrics = [];

                for (const [key, value] of Object.entries(this.metrics)) {
                    const lv = Object.assign({}, this.latestMetrics[key]);

                    if (value.hasOwnProperty("properties")) {
                        lv.properties = value.properties;
                    }
                    birthMetrics.push(lv);
                }
                let bMsg = node.brokerConn.createMsg(this.name, "DBIRTH", birthMetrics, f => {});
                if(bMsg) {
                    this.brokerConn.publish(bMsg, !this.shouldBuffer, done);  // send the message
                    this.birthMessageSend = true;
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
        var node = this;
        if (this.brokerConn) {
            this.on("input",function(msg,send,done) {
                // Handle Command
                if (msg.hasOwnProperty("command")) {
                    if (msg.command.hasOwnProperty("device")) {
                        if (msg.command.device.rebirth) {
                            if (this.birthMessageSend) {
                                this.sendDDeath();
                            }
                            this.trySendBirth();
                        }
                        if (msg.command.device.death) {
                            if (this.birthMessageSend) {
                                this.sendDDeath();
                            }
                        }

                    };
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
                            }else if (!node.dataTypes.includes(value.dataType)) {
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
                        msg.payload.metrics.forEach(m => {

                            if (!m.hasOwnProperty("name")){
                                this.warn(RED._("mqtt-sparkplug-plus.errors.missing-attribute-name"));
                            } else if (this.metrics.hasOwnProperty(m.name)) {

                                if (!m.hasOwnProperty("value")) {
                                    //m.is_null = true;
                                    m.value = null; // the Sparkplug-payload module will create the isNull property
                                }

                                // Sparkplug dates are always send a Unix Time
                                if (m.timestamp instanceof Date && !isNaN(m.timestamp)) {
                                    m.timestamp = m.timestamp.getTime();
                                }

                                // Type must be send on every message per the specicications (not sure why)
                                // We already know then type, so lets append it if it not already there
                                if (!m.hasOwnProperty("type")) {
                                    m.type = this.metrics[m.name].dataType;
                                }

                                // We dont know how long it will take or when REBIRTH will be send
                                // so always include timewstamp in DBIRTH messages
                                this.latestMetrics[m.name] = JSON.parse(JSON.stringify(m));
                                if (!this.latestMetrics[m.name].hasOwnProperty("timestamp")) {
                                    this.latestMetrics[m.name].timestamp = new Date().getTime(); // We dont know when DBIRTH will be send, so force a timetamp in metric
                                }
                                _metrics.push(m);
                            }else {
                                node.warn(RED._("mqtt-sparkplug-plus.errors.device-unknown-metric", m));
                            }
                        });

                        if (!this.birthMessageSend) {    // Send DBIRTH
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
                Object.keys(this.metrics).forEach(m => {
                    this.latestMetrics[m] = { value : null, name : m, type: this.metrics[m].dataType }
                });
            }
            node.brokerConn.register(node);

            // Handle DCMD Messages
            let options = { qos: 0 };
            let subscribeTopic = `spBv1.0/${this.brokerConn.deviceGroup}/DCMD/${this.brokerConn.eonName}/${this.name}`;
            this.brokerConn.subscribe(subscribeTopic,options,function(topic_,payload_,packet) {
                try {
                    var msg = {
                        topic : topic_,
                        payload : maybeDecompressPayload(sparkplugDecode(payload_))
                    };
                    node.send(msg);
                } catch (e) {
                    node.error(RED._("mqtt-sparkplug-plus.errors.unable-to-decode-message", {type : "DCMD", error: e.toString()}));
                }
            });
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

        // specify the device group id according to the environment variable
        this.prdDeviceGroup = n.deviceGroup||"Sparkplug Devices";
        this.devDeviceGroup = n.devDeviceGroup||"Sparkplug Dev Devices";
        this.prdMode = process.env.NODE_ENV === 'prd' || process.env.NODE_ENV === 'production'
        this.deviceGroup = this.prdMode ? this.prdDeviceGroup : this.devDeviceGroup;

        this.eonName = n.eonName||RED._("mqtt-sparkplug-plus.placeholder.eonname"),
        // Configuration options passed by Node Red
        this.broker = n.broker;
        this.port = n.port;
        this.clientid = n.clientid;
        this.usetls = n.usetls;
        this.usews = n.usews;
        this.verifyservercert = n.verifyservercert;
        this.protocolVersion = n.protocolVersion;
        this.keepalive = n.keepalive;
        this.cleansession = n.cleansession;

        this.compressAlgorithm = n.compressAlgorithm;
        this.aliasMetrics = n.aliasMetrics;

        // Config node state
        this.brokerurl = "";
        this.connected = false;
        this.connecting = false;
        this.closing = false;
        this.options = {};
        this.subscriptions = {};
        this.bdSeq = 0;
        this.seq = 0;

        this.maxQueueSize = 100000;
        // Get information about store forward
        this.enableStoreForward = n.enableStoreForward || false;
        this.devScada = n.devScada || ""
        this.prdScada = n.primaryScada || "";
        this.primaryScada = this.prdMode ? this.prdScada : this.devScada;

        // This will be set by primary SCADA and written via MQTT (OFFLINE or ONLINE)
        this.primaryScadaStatus = "OFFLINE";

        // Queue to store events while primary scada offline
        this.queue = this.context().get("queue");
        if (!this.queue){
            this.queue = [];
            this.context().set("queue", this.queue);
        }

        /**
         * empties the current queue
         */
        this.emptyQueue = async function() {
            if (node.primaryScadaStatus === "ONLINE" && node.connected) {
                var item = this.queue.shift();
                let count = 0;
                while (item && node.primaryScadaStatus === "ONLINE" && node.connected) {

                    if(!Buffer.isBuffer(item.payload))   {
                        item.payload = Buffer.from(item.payload.data)
                    }

                    node.publish(item, true);
                    item = this.queue.shift();
                    this.context().set("queue", this.queue);

                    // Slow down queue empty
                    if (++count % 500 === 0) {
                        await new Promise(resolve => setTimeout(resolve, 250));
                    }
                }
            }
        };

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
                case "BUFFERING": // Online´
                    node.status({fill:"blue",shape:"dot",text:"destination offline"});
                    break;
                default:
                    node.status({fill:"gray",shape:"dot",text:state}); // Unknown State
            }
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
         * @returns the next birth sequence number
         */
        this.nextBdseq = function() {
            if (this.bdSeq > 255) {
                this.bdSeq = 0;
            }
            return this.bdSeq++;
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
                    seq : that.nextSeq(),
                    metrics : metrics
                }
            };

            if (node.aliasMetrics) {
                node.addAliasMetrics(msgType, msg.payload.metrics);
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
            metrics.forEach(metric => {
                if (!node.metricsAliasMap.hasOwnProperty(metric.name)) {
                    node.metricsAliasMap[metric.name] = ++node.nextMetricAlias;
                }
                var alias = node.metricsAliasMap[metric.name];
                if (msgType != "NBIRTH" && msgType != "DBIRTH") {
                    delete metric.name;
                }
                metric.alias = alias;
            });
        }

        /**
         *
         * @returns node death payload and topic
         */
        this.getDeathPayload = function() {
            let metric = [ {
                    name : "bdSeq",
                    value : this.bdSeq,
                    type : "uint64"
                }];
            return node.createMsg("", "NDEATH", metric,  x=>{});
        };

        /**
         * Send NBirth Message
         */
        this.sendBirth = function() {
            this.seq = 0;
            var birthMessageMetrics = [

                {
                    "name" : "Node Control/Rebirth",
                    "type" : "Boolean",
                    "value": false
                },
                {
                    "name" : "bdSeq",
                    "type" : "uint64",
                    "value": this.bdSeq,
                }];
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
        if (typeof this.usews === 'undefined') {
            this.usews = false;
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
            let state = node.connected ? "CONNECTED" : "DISCONNECTED";
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
            delete node.users[mqttNode.id];
            if (node.closing) {
                return done();
            }
            if (Object.keys(node.users).length === 0) {
                if (node.client && node.client.connected) {
                    // Send close message
                    let msg = this.getDeathPayload();
                    node.publish(msg, false, function(err) {
                        //node.client.end(done);
                        node.client.end(true, {}, done);
                    });
                    return;
                } else {
                    node.client.end();
                    return done();
                }
            }
            done();
        };

        /**
         * Connect to the MQTT Broker
         */
        this.connect = function () {
            if (!node.connected && !node.connecting) {
                node.connecting = true;
                try {
                    node.options.will = this.getDeathPayload();
                    node.serverProperties = {};
                    node.client = mqtt.connect(node.brokerurl ,node.options);
                    node.client.setMaxListeners(0);
                    // Register successful connect or reconnect handler
                    node.client.on('connect', function (connack) {
                        node.connecting = false;
                        node.connected = true;
                        node.log(RED._("mqtt-sparkplug-plus.state.connected",{broker:(node.clientid?node.clientid+"@":"")+node.brokerurl}));
                        node.log(node.prdMode ? "Production Mode" : "Development Mode")
                        for (var id in node.users) {
                            if (node.users.hasOwnProperty(id)) {
                                let state = node.enableStoreForward && node.primaryScadaStatus === "OFFLINE"  && node.users[id].shouldBuffer === true ? "BUFFERING" : "CONNECTED";
                                node.setConnectionState(node.users[id], state);
                            }
                        }

                        // Not sure if connect will be called after a reconnect?? Need to check and delete if not needed
                        node.emptyQueue();
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

                        // Subscribe to Primary SCADA status if store forward is enabled.
                        if (node.enableStoreForward === true) {
                            let options = { qos: 0 };

                            // SPb 2.0 Support
                            let primaryScadaTopic = `STATE/${node.primaryScada}`;
                            node.subscribe(primaryScadaTopic,options,function(topic_,payload_,packet) {
                                let status = payload_.toString();
                                node.primaryScadaStatus = status;
                                for (var id in node.users) {
                                    if (node.users.hasOwnProperty(id)) {
                                        let state = node.enableStoreForward && node.primaryScadaStatus === "OFFLINE"  && node.users[id].shouldBuffer === true ? "BUFFERING" : "CONNECTED";
                                        node.setConnectionState(node.users[id], state);
                                    }
                                }
                                node.emptyQueue();
                            });

                            // SPb 3.0 Support
                            let primaryScadaTopicv3 = `spBv1.0/STATE/${node.primaryScada}`;
                            node.subscribe(primaryScadaTopicv3,options,function(topic_,payload_,packet) {
                                let payload = payload_.toString();

                                try {
                                    var pss = JSON.parse(payload);
                                    node.primaryScadaStatus = pss.hasOwnProperty("online") ? (pss.online ? "ONLINE" : "OFFLINE") : "OFFLINE";
                                } catch{
                                    node.warn("Invalid Primary SCADA State:" + payload)
                                    node.primaryScadaStatus = "OFFLINE";
                                }

                                for (var id in node.users) {
                                    if (node.users.hasOwnProperty(id)) {
                                        let state = node.enableStoreForward && node.primaryScadaStatus === "OFFLINE"  && node.users[id].shouldBuffer === true ? "BUFFERING" : "CONNECTED";
                                        node.setConnectionState(node.users[id], state);
                                    }
                                }
                                node.emptyQueue();
                            });
                        }
                        // Send Node Birth
                        node.sendBirth();
                        node.nextBdseq(); // Next connect will use next bdSeq
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

                                let bMsg = this.getDeathPayload();
                                if(bMsg) {
                                    node.publish(bMsg, !this.shouldBuffer, f => {});  // send the message
                                }

                                node.sendBirth();
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
                    node.client.removeListener('message',sub[ref].handler);
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

            if (node.connected && (!node.enableStoreForward || (node.primaryScadaStatus === "ONLINE" && node.queue.length === 0) || bypassQueue)) {
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
                if (node.queue.length === node.maxQueueSize) {
                    node.queue.shift();
                    node.context().set("queue", node.queue);
                    //console.log("Queue Size", node.queue.length);
                }else if (node.queue.length  === node.maxQueueSize-1) {
                    node.warn(RED._("mqtt-sparkplug-plus.errors.buffer-full"));
                }
                node.queue.push(msg);
                node.context().set("queue", node.queue);
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
                        payload = maybeDecompressPayload(sparkplugDecode(payload));

                        var msg = {topic:topic, payload:payload, qos:packet.qos, retain:packet.retain};
                        node.send(msg);
                    } catch (e) {
                        node.error(RED._("mqtt-sparkplug-plus.errors.unable-to-decode-message", {type : "", error: e.toString()}));
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

                        try{
                            if (this.brokerConn.compressAlgorithm) {
                                msg.payload =  compressPayload(msg.payload, { algorithm : this.brokerConn.compressAlgorithm});
                            }
                        }
                        catch (e) {
                            this.warn(RED._("mqtt-sparkplug-plus.errors.unable-to-encode-message", { error: e.toString()}));
                        }

                        try {
                            msg.payload =  sparkplugEncode(msg.payload);
                            this.brokerConn.publish(msg, !this.shouldBuffer, done);  // send the message
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
