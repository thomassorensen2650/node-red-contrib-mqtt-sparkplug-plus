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

    /**
     * Sparkplug dates are always send a Unix Time. This function attached the timestamp to the object
     * and converts in to unix time (EPOC) if required. If timestsamp is invalid, then the current time will be added
     * @param {object} object object to add timestamp to
     * @param {Date|Number} timestamp the timestamp to add
     * @returns Object with Timestamp
     */
    /*function addTimestampToObject(object, timestamp) {
        // 
        if (timestamp instanceof Date && !isNaN(timestamp)) {
            timestamp = timestamp.getTime();
        }else if (!isNaN(timestamp)){
            timestamp = new Date().getTime(); //TODO : We should add a warning here
        }
        object.timestamp = timestamp;
        return object;
    }; */

    /**
     * Sparkplug Encode Payload
     * @param {object} payload object to encode 
     * @returns a sparkplug B encoded Buffer
     */
    function sparkplugEncode(payload) {
        // return JSON.stringify(payload); // for debugging
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
        this.broker = n.broker;
        this.name = n.name||"Sparkplug Device";
        this.latestMetrics = {};
        this.metrics = n.metrics || {};
        this.birthMessageSend = false;
        
        this.shouldBuffer = true; // hardcoded / Devices always buffers
        /**
         * try to send Sparkplug Birth Messages
         * @param {function} done Node-Red Done Function 
         */
        this.trySendBirth = function(done) {           
            let readyToSend = Object.keys(this.metrics).every(m => this.latestMetrics.hasOwnProperty(m));
            if (readyToSend) {
                let bMsg = node.brokerConn.createMsg(this.name, "DBIRTH", Object.values(this.latestMetrics), done);
                if(bMsg) {
                    this.brokerConn.publish(bMsg, !this.shouldBuffer, done);  // send the message 
                    this.birthMessageSend = true;
                }
            }
        }

        this.brokerConn = RED.nodes.getNode(this.broker);
        var node = this;
        if (this.brokerConn) {
            this.on("input",function(msg,send,done) {
                if (msg.hasOwnProperty("payload") && typeof msg.payload === 'object' && msg.payload !== null && !Array.isArray(msg.payload)) {
                 
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

                        if (!this.brokerConn.connected) {
                            // we dont want to publish anything if we are not connected
                            // if we publish here, then the messages will be queued by the MQTT Client
                            // and we need NBIRTH to be seq 0
                        }
                        else if (!this.birthMessageSend) {    // Send DBIRTH
                            this.trySendBirth(done);
                        }else if (_metrics.length > 0) { // SEND DDATA
                            let dMsg = this.brokerConn.createMsg(this.name, "DDATA", _metrics, done);
                            if (dMsg) {
                        //        if (msg.payload.timestamp) {
                        //           addTimestampToObject(dMsg, msg.payload.timestamp)
                        //        }
                                this.brokerConn.publish(dMsg, !this.shouldBuffer, done); 
                            }
                        }
                    }else 
                    {
                        node.error(RED._("mqtt-sparkplug-plus.errors.device-no-metrics"));
                        done();
                    }
                } else {
                    node.error(RED._("mqtt-sparkplug-plus.errors.payload-type-object"));
                    done();
                }
            }); // end input
            node.brokerConn.register(node);
            
            // Handle DCMD Messages
            let options = { qos: 0 };
            let subscribeTopic = `spBv1.0/${this.brokerConn.deviceGroup}/DCMD/${this.brokerConn.eonName}/${this.name}`;
            this.brokerConn.subscribe(subscribeTopic,options,function(topic_,payload_,packet) {
                try {
                    var msg = {
                        topic : topic_,
                        payload : sparkplugDecode(payload_)
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
        this.deviceGroup = n.deviceGroup||"Sparkplug Devices";
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

        // Config node state
        this.brokerurl = "";
        this.connected = false;
        this.connecting = false;
        this.closing = false;
        this.options = {};
     
        this.subscriptions = {};

        this.seq = 0;

        // Get information about store forward
        this.enableStoreForward = n.enableStoreForward || false;
        this.primaryScada = n.primaryScada || "";

        // This will be set by primary SCADA and written via MQTT (OFFLINE or ONLINE)
        this.primaryScadaStatus = "OFFLINE";

        // Queue to store events while 
        this.queue = this.context().get("queue");
        if (!this.queue){
            this.queue = [];
        }

        /**
         * empties the current queue
         */
        this.emptyQueue = function() {
            if (node.primaryScadaStatus === "ONLINE" && node.connected) {
                var item = this.queue.shift();
                while (item) {
                    node.publish(item, true);
                    item = this.queue.shift();
                }
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
                case "BUFFERING": // Online´
                    node.status({fill:"blue",shape:"dot",text:"buffering"});
                    break;
                default:
                    node.status({fill:"gray",shape:"dot",text:state}); // Unknown State
            }
        }

        
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

            try {
                msg.payload = sparkplugEncode(msg.payload); 
            }catch (e) {
                node.error(RED._("mqtt-sparkplug-plus.errors.unable-to-encode-message", {type : msgType, error: e.toString()}));
                done(e);
                return null;
            }
            return msg;   
        };

        /**
         * 
         * @returns node death payload and topic
         */
        this.getDeathPayload = function() {
            let payload = {
                timestamp : new Date().getTime(),
                metric : [ {
                    name : "bdSeq", 
                    value : 0, 
                    type : "uint64"
                }]
            };
            let msg = {
                topic : `spBv1.0/${this.deviceGroup}/NDEATH/${this.eonName}`,
                payload : sparkplugEncode(payload),
                qos : 0,
                retain : false
            };
            return msg;
        };

        /**
         * Send Birth Message
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
                    "type" : "Int8",
                    "value": 0,
                }];
            var nbirth = node.createMsg("", "NBIRTH", birthMessageMetrics, x=>{});
            
            node.publish(nbirth);
            for (var id in node.users) {
                if (node.users.hasOwnProperty(id) && node.users[id].trySendBirth) {
                    node.users[id].birthMessageSend = false;
                    node.users[id].trySendBirth(x=>{});
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
        this.options.will = this.getDeathPayload();
        
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
                        node.client.end(done);
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
                        }
                        // Send Node Birth
                        node.sendBirth();
                    });

                    node.client.on("reconnect", function() {
                        for (var id in node.users) {
                            if (node.users.hasOwnProperty(id)) {
                                node.setConnectionState(node.users[id], "RECONNECT");
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
                payload = sparkplugDecode(payload);
                if (payload.hasOwnProperty("metrics") && Array.isArray(payload.metrics)){
                    payload.metrics.forEach(m => {
                        if (typeof m === 'object' && m.hasOwnProperty("name") && m.name) {
                            if (m.name.toLowerCase() === "node control/rebirth") {
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
                node.queue.push(msg);
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
                        payload = sparkplugDecode(payload);
                        var msg = {topic:topic, payload:payload, qos:packet.qos, retain:packet.retain};

                        if ((node.brokerConn.broker === "localhost")||(node.brokerConn.broker === "127.0.0.1")) {
                            msg._topic = topic;
                        }
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
        this.shouldBuffer = false; // hardcoded / buffering commands are a bad idea... if we enable, then it shnould come with a big warning.
        
        this.brokerConn = RED.nodes.getNode(this.broker);
        var node = this;

        if (this.brokerConn) {            
            this.on("input",function(msg,send,done) {

                // abort if not connected and node is not configured to buffer
                if (!node.brokerConn.connected && this.shouldBuffer !== true) {
                    console.log("Abort", node.brokerConn.connected, this.shouldBuffer);
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
                        msg.payload =  sparkplugEncode(msg.payload);
                        this.brokerConn.publish(msg, !this.shouldBuffer, done);  // send the message
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