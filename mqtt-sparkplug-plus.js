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

module.exports = function(RED) {
    "use strict";
    var mqtt = require("mqtt");
    var spPayload = require('sparkplug-payload').get("spBv1.0");
    //var util = require("util");
    //var isUtf8 = require('is-utf8');
    var HttpsProxyAgent = require('https-proxy-agent');
    var url = require('url');

    // Problems to solve : 
    // When to send EoN Birth and Death
    // When to send DBirth (Solved)
    // 
    // Tasks: 
    // Handle NCMDs, DCMDs
    // 

    // MQTT Sparkplug B message

    function MQTTSparkplugDeviceNode(n) {
        RED.nodes.createNode(this,n);
        this.broker = n.broker;

        this.deviceGroup = n.deviceGroup||"Sparkplug Devices";
        this.name = n.name||"Sparkplug Device";

        this.birthMessageSend = false;
        this.latestMetrics = {};
        this.metrics = n.metrics || {};
        this.seq = 0;

        /**
         * 
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
         * @param {string} msgType the message type (DBIRTH, DDATA) 
         * @param {*} metrics The metrics to include in the payload
         * @returns a encoded sparkplug B message
         */
        this.createMsg = function(msgType, metrics, done) {
            let msg = {
                topic : `spBv1.0/${this.deviceGroup}/${msgType}/${this.name}/`,
                payload : {
                    timestamp : new Date().getTime(),
                    seq : this.nextSeq(), 
                    metrics : metrics
                }
            };
            try {
                msg.payload = this.encode(msg.payload); 
            }catch (e) {
                // TODO
                done(e);
                return null;
            }
            return msg;   
        };

        /**
         * 
         * @param {object} payload object to encode 
         * @returns a sparkplug B encoded Buffer
         */
        this.encode = function(payload) {
            return spPayload.encodePayload(payload);
        }
        
        /**
         * 
         * @param {Number[]} payload Sparkplug B encoded Payload
         * @returns {Object} decoded JSON object
         */
        this.decode = function(payload) {
            var buffer = Buffer.from(payload);
            return spPayload.decodePayload(buffer);
        }

        this.brokerConn = RED.nodes.getNode(this.broker);
        var node = this;
        if (this.brokerConn) {

            this.status({fill:"red",shape:"ring",text:"node-red:common.status.disconnected"});
            this.on("input",function(msg,send,done) {
                if (msg.hasOwnProperty("payload") && Array.isArray(msg.payload)) {

                    let _metrics = [];
                    msg.payload.forEach(m => {
                        if (!m.hasOwnProperty("name")){
                            node.warn("Each metric must have a name attribute")
                        } else if (!m.hasOwnProperty("value")) {
                            node.warn(`The metric ${m.name} can't be written without a value`);
                        } else if (this.metrics.hasOwnProperty(m.name)) {

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
                            node.warn(`The metric ${m.name} is not known by the device`);
                        }
                    });

                    // Send DBIRTH
                    if (!this.birthMessageSend) {
                        let readyToSend = Object.keys(this.metrics).every(m => this.latestMetrics.hasOwnProperty(m));
                        if (readyToSend) {
                            this.seq = 0;
                            let bMsg = this.createMsg("DBIRTH", Object.values(this.latestMetrics), done);
                            if(bMsg) {
                                this.brokerConn.publish(bMsg, done);  // send the message 
                                this.birthMessageSend = true;
                            }
                        }
                    }else if (_metrics.length > 0) {
                        // SEND DDATA
                        let dMsg = this.createMsg("DDATA", _metrics, done);
                        if (dMsg) {
                            this.brokerConn.publish(dMsg,done); 
                        }
                    }
                } else {
                        done();
                }
            }); // end input

            if (this.brokerConn.connected) {
                node.status({fill:"green",shape:"dot",text:"node-red:common.status.connected"});
            }

            // Ok
            if (node.brokerConn.willTopic) {
                node.warn("The last will configured will be removed... It's required for the MQTT Sparkplug connection");
            }
            

            node.brokerConn.register(node);

            // Handle Input Messages
            let options = { qos: 0 };
            let subscribeTopic = `spBv1.0/${this.deviceGroup}/DCMD/${this.name}/`;
            this.brokerConn.subscribe(subscribeTopic,options,function(topic_,payload_,packet) {
                
                try {
                    var msg = {
                        topic : topic_,
                        payload : node.decode(payload_)
                    };
                    node.send(msg);
                } catch (e) {
                    node.error(`Unable to decode DCMD Sparkplug message: ${e.toString()}`);
                }
            });
            this.on('close', function(done) {
                node.brokerConn.deregister(node, done);
            });
        } else {
            this.error(RED._("mqtt.errors.missing-config"));
        }
    }
    RED.nodes.registerType("mqtt sparkplug device",MQTTSparkplugDeviceNode);



    function matchTopic(ts,t) {
        if (ts == "#") {
            return true;
        }
        /* The following allows shared subscriptions (as in MQTT v5)
           http://docs.oasis-open.org/mqtt/mqtt/v5.0/cs02/mqtt-v5.0-cs02.html#_Toc514345522

           4.8.2 describes shares like:
           $share/{ShareName}/{filter}
           $share is a literal string that marks the Topic Filter as being a Shared Subscription Topic Filter.
           {ShareName} is a character string that does not include "/", "+" or "#"
           {filter} The remainder of the string has the same syntax and semantics as a Topic Filter in a non-shared subscription. Refer to section 4.7.
        */
        else if(ts.startsWith("$share")){
            ts = ts.replace(/^\$share\/[^#+/]+\/(.*)/g,"$1");

        }
        var re = new RegExp("^"+ts.replace(/([\[\]\?\(\)\\\\$\^\*\.|])/g,"\\$1").replace(/\+/g,"[^/]+").replace(/\/#$/,"(\/.*)?")+"$");
        return re.test(t);
    }
    
    function MQTTBrokerNode(n) {
        RED.nodes.createNode(this,n);

        // Configuration options passed by Node Red
        this.broker = n.broker;
        this.port = n.port;
        this.clientid = n.clientid;
        this.usetls = n.usetls;
        this.usews = n.usews;
        this.verifyservercert = n.verifyservercert;
        this.compatmode = n.compatmode;
        this.protocolVersion = n.protocolVersion;
        this.keepalive = n.keepalive;
        this.cleansession = n.cleansession;
        this.sessionExpiryInterval = n.sessionExpiry;
        this.topicAliasMaximum = n.topicAliasMaximum;
        this.maximumPacketSize = n.maximumPacketSize;
        this.receiveMaximum = n.receiveMaximum;
        this.userProperties = n.userProperties;//https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901116
        this.userPropertiesType = n.userPropertiesType;

        // Config node state
        this.brokerurl = "";
        this.connected = false;
        this.connecting = false;
        this.closing = false;
        this.options = {};
        this.queue = [];
        this.subscriptions = {};

        if (n.birthTopic) {
            this.birthMessage = {
                topic: n.birthTopic,
                payload: n.birthPayload || "",
                qos: Number(n.birthQos||0),
                retain: n.birthRetain=="true"|| n.birthRetain===true,
                //TODO: add payloadFormatIndicator, messageExpiryInterval, contentType, responseTopic, correlationData, userProperties
            };
            if (n.birthMsg) {
                setStrProp(n.birthMsg, this.birthMessage, "contentType");
                if(n.birthMsg.userProps && /^ *{/.test(n.birthMsg.userProps)) {
                    try {
                        setUserProperties(JSON.parse(n.birthMsg.userProps), this.birthMessage);
                    } catch(err) {}
                }
                n.birthMsg.responseTopic = n.birthMsg.respTopic;
                setStrProp(n.birthMsg, this.birthMessage, "responseTopic");
                n.birthMsg.correlationData = n.birthMsg.correl;
                setBufferProp(n.birthMsg, this.birthMessage, "correlationData");
                n.birthMsg.messageExpiryInterval = n.birthMsg.expiry
                setIntProp(n.birthMsg,this.birthMessage, "messageExpiryInterval")
            }
        }

        if (n.closeTopic) {
            this.closeMessage = {
                topic: n.closeTopic,
                payload: n.closePayload || "",
                qos: Number(n.closeQos||0),
                retain: n.closeRetain=="true"|| n.closeRetain===true,
                //TODO: add payloadFormatIndicator, messageExpiryInterval, contentType, responseTopic, correlationData, userProperties
            };
            if (n.closeMsg) {
                setStrProp(n.closeMsg, this.closeMessage, "contentType");
                if(n.closeMsg.userProps && /^ *{/.test(n.closeMsg.userProps)) {
                    try {
                        setUserProperties(JSON.parse(n.closeMsg.userProps), this.closeMessage);
                    } catch(err) {}
                }
                n.closeMsg.responseTopic = n.closeMsg.respTopic;
                setStrProp(n.closeMsg, this.closeMessage, "responseTopic");
                n.closeMsg.correlationData = n.closeMsg.correl;
                setBufferProp(n.closeMsg, this.closeMessage, "correlationData");
                n.closeMsg.messageExpiryInterval = n.closeMsg.expiry
                setIntProp(n.birthMsg,this.closeMessage, "messageExpiryInterval")
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
            this.warn(RED._("mqtt.errors.nonclean-missingclientid"));
        }

        // Build options for passing to the MQTT.js API
        this.options.clientId = this.clientid || 'mqtt_' + RED.util.generateId();
        this.options.username = this.username;
        this.options.password = this.password;
        this.options.keepalive = this.keepalive;
        this.options.clean = this.cleansession;
        this.options.reconnectPeriod = RED.settings.mqttReconnectTime||5000;
        if (this.compatmode == "true" || this.compatmode === true || this.protocolVersion == 3) {
            this.options.protocolId = 'MQIsdp';
            this.options.protocolVersion = 3;
        } else if ( this.protocolVersion == 5 ) {
            this.options.protocolVersion = 5;
            this.options.properties = {};
            this.options.properties.requestResponseInformation = true;
            this.options.properties.requestProblemInformation = true;
            if(this.userProperties && /^ *{/.test(this.userProperties)) {
                try {
                    setUserProperties(JSON.parse(this.userProperties), this.options.properties);
                } catch(err) {}
            }
            if (this.sessionExpiryInterval && this.sessionExpiryInterval !== "0") {
                setIntProp(this,this.options.properties,"sessionExpiryInterval");
            }
        }
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

        if (n.willTopic) {
            this.options.will = {
                topic: n.willTopic,
                payload: n.willPayload || "",
                qos: Number(n.willQos||0),
                retain: n.willRetain=="true"|| n.willRetain===true,
                //TODO: add willDelayInterval, payloadFormatIndicator, messageExpiryInterval, contentType, responseTopic, correlationData, userProperties
            };
            if (n.willMsg) {
                this.options.will.properties = {};

                setStrProp(n.willMsg, this.options.will.properties, "contentType");
                if(n.willMsg.userProps && /^ *{/.test(n.willMsg.userProps)) {
                    try {
                        setUserProperties(JSON.parse(n.willMsg.userProps), this.options.will.properties);
                    } catch(err) {}
                }
                n.willMsg.responseTopic = n.willMsg.respTopic;
                setStrProp(n.willMsg, this.options.will.properties, "responseTopic");
                n.willMsg.correlationData = n.willMsg.correl;
                setBufferProp(n.willMsg, this.options.will.properties, "correlationData");
                n.willMsg.willDelayInterval = n.willMsg.delay
                setIntProp(n.willMsg,this.options.will.properties, "willDelayInterval")
                n.willMsg.messageExpiryInterval = n.willMsg.expiry
                setIntProp(n.willMsg,this.options.will.properties, "messageExpiryInterval")
                this.options.will.payloadFormatIndicator = true;
            }
        }

        // console.log(this.brokerurl,this.options);

        // Define functions called by MQTT in and out nodes
        var node = this;
        this.users = {};

        this.register = function(mqttNode) {
            node.users[mqttNode.id] = mqttNode;
            if (Object.keys(node.users).length === 1) {
                node.connect();
            }
        };

        this.deregister = function(mqttNode,done) {
            delete node.users[mqttNode.id];
            if (node.closing) {
                return done();
            }
            if (Object.keys(node.users).length === 0) {
                if (node.client && node.client.connected) {
                    // Send close message
                    if (node.closeMessage) {
                        node.publish(node.closeMessage,function(err) {
                            node.client.end(done);
                        });
                    } else {
                        node.client.end(done);
                    }
                    return;
                } else {
                    node.client.end();
                    return done();
                }
            }
            done();
        };

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
                        node.topicAliases = {};
                        node.log(RED._("mqtt.state.connected",{broker:(node.clientid?node.clientid+"@":"")+node.brokerurl}));
                        if(node.options.protocolVersion == 5 && connack && connack.hasOwnProperty("properties")) {
                            if(typeof connack.properties == "object") {
                                //clean & assign all props sent from server.
                                setIntProp(connack.properties, node.serverProperties, "topicAliasMaximum", 0);
                                setIntProp(connack.properties, node.serverProperties, "receiveMaximum", 0);
                                setIntProp(connack.properties, node.serverProperties, "sessionExpiryInterval", 0, 0xFFFFFFFF);
                                setIntProp(connack.properties, node.serverProperties, "maximumQoS", 0, 2);
                                setBoolProp(connack.properties, node.serverProperties, "retainAvailable",true);
                                setBoolProp(connack.properties, node.serverProperties, "wildcardSubscriptionAvailable", true);
                                setBoolProp(connack.properties, node.serverProperties, "subscriptionIdentifiersAvailable", true);
                                setBoolProp(connack.properties, node.serverProperties, "sharedSubscriptionAvailable");
                                setIntProp(connack.properties, node.serverProperties, "maximumPacketSize", 0);
                                setIntProp(connack.properties, node.serverProperties, "serverKeepAlive");
                                setStrProp(connack.properties, node.serverProperties, "responseInformation");
                                setStrProp(connack.properties, node.serverProperties, "serverReference");
                                setStrProp(connack.properties, node.serverProperties, "assignedClientIdentifier");
                                setStrProp(connack.properties, node.serverProperties, "reasonString");
                                setUserProperties(connack.properties, node.serverProperties);
                                // node.debug("CONNECTED. node.serverProperties ==> "+JSON.stringify(node.serverProperties));//TODO: remove
                            }
                        }
                        for (var id in node.users) {
                            if (node.users.hasOwnProperty(id)) {
                                node.users[id].status({fill:"green",shape:"dot",text:"node-red:common.status.connected"});
                            }
                        }
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

                        // Send any birth message
                        if (node.birthMessage) {
                            node.publish(node.birthMessage);
                        }
                    });
                    node.client.on("reconnect", function() {
                        for (var id in node.users) {
                            if (node.users.hasOwnProperty(id)) {
                                node.users[id].status({fill:"yellow",shape:"ring",text:"node-red:common.status.connecting"});
                            }
                        }
                    });
                    //TODO: what to do with this event? Anything? Necessary?
                    node.client.on("disconnect", function(packet) {
                        //Emitted after receiving disconnect packet from broker. MQTT 5.0 feature.
                        var rc = packet && packet.properties && packet.properties.reasonString;
                        var rc = packet && packet.properties && packet.reasonCode;
                        //TODO: If keeping this event, do we use these? log these?
                    });
                    // Register disconnect handlers
                    node.client.on('close', function () {
                        if (node.connected) {
                            node.connected = false;
                            node.log(RED._("mqtt.state.disconnected",{broker:(node.clientid?node.clientid+"@":"")+node.brokerurl}));
                            for (var id in node.users) {
                                if (node.users.hasOwnProperty(id)) {
                                    node.users[id].status({fill:"red",shape:"ring",text:"node-red:common.status.disconnected"});
                                }
                            }
                        } else if (node.connecting) {
                            node.log(RED._("mqtt.state.connect-failed",{broker:(node.clientid?node.clientid+"@":"")+node.brokerurl}));
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

        this.subscriptionIds = {};
        this.subid = 1;
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

        this.unsubscribe = function (topic, ref, removed) {
            ref = ref||0;
            var sub = node.subscriptions[topic];
            // var _debug = `unsubscribe for topic ${topic} called... ` ; //TODO: remove
            if (sub) {
                // _debug += "sub found. " //TODO: remove
                if (sub[ref]) {
                    // debug(`this.unsubscribe - removing handler ref ${ref} for ${topic} `); //TODO: remove
                    // _debug += `removing handler ref ${ref} for ${topic}. `
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
                            // _debug += `calling client.unsubscribe to remove topic ${topic}` //TODO: remove
                            node.client.unsubscribe(topic);
                        }
                    }
                //}
            } else {
                // _debug += "sub not found! "; //TODO: remove
            }
            // node.debug(_debug); //TODO: remove

        };
        this.topicAliases = {};

        this.publish = function (msg,done) {
            if (node.connected) {
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
                //https://github.com/mqttjs/MQTT.js/blob/master/README.md#mqttclientpublishtopic-message-options-callback
                if(node.options.protocolVersion == 5) {
                    options.properties = options.properties || {};
                    setStrProp(msg, options.properties, "responseTopic");
                    setBufferProp(msg, options.properties, "correlationData");
                    setStrProp(msg, options.properties, "contentType");
                    setIntProp(msg, options.properties, "messageExpiryInterval", 0);
                    setUserProperties(msg.userProperties, options.properties);
                    setIntProp(msg, options.properties, "topicAlias", 1, node.serverProperties.topicAliasMaximum || 0);
                    setBoolProp(msg, options.properties, "payloadFormatIndicator");
                    //FUTURE setIntProp(msg, options.properties, "subscriptionIdentifier", 1, 268435455);
                    if (options.properties.topicAlias) {
                        if (!node.topicAliases.hasOwnProperty(options.properties.topicAlias) && msg.topic == "") {
                            done("Invalid topicAlias");
                            return
                        }
                        if (node.topicAliases[options.properties.topicAlias] === msg.topic) {
                            msg.topic = ""
                        } else {
                            node.topicAliases[options.properties.topicAlias] = msg.topic
                        }
                    }
                }

                node.client.publish(msg.topic, msg.payload, options, function(err) {
                    done && done(err);
                    return
                });
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

    RED.nodes.registerType("mqtt-sparkplug-broker",MQTTBrokerNode,{
        credentials: {
            user: {type:"text"},
            password: {type: "password"}
        }
    });

};