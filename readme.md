# MQTT Sparkplug implementation for Node-Red

MQTT-Sparkplug-Plus is a set of Node-Red nodes, that will enable Node-Red to communicate with other client over MQTT using the sparkplug b protocol. The package contains the followings nodes:

## mqtt sparkplug device
The *mqtt sparkplug device* act as a [Sparkplug B complient](https://s3.amazonaws.com/ignition-modules/Current/Sparkplug+Specification.pdf) EoN Node. 

The node will connect to an MQTT broker (server) and act as an MQTT Edge of Network (EoN) Node. 

The client will handle the following message types automatically: 

* NBIRTH
* DBIRTH
* NCMD : REBIRTH
* NDEATH

The following message types can be implemented by the user:

* DDATA (from node input)
* DCMD (send as output to Node-Red)
* DDEATH (set via a msg command)

The following sparkplug featues can also be handled by the node:
* Buffering when primary SCADA is not available
* Compression
* Metric Alias

### Input
One or more metrics can be written to the **mqtt sparkplug device** by passing the metrics details to the input of the **mqtt sparkplug device**. A birth message will not be send before all metrics have been received at least once. so make sure to pass all metrics as soon as possible on start up.

The **mqtt sparkplug device** expects the metrics in the following input payload format. DataTypes can be added, but it will also be added automaticly by the node if omitted. if a metric does not have a value then a "is_null" attribute is added to the metric. Timestamps are optional per the specification. If timetamp to the metrics are not supplied, the the current time will still be added to the DBIRTH message (nothing will be added to the DDATA). 

```javascript
msg.payload = {
    "metrics": [
        {
            "name": "testing/test1",
            "timestamp" : new Date(), // Timestamp  is optional.
            "value": 11
        },
        {
            "name": "testing/test2",
            "value": 12
        }
    ]
}
```

### Dynamic metric definitions

Metrics definitions should normally be setup via the UI, but in some cases its beneficial to configure the metrics via code. This can be done by configuring the metrics using the `msg.definition` attribute.

The following example shows a message that also sets the definition. __DO NOT__ include definition is each message, as it will trigger rebirth each time a valid `msg.definition` is processed by the node.

Setting metrics dynamicly also also allows you to set properties (like engineering units) for the metrics. This functionality is currently not supported when configuring metrics via the UI.

The example belows shows how to set definitions via code (payload is optional):
```javascript
msg = {
    "definition" : {
        "TEST/TEST" : {
            "dataType" : "Int32",
            "properties": {
                "engUnits": {
                    "type": "String",
                    "value": "inHg"
                }
			}, 
        }
    },
    "payload" : {
        "metrics" : [
        {
            "name" : "TEST/TEST",
            "value" : 5
        }]
    }
};
```

_If the definition set set after the NBIRTH has been sent, them a REBIRTH is issued to notify clients about the new definition._

### Commands

Commands can be used to force REBIRTH or to send DDEATH to a device.  Sending DDEATH is a good way to indicate that a connected device is offline. If a DDEANTH is send, a new birth message will be send on the next metric payload to the device or when a rebirth command is send.

Rebirth Example:
```javascript    
msg = {
    "command" : {
        "device" : {
            "rebirth" : true
        }
    }   
```

Death Example:
```javascript  
msg = {
    "command" : {
        "device" : {
            "death" : true
        }
    }   
```

## mqtt sparkplug in
The *mqtt sparkplug in* node makes it possible to subscribe to sparkplug b mqtt topics. The node is almost identical to the default node-red *mqtt in* node, but it will decode the sparkplug/protobuf messages and deliver them in json.

## mqtt sparkplug out
The *mqtt sparkplug in* node makes it possible to publish sparkplug b mqtt messages. The node almost identical to the default node-red *mqtt out* node, but it will encode the sparkplug/protobuf payload before sending message.

# Installation
npm install node-red-contrib-mqtt-sparkplug-plus

# Usage
The easiest way to get started is to start with the example that is provided with the module.

## From Example
1. Open the Node-Red Export Dialog
2. Select the Examples Tab
3. Navigate to Node-red-contrib-mqtt-sparkplug-plus, and select the Simple Device Example
4. Deploy changes to Node-Red
5. Press the "Send Metrics" Inject node to write metrics to the new device (This will trigger a DBIRTH and NDATA first and pressed and a NDATA each time after that)

 You'll need a MQTT broker running on your local computer

## Manual Configuration
1. Drag a **mqtt sparkplug device** to the Node-Red Runtime. 
2. Configure MQTT broker connection for the device
3. Configure the name (this will be the name used in the messages) and the metrics
4. Configure upstream node-red nodes to send metrics data to the **mqtt sparkplug device** 
5. Configure downstream node-red nodes to handle NCMDs (write commands)