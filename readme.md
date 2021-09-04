# MQTT Sparkplug implementation for Node-Red

MQTT-Sparkplug-Plus is a set of Node-Red nodes, that will enable Node-Red to act as a [Sparkplug B complient](https://s3.amazonaws.com/ignition-modules/Current/Sparkplug+Specification.pdf) EoN Node. 

The client will connect to an MQTT broker (server) and act as an MQTT Edge of Network (EoN) Node. The client current handles the following messages: 


* NBIRTH
* DBIRTH
* NCMD : REBIRTH
* NDEATH
* DDATA (from node input)
* DCMD (send as output to Node-Red)

The following features are not yet supported:
* Non-metrics (body)
* MQTT Broker redundancy
* Buffering when primary SCADA is not available

# Installation

npm install node-red-contrib-mqtt-sparkplug-plus

# Usage
The easiest way to get started is to start with the example that is provided with the module.

## From Example
1. Open the Node-Red Export Dialog
2. Select the Examples Tab
3. Navigate to Node-red-contrib-mqtt-sparkplug-plus, and select the Simple Device Example
4. Deploy changes to Node-Red
5. Press the "Send Metrics" Inject node to write metrics to the new device (You'll need a MQTT broker running on your local computer) 

## Manual Configuration
1. Drag a **mqtt sparkplug device** to the Node-Red Runtime. 
2. Configure MQTT broker connection for the device
3. Configure the name (this will be the name used in the messages) and the metrics
4. Configure upstream node-red nodes to send metrics data to the **mqtt sparkplug device** 
5. Configure downstream node-red nodes to handle NCMDs (write commands)

## Input
One or more metrics can be written to the **mqtt sparkplug device** by passing the metrics details to the input of the **mqtt sparkplug device**. A birth message will not be send before all metrics have been received at least once. so make sure to pass all metrics on start up.

The **mqtt sparkplug device** expects the metrics in the following input payload format. DataTypes can be added, but it will be added automaticly by the node if omitted. if a metric does not have a value then a "is_null" attribute is added to the metric. Timestamps are optional per the specification. If timetamp to the metrics are not supplied, the the current time will still be added to the DBIRTH message (nothing will be added to the DDATA). 

```
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
