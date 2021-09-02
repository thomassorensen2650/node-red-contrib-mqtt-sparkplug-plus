# MQTT Sparkplug implementation for Node-Red

MQTT-Sparkplug-Plus is a set of Node-Red nodes, that will enable Node-Red to act as a [Sparkplug complient](https://s3.amazonaws.com/ignition-modules/Current/Sparkplug+Specification.pdf) EoN Node. 

The client will connect to an MQTT broker (server) and act as an MQTT Edge of Network (EoN) Node. The client current handles the following messages:


* NBIRTH
* DBIRTH
* NCMD : REBIRTH
* NDEATH
* DDATA (from node input)
* DCMD (send as output to Node-Red)
* DDATA (Input to the node)

The following features are not yet supported:
* Non-metrics (body)
* MQTT Broker redundancy
* Buffering when primary SCADA is not available

# Installation

npm install node-red-contrib-mqtt-sparkplug-plus

# Usage
1. Drag a **mqtt sparkplug device** to the Node-Red Runtime. 
2. Configure MQTT broker connection for the device
3. Configure the name (this will be the name used in the messages) and the metrics
4. Configure upstream node-red nodes to send metrics data to the **mqtt sparkplug device** 
5. Configure downstream node-red nodes to handle NCMDs (write commands)

## Input
The **mqtt sparkplug device** expects the metrics in the following input payload format

```
msg.payload = {
    "metrics": [
        {
            "name": "testing/test1",
            "value": 11
        },
        {
            "name": "testing/test2",
            "value": 12
        }
    ]
}
```
