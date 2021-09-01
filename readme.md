# MQTT Sparkplug implementation for Node-Red

MQTT-Sparkplug-Plus is a set of Node-Red nodes, that will enable Node-Red to act as a [Sparkplug complient](https://s3.amazonaws.com/ignition-modules/Current/Sparkplug+Specification.pdf) EoN Node. 

The nodes will take care of most of the protocol specific messages.

The client will connect to an MQTT broker (server) and act as an MQTT Edge of Network (EoN) Node. The client can currently handle the following messages:
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
