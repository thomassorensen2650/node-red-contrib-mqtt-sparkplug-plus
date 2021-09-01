# MQTT Sparkplug implementation for Node-Red

MQTT-Sparkplug-Plus is a set of Node-Red nodes, that will enable Node-Red to act as a [Sparkplug complient](https://s3.amazonaws.com/ignition-modules/Current/Sparkplug+Specification.pdf) EoN Node. 

The nodes will take care of most of the protocol specific messag. 

The client will connect to an MQTT broker (server) and act as an MQTT Edge of Network (EoN) Node. The client current handles the following features:
* NBIRTH
* DBIRTH
* NCMD : REBIRTH
* NDEATH
* DDATA (from node input)
* DCMD (send as output to Node-Red)

The following features are not supported yet:
* Non-metrics (body)
* MQTT Broker redundancy

Pull requrest are welcome.
