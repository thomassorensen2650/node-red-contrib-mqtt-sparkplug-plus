# MQTT Sparkplug implementation for Node-Red

MQTT-Sparkplug-Plus is a set of Node-Red nodes that will enable Node-Red to act as a (Sparkplug complient)<https://s3.amazonaws.com/ignition-modules/Current/Sparkplug+Specification.pdf> SoN Node using the Sparkplug Specification from Cirrus Link Solutions.


The goal with this client is to make it as simple and painless as possible to use MQTT Sparkplug B with Node-Red. The client will try to take care of as much of the protocol specific messages as possible.

The client will connect to an MQTT Server and act as an MQTT Edge of Network (EoN) Node. The client current handles the following features:
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
