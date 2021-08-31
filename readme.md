# MQTT Sparkplug implementation for Node-Red

MQTT-Sparkplug-Plus is a Node-Red client that will enable Node-Red to act as a EoN Node using the Sparkplug Specification from Cirrus Link Solutions.

<https://s3.amazonaws.com/ignition-modules/Current/Sparkplug+Specification.pdf>

The focus of this client is to make it as simple as possible to use MQTT Sparkplug B with Node-Red. The client will try to take care of as much of the protocol specific messages.

The client will connect to an MQTT Server and act as an MQTT Edge of Network (EoN) Node. It will publish birth certificates (NBIRTH/DBIRTH)), node data messages (NDATA), and process node command messages (NCMD) that have been sent from another MQTT client. It will also send out death certificates (NDEATH).

The client also provides a interface for Node-Red messags to publish NDATA requrest. 

Pull requrest are welcome.
