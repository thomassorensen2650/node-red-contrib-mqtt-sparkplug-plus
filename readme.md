# MQTT Sparkplug implementation for Node-Red

[![codecov](https://codecov.io/gh/thomassorensen2650/node-red-contrib-mqtt-sparkplug-plus/branch/main/graph/badge.svg)](https://codecov.io/gh/thomassorensen2650/node-red-contrib-mqtt-sparkplug-plus)

MQTT-Sparkplug-Plus is a set of Node-Red nodes, that will enable Node-Red to communicate with other client over MQTT using the sparkplug b protocol. The package contains the followings nodes:

## mqtt sparkplug device
The *mqtt sparkplug device* act as a [Sparkplug B complient](https://s3.amazonaws.com/ignition-modules/Current/Sparkplug+Specification.pdf) EoN Node. 

The node will connect to an MQTT broker (server) and act as an MQTT Edge of Network (EoN) Node. 

The client will handle the following message types automatically: 

* NBIRTH
* DBIRTH
* NCMD : REBIRTH
* NDEATH — as the MQTT Last Will, and published explicitly when Node-Red shuts
  down or the flow is redeployed
* DDEATH — published for each born device on shutdown or redeploy

The following message types can be implemented by the user:

* DDATA (from node input)
* DCMD (send as output to Node-Red)
* DDEATH (set via a msg command)

The following sparkplug featues can also be handled by the node:
* [Primary Host](#primary-host-destination) — withhold NBIRTH until a nominated host is ONLINE
* Store Forward — buffer data while that host is offline
* Compression
* Metric Alias
* [Templates / UDTs](#templates-udts)

### Input
One or more metrics can be written to the **mqtt sparkplug device** by passing the metrics details to the input of the **mqtt sparkplug device**. The default behaviour is to wait for a value for each of the metrics before sending birth message. so make sure to pass all metrics on start up. This functionality can be disabled by selecting **Send Birth Immediately** on the advanted settings for the device.


A birth message will not be send before all metrics have been received at least once. so make sure to pass all metrics as soon as possible on start up.

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

Example sending DataSet:
```javascript
msg.payload = {
    "metrics": [
        {
            "name": "a",
            "value": {
                "numOfColumns": 2,
                "types": [
                    "String",
                    "String"
                ],
                "columns": [
                    "Col1",
                    "OtherCol"
                ],
                "rows": [
                    [
                        "a",
                        "A"
                    ],
                    [
                        "v",
                        "B"
                    ]
                ]
            }
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

### Primary Host (Destination)

The broker config node has a **Primary Host ID** field (labelled *Destination*).
Setting it makes the Edge Node hold back its NBIRTH until that host application
publishes an ONLINE `STATE` message, so no data is announced to a SCADA system that
is not listening. Leave it empty and the node births as soon as it connects.

**Primary Host and Store Forward are independent.** The Primary Host ID governs
*whether the node births*; Store Forward governs *whether data is buffered* while
that host is offline. You can use either on its own:

| Primary Host ID | Store Forward | Behaviour |
|---|---|---|
| empty | off | Births on connect, publishes immediately |
| empty | on | **Not a useful combination.** With no host to go ONLINE the node treats it as permanently offline and buffers data indefinitely. The editor warns about this. |
| set | off | Waits for ONLINE STATE before NBIRTH; data sent while the host is offline is dropped |
| set | on | Waits for ONLINE STATE before NBIRTH; data sent while the host is offline is queued and flushed when it returns |

While a configured host is offline the node also publishes an NDEATH and reconnects,
so subscribers see it leave and re-birth around the outage rather than going quiet.

> **Upgrading to 3.0.0 — if an Edge Node stops birthing, check this field.** Before
> 3.0.0 the Primary Host ID had no effect unless Store Forward was enabled, and the
> editor hid the field whenever Store Forward was unchecked. A configuration saved
> back then can therefore carry a Primary Host ID that was never set deliberately and
> was never visible — and which now withholds NBIRTH. In 3.0.0 the field is always
> shown: open the broker configuration, clear it, and the node births on connect
> again. A node in this state reads *waiting for primary host* and logs that it is
> waiting.

### Node status

| Status | Meaning |
|---|---|
| 🟢 connected | Connected to the broker and publishing |
| 🔴 disconnected | Not connected to the broker |
| 🟡 connecting | Connection attempt in progress |
| 🔵 waiting for primary host (ring) | Broker connected, but the configured Primary Host is not ONLINE, so NBIRTH is withheld |
| 🔵 buffering - primary host offline (dot) | As above, and Store Forward is queueing data meanwhile |
| ⚪ awaiting connect command | Manual birth mode; waiting for `msg.command.node.connect` |

### Templates (UDTs)

Templates (also called User Defined Types) allow you to define reusable complex data structures. A Template Definition is published in the **NBIRTH** message so subscribers know the structure upfront. Devices then publish **Template Instances** — metrics whose value is a typed set of sub-metrics matching that definition.

#### 1. Define the template on the broker config node

Open the broker config node, go to the **Advanced** tab and click **Add Template**. Fill in:

| Field | Example | Notes |
|---|---|---|
| Name | `MotorType` | Referenced by devices as the metric dataType |
| Version | `1.0.0` | Optional, but recommended |
| Metrics | `speed` / `Int32`, `torque` / `Float` | One row per member metric |
| Parameters | `unit` / `String` / `rpm` | Optional default parameter values |

The definition is automatically included in every NBIRTH message.

#### 2. Add a template-typed metric to the device node

In the **mqtt sparkplug device** node, add a metric and select your template name from the **Templates** group in the type dropdown (e.g. `MotorType`). The metric name (e.g. `motor`) becomes the root path for sending values.

#### 3. Send values using flat-path notation

Sub-metrics are addressed using `<metricName>/<memberName>` paths. The node builds the correct nested Template Instance automatically.

```javascript
// Send values for a metric named "motor" of type "MotorType"
// (definition has members: speed: Int32, torque: Float)
msg.payload = {
    metrics: [
        { name: "motor/speed",  value: 1450 },
        { name: "motor/torque", value: 12.3 }
    ]
};
```

- The **first message** that provides all top-level metrics triggers a **DBIRTH**. The Template Instance in the DBIRTH will contain *all* members from the definition (missing ones get a null value).
- Subsequent messages send a **DDATA** with a *partial* Template Instance containing only the members included in that message — as allowed by the Sparkplug B spec.

#### 4. Nested templates

A template metric can itself be of a template type. Use deeper slash paths to address it:

```javascript
// "pump" is of type "PumpType" which has:
//   - rpm: Int32
//   - motor: MotorType  (nested template with speed: Int32, torque: Float)
msg.payload = {
    metrics: [
        { name: "pump/rpm",           value: 3000 },
        { name: "pump/motor/speed",   value: 1450 },
        { name: "pump/motor/torque",  value: 12.3 }
    ]
};
```

#### 5. Dynamic metric definitions with templates

Templates can also be used when setting metrics via `msg.definition`. The dataType should be the template name:

```javascript
msg = {
    definition: {
        "motor": { dataType: "MotorType" },
        "status": { dataType: "String" }
    },
    payload: {
        metrics: [
            { name: "motor/speed",  value: 1450 },
            { name: "motor/torque", value: 12.3 },
            { name: "status",       value: "running" }
        ]
    }
};
```

#### What gets published

| Message | Template content |
|---|---|
| **NBIRTH** | Template Definition (`isDefinition: true`, no `templateRef`) with all member metrics |
| **DBIRTH** | Template Instance (`isDefinition: false`, `templateRef: "MotorType"`) with **all** members — nulls for any not yet received |
| **DDATA** | Template Instance with only the **changed** members (partial instance) |

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

Commands can also be used for dynamic configuration of the Devices and the EoN Nodes (MQTT Server Configuration).

A device can be renamed by setting the follow command. This is especially useful if you have a device with 0 metrics and dynamic metrics.

```javascript    
msg = {
    "command" : {
        "device" : {
            "set_name" : "NEW_NAME"
        }
    }   
```

A EoN Node (MQTT Server) can also be renamed using the following command. If the EoN Node is configuration to manual connection, then the rename command can be combined with a connect command to set the name and connect to the Broker with the new name:

```javascript    
msg = {
    "command" : {
        "node" : {
            "set_name" : "NEW_NAME",
            "set_group" : "NEW_GROUP",
            "connect" : true
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

## Running Unit Tests

The test suite requires a running MQTT broker. By default the tests connect to `mqtt://localhost`; set the `TEST_BROKER` environment variable to override the URL, including credentials if needed.

```bash
# Install dependencies first (only needed once)
npm install

# Run all tests against a broker with no authentication
npm test

# Run all tests with broker credentials
TEST_BROKER=mqtt://admin:admin@localhost npm test

# Run a single test file
TEST_BROKER=mqtt://admin:admin@localhost npx mocha test/sparkplug_device_template_spec.js

# Run a specific test by name
TEST_BROKER=mqtt://admin:admin@localhost npx mocha test/sparkplug_device_template_spec.js --grep "slashes"
```

## Sparkplug TCK

The nodes are checked against the [Eclipse Sparkplug TCK](https://github.com/eclipse-sparkplug/sparkplug)
Edge Node profile, which runs headlessly in CI on every pull request. It covers
session establishment and termination, data, commands, Primary Host behaviour and
complex types (Templates, DataSets and property sets).

```bash
test/tck/run-tck-isolated.sh ref/sparkplug
```

The harness needs port **1883** free — the TCK's own utility clients hardcode
`tcp://localhost:1883`, so it cannot be moved. See
[test/tck/README.md](test/tck/README.md) for setup, how results are interpreted,
and the known TCK issues the harness works around.

# Contributions
Contributions are welcome. Please discuss new features before creating PR, and please try to add unit test for new features if possible.
