### Unreleased : session lifecycle conformance

These issues were catalogued by the `node-red-contrib-mqtt-sparkplug-plus-wmonitor`
fork (ISC, Michael Sadowski), which branched from v2.2.4 and fixed them there. They
were re-verified and fixed independently against this code base; each one has a test
in `test/sparkplug_lifecycle_spec.js` that fails without its fix.

**Behaviour change:** the configured MQTT protocol version is now actually applied.
It was read from the broker configuration but never passed to the MQTT client, so
every connection was MQTT 3.1.1 regardless of the setting. **A configuration already
set to MQTT 5.0 will now genuinely connect as 5.0** - if the broker treats the two
differently, that changes on upgrade. With 5.0 the Edge Node also sets Clean Start
and a Session Expiry Interval of 0, so no session outlives its connection
(tck-id-principles-persistence-clean-session-50).

**Behaviour change:** metric aliases are now allocated per device rather than per
metric name. Two devices under one Edge Node that share a metric name previously
received the *same* alias, which breaks the requirement that an alias be unique
across the Edge Node's entire set of metrics (tck-id-payloads-alias-uniqueness).
Alias numbers therefore differ from previous releases; they are republished in every
birth message, so subscribers pick the new mapping up automatically.

Fixed:
- bdSeq now wraps from 255 to 0. It previously reached 256 before wrapping, one step
  outside the permitted range.
- bdSeq is taken, and the matching NDEATH re-registered as the will, for *every*
  CONNECT - including the automatic reconnects MQTT.js performs on its own.
  Previously only a deliberate connect advanced it, so every session after the first
  reused the first session's bdSeq, which
  tck-id-topics-nbirth-bdseq-increment forbids.
- bdSeq is kept in global context instead of the config node's own context, which
  Node-RED clears on redeploy. Surviving a full process restart additionally needs
  `contextStorage` configured in `settings.js`; without it behaviour is unchanged.
- The will is registered with an explicit retained flag of false
  (tck-id-message-flow-edge-node-birth-publish-will-message-will-retained).
- A negative Int64 metric no longer decodes as a huge positive number. Sparkplug
  carries Int64 in an unsigned protobuf field and `sparkplug-payload` only converts
  it back when its own `instanceof Long` check matches - which it does not, because
  protobufjs builds the value from a different copy of `long`. -42 arrived as
  18446744073709551574.
- MQTT connection errors are reported instead of being silently discarded, so a
  rejected certificate or bad credentials is visible rather than presenting as a
  connection that never comes up. Repeats of the same error are logged once, and
  reported again after a successful connection in between.
- Every metric in an NBIRTH now carries a timestamp. Template definitions come
  straight from the configuration and had none
  (tck-id-payloads-name-birth-data-requirement).
- With MQTT 5.0 the DISCONNECT sent on an intentional shutdown carries the
  'Disconnect with Will Message' reason code (0x04), so the broker publishes the will
  as well rather than discarding it
  (tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt50).
- MQTT.js no longer queues QoS 0 publishes while offline and flushes them after a
  reconnect. They arrived after the new NBIRTH had reset the sequence number, so the
  host saw the sequence jump backwards and asked for a rebirth. Replaying buffered
  data is Store Forward's job, and it replays with current sequence numbers.

### 3.0.0 : Sparkplug TCK conformance

**Behaviour change:** the Primary Host ID ("Destination") is now independent of
Store Forward. Setting it makes the Edge Node withhold NBIRTH until that host
publishes an ONLINE STATE message, whether or not Store Forward is enabled;
previously it had no effect unless Store Forward was on. The editor used to hide
the field whenever Store Forward was unchecked, so a configuration saved by an
older version may carry a Primary Host ID that was never set deliberately and was
never visible. **If an Edge Node stops birthing after upgrading, open its broker
configuration and clear the Primary Host ID** - the field is always shown in
3.0.0, the node status reads "waiting for primary host", and it logs that it is
waiting. Store Forward now governs buffering only, and on its own - with no
Primary Host ID - it buffers indefinitely, which the editor now warns about.

Fixed:
- NBIRTH is sent once per MQTT session; a repeated STATE ONLINE no longer emits a
  second NBIRTH re-using the same bdSeq (tck-id-topics-nbirth-bdseq-increment).
- A DISCONNECT packet is now sent after the NDEATH on an intentional disconnect
  (tck-id-operational-behavior-edge-node-intentional-disconnect-packet), without
  breaking reconnect.
- DBIRTH is no longer published while NBIRTH is being withheld pending a Primary
  Host, which could announce a device under an edge node that had never birthed.
- A rename via `set_name` / `set_group` births again under the new identity.

Changed:
- NDEATH and DDEATH are now published when Node-RED shuts down or the flow is
  redeployed. Previously neither was: the node disconnected cleanly, and a clean
  DISCONNECT tells the broker to discard the Last Will, so subscribers saw an edge
  node simply go quiet and never learned it had gone offline
  (tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt311).
- When a configured Primary Host goes from ONLINE to OFFLINE, the Edge Node now
  publishes an NDEATH, drops the MQTT connection and reconnects, rather than
  staying connected and silent. Subscribers therefore see the edge node leave and
  re-birth around a host outage instead of receiving no signal at all.
- STATE messages carrying a timestamp older than the last one seen are ignored, so
  a retained or out-of-order STATE can no longer drive the Primary Host status
  backwards.
- The Last Will is registered at QoS 1 rather than QoS 0, so the broker is required
  to acknowledge the NDEATH it publishes on an unexpected disconnect.
- A DBIRTH payload now carries the same timestamp as the metrics inside it, rather
  than one taken at publish time a moment later.

Added:
- Two Primary Host node statuses, so a withheld NBIRTH is visible rather than
  looking like a hang: **waiting for primary host** (blue ring) when the broker is
  connected but the host is not yet ONLINE, and **buffering - primary host
  offline** (blue dot) when data is also being queued. The latter replaces the
  previous "destination offline" text.

### 2.2.4 : Maintenance Release
Fixed:
- Templates with DataSets would throw an error when trying to send NBIRTH.

### 2.2.3 : Maintenance Release
Fixed:
- #86 - EventEmitter memory leak
- #92 - Rebirth command break with metric alias

### 2.2.2 : Maintenance Release
Fixed:
- Issue with template sub-metrics and rebirth where cached metrics would not be in the DBirth

### 2.2.1 : Maintenance Release
Fixed:
- Issue where template names and metrics based on templates could only be 1 level deep.

### 2.2.0 : Feature Realse
- #93 - Fixed issue where Metrics in NBirth would not get timestamp assigned
- #71 - Added support for Templates (UDTs) with new UI view
- #89 - Added unit test to ensure that NDEATH did not include seq

### 2.1.11: Maintenance Release
Fixed:
- #80 - Node sends message content of 'NDEATH'
- #79 - metric timestamp not optional
- #68 - All metrics are now cloned before any modifications.
Added:
- Added support for string timestamps
  
### 2.1.10: Maintenance Release
Fixed:
- #68 - shaddow copy
- Rebirth should ignore alias.

### 2.1.9: Maintenance Release
Fixed:
- Fixed #64 - bdSeq increase on rebirth

### 2.1.8: Maintenance Release
Fixed:
- Fixed NBIRTH on Primary Application state change as reported in #65 

### 2.1.7: Maintenance Release
Fixed:
- Loosen the dependency requirement so that it now works with Node14 / Node-red 2.0. 

### 2.1.2: Maintenance Release
Added:
- Added check to verify DCMD topic is correct. (This should never happen, but just in case)
- 
### 2.1.1: Maintenance Release
Added:
- Added support for unsigned integers

Fixed:
- Issue where old MQTT topic will be used when Devices was renamed.
- Timestamp was not added to historical metric values

### 2.1.0: Maintenance Release
Added:
- Option to buffer when not conected

Fixed:
- Renamed primary SCADA to primary Application per. Sparkplug B. Spec.

### 2.0.1: Maintenance Release
Fixed:
- Dynamic DataSet fix

### 2.0.1: Maintenance Release
Fixed:
- Moved Broker Reconnect to Connection Tab
- Fixed incorrect information in documentation
- Fixed unit failed unit test.

### 2.0.0: Major Release

New:
- Added support for DataSets
- Redesigned Broker configuration UI
- Added support for manual connection of the EoN
- Added connect command for EoN
- Added set_name command for EoN node
- Added set_name for device
- Added set_group for EoN.
- Support for parameter sorting
- Updated all dependencies to newest versions. 

Fixed:
- MQTT In now converts seq from Long to Number
- Timestamps are now automaticly converted from Long to Date
- DCMD commands for the devices using aliases are not converted back to names correctly.
- Mqtt In node will only parse topic in the Sparkplug namespace (MQTT in can now be used for other topics 
than sparkplug B)

### 1.4.1: Maintenance Release

 Fixed:
- bdSeq now acts per v3 spec.

### 1.4.0: Maintenance Release

 New:
- Added Birth Immediately option to allow sending DBirth on start up
- Added support for Sparkplug B V3.0.0 style Primary SCADA STATE 

### 1.3.2: Maintenance Release

 New:
- Added support metric alias


### 1.3.1: Maintenance Release

 New:
- Added support for device command (rebirth and death)

Fixed:
- Fixed minor issue that would make close on node-red redeply timeout.

### 1.3.0: Maintenance Release

 New:
- _mqtt sparkplug device_ added support for metric properties (property sets)

### 1.2.0: Maintenance Release

 Fixed:
- _mqtt sparkplug device_ datatype were not added to UI.


### 1.2.0: Maintenance Release
New:
 - _mqtt sparkplug device_ node now supports dynanic metrics (metrics can be defined via msg.definition)
 
 Fixed:
- _mqtt sparkplug device_ rebirth now sends correct NDEATH before NBIRTH

### 1.1.0: Maintenance Release

New:
 - _mqtt sparkplug in_ and _mqtt sparkplug device_ node now supports compression (DEFLATE and GZIP)
 - _mqtt sparkplug out_ supports topic defined in input message
 - Invalid mesasges to _mqtt sparkplug out_ without metric types, are not caught and a more friendly error message is now shown.

### 1.0.1: Maintenance Release

Fixed:
- Added missing dependency to MQTT that caused issues loading the nodes on some systems. 

### 1.0.0: Major Release

Fixed:
- Null values are now correctly serialized.

New:
- Store Forward when primary SCADA is offline can be enabled
- Added documentation for the *mqtt-sparkplug-broker* configuration node
- Added new *mqtt sparkplug out* node

### 0.0.5: Maintenance Release

Fixed:
 - Updated documentation
 - Standadized how invalid responses are handled
 - Unit tests coverage is now 80%-90%
 - majority of texts are from message catalog (i18n)

New:
 - Added _mqtt sparkplug in_ node (clone of mqtt in with sparkplug decoding)
 - Udated colors and logos of nodes.

### 0.0.4: Maintenance Release

Fixed
 - Removed dead code
 - Updated documentation with Optional Metrics timestamp (#1)
 - Moved more messages to message catalog (internationalization)
 - Support for metrics with NULL values
 - Added this change log
 - MQTT lib buffer functionality is now enabled. This will enable buffering of messages when the node is not connected to a broker (It still need to connect before it starts buffering).
 - started adding unit tests (very limited coverage)

#### 0.0.3: Initial Release

 - First released version. 
