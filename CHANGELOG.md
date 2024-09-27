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
