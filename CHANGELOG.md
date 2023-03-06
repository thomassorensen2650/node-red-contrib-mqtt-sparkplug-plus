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
