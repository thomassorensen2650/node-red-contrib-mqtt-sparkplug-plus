### 1.0.0: Major Release

Fixed:
- Null values are now correctly serialized.

New:
- Store Forward when primary SCADA is offline can be enabled
- Added documentation for the *mqtt-sparkplug-broker* configuration node

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