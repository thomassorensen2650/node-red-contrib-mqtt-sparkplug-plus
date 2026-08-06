# Sparkplug TCK harness

Runs the [Eclipse Sparkplug TCK](https://github.com/eclipse-sparkplug/sparkplug)
(Edge Node profile) against the nodes in this repo, unattended.

The TCK is documented as a manual process driven from a Nuxt web console. It
doesn't have to be: the console is only an MQTT client, and the TCK's whole
control plane is plain MQTT topics (`SPARKPLUG_TCK/TEST_CONTROL`,
`SPARKPLUG_TCK/RESULT`, `SPARKPLUG_TCK/LOG`). `run-tck.js` speaks that protocol
directly and drives the nodes in-process with `node-red-node-test-helper`.

None of the Edge Node tests call the TCK's `prompt()` - only the Host profile
tests do - and the TCK brings its own simulated Host Application online, so
there is nothing for a human to do.

| File | Purpose |
|---|---|
| `run-tck.js` | the harness - drives the TCK and the nodes, exits non-zero on failure |
| `tck-flow.js` | parameterized Node-RED flow fixture |
| `start-broker.sh` | downloads HiveMQ, installs the TCK extension, runs it |
| `hivemq-config.xml` | broker config (TCP listener only, usage stats off) |

## Running locally

```bash
# 1. Clone the spec/TCK repo (once). CI does this itself at a pinned tag.
git clone --branch v3.0.0 https://github.com/eclipse-sparkplug/sparkplug.git ref/sparkplug

# 2. Build the TCK HiveMQ extension (once, ~1 min).
#    Note: tck/ ships a gradlew *without* its wrapper jar, so use the root
#    wrapper with -p tck.
(cd ref/sparkplug && ./gradlew -p tck hivemqExtensionZip)

# 3. Start the broker (terminal 1). Wait for "Started TCP Listener".
npm run tck:broker

# 4. Run the tests (terminal 2).
npm run tck                        # all five
npm run tck ReceiveCommandTest     # or just one
```

Useful env vars: `TCK_BROKER`, `TCK_VERBOSE=1`, `TCK_TEST_TIMEOUT_MS`,
`TCK_HOST_APP_ID`, `TCK_GROUP_ID`.

## Things that will bite you

**The broker must be on port 1883.** Not configurable. The TCK's internal
utility clients - the simulated Host Application and Edge Node it uses to drive
tests - construct `new HostApplication()`, which hardcodes
`tcp://localhost:1883` (`TCK.java:53`, `HostApplication.java:50`). There is no
system property or config for it. If something else owns 1883 (a mosquitto
container, say), the TCK will happily publish STATE to *that* broker instead and
every test will hang waiting for a birth that never comes.

`start-broker.sh` accepts `HIVEMQ_PORT` for convenience, but anything other than
1883 only works for tests that never need the simulated host - which is none of
them.

**Restart the broker between full runs.** The TCK's `Monitor` keeps a
per-edge-node `bdSeq` map for the lifetime of the broker process and only clears
`testResults` between tests, never that map. A second run of the same test would
see the node restart at bdSeq 0 and report a spurious
`topics-nbirth-bdseq-increment` failure. The harness already gives each test its
own edge node ID for this reason, but that only helps within one broker session.

**HiveMQ CE 2021.1 does not run on Apple Silicon.** The TCK's own
`runHivemqWithExtension` Gradle task pins it, and its bundled JNA has no arm64
native library (`UnsatisfiedLinkError` from `MacCentralProcessor`).
`start-broker.sh` uses a current HiveMQ CE instead, which runs on both
architectures.

**`payloads-dbirth-timestamp` / `topics-dbirth-timestamp` are timing-flaky on
loopback.** The TCK requires `payloadTimestamp < messageReceivedTime`, strictly
(`SessionEstablishmentTest.java:886`). The node stamps the payload and publishes
microseconds later, so over localhost the two land in the same millisecond often
enough that this assertion flips between runs. It is a TCK strictness artifact,
not a defect in the node. If CI proves noisy, re-run before investigating.

## What counts as a pass

The TCK appends `but INCOMPLETE` to its `OVERALL` line whenever a non-Monitor
assertion is `NOT EXECUTED`. Some assertions cannot apply to this node at all -
the MQTT 5.0 session variant (the node uses 3.1.1), metric aliases, and the
optional templates group - so `PASS but INCOMPLETE` is the best achievable
result for this configuration.

The harness therefore does not key off `OVERALL` alone. It accepts a test when
`OVERALL` starts with `PASS`, no assertion failed, and every `NOT EXECUTED`
assertion is in the `EXPECTED_NOT_EXECUTED` allowlist in `run-tck.js` (each
entry carries the reason). Anything unexecuted for a reason not on that list
fails the run, so new coverage gaps stay visible instead of hiding behind
"INCOMPLETE".

Full per-assertion output is written to `tck-results.json`, and the TCK's own
log ends up in `.tck-broker/hivemq-ce-*/bin/SparkplugTCKResults.log`.
