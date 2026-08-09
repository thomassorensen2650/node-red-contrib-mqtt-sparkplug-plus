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
them. Worse, the failure is quiet: the TCK connects to whatever *is* on 1883 and
logs `Host ... successfully created` regardless, so the only symptom is every
test timing out waiting for a birth.

**The TCK can deadlock against itself on a small machine.** Starting a test runs
entirely inside the TCK's publish interceptor:
`PublishInterceptor.onInboundPublish` -> `TCK.newTest` ->
`HostApplication.hostPrepare` -> `host.connect()` - a *blocking* Paho connect back
into the very broker running that interceptor. HiveMQ does not acknowledge the
triggering publish until the interceptor returns, so that connect has to be
serviced by some other thread. HiveMQ sizes those pools from
`Runtime.availableProcessors()`, and on a 2-vCPU CI runner there is nothing spare:
the connect stalls until Paho gives up ~120s later and the test dies with
`Error starting test edge.<name>`. All the harness sees is
`timed out ... publish to SPARKPLUG_TCK/TEST_CONTROL`, which looks like a slow
network and is nothing of the sort - it is localhost.

There is no known way to prevent this from outside the TCK, so the harness makes
it cheap instead: the control publish is bounded at 10s and a timeout fails the
attempt immediately, rather than continuing into a guaranteed NBIRTH timeout and
another blocked publish. A dead attempt costs ~10s instead of ~120s, and
`run-tck-isolated.sh` retries against a fresh broker. That is why `TCK_ATTEMPTS`
is 3 in CI: roughly half of first attempts stall, and retries have so far always
succeeded.

To recognise it in a broker log: `Creating new host "..."` with no following
`Host ... successfully created`, and `Error starting test edge.<name>` about two
minutes later if nothing kills the broker first.

**Tried and rejected:** `-XX:ActiveProcessorCount=8` on the broker JVM, on the
theory that HiveMQ sizes those pools from `Runtime.availableProcessors()`. It is
decisive in a 2-CPU Linux container - host creation goes from a ~120s timeout at
`=2` to ~390ms at `=8` - but it does **not** help on GitHub runners: CI run
31302191045 had the flag applied and still stalled on 3 of 5 first attempts. Don't
re-derive this. A thread dump of the broker at the moment of the stall is the next
useful step; guessing from the outside has failed twice.

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
assertion is `NOT EXECUTED` (`Results.java:229`). The two halves are independent:
`PASS` is the verdict, `but INCOMPLETE` is coverage. `PASS but INCOMPLETE` means
nothing failed and something was never exercised - it is not a partial failure.

Only two assertions cannot apply to this node at all: the MQTT 5.0 session and
NDEATH variants, since it connects with 3.1.1 and the `-311` variants are asserted
instead. Those are the entire `EXPECTED_NOT_EXECUTED` allowlist, so
`SessionEstablishmentTest` and `SessionTerminationTest` report `PASS but
INCOMPLETE` and the other four report a clean `PASS`.

Everything else the node supports is exercised, because an unexercised feature is
an assertion that never runs rather than one that passes:

- **Metric aliases** - `aliasMetrics: true`. The TCK only records
  `payloads-alias-uniqueness` inside `if (current.hasAlias())`, and
  `payloads-alias-birth-requirement` passes vacuously without them.
- **Templates** - the fixture defines `TckTemplate` and the device births an
  instance of it, so all ten `payloads-template-*` assertions run.
  `SendDataTest` went from `PASS but INCOMPLETE` (10 unexecuted) to `PASS`.
- **DataSets and property sets** - a `DataSet` metric and a property-bearing
  metric, which is what makes `SendComplexDataTest` (47 assertions) worth running.

The harness therefore does not key off `OVERALL` alone. It accepts a test when
`OVERALL` starts with `PASS`, no assertion failed, and every `NOT EXECUTED`
assertion is in the `EXPECTED_NOT_EXECUTED` allowlist in `run-tck.js` (each
entry carries the reason). Anything unexecuted for a reason not on that list
fails the run, so new coverage gaps stay visible instead of hiding behind
"INCOMPLETE".

Full per-assertion output is written to `tck-results.json`, and the TCK's own
log ends up in `.tck-broker/hivemq-ce-*/bin/SparkplugTCKResults.log`.

## Where the TCK is wrong, and what the fixture does about it

Three assertions read a Sparkplug *datatype code* as though it were a protobuf
*field number*. In each case the node emits what the specification text requires
and the TCK rejects it. The fixture works around them; the node is deliberately
**not** bent to fit, since emitting the shape the TCK wants would misreport
conformance.

- **`payloads-template-instance-members-data`** - the text says an instance in
  NDATA/DDATA "MAY include only a subset of the members", but
  `SendDataTest.checkInstance` requires the full set (`found && (DBIRTH||NDATA||DDATA)`)
  and `Utils.setResultIfNotFail` makes the FAIL sticky. The node publishes only
  changed sub-metrics, which is what the spec permits. The fixture updates every
  member in one DDATA so the partial instance equals the full one.
- **`payloads-propertyset-quality-value-type`** - the text says the type "MUST be
  a value of 3 which represents a Signed 32-bit Integer", which is what the node
  emits; `checkQualityCodeRequirement` compares it to
  `ValueCase.LONG_VALUE.getNumber()`, i.e. 4, so only an Int64 passes. `Quality`
  is an optional key, so the fixture omits it - and the TCK then passes both
  quality assertions by default.
- **`payloads-metric-propertyvalue-type-type` / `-value`** -
  `checkPropertiesValidType` runs the datatype through `ValueCase.forNumber()`,
  which is non-null only for 3..10. That rejects `String` (12) and `Boolean` (11),
  so an ordinary `engUnit: "rpm"` property fails. The fixture uses numeric
  property types only, which means string-valued properties cannot be covered here.

Also seen, and not a failure: `payloads-metric-datatype-not-req` returns `MAYBE`.
It is a SHOULD - datatype ought to be omitted from metrics in NDATA/NCMD/DDATA/DCMD
- and the node includes it. Legal, but a candidate if DDATA size ever matters.
