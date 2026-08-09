#!/usr/bin/env bash
#
# Start a HiveMQ broker with the Eclipse Sparkplug TCK extension loaded.
#
# Used both locally and by .github/workflows/tck.yml. The TCK's own Gradle task
# (runHivemqWithExtension) pins HiveMQ CE 2021.1, whose bundled JNA has no arm64
# native library - it dies on Apple Silicon. We download a current HiveMQ CE
# instead, which runs on both arm64 and x86_64.
#
# Usage:
#   test/tck/start-broker.sh <path-to-sparkplug-checkout>
#
# Env:
#   TCK_BROKER_CPUS   how many processors the broker JVM sizes its thread pools
#                     for, independently of the machine's core count (default 8).
#                     See the JAVA_OPTS comment at the bottom - this is what
#                     stops the TCK deadlocking against itself on a small runner.
#
# Runs in the foreground; Ctrl-C to stop.

set -euo pipefail

SPARKPLUG_DIR="${1:-ref/sparkplug}"
HIVEMQ_VERSION="${HIVEMQ_VERSION:-2024.9}"
WORK_DIR="${TCK_WORK_DIR:-.tck-broker}"
CACHE_DIR="${TCK_CACHE_DIR:-$WORK_DIR/cache}"
# Overridable so the TCK broker can coexist with a mosquitto already on 1883
# (the mocha suite's broker). Keep TCK_BROKER in run-tck.js in sync.
#
# Careful: this only moves the listener. The TCK's own utility clients hardcode
# tcp://localhost:1883 (HostApplication.java), so on any other port its simulated
# host connects to whatever else happens to be on 1883 - it will even log
# "successfully created" - and the node under test never sees STATE. Only change
# this if nothing else is listening on 1883.
HIVEMQ_PORT="${HIVEMQ_PORT:-1883}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXTENSION_ZIP="$SPARKPLUG_DIR/tck/build/hivemq-extension/sparkplug-tck-3.0.0.zip"

if [ ! -f "$EXTENSION_ZIP" ]; then
    echo "TCK extension not built. Run:"
    echo "  (cd $SPARKPLUG_DIR && ./gradlew -p tck hivemqExtensionZip)"
    echo ""
    echo "Note: the tck/ directory ships a gradlew without its wrapper jar, so the"
    echo "root wrapper with -p tck is the way to build it."
    exit 1
fi

mkdir -p "$CACHE_DIR"
HIVEMQ_ZIP="$CACHE_DIR/hivemq-ce-$HIVEMQ_VERSION.zip"

if [ ! -f "$HIVEMQ_ZIP" ]; then
    echo "Downloading HiveMQ CE $HIVEMQ_VERSION..."
    curl -fsSL -o "$HIVEMQ_ZIP" \
        "https://github.com/hivemq/hivemq-community-edition/releases/download/$HIVEMQ_VERSION/hivemq-ce-$HIVEMQ_VERSION.zip"
fi

HIVEMQ_HOME="$WORK_DIR/hivemq-ce-$HIVEMQ_VERSION"
if [ ! -d "$HIVEMQ_HOME" ]; then
    echo "Unpacking HiveMQ CE $HIVEMQ_VERSION..."
    unzip -q "$HIVEMQ_ZIP" -d "$WORK_DIR"
fi

# Install / refresh the TCK extension.
rm -rf "$HIVEMQ_HOME/extensions/sparkplug-tck"
unzip -q -o "$EXTENSION_ZIP" -d "$HIVEMQ_HOME/extensions"

sed "s|<port>1883</port>|<port>$HIVEMQ_PORT</port>|" \
    "$SCRIPT_DIR/hivemq-config.xml" > "$HIVEMQ_HOME/conf/config.xml"

# The TCK appends to its results log, so a previous failing run would otherwise
# be reported again. The UserGuide calls this out explicitly.
rm -f "$HIVEMQ_HOME/bin/SparkplugTCKResults.log"

# The TCK extension connects a Paho client back into this broker from inside its
# own publish interceptor: PublishInterceptor.onInboundPublish -> TCK.newTest ->
# HostApplication.hostPrepare -> host.connect(). HiveMQ does not ack the publish
# that triggered it until the interceptor returns, so the connect has to be
# serviced by some thread other than the one blocked waiting for it. HiveMQ sizes
# those pools from Runtime.availableProcessors(); on a 2-vCPU CI runner there is
# nothing spare and the self-connect deadlocks until Paho gives up ~120s later,
# taking the test with it ("Error starting test edge.<name>").
#
# Sizing the pools independently of the core count is enough. Measured in a
# 2-CPU Linux container: with ActiveProcessorCount=2 host creation hits the ~120s
# timeout every time; at 8 it completes in ~390ms and the test passes in 4s.
# run.sh appends to whatever JAVA_OPTS it inherits.
export JAVA_OPTS="${JAVA_OPTS:-} -XX:ActiveProcessorCount=${TCK_BROKER_CPUS:-8}"

echo "Starting HiveMQ CE $HIVEMQ_VERSION with the Sparkplug TCK extension on port $HIVEMQ_PORT..."
exec "$HIVEMQ_HOME/bin/run.sh"
