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
# Runs in the foreground; Ctrl-C to stop.

set -euo pipefail

SPARKPLUG_DIR="${1:-ref/sparkplug}"
HIVEMQ_VERSION="${HIVEMQ_VERSION:-2024.9}"
WORK_DIR="${TCK_WORK_DIR:-.tck-broker}"
CACHE_DIR="${TCK_CACHE_DIR:-$WORK_DIR/cache}"
# Overridable so the TCK broker can coexist with a mosquitto already on 1883
# (the mocha suite's broker). Keep TCK_BROKER in run-tck.js in sync.
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

echo "Starting HiveMQ CE $HIVEMQ_VERSION with the Sparkplug TCK extension on port $HIVEMQ_PORT..."
exec "$HIVEMQ_HOME/bin/run.sh"
