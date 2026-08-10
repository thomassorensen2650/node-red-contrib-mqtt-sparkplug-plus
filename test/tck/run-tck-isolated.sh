#!/usr/bin/env bash
#
# Drive the Sparkplug TCK one test at a time, each against a freshly started broker.
#
# Why not just run all five in one go (which is what `node test/tck/run-tck.js`
# with no arguments does)? Because the TCK's Monitor keeps per-edge-node state -
# notably the `edgeBdSeqs` map it uses for the bdSeq-increment assertions - for
# the whole broker session, and clears only `testResults` between tests. Earlier
# tests therefore decide later tests' results. Measured over ten historical runs
# of unchanged code, a shared broker produced anywhere from 1/5 to 4/5 passing
# and never once 5/5; one broker per test produced 5/5 repeatably.
#
# Usage:  test/tck/run-tck-isolated.sh [path-to-sparkplug-checkout]
#
# Env:
#   TCK_ATTEMPTS        attempts per test, each with a fresh broker (default 2)
#   TCK_BROKER_SETTLE   seconds to wait after the listener is up (default 5)
#   TCK_RESULTS_DIR     where per-test result JSON is written (default tck-results)
#   TCK_LOG_DIR         where per-test broker logs are written (default tck-logs)
#
# Exits non-zero if any test failed every attempt.

set -uo pipefail

SPARKPLUG_DIR="${1:-ref/sparkplug}"
ATTEMPTS="${TCK_ATTEMPTS:-2}"
SETTLE="${TCK_BROKER_SETTLE:-5}"
RESULTS_DIR="${TCK_RESULTS_DIR:-tck-results}"
LOG_DIR="${TCK_LOG_DIR:-tck-logs}"
BROKER_PORT="${HIVEMQ_PORT:-1883}"

TESTS=(
    SessionEstablishmentTest
    SessionTerminationTest
    SendDataTest
    SendComplexDataTest
    ReceiveCommandTest
    PrimaryHostTest
)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$RESULTS_DIR" "$LOG_DIR"

# bash's /dev/tcp rather than nc, which is not guaranteed to be installed.
port_in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$BROKER_PORT") >/dev/null 2>&1; }

stop_broker() {
    pkill -f "hivemq" >/dev/null 2>&1
    # HiveMQ also binds an RMI/JMX port; starting a new JVM before the old one
    # has fully exited dies with a BindException. Wait for both the process and
    # the MQTT port to go away.
    for _ in $(seq 1 30); do pgrep -f "hivemq" >/dev/null 2>&1 || break; sleep 1; done
    for _ in $(seq 1 30); do port_in_use || break; sleep 1; done
}

# $1 = log file to write
start_broker() {
    local log="$1"
    stop_broker
    : > "$log"
    nohup "$SCRIPT_DIR/start-broker.sh" "$SPARKPLUG_DIR" > "$log" 2>&1 &
    for _ in $(seq 1 90); do
        if grep -q "Started TCP Listener" "$log" 2>/dev/null; then
            # The listener is the last startup line the broker prints, but the
            # extension's own utility clients settle a moment later and a test
            # driven before that fails in setup.
            sleep "$SETTLE"
            return 0
        fi
        if grep -q "Could not bind any listener\|BindException\|Shutdown completed" "$log" 2>/dev/null; then
            return 1
        fi
        sleep 2
    done
    return 1
}

echo "Sparkplug TCK - Edge Node profile, one broker per test"
echo "  sparkplug : $SPARKPLUG_DIR"
echo "  attempts  : $ATTEMPTS"
echo ""

declare -a FAILED=()
for t in "${TESTS[@]}"; do
    passed=0
    for attempt in $(seq 1 "$ATTEMPTS"); do
        log="$LOG_DIR/hivemq-$t-$attempt.log"
        if ! start_broker "$log"; then
            echo "  $t: broker failed to start (attempt $attempt)"
            tail -20 "$log" || true
            continue
        fi
        # A retry MUST get a fresh broker for the same reason the tests do -
        # re-running against the broker that just saw the failure is contaminated.
        if TCK_RESULTS_FILE="$RESULTS_DIR/$t.json" \
           node "$SCRIPT_DIR/run-tck.js" "$t" 2>&1 | sed "s/^/  [$t] /"; then
            if [ "$attempt" -gt 1 ]; then
                echo "  $t: PASS (attempt $attempt)"
            else
                echo "  $t: PASS"
            fi
            passed=1
            break
        fi
        echo "  $t: failed attempt $attempt of $ATTEMPTS"
    done
    [ "$passed" -eq 1 ] || FAILED+=("$t")
done

stop_broker

echo ""
echo "=== Summary ==="
for t in "${TESTS[@]}"; do
    if printf '%s\n' "${FAILED[@]:-}" | grep -qx "$t"; then
        echo "  FAIL  $t"
    else
        echo "  PASS  $t"
    fi
done
echo ""
echo "$(( ${#TESTS[@]} - ${#FAILED[@]} ))/${#TESTS[@]} passed"
echo "Per-test detail in $RESULTS_DIR/, broker logs in $LOG_DIR/"

[ "${#FAILED[@]}" -eq 0 ] || exit 1
