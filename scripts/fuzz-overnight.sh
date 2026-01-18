#!/bin/bash
#
# Overnight fuzz testing script for tinykvs
#
# Usage:
#   ./scripts/fuzz-overnight.sh              # Run all fuzz tests (~25min each, ~8h total)
#   ./scripts/fuzz-overnight.sh 1h           # Run each test for 1 hour
#   ./scripts/fuzz-overnight.sh 10m FuzzDecodeBlock  # Run specific test for 10 minutes
#   FUZZ_PARALLEL=8 ./scripts/fuzz-overnight.sh      # Use 8 workers (default: 4)
#
# The script will:
#   - Run all fuzz targets (or a specific one if specified)
#   - Save output to logs/fuzz-YYYY-MM-DD/
#   - Continue to next test if one finishes or crashes
#   - Print a summary at the end
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Default time per fuzz target (8 hours / 19 targets ≈ 25 minutes each)
TIME_PER_TARGET="${1:-25m}"
SPECIFIC_TARGET="${2:-}"

# Limit parallel workers to avoid memory exhaustion (default: 4)
FUZZ_PARALLEL="${FUZZ_PARALLEL:-4}"

# Create log directory
DATE=$(date +%Y-%m-%d-%H%M%S)
LOG_DIR="$PROJECT_ROOT/logs/fuzz-$DATE"
mkdir -p "$LOG_DIR"

# All fuzz targets
CORE_TARGETS="FuzzDecodeValue FuzzDecodeValueZeroCopy FuzzDecodeBlock FuzzValueRoundTrip FuzzDecodeEntry FuzzEntryRoundTrip FuzzBlockRoundTrip FuzzDecodeMsgpack FuzzRecordValueEncode FuzzStoreOperations FuzzPrefixCompare"

CMD_TARGETS="FuzzShellSQL FuzzShellJSONInsert FuzzShellCommands FuzzShellOrderBy FuzzShellBinaryFunctions FuzzShellAggregations FuzzShellNestedFields FuzzShellHexValues"

# Track results
RESULTS_FILE="$LOG_DIR/results.txt"
CRASHED_FILE="$LOG_DIR/crashed.txt"
touch "$RESULTS_FILE" "$CRASHED_FILE"
TOTAL_FINDINGS=0

# Cleanup on exit
cleanup() {
    echo ""
    echo "=========================================="
    echo "Fuzz testing interrupted or completed"
    echo "=========================================="
    print_summary
    exit 0
}
trap cleanup SIGINT SIGTERM

print_summary() {
    echo ""
    echo "=========================================="
    echo "FUZZ TESTING SUMMARY"
    echo "=========================================="
    echo "Log directory: $LOG_DIR"
    echo "Time per target: $TIME_PER_TARGET"
    echo ""

    if [ -s "$CRASHED_FILE" ]; then
        echo "⚠️  CRASHED TARGETS:"
        while read -r target; do
            echo "   - $target (see $LOG_DIR/$target.log)"
        done < "$CRASHED_FILE"
        echo ""
    fi

    echo "Results:"
    if [ -s "$RESULTS_FILE" ]; then
        cat "$RESULTS_FILE" | while read -r line; do
            echo "   $line"
        done
    fi

    echo ""
    echo "Total new corpus entries: $TOTAL_FINDINGS"
    echo ""
    echo "To view logs:"
    echo "   ls -la $LOG_DIR/"
    echo ""
    echo "To check for failures:"
    echo "   grep -l 'FAIL\|panic\|crash' $LOG_DIR/*.log 2>/dev/null || echo 'No failures found'"
}

run_fuzz_target() {
    local pkg="$1"
    local target="$2"
    local logfile="$LOG_DIR/$target.log"

    echo ""
    echo "=========================================="
    echo "Running $target for $TIME_PER_TARGET"
    echo "Package: $pkg"
    echo "Log: $logfile"
    echo "Started: $(date)"
    echo "=========================================="

    # Run fuzz test, capture output
    # Use ^...$ anchors for exact match (otherwise FuzzDecodeValue matches FuzzDecodeValueZeroCopy)
    # Use -parallel to limit workers and avoid memory exhaustion
    set +e
    go test -fuzz="^${target}\$" -fuzztime="$TIME_PER_TARGET" -parallel="$FUZZ_PARALLEL" -v "$pkg" 2>&1 | tee "$logfile"
    local exit_code=$?
    set -e

    # Check results
    if [ $exit_code -ne 0 ]; then
        echo "⚠️  $target exited with code $exit_code"
        echo "$target" >> "$CRASHED_FILE"
        echo "$target: CRASHED (exit $exit_code)" >> "$RESULTS_FILE"
    else
        # Count new corpus entries (use grep + wc to avoid issues with grep -c)
        local new_entries
        new_entries=$(grep "new interesting input" "$logfile" 2>/dev/null | wc -l | tr -d ' ')
        new_entries=${new_entries:-0}
        TOTAL_FINDINGS=$((TOTAL_FINDINGS + new_entries))
        echo "$target: OK (+$new_entries corpus entries)" >> "$RESULTS_FILE"
        echo "✓ $target completed (+$new_entries new entries)"
    fi

    echo "Finished: $(date)"
}

is_in_list() {
    local target="$1"
    local list="$2"
    for item in $list; do
        if [ "$item" = "$target" ]; then
            return 0
        fi
    done
    return 1
}

echo "=========================================="
echo "TinyKVS Overnight Fuzz Testing"
echo "=========================================="
echo "Started: $(date)"
echo "Time per target: $TIME_PER_TARGET"
echo "Parallel workers: $FUZZ_PARALLEL"
echo "Log directory: $LOG_DIR"
echo ""

if [ -n "$SPECIFIC_TARGET" ]; then
    # Run specific target
    echo "Running specific target: $SPECIFIC_TARGET"

    # Determine which package
    if is_in_list "$SPECIFIC_TARGET" "$CORE_TARGETS"; then
        run_fuzz_target "." "$SPECIFIC_TARGET"
    elif is_in_list "$SPECIFIC_TARGET" "$CMD_TARGETS"; then
        run_fuzz_target "./cmd/tinykvs" "$SPECIFIC_TARGET"
    else
        echo "Unknown fuzz target: $SPECIFIC_TARGET"
        echo ""
        echo "Available targets:"
        echo "  Core: $CORE_TARGETS"
        echo "  Cmd:  $CMD_TARGETS"
        exit 1
    fi
else
    # Count targets
    core_count=0
    for t in $CORE_TARGETS; do core_count=$((core_count + 1)); done
    cmd_count=0
    for t in $CMD_TARGETS; do cmd_count=$((cmd_count + 1)); done
    total_targets=$((core_count + cmd_count))

    echo "Running all $total_targets fuzz targets"
    echo ""

    # Core package targets
    for target in $CORE_TARGETS; do
        run_fuzz_target "." "$target"
    done

    # Cmd package targets
    for target in $CMD_TARGETS; do
        run_fuzz_target "./cmd/tinykvs" "$target"
    done
fi

print_summary
