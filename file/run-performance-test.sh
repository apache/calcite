#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."  # Go up to calcite root
DATA_DIR="/tmp/calcite_perf_data"
CACHE_DIR="/tmp/calcite_hll_cache"
RESULTS_DIR="$PROJECT_ROOT/performance_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="$RESULTS_DIR/results_${TIMESTAMP}.json"

# Engines to test
ENGINES=("linq4j" "parquet" "parquet+hll" "parquet+vec" "parquet+all" "arrow" "vectorized" "duckdb")

# Row counts to test (configurable via environment variable)
if [ -z "$ROW_COUNTS" ]; then
    ROW_COUNTS="1000 10000 100000 250000"
fi

# HLL threshold (configurable via environment variable)
if [ -z "$HLL_THRESHOLD" ]; then
    HLL_THRESHOLD="10"  # Low default for test data
fi
export HLL_THRESHOLD

# Cache pre-loading (configurable via environment variable, default: true)
if [ -z "$PRELOAD_HLL_CACHE" ]; then
    PRELOAD_HLL_CACHE="true"
fi
export PRELOAD_HLL_CACHE

# Scenarios to test
SCENARIOS=("simple_aggregation" "count_distinct_single" "count_distinct_multiple" "filtered_aggregation" "group_by_count_distinct" "complex_join")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=================================================="
echo "Apache Calcite File Adapter Performance Test"
echo "=================================================="
echo "Timestamp: $(date)"
echo "Configuration:"
echo "  Row counts: $ROW_COUNTS"
echo "  HLL threshold: $HLL_THRESHOLD"
echo "  Cache pre-loading: $PRELOAD_HLL_CACHE"
echo "  Engines: ${ENGINES[@]}"
echo "  Scenarios: ${SCENARIOS[@]}"
echo ""

# Create directories
mkdir -p "$DATA_DIR"
mkdir -p "$CACHE_DIR"
mkdir -p "$RESULTS_DIR"

# Clean up function
cleanup() {
    echo -e "${YELLOW}Cleaning up caches...${NC}"
    rm -rf /tmp/.parquet_cache
    rm -rf "$CACHE_DIR"/*
    rm -f /tmp/*.apericio_stats
    rm -f /tmp/*.hll
    rm -f /tmp/*.stats
}

# Build the project first
echo -e "${GREEN}Building project...${NC}"
cd "$PROJECT_ROOT"
./gradlew :file:testClasses -q

# Get the classpath
CLASSPATH=$(./gradlew :file:printTestClasspath -q 2>/dev/null | tail -1)
if [ -z "$CLASSPATH" ]; then
    echo -e "${RED}Failed to get classpath. Building with full output...${NC}"
    ./gradlew :file:testClasses
    CLASSPATH=$(./gradlew :file:printTestClasspath -q | tail -1)
fi

# Initialize results file
echo "[" > "$RESULTS_FILE"
FIRST_RESULT=true

# Main test loop
TOTAL_TESTS=$(($(echo $ROW_COUNTS | wc -w) * ${#ENGINES[@]} * ${#SCENARIOS[@]}))
CURRENT_TEST=0

for rows in $ROW_COUNTS; do
    echo ""
    echo -e "${GREEN}Testing with $rows rows${NC}"
    echo "----------------------------------------"

    for scenario in "${SCENARIOS[@]}"; do
        echo ""
        echo -e "${YELLOW}Scenario: $scenario${NC}"

        for engine in "${ENGINES[@]}"; do
            CURRENT_TEST=$((CURRENT_TEST + 1))
            PROGRESS=$((CURRENT_TEST * 100 / TOTAL_TESTS))

            echo -n "[$PROGRESS%] Testing $engine... "

            # Clean caches before each run
            cleanup > /dev/null 2>&1

            # Run the test in isolated JVM with SIMD optimizations
            RESULT=$(java -cp "$CLASSPATH" \
                --add-modules=jdk.incubator.vector \
                --enable-preview \
                -XX:+UnlockExperimentalVMOptions \
                -XX:+UseVectorApi \
                org.apache.calcite.adapter.file.performance.SingleEngineRunner \
                "$engine" "$rows" "$scenario" "$DATA_DIR" "$CACHE_DIR" 2>/dev/null)

            if [ $? -eq 0 ]; then
                # Add comma if not first result
                if [ "$FIRST_RESULT" = false ]; then
                    echo "," >> "$RESULTS_FILE"
                fi
                FIRST_RESULT=false

                # Append result to file
                echo -n "$RESULT" >> "$RESULTS_FILE"

                # Extract execution time for display
                EXEC_TIME=$(echo "$RESULT" | grep -o '"executionTimeMs":[0-9]*' | cut -d: -f2)

                # Check if HLL warning present
                if echo "$RESULT" | grep -q '"warning"'; then
                    echo -e "${RED}${EXEC_TIME}ms ⚠️  (HLL not working!)${NC}"
                else
                    echo -e "${GREEN}${EXEC_TIME}ms ✓${NC}"
                fi
            else
                echo -e "${RED}FAILED ✗${NC}"
            fi
        done
    done
done

# Close JSON array
echo "" >> "$RESULTS_FILE"
echo "]" >> "$RESULTS_FILE"

echo ""
echo "=================================================="
echo -e "${GREEN}Test complete!${NC}"
echo "Results saved to: $RESULTS_FILE"
echo ""

# Generate report
echo -e "${GREEN}Generating performance report...${NC}"
TEMP_REPORT="$RESULTS_DIR/performance_report_${TIMESTAMP}.md"
MAIN_REPORT="$SCRIPT_DIR/engine_performance_report.md"

java -cp "$CLASSPATH" \
    --add-modules=jdk.incubator.vector \
    --enable-preview \
    -XX:+UnlockExperimentalVMOptions \
    -XX:+UseVectorApi \
    org.apache.calcite.adapter.file.performance.PerformanceReportGenerator \
    "$RESULTS_FILE" "$TEMP_REPORT"

# Also update the main report file
java -cp "$CLASSPATH" \
    --add-modules=jdk.incubator.vector \
    --enable-preview \
    -XX:+UnlockExperimentalVMOptions \
    -XX:+UseVectorApi \
    org.apache.calcite.adapter.file.performance.PerformanceReportGenerator \
    "$RESULTS_FILE" "$MAIN_REPORT"

echo -e "${GREEN}Report saved to: $TEMP_REPORT${NC}"
echo -e "${GREEN}Main report updated: $MAIN_REPORT${NC}"
echo ""

# Final cleanup
cleanup

echo "Done!"
