#!/bin/bash

# Run full regression tests for a Calcite adapter
# Usage: ./run-adapter-regression.sh <adapter-name> [options]
#
# Examples:
#   ./run-adapter-regression.sh file
#   ./run-adapter-regression.sh splunk
#   ./run-adapter-regression.sh sharepoint-list
#   ./run-adapter-regression.sh file --engine=PARQUET
#   ./run-adapter-regression.sh splunk --env-vars

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
ADAPTER=""
ENGINE_TYPE=""
TIMEOUT="3600" # 1 hour default timeout
PARALLEL="true"
ENV_VARS=""
VERBOSE="false"
CLEAN_FIRST="false"

# Function to print usage
print_usage() {
    echo "Usage: $0 <adapter-name> [options]"
    echo ""
    echo "Adapters:"
    echo "  file              - File adapter tests"
    echo "  splunk            - Splunk adapter tests"
    echo "  sharepoint-list   - SharePoint List adapter tests"
    echo "  cloud-governance  - Cloud Governance adapter tests"
    echo "  spark             - Spark adapter tests"
    echo ""
    echo "Options:"
    echo "  --engine=TYPE     - For file adapter: PARQUET, DUCKDB, LINQ4J, ARROW (default: all)"
    echo "  --timeout=SECONDS - Test timeout in seconds (default: 3600)"
    echo "  --no-parallel     - Disable parallel test execution"
    echo "  --env-vars        - Use environment variables for credentials (splunk/sharepoint)"
    echo "  --verbose         - Enable verbose output (--info flag)"
    echo "  --debug           - Enable debug output (--debug flag)"
    echo "  --clean           - Clean before testing"
    echo "  --continue        - Continue on test failures"
    echo "  --specific=PATTERN - Run only tests matching pattern"
    echo "  --help            - Show this help message"
    echo ""
    echo "Environment variables for Splunk:"
    echo "  CALCITE_TEST_SPLUNK=true"
    echo "  SPLUNK_URL, SPLUNK_USER, SPLUNK_PASSWORD"
    echo ""
    echo "Environment variables for SharePoint:"
    echo "  SHAREPOINT_INTEGRATION_TESTS=true"
    echo "  SHAREPOINT_TENANT_ID, SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET, SHAREPOINT_SITE_URL"
    echo ""
    echo "Environment variables for File adapter:"
    echo "  CALCITE_FILE_ENGINE_TYPE=PARQUET|DUCKDB|LINQ4J|ARROW"
}

# Parse arguments
if [ $# -eq 0 ]; then
    print_usage
    exit 1
fi

ADAPTER=$1
shift

# Parse options
while [ $# -gt 0 ]; do
    case $1 in
        --engine=*)
            ENGINE_TYPE="${1#*=}"
            ;;
        --timeout=*)
            TIMEOUT="${1#*=}"
            ;;
        --no-parallel)
            PARALLEL="false"
            ;;
        --env-vars)
            ENV_VARS="true"
            ;;
        --verbose)
            VERBOSE="true"
            ;;
        --debug)
            DEBUG="true"
            ;;
        --clean)
            CLEAN_FIRST="true"
            ;;
        --continue)
            CONTINUE="true"
            ;;
        --specific=*)
            SPECIFIC_TESTS="${1#*=}"
            ;;
        --help)
            print_usage
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            print_usage
            exit 1
            ;;
    esac
    shift
done

# Build gradle command
GRADLE_CMD="./gradlew"
GRADLE_ARGS=""

# Add timeout using gtimeout (macOS) or timeout (Linux)
if command -v gtimeout &> /dev/null; then
    GRADLE_CMD="gtimeout $TIMEOUT $GRADLE_CMD"
elif command -v timeout &> /dev/null; then
    GRADLE_CMD="timeout $TIMEOUT $GRADLE_CMD"
else
    echo -e "${YELLOW}Warning: timeout command not found. Tests will run without timeout.${NC}"
fi

# Add clean task if requested
if [ "$CLEAN_FIRST" = "true" ]; then
    GRADLE_ARGS="$GRADLE_ARGS :$ADAPTER:cleanTest"
fi

# Add main test task
GRADLE_ARGS="$GRADLE_ARGS :$ADAPTER:test"

# Add specific test pattern if provided
if [ -n "$SPECIFIC_TESTS" ]; then
    GRADLE_ARGS="$GRADLE_ARGS --tests \"$SPECIFIC_TESTS\""
fi

# Add continue flag if requested
if [ "$CONTINUE" = "true" ]; then
    GRADLE_ARGS="$GRADLE_ARGS --continue"
fi

# Add parallel execution settings
if [ "$PARALLEL" = "false" ]; then
    GRADLE_ARGS="$GRADLE_ARGS --no-parallel --max-workers=1"
fi

# Add output settings
GRADLE_ARGS="$GRADLE_ARGS --console=plain"

if [ "$VERBOSE" = "true" ]; then
    GRADLE_ARGS="$GRADLE_ARGS --info"
fi

if [ "$DEBUG" = "true" ]; then
    GRADLE_ARGS="$GRADLE_ARGS --debug"
fi

# No daemon for better stability in CI/regression
GRADLE_ARGS="$GRADLE_ARGS --no-daemon"

# Set environment variables based on adapter
case $ADAPTER in
    file)
        echo -e "${GREEN}Running File Adapter regression tests${NC}"
        if [ -n "$ENGINE_TYPE" ]; then
            export CALCITE_FILE_ENGINE_TYPE="$ENGINE_TYPE"
            echo "Engine type: $ENGINE_TYPE"
        else
            echo "Running tests for all engine types..."
            # Run tests for each engine type
            for engine in PARQUET DUCKDB LINQ4J ARROW; do
                echo -e "${YELLOW}Testing with engine: $engine${NC}"
                export CALCITE_FILE_ENGINE_TYPE="$engine"
                eval "$GRADLE_CMD $GRADLE_ARGS"
                if [ $? -ne 0 ]; then
                    echo -e "${RED}Tests failed for engine: $engine${NC}"
                    if [ "$CONTINUE" != "true" ]; then
                        exit 1
                    fi
                fi
            done
            echo -e "${GREEN}All engine tests completed${NC}"
            exit 0
        fi
        ;;
    
    splunk)
        echo -e "${GREEN}Running Splunk Adapter regression tests${NC}"
        export CALCITE_TEST_SPLUNK=true
        if [ "$ENV_VARS" != "true" ]; then
            echo -e "${YELLOW}Note: Using default Splunk test instance. Use --env-vars to specify custom credentials.${NC}"
        fi
        ;;
    
    sharepoint-list)
        echo -e "${GREEN}Running SharePoint List Adapter regression tests${NC}"
        export SHAREPOINT_INTEGRATION_TESTS=true
        if [ "$ENV_VARS" != "true" ]; then
            echo -e "${YELLOW}Note: SharePoint tests require environment variables. Use --env-vars and set:${NC}"
            echo "  SHAREPOINT_TENANT_ID, SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET, SHAREPOINT_SITE_URL"
        fi
        ;;
    
    cloud-governance)
        echo -e "${GREEN}Running Cloud Governance Adapter regression tests${NC}"
        ;;
    
    spark)
        echo -e "${GREEN}Running Spark Adapter regression tests${NC}"
        ;;
    
    *)
        echo -e "${RED}Unknown adapter: $ADAPTER${NC}"
        print_usage
        exit 1
        ;;
esac

# Print the command being executed
echo "Executing: $GRADLE_CMD $GRADLE_ARGS"
echo "----------------------------------------"

# Execute the gradle command
eval "$GRADLE_CMD $GRADLE_ARGS"
RESULT=$?

# Report results
echo "----------------------------------------"
if [ $RESULT -eq 0 ]; then
    echo -e "${GREEN}Regression tests completed successfully!${NC}"
else
    echo -e "${RED}Regression tests failed with exit code: $RESULT${NC}"
    exit $RESULT
fi