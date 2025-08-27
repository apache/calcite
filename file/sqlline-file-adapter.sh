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
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "$0")"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DEFAULT_MODEL="model.json"
JAR_DIR="$SCRIPT_DIR/lib"
CONFIG_DIR="$SCRIPT_DIR/config"
EXAMPLES_DIR="$SCRIPT_DIR/examples"

# Function to print colored output
print_error() {
    echo -e "${RED}ERROR: $1${NC}" >&2
}

print_success() {
    echo -e "${GREEN}$1${NC}"
}

print_info() {
    echo -e "${YELLOW}$1${NC}"
}

# Function to show usage
show_usage() {
    cat << EOF
Calcite File Adapter SQL Shell

Usage: $SCRIPT_NAME [OPTIONS] [model-file]

OPTIONS:
    -h, --help          Show this help message
    -v, --version       Show version information
    -e, --example       List available example models
    -c, --command       Execute SQL command and exit
    -f, --file          Execute SQL from file and exit
    -o, --output        Output format (csv, json, table)
    -q, --quiet         Suppress banner and info messages

ARGUMENTS:
    model-file          Path to Calcite model JSON file (default: $DEFAULT_MODEL)

EXAMPLES:
    $SCRIPT_NAME                           # Use default model.json
    $SCRIPT_NAME mymodel.json              # Use custom model file
    $SCRIPT_NAME -e                        # List example models
    $SCRIPT_NAME examples/csv-model.json   # Use example model
    $SCRIPT_NAME -c "SELECT * FROM t1"    # Execute single query
    $SCRIPT_NAME -f queries.sql           # Execute queries from file

EOF
}

# Function to show version
show_version() {
    echo "Calcite File Adapter SQL Shell"
    echo "Version: 1.0.0"
    echo "Apache Calcite Version: $(get_calcite_version)"
}

# Function to get Calcite version from JAR
get_calcite_version() {
    # This would extract version from the JAR manifest
    echo "1.38.0"  # Placeholder
}

# Function to list example models
list_examples() {
    print_info "Available example models:"
    echo ""
    if [ -d "$EXAMPLES_DIR" ]; then
        for example in "$EXAMPLES_DIR"/*.json; do
            if [ -f "$example" ]; then
                basename "$example"
                # Extract description from model file if available
                desc=$(grep -m1 '"description"' "$example" 2>/dev/null | sed 's/.*"description".*:.*"\(.*\)".*/  \1/')
                if [ -n "$desc" ]; then
                    echo "  $desc"
                fi
                echo ""
            fi
        done
    else
        echo "No examples directory found at: $EXAMPLES_DIR"
    fi
}

# Function to check if running from distribution
is_distribution() {
    [ -d "$JAR_DIR" ] && [ -f "$JAR_DIR/calcite-file-adapter.jar" ]
}

# Function to build classpath
build_classpath() {
    if is_distribution; then
        # Running from distribution
        echo "$JAR_DIR/*"
    else
        # Running from development environment
        if [ -f "../gradlew" ]; then
            ../gradlew :file:build -x test >/dev/null 2>&1
            ../gradlew :file:printClasspath -q 2>/dev/null
        else
            print_error "Not in a distribution package and gradle not found"
            exit 1
        fi
    fi
}

# Parse command line arguments
MODEL_FILE=""
SQL_COMMAND=""
SQL_FILE=""
OUTPUT_FORMAT="table"
QUIET=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -v|--version)
            show_version
            exit 0
            ;;
        -e|--example)
            list_examples
            exit 0
            ;;
        -c|--command)
            SQL_COMMAND="$2"
            shift 2
            ;;
        -f|--file)
            SQL_FILE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FORMAT="$2"
            shift 2
            ;;
        -q|--quiet)
            QUIET=true
            shift
            ;;
        -*)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
        *)
            MODEL_FILE="$1"
            shift
            ;;
    esac
done

# Set default model file if not provided
if [ -z "$MODEL_FILE" ]; then
    MODEL_FILE="$DEFAULT_MODEL"
fi

# Check if model file exists
if [ ! -f "$MODEL_FILE" ]; then
    print_error "Model file '$MODEL_FILE' not found"
    echo ""
    echo "Try one of these:"
    echo "  1. Create a model.json file"
    echo "  2. Specify a different model file"
    echo "  3. Use an example: $SCRIPT_NAME -e"
    exit 1
fi

# Get absolute path of model file
MODEL_PATH="$(cd "$(dirname "$MODEL_FILE")" && pwd)/$(basename "$MODEL_FILE")"

# Build classpath
if [ "$QUIET" = false ]; then
    print_info "Initializing Calcite File Adapter..."
fi

CLASSPATH=$(build_classpath)
if [ $? -ne 0 ]; then
    print_error "Failed to build classpath"
    exit 1
fi

# Prepare sqlline arguments
SQLLINE_ARGS=(
    -d org.apache.calcite.jdbc.Driver
    -u "jdbc:calcite:model=$MODEL_PATH"
    -n admin
    -p admin
)

# Add output format
case $OUTPUT_FORMAT in
    csv)
        SQLLINE_ARGS+=(--outputformat=csv)
        ;;
    json)
        SQLLINE_ARGS+=(--outputformat=json)
        ;;
    table|*)
        SQLLINE_ARGS+=(--outputformat=table)
        ;;
esac

# Add quiet mode
if [ "$QUIET" = true ]; then
    SQLLINE_ARGS+=(--silent=true)
fi

# Handle command execution
if [ -n "$SQL_COMMAND" ]; then
    echo "$SQL_COMMAND" | java -cp "$CLASSPATH" sqlline.SqlLine "${SQLLINE_ARGS[@]}"
elif [ -n "$SQL_FILE" ]; then
    if [ ! -f "$SQL_FILE" ]; then
        print_error "SQL file '$SQL_FILE' not found"
        exit 1
    fi
    java -cp "$CLASSPATH" sqlline.SqlLine "${SQLLINE_ARGS[@]}" < "$SQL_FILE"
else
    # Interactive mode
    if [ "$QUIET" = false ]; then
        print_success "Connected to Calcite File Adapter"
        echo "Model: $MODEL_PATH"
        echo "Type '!help' for help, '!quit' to exit"
        echo ""
    fi

    java -cp "$CLASSPATH" sqlline.SqlLine "${SQLLINE_ARGS[@]}"
fi
