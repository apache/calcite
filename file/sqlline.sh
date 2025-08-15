#!/bin/bash

# Script to run sqlline with the file adapter and a given model file
# Usage: ./sqlline.sh [model-file]

# Set default model file if not provided
MODEL_FILE="${1:-model.json}"

# Check if model file exists
if [ ! -f "$MODEL_FILE" ]; then
    echo "Error: Model file '$MODEL_FILE' not found"
    echo "Usage: $0 [model-file]"
    exit 1
fi

# Get the absolute path of the model file
MODEL_PATH=$(cd "$(dirname "$MODEL_FILE")" && pwd)/$(basename "$MODEL_FILE")

# Build the project if needed
echo "Building project..."
../gradlew :file:build -x test

# Get the classpath
CLASSPATH=$(../gradlew :file:printClasspath -q)

# Run sqlline
echo "Starting sqlline with model: $MODEL_PATH"
echo "----------------------------------------"

java -cp "$CLASSPATH" sqlline.SqlLine \
    -d org.apache.calcite.jdbc.Driver \
    -u "jdbc:calcite:model=$MODEL_PATH" \
    -n admin \
    -p admin