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
