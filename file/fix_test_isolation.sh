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
echo "Analyzing and fixing test isolation issues..."

# Tests that need @Isolated due to static state or engine-specific behavior
ISOLATED_TESTS=(
  "CsvTypeInferenceTest"
  "RefreshEndToEndTest"
  "JsonPathRefreshTest"
  "FileAdapterTest"
  "MaterializedViewParquetQueryTest"
  "CompressedFileTest"
  "YamlFileTest"
  "RecursiveDirectoryComprehensiveTest"
  "MarkdownTableTest"
  "SimpleJsonPathRefreshTest"
)

for test in "${ISOLATED_TESTS[@]}"; do
  file=$(find src/test/java -name "${test}.java" | head -1)
  if [ -f "$file" ]; then
    # Check if @Isolated is already present
    if ! grep -q "@Isolated" "$file"; then
      echo "Adding @Isolated to $test"
      # Add the import if not present
      if ! grep -q "import org.junit.jupiter.api.parallel.Isolated;" "$file"; then
        sed -i '' '/^import org.junit.jupiter.api/a\
import org.junit.jupiter.api.parallel.Isolated;' "$file"
      fi
      # Add @Isolated annotation before the class
      sed -i '' '/^public class '"$test"'/i\
@Isolated  // Required due to engine-specific behavior and shared state' "$file"
    else
      echo "$test already has @Isolated"
    fi
  fi
done

echo "Test isolation fixes completed."
