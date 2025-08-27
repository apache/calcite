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
echo "Removing @Isolated annotations from test files..."

# List of files to process
files=(
    "src/test/java/org/apache/calcite/adapter/file/CsvTypeInferenceTest.java"
    "src/test/java/org/apache/calcite/adapter/file/OracleLexicalSettingsTest.java"
    "src/test/java/org/apache/calcite/adapter/file/conversion/ParquetAutoConversionTest.java"
    "src/test/java/org/apache/calcite/adapter/file/core/FileAdapterCapabilitiesTest.java"
    "src/test/java/org/apache/calcite/adapter/file/core/FileReaderTest.java"
    "src/test/java/org/apache/calcite/adapter/file/docx/DocxTableTest.java"
    "src/test/java/org/apache/calcite/adapter/file/format/CompressedFileTest.java"
    "src/test/java/org/apache/calcite/adapter/file/markdown/MarkdownTableTest.java"
    "src/test/java/org/apache/calcite/adapter/file/materialization/MaterializationTest.java"
    "src/test/java/org/apache/calcite/adapter/file/materialization/MaterializedViewQueryTest.java"
    "src/test/java/org/apache/calcite/adapter/file/metadata/MetadataCatalogIntegrationTest.java"
    "src/test/java/org/apache/calcite/adapter/file/refresh/FileConversionEndToEndTest.java"
    "src/test/java/org/apache/calcite/adapter/file/refresh/JsonPathRefreshTest.java"
    "src/test/java/org/apache/calcite/adapter/file/refresh/RefreshEndToEndTest.java"
    "src/test/java/org/apache/calcite/adapter/file/refresh/SimpleJsonPathRefreshTest.java"
    "src/test/java/org/apache/calcite/adapter/file/temporal/NullTimestampQueryTest.java"
)

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "Processing: $file"

        # Remove the import statement
        sed -i '' '/^import org\.junit\.jupiter\.api\.parallel\.Isolated;$/d' "$file"

        # Remove @Isolated annotation (handles both standalone and with other annotations)
        sed -i '' '/@Isolated$/d' "$file"

        # Also handle cases where @Isolated might be on same line with class declaration
        sed -i '' 's/@Isolated //' "$file"

        echo "  ✓ Removed @Isolated from $file"
    else
        echo "  ⚠ File not found: $file"
    fi
done

echo "Done! All @Isolated annotations have been removed."
