#!/bin/bash

# Remove @Isolated annotations from all test files
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