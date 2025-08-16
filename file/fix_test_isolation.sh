#!/bin/bash

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