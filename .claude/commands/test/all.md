# Run All Adapter Tests

Execute comprehensive test suite across all Calcite adapters.

## Test Execution Plan

Run tests for each adapter in sequence:

### 1. File Adapter (All Engines)
```bash
for engine in PARQUET DUCKDB LINQ4J ARROW; do
  CALCITE_FILE_ENGINE_TYPE=$engine gtimeout 1800 ./gradlew :file:test --continue --console=plain
done
```

### 2. Splunk Adapter
```bash
CALCITE_TEST_SPLUNK=true gtimeout 1800 ./gradlew :splunk:test --continue --console=plain
```

### 3. SharePoint List Adapter
```bash
SHAREPOINT_INTEGRATION_TESTS=true gtimeout 1800 ./gradlew :sharepoint-list:test --continue --console=plain
```

### 4. Cloud Governance Adapter
```bash
gtimeout 1800 ./gradlew :cloud-governance:test --continue --console=plain
```

### 5. Spark Adapter
```bash
gtimeout 1800 ./gradlew :spark:test --continue --console=plain
```

## Execution Options

- Run all adapters sequentially to avoid resource conflicts
- Use `--continue` to complete all tests even with failures
- Generate comprehensive test report at completion
- Total estimated time: 2-3 hours

## Results Summary

After all tests complete:
1. Display summary table with pass/fail counts per adapter
2. List any consistently failing tests
3. Report total execution time
4. Generate JaCoCo coverage report if enabled
5. Identify any flaky tests that need attention

## Prerequisites

Ensure environment is configured:
- All required environment variables set
- Sufficient disk space (10GB+ recommended)
- Network access for integration tests
- Database/service dependencies running
