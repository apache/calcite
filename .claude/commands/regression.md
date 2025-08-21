letse# Run Full Regression Tests for Calcite Adapter

Run comprehensive regression tests for the specified Calcite adapter: $ARGUMENTS

## Test Execution Strategy

Based on the adapter specified, execute the following test suites:

### File Adapter
If adapter is "file" or empty, run tests for all engine types:
1. PARQUET engine tests
2. DUCKDB engine tests
3. LINQ4J engine tests
4. ARROW engine tests

Each engine run with:
- `CALCITE_FILE_ENGINE_TYPE=[engine] gtimeout 1800 ./gradlew :file:test --continue --console=plain`

### Splunk Adapter
If adapter is "splunk":
- Set `CALCITE_TEST_SPLUNK=true`
- Run: `gtimeout 1800 ./gradlew :splunk:test --continue --console=plain`

### SharePoint List Adapter
If adapter is "sharepoint" or "sharepoint-list":
- Set `SHAREPOINT_INTEGRATION_TESTS=true`
- Run: `gtimeout 1800 ./gradlew :sharepoint-list:test --continue --console=plain`
- Note: Requires environment variables for authentication

### Cloud Governance Adapter
If adapter is "cloud-governance":
- Run: `gtimeout 1800 ./gradlew :cloud-governance:test --continue --console=plain`

### Spark Adapter
If adapter is "spark":
- Run: `gtimeout 1800 ./gradlew :spark:test --continue --console=plain`

### All Adapters
If adapter is "all":
- Run regression tests for each adapter sequentially
- Report summary of results at the end

## Test Configuration

- Use extended timeout (1800-3600 seconds) for comprehensive testing
- Enable `--continue` flag to run all tests even if some fail
- Use `--console=plain` for clean, readable output
- Run with `--no-daemon` for stability
- Clean test outputs before running if flaky tests detected

## Results Reporting

After completion:
1. Summarize test results (passed/failed/skipped)
2. Identify any flaky or consistently failing tests
3. Report total execution time
4. Suggest next steps if failures occur

## Environment Requirements

Verify environment setup:
- Java version compatibility
- Gradle wrapper availability
- Required environment variables set
- Sufficient disk space for test artifacts
- Network connectivity for integration tests
