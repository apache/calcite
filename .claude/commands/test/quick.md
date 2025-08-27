# Quick Test Run

Run a quick subset of tests for rapid development feedback: $ARGUMENTS

## Test Selection Strategy

Based on recent changes, run:
1. Tests for modified files
2. Related integration tests
3. Critical regression tests

## Execution

For the specified adapter ($ARGUMENTS):

### File Adapter Quick Tests
```bash
CALCITE_FILE_ENGINE_TYPE=PARQUET gtimeout 300 ./gradlew :file:test --tests "*FileAdapterTest" --tests "*CsvTypeInferenceTest" --console=plain
```

### Splunk Adapter Quick Tests
```bash
CALCITE_TEST_SPLUNK=true gtimeout 300 ./gradlew :splunk:test --tests "*SplunkAdapterQueryTest" --console=plain
```

### SharePoint Quick Tests
```bash
SHAREPOINT_INTEGRATION_TESTS=true gtimeout 300 ./gradlew :sharepoint-list:test --tests "*SharePointListIntegrationTest" --console=plain
```

## Features

- Short timeout (300 seconds) for quick feedback
- Focus on most commonly failing tests
- Skip slow performance tests
- Run only essential integration tests
- Provide immediate pass/fail status

Use this for rapid iteration during development, followed by full regression before commit.
