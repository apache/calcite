# Test Specific Calcite Adapter

Run tests for a specific Calcite adapter with custom options: $ARGUMENTS

## Usage Examples

- `file` - Run all file adapter tests
- `file PARQUET` - Run file adapter with PARQUET engine only
- `splunk IntegrationTest` - Run Splunk integration tests only
- `sharepoint-list` - Run all SharePoint tests
- `file DUCKDB CsvTypeInferenceTest` - Run specific test class with DuckDB engine

## Execution Logic

Parse arguments to determine:
1. **Adapter name** (first argument)
2. **Engine type** (second argument, if applicable for file adapter)
3. **Test pattern** (remaining arguments)

### File Adapter Testing
```bash
CALCITE_FILE_ENGINE_TYPE=[ENGINE] gtimeout 600 ./gradlew :file:test [--tests PATTERN] --console=plain
```
Supported engines: PARQUET, DUCKDB, LINQ4J, ARROW

### Splunk Adapter Testing
```bash
CALCITE_TEST_SPLUNK=true gtimeout 600 ./gradlew :splunk:test [--tests PATTERN] --console=plain
```

### SharePoint Adapter Testing
```bash
SHAREPOINT_INTEGRATION_TESTS=true gtimeout 600 ./gradlew :sharepoint-list:test [--tests PATTERN] --console=plain
```

## Quick Test Mode

For rapid feedback during development:
- Use shorter timeout (600 seconds)
- Run specific test patterns only
- Skip clean task for faster startup
- Use parallel execution when possible

## Test Categories

Automatically detect and run by category:
- `*UnitTest` - Quick unit tests
- `*IntegrationTest` - Integration tests requiring external services
- `*PerformanceTest` - Performance benchmarks
- `*RegressionTest` - Regression test suite

Report execution time and resource usage for performance analysis.