<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# File Adapter Testing Guide

This guide covers testing practices, test organization, and how to run tests for the Calcite File Adapter.

## Running Tests

### Run All File Adapter Tests
```bash
# From the calcite root directory
./gradlew :file:test

# With test output
./gradlew :file:test --info

# Run a specific test class
./gradlew :file:test --tests org.apache.calcite.adapter.file.FileAdapterTest

# Run a specific test method
./gradlew :file:test --tests org.apache.calcite.adapter.file.FileAdapterTest.testGlobJsonFile
```

### Test Categories

The file adapter tests are organized into several categories:

#### Core Functionality Tests
- `FileAdapterTest` - Basic file reading, glob patterns, and query execution
- `JsonFileTest` - JSON file handling and type inference
- `JsonFlattenTest` - JSON flattening functionality for nested structures
- `CsvFileTest` - CSV parsing, headers, and data types

#### Performance Tests
- `FileVectorizedPerformanceTest` - Execution engine performance benchmarks
- `ActualPerformanceTest` - Real-world performance scenarios
- `SpilloverPerformanceTest` - Large dataset spillover testing

#### Enhanced Features Tests
- `ExcelFileTest` - Excel file reading and sheet handling
- `MultiTableExcelTest` - Multi-table detection in Excel files
- `MultiTableHtmlTest` - HTML table extraction
- `RecursiveDirectoryTest` - Recursive file discovery
- `RefreshableTableTest` - Auto-refresh functionality
- `MaterializedViewUtilTest` - Materialized view operations

#### Integration Tests
- `GlobParquetTableTest` - Glob patterns with Parquet files
- `ExcelNamingTest` - File naming conventions and conflicts

## Test Data Organization

Test data files are located in `src/test/resources/`:

```
src/test/resources/
├── testdata/
│   ├── sales.json          # Sample JSON data
│   ├── products.csv        # Sample CSV data
│   ├── excel_data.xlsx     # Excel test files
│   ├── partitioned/        # Partitioned table test data
│   │   ├── year=2024/
│   │   │   └── month=01/
│   │   │       └── data.parquet
│   └── large/              # Large dataset tests
│       └── million_rows.csv
├── materialized_views/     # MV test configurations
└── lots_of_tables.xlsx    # Multi-table Excel test
```

## Writing Tests

### Basic Test Structure

```java
public class YourFeatureTest {
    @TempDir
    Path tempDir;  // JUnit 5 temporary directory

    @BeforeEach
    public void setUp() throws Exception {
        // Create test data files
        createTestFiles();
    }

    @Test
    public void testYourFeature() throws Exception {
        // Create schema configuration
        Map<String, Object> operand = new HashMap<>();
        operand.put("directory", tempDir.toString());
        operand.put("executionEngine", "parquet");

        // Create schema
        FileSchema schema = new FileSchema(null, operand);

        // Execute query
        String sql = "SELECT * FROM your_table";
        // ... test assertions
    }
}
```

### Testing Execution Engines

```java
@ParameterizedTest
@ValueSource(strings = {"linq4j", "vectorized", "arrow", "parquet"})
public void testWithAllEngines(String engine) throws Exception {
    Map<String, Object> operand = new HashMap<>();
    operand.put("executionEngine", engine);
    // ... test with each engine
}
```

### Testing Spillover

```java
@Test
public void testSpillover() throws Exception {
    // Configure low memory threshold to force spillover
    Map<String, Object> operand = new HashMap<>();
    operand.put("memoryThreshold", 1024L); // 1KB - forces spillover
    operand.put("spillDirectory", tempDir.resolve("spill").toString());

    // Create large dataset
    createLargeTestFile(1_000_000); // 1M rows

    // Verify spillover occurs
    // ... execute query and check spill files created
}
```

### Testing Partitioned Tables

```java
@Test
public void testPartitionedTable() throws Exception {
    // Create partitioned directory structure
    createPartitionedData();

    Map<String, Object> partitionConfig = Map.of(
        "name", "sales",
        "pattern", "sales/**/*.parquet",
        "partitions", Map.of(
            "style", "hive",
            "columns", Arrays.asList(
                Map.of("name", "year", "type", "INTEGER"),
                Map.of("name", "month", "type", "INTEGER")
            )
        )
    );

    operand.put("partitionedTables", Arrays.asList(partitionConfig));

    // Test partition pruning
    String sql = "SELECT * FROM sales WHERE year = 2024 AND month = 1";
    // ... verify only relevant partitions are read
}
```

## Common Test Patterns

### Creating Test Files

```java
private void createTestCsvFile(File file, boolean withHeader) throws IOException {
    try (PrintWriter writer = new PrintWriter(file)) {
        if (withHeader) {
            writer.println("id,name,amount");
        }
        writer.println("1,Widget,100.50");
        writer.println("2,Gadget,200.75");
    }
}

private void createTestJsonFile(File file) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> data = Arrays.asList(
        Map.of("id", 1, "name", "Widget", "amount", 100.50),
        Map.of("id", 2, "name", "Gadget", "amount", 200.75)
    );
    mapper.writeValue(file, data);
}

// Creating nested JSON for flattening tests
private void createNestedJsonFile(File file) throws IOException {
    String json = "[\n" +
        "  {\n" +
        "    \"id\": 1,\n" +
        "    \"name\": \"John\",\n" +
        "    \"address\": {\n" +
        "      \"street\": \"123 Main St\",\n" +
        "      \"city\": \"Anytown\"\n" +
        "    },\n" +
        "    \"tags\": [\"customer\", \"vip\"]\n" +
        "  }\n" +
        "]";
    Files.write(file.toPath(), json.getBytes(StandardCharsets.UTF_8));
}
```

### Verifying Results

```java
private void assertQueryResults(String sql, Consumer<ResultSet> validator)
    throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery(sql)) {
        validator.accept(rs);
    }
}

// Usage
assertQueryResults("SELECT COUNT(*) FROM products", rs -> {
    assertTrue(rs.next());
    assertEquals(100, rs.getInt(1));
});
```

### Testing JSON Flattening

The JSON flattening feature can be tested using the `JsonFlattenTest` class:

```java
@Test
void testJsonFlattening() {
    // Test flattened JSON table access
    sql("sales-json-flatten", "select * from NESTED_FLAT")
        .returns("id=1; name=John Doe; address.street=123 Main St; "
            + "address.city=Anytown; address.zip=12345; tags=customer,vip,active")
        .ok();
}

// Model configuration for flattening
{
  "tables": [{
    "name": "NESTED_FLAT",
    "url": "nested.json",
    "format": "json",
    "flatten": true
  }]
}
```

Key test scenarios for JSON flattening:
- Nested objects converted to dot notation
- Arrays converted to delimited strings
- Query filtering on flattened columns
- Null handling in nested structures
- Maximum depth handling (3 levels)

### Testing File Locking

```java
@Test
public void testConcurrentAccess() throws Exception {
    File testFile = new File(tempDir.toFile(), "concurrent.csv");
    createTestCsvFile(testFile, true);

    // Simulate concurrent access
    ExecutorService executor = Executors.newFixedThreadPool(10);
    List<Future<Integer>> futures = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
        futures.add(executor.submit(() -> {
            // Execute query on same file
            return queryRowCount("SELECT * FROM concurrent");
        }));
    }

    // Verify all threads succeed
    for (Future<Integer> future : futures) {
        assertEquals(2, future.get().intValue());
    }
}
```

## Test Configuration

### Memory Settings for Tests

```java
// In build.gradle.kts
test {
    maxHeapSize = "2G"  // Increase for performance tests
    jvmArgs("-XX:+UseG1GC")
}
```

### Timeout Configuration

```java
@Test
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public void testLargeDataset() {
    // Test that might take longer
}
```

## Debugging Tests

### Enable Debug Logging

```java
@BeforeAll
public static void enableDebugLogging() {
    System.setProperty("calcite.debug", "true");
    LoggerFactory.getLogger(FileSchema.class).setLevel(Level.DEBUG);
}
```

### Inspect Spillover Files

```java
@AfterEach
public void checkSpilloverCleanup() {
    File spillDir = new File(System.getProperty("java.io.tmpdir"), "calcite_spill");
    if (spillDir.exists()) {
        File[] spillFiles = spillDir.listFiles();
        if (spillFiles != null && spillFiles.length > 0) {
            System.out.println("Warning: " + spillFiles.length + " spill files not cleaned up");
        }
    }
}
```

## Performance Testing Guidelines

### Benchmark Structure

```java
@Test
public void benchmarkExecutionEngines() throws Exception {
    Map<String, Long> results = new LinkedHashMap<>();

    for (String engine : Arrays.asList("linq4j", "vectorized", "arrow", "parquet")) {
        long startTime = System.currentTimeMillis();

        // Run query multiple times for accuracy
        for (int i = 0; i < 10; i++) {
            executeCountQuery(engine);
        }

        long duration = System.currentTimeMillis() - startTime;
        results.put(engine, duration / 10); // Average
    }

    // Log results
    results.forEach((engine, time) ->
        System.out.println(engine + ": " + time + "ms"));
}
```

### Memory Usage Testing

```java
@Test
public void testMemoryUsage() throws Exception {
    Runtime runtime = Runtime.getRuntime();
    long beforeMemory = runtime.totalMemory() - runtime.freeMemory();

    // Execute operation
    processLargeFile();

    long afterMemory = runtime.totalMemory() - runtime.freeMemory();
    long memoryUsed = afterMemory - beforeMemory;

    System.out.println("Memory used: " + (memoryUsed / 1024 / 1024) + "MB");
    assertTrue(memoryUsed < 100 * 1024 * 1024, "Should use less than 100MB");
}
```

## Known Test Issues

### Platform-Specific Tests

Some tests may behave differently on different platforms:

- **File path tests**: Use `File.separator` instead of hardcoded slashes
- **Line ending tests**: Be aware of CRLF vs LF differences
- **Timezone tests**: Some date/time tests may be timezone-sensitive

### Flaky Tests

If a test is intermittently failing:

1. Check for race conditions in concurrent tests
2. Verify proper cleanup in `@AfterEach` methods
3. Look for hardcoded timestamps or dates
4. Check for dependencies on file system timing

## Test Coverage

### Running Coverage Reports

```bash
# Generate test coverage report
./gradlew :file:jacocoTestReport

# View report at:
# file/build/reports/jacoco/test/html/index.html
```

### Coverage Goals

- Core functionality: >90% coverage
- Error handling paths: >80% coverage
- Performance code: Focus on correctness over coverage

## Continuous Integration

Tests are automatically run on:
- Every pull request
- Every commit to main branch
- Nightly builds with extended test suites

### CI-Specific Tests

Some tests are marked for CI-only execution:

```java
@Test
@EnabledIfEnvironmentVariable(named = "CI", matches = "true")
public void testCIOnly() {
    // Tests that should only run in CI
}
```

## Best Practices

1. **Use @TempDir**: Always use JUnit's temporary directory support
2. **Clean up resources**: Use try-with-resources for connections/streams
3. **Test isolation**: Each test should be independent
4. **Descriptive names**: Test method names should describe what they test
5. **Fast tests**: Keep individual tests under 1 second when possible
6. **Mock external dependencies**: Use test doubles for external services
7. **Assert specific values**: Avoid generic assertions
8. **Test edge cases**: Empty files, malformed data, large datasets

## Troubleshooting

### Common Test Failures

1. **"File not found"**: Check working directory and relative paths
2. **"Permission denied"**: Ensure temp directory is writable
3. **"Out of memory"**: Increase test heap size or reduce test data
4. **"Timeout"**: Check for infinite loops or excessive data processing
5. **"Port already in use"**: Kill lingering test processes

### Debug Commands

```bash
# Run with full stack traces
./gradlew :file:test --stacktrace

# Run with debug output
./gradlew :file:test --debug

# Run single test with debugging
./gradlew :file:test --tests FileAdapterTest.testCsvFile --debug-jvm
```
