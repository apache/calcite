# SEC Adapter Development Guidelines

## Java Code Standards

### Temporal Types
- **NEVER** use deprecated `java.sql.Time` - always use `java.time.LocalTime` instead
- **NEVER** use Java functions that might misapply local timezone offsets when computing day offsets from epoch
- **ALWAYS** use UTC-based calculations explicitly for SEC filing dates and timestamps
- SEC filing dates are in Eastern Time (ET) - convert appropriately when storing/retrieving

### Code Quality
- **NEVER** create Forbidden API violations - fix any existing ones immediately
- **ALWAYS** follow proper Java code style conventions
- **ALWAYS** apply DRY (Don't Repeat Yourself) principle when refactoring
- **NEVER** add backward compatibility unless explicitly requested
- **NEVER** remove features unilaterally - only simplify or improve them
- **ALWAYS** maintain SEC EDGAR compliance with rate limits and user agent requirements

### Debugging
- **NEVER** use `System.out`/`System.err` in production code
- **ALWAYS** use `logger.debug()` with lazy evaluation:
  ```java
  // GOOD: String only built if debug enabled
  logger.debug("Downloaded filing: {}", () -> filing.getAccessionNumber());

  // BAD: String always built
  logger.debug("Downloaded filing: " + filing.getAccessionNumber());
  ```

## File Adapter Integration Standards

### Core Architectural Principle
**The SEC adapter MUST maximize reuse of the file adapter as its foundation.** The file adapter provides robust infrastructure for data fetching, caching, storage, HTML processing, and partitioned table support. SEC-specific code should ONLY handle XBRL parsing, SEC metadata, and EDGAR-specific requirements.

### Mandatory File Adapter Usage

#### Data Fetching and Storage
- **MUST** use file adapter's `HttpStorageProvider` for all HTTP operations
- **NEVER** implement custom HTTP clients or downloaders
- Create `SecHttpStorageProvider extends HttpStorageProvider` for SEC-specific needs:
  ```java
  public class SecHttpStorageProvider extends HttpStorageProvider {
    // Add SEC User-Agent header
    // Implement 100ms rate limiting between requests
    // Add EDGAR-specific retry logic
  }
  ```
- Use file adapter's `PersistentStorageCache` for all caching needs
- **NEVER** implement custom caching mechanisms

#### HTML Scraping and Processing
- **ALWAYS** use file adapter's `HtmlToJsonConverter` for HTML table extraction
- **ALWAYS** use `HtmlCrawler` for multi-page navigation (e.g., filing exhibits)
- **ALWAYS** use `HtmlTableScanner` for detecting and parsing HTML tables
- **NEVER** write custom HTML parsing code - enhance file adapter if needed
- Example for Wikipedia/web scraping:
  ```java
  // GOOD: Use file adapter's HTML processing
  List<File> jsonFiles = HtmlToJsonConverter.convert(
      htmlFile, outputDir, "TO_LOWER", baseDirectory);
  
  // BAD: Custom HTML parsing
  Document doc = Jsoup.parse(html); // NO!
  ```

#### Schema and Table Implementation
- **MUST** extend `FileSchema` for SEC schema implementation
- **NEVER** implement schema from scratch
- Use file adapter's table implementations:
  - `PartitionedParquetTable` for partitioned SEC filings
  - `RefreshableParquetCacheTable` for auto-refreshing data
  - `MaterializedViewTable` for pre-computed aggregations
  - `GlobParquetTable` for pattern-based file discovery

#### Data Conversion Pipeline
- **MUST** use `FileConversionManager` for all format conversions
- Register XBRL converter with FileConversionManager:
  ```java
  conversionManager.registerConverter("xbrl", new XbrlToParquetConverter());
  ```
- **NEVER** implement standalone conversion logic
- Use file adapter's `ParquetConversionUtil` for Parquet operations

#### Partitioning Strategy
- **MUST** use file adapter's `PartitionDetector` for partition discovery
- Configure `PartitionedTableConfig` for SEC patterns:
  ```java
  config.setPartitionPattern("cik={cik}/filing_type={type}/year={year}");
  ```
- **NEVER** implement custom partition detection

#### Caching and Refresh
- **MUST** use `StorageCacheManager` for cache lifecycle
- Implement `RefreshInterval` for periodic updates:
  ```java
  RefreshInterval.of(Duration.ofHours(24)) // Daily refresh
  ```
- Use `AbstractRefreshableTable` as base for custom refresh logic

### Required Enhancements to File Adapter

When the SEC adapter needs functionality not in the file adapter:

1. **First Choice**: Enhance the file adapter
   - Add the capability to file adapter if it's generally useful
   - Example: Additional HTTP header support, custom selectors

2. **Second Choice**: Extend file adapter classes
   - Create SEC-specific subclasses only for truly SEC-specific needs
   - Example: `SecHttpStorageProvider` for EDGAR rate limiting

3. **Last Resort**: SEC-only implementation
   - Only for XBRL parsing and SEC regulatory requirements
   - Must document why file adapter cannot handle it

### Migration Requirements for Existing Code

#### Phase 1: Storage Provider Migration
- Replace `EdgarDownloader` with `SecHttpStorageProvider`
- Remove custom HTTP connection code
- Use file adapter's retry and cache mechanisms

#### Phase 2: HTML Processing Migration
- Replace custom Jsoup usage with `HtmlToJsonConverter`
- Convert Wikipedia scraper to use `HtmlCrawler`
- Remove redundant HTML parsing utilities

#### Phase 3: Schema Integration
- Refactor `SecSchemaFactory` to create `FileSchema` instances
- Remove `SecToParquetConverter`, use `FileConversionManager`
- Integrate with file adapter's metadata system

#### Phase 4: Table Implementation
- Replace custom table classes with file adapter tables
- Use `PartitionedParquetTable` for filing organization
- Implement refresh using file adapter's mechanisms

### Testing with File Adapter

- Test SEC adapter through file adapter's testing framework
- Use `FileSchema` test utilities for integration tests
- Leverage file adapter's mock storage providers for unit tests

### Benefits of This Approach

1. **Code Reduction**: Eliminates ~60% of custom SEC adapter code
2. **Consistency**: Uniform behavior across all file-based adapters
3. **Performance**: Benefits from file adapter optimizations
4. **Maintenance**: Single codebase for common functionality
5. **Features**: Automatically gains new file adapter capabilities

## SEC-Specific Standards

### EDGAR API Compliance
- **ALWAYS** set User-Agent header: `"Apache Calcite SEC Adapter (apache-calcite@apache.org)"`
- **ALWAYS** respect SEC rate limits (10 requests per second)
- **NEVER** make concurrent requests to EDGAR without throttling
- **ALWAYS** cache downloaded filings to avoid unnecessary re-downloads

### CIK Handling
- **ALWAYS** normalize CIKs to 10-digit format with leading zeros
- Support both ticker symbols and CIK numbers in configuration
- Use `CikRegistry` for ticker-to-CIK resolution
- Support predefined groups (FAANG, MAGNIFICENT7, etc.)
- **ALWAYS** validate CIK formats before making EDGAR requests

### Filing Types
- Default filing types: `["10-K", "10-Q", "8-K"]`
- Support all 15 major filing types when requested
- **ALWAYS** normalize filing type names (remove hyphens for storage)
- Partition data by CIK/filing-type/date for efficient querying

### Data Storage
- **ALWAYS** use partitioned Parquet format for processed SEC data
- Partition strategy: `/cik={cik}/filing_type={type}/year={year}/`
- Cache directory structure:
  - Raw XBRL: `{baseDirectory}/sec-cache/raw/{cik}/{accession}/`
  - Processed: `{baseDirectory}/sec-cache/processed/{partition}/`
- **NEVER** store sensitive API keys or credentials in code

## Testing Standards

### Temporal Testing
- **ALWAYS** use numeric values (epoch millis, day counts) for temporal expectations
- **NEVER** use formatted date strings as test expectations
- SEC filing dates should be tested as:
  - Filing date (Eastern Time)
  - Period end date (company fiscal calendar)
  - Acceptance timestamp (UTC)

### SQL Identifier Conventions
- **DEFAULT SETTINGS** (SEC adapter enforces these):
  - Lex: `ORACLE`
  - Unquoted casing: `TO_LOWER`
  - Name generation: `SMART_CASING` (lower snake_case)

### Test Data
- **ALWAYS** use test mode for unit tests (mock EDGAR responses)
- Integration tests should use cached test data when possible
- **NEVER** make live EDGAR requests in unit tests
- Performance tests should use realistic data volumes

### Test Execution
- SEC adapter tests require network access for integration tests
- Use `@Tag("integration")` for tests requiring EDGAR access
- Use `@Tag("unit")` for pure unit tests with mocked data
- Extended timeouts needed for large company downloads
- **Remember**: By default only unit tests run. Use `-PincludeTags=integration` for integration tests

### Test Debugging Standards

#### Fix-First Policy
- **REQUIRED**: When a test fails, debug and fix it
- **PROHIBITED**: Creating alternative tests to bypass failures
- **PROHIBITED**: Creating `TestSecDownload2.java` when `TestSecDownload.java` fails

#### Debugging SEC Adapter Tests
1. **Check test configuration first**:
   - Verify model.json settings
   - Confirm testMode and ephemeralCache flags
   - Validate mock data generation

2. **Trace execution path**:
   - Add logging to SecSchemaFactory
   - Trace JDBC query execution
   - Monitor file I/O operations

3. **Temporary debugging aids**:
   - Can create debug tests in `build/temp-tests/`
   - Must use `@Tag("debug")` and `@Disabled`
   - Delete immediately after resolution

#### Example: Proper Test Debugging
```java
// BAD: Creating new test to avoid fixing
@Test
void testDJIDataNew() { // NO! Fix testDJIData instead
    // ...
}

// GOOD: Debug the original test
@Test
void testDJIData() {
    // Add temporary logging
    logger.debug("Query: {}", sql);
    logger.debug("Result count: {}", resultSet.getRow());
    
    // Fix the actual issue
    // Remove debug code after fixing
}
```

## Connection Configuration

### JDBC URL Format
- Primary format: `jdbc:sec:ciks=TICKER[,TICKER2...]`
- Parameters use `&` separator: `jdbc:sec:ciks=AAPL&startYear=2020`
- **ALWAYS** validate connection parameters before processing

### Required Parameters
- `ciks`: Company identifiers (tickers, CIKs, or groups) - REQUIRED
- All other parameters have smart defaults

### Optional Parameters
- `filingTypes`: Comma-separated filing types
- `startYear`/`endYear`: Date range for filings
- `dataDirectory`: Base directory for data storage
- `embeddingDimension`: Vector embedding size (default: 128)
- `executionEngine`: Query engine (default: duckdb)
- `debug`: Enable debug logging

### Parameter Precedence
1. Connection URL parameters (highest)
2. Environment variables
3. Model file configuration
4. Smart defaults (lowest)

## Development Workflow

### Code Review & Cleanup Command
When asked to "cleanup debug code":
1. **Application Code**: Remove or convert `System.out`/`System.err` to logger.debug()
2. **Test Organization**: Tag tests appropriately (`@Tag("unit")`, `@Tag("integration")`)
3. **Dead Code**: Identify, report, and await approval before removal
4. **Cache Management**: Clean up old cache files based on retention policy

### SEC Data Processing Pipeline
1. **Download**: Fetch from EDGAR with proper headers and rate limiting
2. **Validate**: Check XBRL structure and required fields
3. **Convert**: Transform to Parquet with proper partitioning
4. **Index**: Create metadata for efficient querying
5. **Cache**: Store processed data with appropriate TTL

## Fallback Pattern Guidelines

### Core Principle
**Fallbacks should be exceptional, not routine.** Hardcoded data and silent fallbacks mask bugs in primary data fetching mechanisms, leading to undetected failures in production.

### Required Fallback Patterns

#### Configuration-Based Control
- **ALWAYS** use configuration flags to control fallback behavior:
  ```java
  // GOOD: Configurable fallback behavior
  boolean fallbackEnabled = config.getBoolean("sec.fallback.enabled", false);
  if (primaryFetchFailed) {
    if (fallbackEnabled) {
      LOGGER.warning("Primary fetch failed, using fallback: " + failureReason);
      return getFallbackData();
    } else {
      throw new DataFetchException("Primary fetch failed: " + failureReason);
    }
  }
  ```

#### Proper Logging
- **ALWAYS** log at WARNING level when using fallbacks
- Include specific failure reason and fallback source
- Track metrics/counters for fallback usage:
  ```java
  // GOOD: Clear logging with metrics
  if (useFallback) {
    LOGGER.warning("EDGAR API failed after 3 retries, using cached data from " + cacheDate);
    fallbackCounter.increment("edgar_api_failure");
    return cachedData;
  }
  ```

#### Test Mode Behavior
- **ALWAYS** fail fast in test environments:
  ```java
  // GOOD: Different behavior for tests
  if (isTestMode() || !config.getBoolean("sec.fallback.enabled", false)) {
    throw new DataUnavailableException("Wikipedia scraping failed: " + e.getMessage());
  }
  // Only in production with explicit config
  return getHardcodedData();
  ```

### Prohibited Patterns

#### Silent Fallbacks
- **NEVER** fall back without logging:
  ```java
  // BAD: Silent fallback masks failures
  try {
    return fetchFromAPI();
  } catch (Exception e) {
    return hardcodedData; // NO! This hides the failure
  }
  ```

#### Cascading Fallbacks
- **NEVER** implement multiple fallback layers that obscure root cause:
  ```java
  // BAD: Multiple fallbacks hide the real problem
  try {
    return fetchFromWikipedia();
  } catch (Exception e1) {
    try {
      return fetchFromSEC();
    } catch (Exception e2) {
      try {
        return fetchFromCache();
      } catch (Exception e3) {
        return hardcodedData; // Which method actually failed?
      }
    }
  }
  ```

#### Hardcoded Production Data
- **NEVER** embed production data in code:
  ```java
  // BAD: Hardcoded data in production path
  private List<String> getSP500Companies() {
    if (fetchFailed) {
      // This should be in test resources or config files
      return Arrays.asList("AAPL", "MSFT", "GOOGL", ...);
    }
  }
  ```

### Implementation Requirements

#### Configuration Properties
Add these configuration properties:
- `sec.fallback.enabled`: Enable/disable all fallbacks (default: `false` in tests, `true` in production)
- `sec.fallback.log.level`: Log level for fallback events (default: `WARNING`)
- `sec.fallback.fail.on.stale`: Fail if cached data exceeds TTL (default: `true` in tests)

#### Circuit Breaker Pattern
Implement circuit breakers to prevent repeated failures:
```java
private final CircuitBreaker edgarCircuit = CircuitBreaker.ofDefaults("edgar-api");

public List<String> fetchData() {
  return edgarCircuit.executeSupplier(() -> {
    return fetchFromEDGAR();
  }).recover(throwable -> {
    if (config.getBoolean("sec.fallback.enabled", false)) {
      LOGGER.warning("Circuit breaker open, using fallback: " + throwable.getMessage());
      return getCachedData();
    }
    throw new DataFetchException("EDGAR API circuit breaker open", throwable);
  });
}
```

#### Fallback Metrics
Track and expose fallback usage:
- Count of fallback invocations by type
- Timestamp of last primary success
- Data staleness indicators
- Circuit breaker status

### Testing Guidelines

#### Unit Tests
- **ALWAYS** disable fallbacks in unit tests
- Test primary method failure scenarios explicitly
- Verify proper exception propagation

#### Integration Tests
- Test fallback behavior with explicit configuration
- Verify logging and metrics are recorded
- Separate tests for primary and fallback paths

#### Example Test Pattern
```java
@Test
public void testPrimaryMethodFailureThrowsException() {
  // Disable fallbacks for this test
  config.setProperty("sec.fallback.enabled", false);
  
  // Mock primary method to fail
  when(secAPI.fetch()).thenThrow(new IOException("Network error"));
  
  // Should throw, not fall back
  assertThrows(DataFetchException.class, () -> dataFetcher.getData());
}

@Test
@Tag("integration")
public void testFallbackWithExplicitConfig() {
  // Explicitly enable fallback
  config.setProperty("sec.fallback.enabled", true);
  
  // Verify fallback is used and logged
  List<String> data = dataFetcher.getData();
  assertNotNull(data);
  verify(logger).warning(contains("using fallback"));
}
```

## Source Organization Standards

### Core Principle
**Maintain strict Maven/Gradle standard directory structure.** All code must be properly organized under the `src/` directory hierarchy. No source files should exist in module root directories.

### Directory Structure Requirements

#### Standard Java Project Layout
```
sec/
├── src/
│   ├── main/
│   │   ├── java/           # Production Java code
│   │   │   └── org/apache/calcite/adapter/sec/
│   │   ├── resources/      # Production resources
│   │   └── scripts/        # Production scripts (optional)
│   └── test/
│       ├── java/           # Test Java code  
│       │   └── org/apache/calcite/adapter/sec/
│       └── resources/      # Test resources
├── examples/               # Example applications (optional)
├── scripts/                # Build/utility scripts
├── docs/                   # Documentation
└── build.gradle.kts        # Build configuration
```

### File Placement Rules

#### Production Code
- **Location**: `src/main/java/org/apache/calcite/adapter/sec/`
- **NEVER** place production Java files in module root
- All classes must be in proper packages

#### Test Code
- **Location**: `src/test/java/org/apache/calcite/adapter/sec/`
- **NEVER** place test files in module root
- **NEVER** use default package for tests
- Test class naming: `*Test.java` suffix required
- Integration tests: Use `@Tag("integration")`
- Performance tests: Use `@Tag("performance")`

#### Resources
- **Production**: `src/main/resources/`
- **Test**: `src/test/resources/`
- Model files, configuration, test data belong here
- **NEVER** place resources in Java directories

#### Scripts and Tools
- **Build scripts**: Module root (`build.gradle.kts`, `pom.xml`)
- **Utility scripts**: `scripts/` subdirectory
- **NEVER** place `.sh` or `.bat` files in root with "Test" prefix

### Prohibited Practices

#### Never in Root Directory
```java
// BAD: Files that should NEVER exist in /sec/ root
/sec/TestDJIModel.java           ❌ Test file in root
/sec/TestWikipediaScraper.java   ❌ Test file in root  
/sec/RunRealDJIDownload.java     ❌ Runner in root
/sec/TestDJIScraper.sh           ❌ Test script in root
/sec/*.class                     ❌ Compiled files
```

#### Never Mix Concerns
- **NEVER** place test files alongside production code
- **NEVER** use production directories for examples or experiments
- **NEVER** commit compiled `.class` files anywhere

### Proper Organization Examples

#### Correct Test Placement
```java
// GOOD: Properly packaged test
src/test/java/org/apache/calcite/adapter/sec/SecDriverTest.java

// GOOD: Integration test with proper tag
@Tag("integration")
public class EdgarDownloadIntegrationTest { }

// GOOD: Example/demo in test with tag
@Tag("example")
@Disabled("Example code - not a test")
public class SecConnectionExample { }
```

#### Correct Resource Placement
```
// GOOD: Test resources properly placed
src/test/resources/test-model.json
src/test/resources/mock-edgar-response.xml

// GOOD: Production resources
src/main/resources/cik-registry.json
src/main/resources/sec-model-template.json
```

### Build Artifacts

#### Output Directories
- **Gradle**: `build/` directory for all outputs
- **Maven**: `target/` directory for all outputs
- **NEVER** commit these directories

#### GitIgnore Requirements
```gitignore
# Required .gitignore entries
*.class
*.jar
build/
target/
out/
.gradle/
```

### Migration Guidelines

#### Moving Misplaced Files
1. **Identify** all files in wrong locations
2. **Create** proper package structure if missing
3. **Move** files to correct locations with proper packages
4. **Update** package declarations and imports
5. **Delete** any `.class` files
6. **Update** build configuration if needed
7. **Test** that build still works

#### Example Migration
```bash
# BAD: Current structure
/sec/TestDJIModel.java
/sec/TestDJIModel.class

# Step 1: Move to proper location
mv /sec/TestDJIModel.java \
   /sec/src/test/java/org/apache/calcite/adapter/sec/DJIModelTest.java

# Step 2: Update package in file
package org.apache.calcite.adapter.sec;

# Step 3: Delete compiled files
rm /sec/*.class

# Step 4: Update .gitignore
echo "*.class" >> .gitignore
```

### Enforcement Rules

#### Build-Time Checks
- Configure build to fail if files exist in wrong locations
- Add Maven Enforcer or Gradle task to verify structure
- CI/CD pipeline should validate organization

#### Code Review Requirements
- **REJECT** PRs with files in root directory
- **REJECT** PRs with `.class` files
- **REJECT** PRs with tests in default package
- **REQUIRE** proper package structure

### Special Cases

#### Example Applications
Two acceptable approaches:
1. **Separate examples module**: `sec-examples/` as sibling module
2. **Tagged test examples**: In `src/test/java/` with `@Tag("example")` and `@Disabled`

#### Performance/Load Tests
- Place in `src/test/java/`
- Use `@Tag("performance")` or `@Tag("load")`
- May be excluded from default test runs

#### Integration Test Data
- Large test files: `src/test/resources/data/`
- Consider external storage with documentation
- Never commit massive test data files

### Documentation Files
- **README.md**: Module root (allowed)
- **CLAUDE.md**: Module root (allowed)
- **Design docs**: `docs/` subdirectory
- **NEVER** scatter `.md` files throughout source tree

## JDBC Integration Testing Standards

### Core Principle
**The SEC adapter is fundamentally a JDBC adapter - test it as such.** All integration tests must interact with the adapter through its JDBC interface using model files and SQL queries. Direct API calls or internal class usage in integration tests violates this principle.

### Standard Test Pattern
Every integration test MUST follow this pattern:
1. **Create Model JSON** → 2. **Establish JDBC Connection** → 3. **Execute SQL Query** → 4. **Validate Results**

### Test Structure Template

```java
@Tag("integration")
public class SecQueryIntegrationTest {
    private String testDataDir;
    private String modelPath;
    
    @BeforeEach
    void setUp() throws Exception {
        // Create unique test directory - NEVER use @TempDir
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String testName = testInfo.getTestMethod().get().getName();
        testDataDir = "build/test-data/" + getClass().getSimpleName() + "/" + testName + "_" + timestamp;
        Files.createDirectories(Paths.get(testDataDir));
        
        // Create test model
        modelPath = createTestModel();
    }
    
    @Test
    void testBasicQuery() throws Exception {
        Properties props = new Properties();
        props.setProperty("lex", "ORACLE");
        props.setProperty("unquotedCasing", "TO_LOWER");
        
        try (Connection conn = DriverManager.getConnection(
                "jdbc:calcite:model=" + modelPath, props)) {
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT cik, company_name, fiscal_year " +
                     "FROM sec_filings " +
                     "WHERE fiscal_year >= 2020")) {
                
                // Validate results
                assertTrue(rs.next());
                assertEquals(10, rs.getString("cik").length());
                assertTrue(rs.getInt("fiscal_year") >= 2020);
                
                // Validate metadata
                ResultSetMetaData meta = rs.getMetaData();
                assertEquals(3, meta.getColumnCount());
                assertEquals("cik", meta.getColumnName(1).toLowerCase());
            }
        }
    }
    
    @AfterEach
    void tearDown() {
        // Manual cleanup - NEVER rely on @TempDir
        try {
            if (testDataDir != null && Files.exists(Paths.get(testDataDir))) {
                Files.walk(Paths.get(testDataDir))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            }
        } catch (IOException e) {
            // Log but don't fail test
            System.err.println("Warning: Could not clean test directory: " + e.getMessage());
        }
    }
    
    private String createTestModel() throws Exception {
        String model = """
            {
              "version": "1.0",
              "defaultSchema": "SEC",
              "schemas": [{
                "name": "SEC",
                "type": "custom",
                "factory": "org.apache.calcite.adapter.sec.SecSchemaFactory",
                "operand": {
                  "directory": "%s",
                  "ephemeralCache": true,
                  "testMode": true,
                  "sec.fallback.enabled": false,
                  "ciks": ["AAPL"],
                  "startYear": 2020,
                  "endYear": 2021
                }
              }]
            }
            """.formatted(testDataDir);
        
        Path modelFile = Paths.get(testDataDir, "model.json");
        Files.writeString(modelFile, model);
        return modelFile.toString();
    }
}
```

### Test Directory Management

#### Required Pattern
- **NEVER use JUnit @TempDir** - causes cleanup failures with parallel tests
- Use build directory pattern: `build/test-data/{TestClass}/{testMethod}_{timestamp}/`
- Add timestamp or UUID for parallel safety
- Manual cleanup in @AfterEach with error handling

#### Directory Structure
```
build/
└── test-data/
    └── SecQueryIntegrationTest/
        ├── testBasicSelect_20240104_143022/
        │   ├── model.json
        │   └── cache/
        └── testAggregation_20240104_143025/
            ├── model.json
            └── cache/
```

### Model File Requirements

#### Test Model Configuration
```json
{
  "version": "1.0",
  "defaultSchema": "SEC",
  "schemas": [{
    "name": "SEC",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.sec.SecSchemaFactory",
    "operand": {
      "directory": "build/test-data/...",
      "ephemeralCache": true,        // Required for test isolation
      "testMode": true,               // Disables production features
      "sec.fallback.enabled": false,  // No fallbacks in tests
      "ciks": ["TEST1", "TEST2"],
      "useMockData": true             // Use test data, not real EDGAR
    }
  }]
}
```

### Query Testing Requirements

#### Required Test Coverage
1. **Schema Discovery**
   ```sql
   SELECT * FROM information_schema.tables WHERE table_schema = 'SEC'
   SELECT * FROM information_schema.columns WHERE table_name = 'sec_filings'
   ```

2. **Basic Queries**
   ```sql
   SELECT * FROM sec_filings WHERE cik = ?
   SELECT COUNT(*) FROM financial_statements
   ```

3. **Complex Queries**
   ```sql
   -- Joins
   SELECT f.*, s.* FROM sec_filings f 
   JOIN financial_statements s ON f.accession = s.accession
   
   -- Aggregations
   SELECT cik, AVG(revenue) as avg_revenue 
   FROM financial_statements 
   GROUP BY cik
   ```

4. **Error Conditions**
   - Invalid CIK format
   - Malformed SQL
   - Missing required parameters
   - Timeout handling

### Test Categories and Tagging

```java
@Tag("unit")        // Mock JDBC, no real connections, no file I/O
@Tag("integration") // Real JDBC connections, local test data
@Tag("e2e")         // End-to-end with real EDGAR data (manual only)
@Tag("performance") // Large dataset tests, excluded from CI
@Tag("manual")      // Requires external resources or credentials
```

### Prohibited Testing Patterns

#### Never Do This
```java
// BAD: main() method test
public class TestSEC {
    public static void main(String[] args) {
        // DON'T DO THIS
    }
}

// BAD: Direct API usage
SecDataFetcher fetcher = new SecDataFetcher();
List<String> data = fetcher.fetchData(); // NO!

// BAD: System.out for validation
System.out.println("Result: " + result); // Use assertions!

// BAD: Hardcoded paths
String modelPath = "/Users/john/test/model.json"; // NO!

// BAD: @TempDir usage
@TempDir
Path tempDir; // Causes parallel test failures!
```

### PreparedStatement Usage

```java
@Test
void testPreparedStatement() throws Exception {
    try (Connection conn = getTestConnection()) {
        String sql = "SELECT * FROM sec_filings WHERE cik = ? AND fiscal_year = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, "0000320193");  // Apple's CIK
            pstmt.setInt(2, 2023);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("0000320193", rs.getString("cik"));
                assertEquals(2023, rs.getInt("fiscal_year"));
            }
        }
    }
}
```

### Resource Management Pattern

```java
@Test
void testWithResources() throws Exception {
    // Always use try-with-resources
    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
        
        // Process results
        while (rs.next()) {
            // Assertions
        }
    } // Auto-cleanup of all resources
}
```

### Parallel Test Safety

```java
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)  // Safe with proper directory management
class SecParallelTest {
    @Test
    void test1() {
        String uniqueDir = createUniqueTestDir();
        // Test implementation
    }
    
    @Test
    void test2() {
        String uniqueDir = createUniqueTestDir();
        // Test implementation
    }
    
    private String createUniqueTestDir() {
        return "build/test-data/" + 
               getClass().getSimpleName() + "/" +
               Thread.currentThread().getName() + "_" +
               System.nanoTime();
    }
}
```

### Migration Checklist

For existing tests that need migration:
1. ✓ Convert main() methods to @Test methods
2. ✓ Replace direct API calls with JDBC queries
3. ✓ Create model.json files for configuration
4. ✓ Use build directory for test data
5. ✓ Remove @TempDir annotations
6. ✓ Add proper @Tag annotations
7. ✓ Implement try-with-resources
8. ✓ Replace System.out with assertions
9. ✓ Add ResultSetMetaData validation
10. ✓ Ensure parallel execution safety

## SEC-Specific Principles

### Financial Data Integrity
- **ALWAYS** preserve original precision of financial values
- **NEVER** round or truncate monetary amounts during processing
- Store amounts in smallest unit (cents) as BIGINT when possible
- Maintain audit trail of data transformations

### XBRL Processing
- Support both inline XBRL (iXBRL) and traditional XBRL formats
- **ALWAYS** extract both primary documents and detailed tags
- Preserve context (period, segment, scenario) for each fact
- Handle amendments (10-K/A, 10-Q/A) appropriately

### Performance Optimization
- Use prepared statements for repeated queries
- Leverage DuckDB's native Parquet support for analytics
- Implement incremental data updates (only download new filings)
- Use bloom filters for efficient CIK/ticker lookups

## Documentation Standards

- **ALWAYS** document SEC EDGAR API changes and limitations
- Include examples of common financial queries
- Document CIK registry updates and ticker mappings
- **NEVER** create documentation unless explicitly requested

## Important SEC Adapter Rules

- **ALWAYS** respect SEC EDGAR terms of service
- **NEVER** bypass rate limits or access restrictions
- **ALWAYS** handle network failures gracefully with exponential backoff
- **NEVER** expose user credentials or API keys in logs
- **ALWAYS** validate financial calculations against SEC originals

## Plan Review Requirement

Before implementing SEC adapter changes:
- Verify EDGAR API compliance
- Check rate limiting implementation
- Ensure proper CIK normalization
- Validate partition strategy
- Confirm caching behavior
- Test with various filing types
- Verify financial data integrity