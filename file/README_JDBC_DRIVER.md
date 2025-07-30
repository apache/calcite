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

# JDBC Driver Development Guide for Calcite File Adapter

## Overview

This guide is for developers creating JDBC drivers that extend the Calcite File Adapter with materialized view capabilities and large dataset spillover support. It provides technical implementation details and best practices for building production-ready JDBC drivers that can handle datasets larger than RAM.

### New Capabilities (Latest Release)

- **🚀 Unlimited Dataset Sizes**: Process TB+ files through automatic disk spillover
- **📊 Memory Management**: Configure per-table memory limits and spillover behavior
- **⚡ Multiple Large Tables**: Reference hundreds of large tables simultaneously
- **📈 Spillover Monitoring**: Track spillover statistics and performance metrics
- **🗂️ Partitioned Tables**: Support for Hive-style and custom partitioned Parquet datasets
- **🔍 Partition Pruning**: Automatic query optimization with partition filters
- **♻️ Auto-Refresh Tables**: Configure refresh intervals for automatic data updates
- **🎯 Custom Regex Partitions**: Extract partitions from any filename pattern
- **📁 Excel/HTML Support**: Automatic table discovery from Excel and HTML files
- **🏆 Performance Engine**: Multiple execution engines with up to 1.6x speedup

### ⚠️ **Format-Specific Limitations**

**True Streaming (Unlimited Size):**
- **CSV/TSV**: ✅ Line-by-line streaming, handles TB+ files
- **Arrow**: ✅ Native columnar streaming support
- **Parquet**: ✅ Row-group based streaming

**Non-Streaming (Initial RAM Requirement):**
- **JSON**: ⚠️ Must parse entire file first, then spillover batches
- **YAML**: ⚠️ Must parse entire document first, then spillover batches
- **Excel**: ⚠️ Must process entire workbook first, then spillover batches

*Note: For JSON files larger than available RAM, consider converting to CSV or using streaming JSON parsers externally.*

## Architecture

### Core Components

```
MaterializedViewJdbcDriver
├── extends org.apache.calcite.jdbc.Driver
├── Connection Management
│   ├── Schema Registration
│   ├── Materialized View Discovery
│   └── Configuration Parsing
└── Runtime Recovery
    ├── Restart Survival
    ├── Storage Management
    └── Error Handling
```

### Class Structure

```java
public class MaterializedViewJdbcDriver extends Driver {
    // Static registration
    static {
        new MaterializedViewJdbcDriver().register();
    }

    // Core methods to implement
    @Override
    public Connection connect(String url, Properties info) throws SQLException;
    @Override
    public boolean acceptsURL(String url) throws SQLException;
    @Override
    protected DriverVersion createDriverVersion();
}
```

## Implementation Steps

### 1. Driver Registration

```java
public class YourMaterializedViewDriver extends Driver {
    private static final Logger LOGGER = Logger.getLogger(YourMaterializedViewDriver.class.getName());

    // Automatic registration via static block
    static {
        new YourMaterializedViewDriver().register();
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        // Define your URL pattern
        return url.startsWith("jdbc:calcite:") &&
               (url.contains("schema=file") || url.contains("materialized_view"));
    }
}
```

### 2. Connection Hook Implementation

```java
@Override
public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
        return null;
    }

    // Get base Calcite connection
    Connection connection = super.connect(url, info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

    // Add your materialized view logic
    setupSchemasAndRecoverMaterializedViews(calciteConnection, url, info);

    return connection;
}
```

### 3. Schema Setup and Recovery

```java
private void setupSchemasAndRecoverMaterializedViews(CalciteConnection connection,
                                                    String url, Properties info) {
    try {
        SchemaPlus rootSchema = connection.getRootSchema();

        // Parse your configuration format
        MaterializedViewConfig config = parseConfiguration(url, info);

        // Setup each schema with materialized view support
        for (SchemaConfig schemaConfig : config.getSchemas()) {
            setupSchemaWithMaterializedViews(rootSchema, schemaConfig);
        }

    } catch (Exception e) {
        LOGGER.severe("Failed to setup schemas: " + e.getMessage());
        throw new RuntimeException("Schema setup failed", e);
    }
}
```

### 4. Configuration Parsing

```java
private MaterializedViewConfig parseConfiguration(String url, Properties info) {
    // Example URL format with spillover configuration:
    // jdbc:calcite:schema=file;storage_path=/mnt/ssd;engine=parquet;batch_size=10000;memory_threshold=67108864;spill_directory=/tmp/calcite

    MaterializedViewConfig config = new MaterializedViewConfig();

    // Parse URL parameters
    String[] urlParts = url.split(";");
    Map<String, String> urlParams = new HashMap<>();

    for (String part : urlParts) {
        if (part.contains("=")) {
            String[] keyValue = part.split("=", 2);
            urlParams.put(keyValue[0], keyValue.length > 1 ? keyValue[1] : "");
        }
    }

    // Extract configuration
    String dataPath = getConfigValue("data_path", urlParams, info, "./data");
    String storagePath = getConfigValue("storage_path", urlParams, info,
                                       System.getProperty("java.io.tmpdir") + "/calcite_mv");
    String engine = getConfigValue("engine", urlParams, info, "parquet");
    int batchSize = Integer.parseInt(getConfigValue("batch_size", urlParams, info, "10000"));

    // NEW: Spillover configuration with comprehensive options
    long memoryThreshold = Long.parseLong(getConfigValue("memory_threshold", urlParams, info, "67108864")); // 64MB default
    String spillDirectory = getConfigValue("spill_directory", urlParams, info,
                                          System.getProperty("java.io.tmpdir") + "/calcite_spill");
    boolean spilloverEnabled = Boolean.parseBoolean(getConfigValue("spillover_enabled", urlParams, info, "true"));

    // Advanced spillover options
    int maxSpillFiles = Integer.parseInt(getConfigValue("max_spill_files", urlParams, info, "100"));
    boolean spillCompression = Boolean.parseBoolean(getConfigValue("spill_compression", urlParams, info, "true"));
    String spillCleanupStrategy = getConfigValue("spill_cleanup", urlParams, info, "auto"); // auto, manual, background

    // Create schema configuration with enhanced spillover support
    SpilloverConfig spilloverConfig = new SpilloverConfig(memoryThreshold, spillDirectory, spilloverEnabled,
                                                         maxSpillFiles, spillCompression, spillCleanupStrategy);

    // NEW: Partitioned table configuration
    String partitionedTablesJson = getConfigValue("partitioned_tables", urlParams, info, null);
    List<PartitionedTableConfig> partitionedTables = null;
    if (partitionedTablesJson != null) {
        partitionedTables = parsePartitionedTables(partitionedTablesJson);
    }

    // NEW: Remote file refresh configuration
    String refreshInterval = getConfigValue("refresh_interval", urlParams, info, null);
    boolean enableRemoteRefresh = Boolean.parseBoolean(
        getConfigValue("enable_remote_refresh", urlParams, info, "true"));

    SchemaConfig schemaConfig = new SchemaConfig("files", dataPath, storagePath, engine,
                                                 batchSize, spilloverConfig, partitionedTables);
    config.addSchema(schemaConfig);

    return config;
}

private String getConfigValue(String key, Map<String, String> urlParams,
                             Properties info, String defaultValue) {
    // Check URL first, then properties, then default
    return urlParams.getOrDefault(key,
           info.getProperty("materialized_view." + key,
           info.getProperty(key, defaultValue)));
}
```

### 5. Schema Registration with Materialized Views

```java
private void setupSchemaWithMaterializedViews(SchemaPlus rootSchema, SchemaConfig config) {
    try {
        LOGGER.info("Setting up schema: " + config.getName());

        // Create execution engine configuration with spillover support
        ExecutionEngineConfig engineConfig = new ExecutionEngineConfig(
            config.getEngineType(),
            config.getBatchSize(),
            config.getStoragePath(),
            config.getSpilloverConfig()
        );

        // Create enhanced file schema with materialized view support
        File dataDirectory = new File(config.getDataPath());
        EnhancedFileSchema schema = EnhancedFileSchema.createDirectorySchema(
            rootSchema,
            config.getName(),
            dataDirectory,
            engineConfig
        );

        // Register the schema
        rootSchema.add(config.getName(), schema);

        // Log discovered materialized views
        Set<String> materializedViews = schema.getMaterializedViewNames();
        if (!materializedViews.isEmpty()) {
            LOGGER.info("Recovered materialized views: " + materializedViews);
        }

    } catch (Exception e) {
        LOGGER.severe("Failed to setup schema " + config.getName() + ": " + e.getMessage());
        throw new RuntimeException("Schema setup failed", e);
    }
}
```

## Configuration Classes

### MaterializedViewConfig

```java
public class MaterializedViewConfig {
    private final List<SchemaConfig> schemas = new ArrayList<>();

    public void addSchema(SchemaConfig schema) {
        schemas.add(schema);
    }

    public List<SchemaConfig> getSchemas() {
        return schemas;
    }
}
```

### SchemaConfig

```java
public class SchemaConfig {
    private final String name;
    private final String dataPath;
    private final String storagePath;
    private final String engineType;
    private final int batchSize;
    private final SpilloverConfig spilloverConfig;
    private final List<PartitionedTableConfig> partitionedTables;

    public SchemaConfig(String name, String dataPath, String storagePath,
                       String engineType, int batchSize, SpilloverConfig spilloverConfig,
                       List<PartitionedTableConfig> partitionedTables) {
        this.name = name;
        this.dataPath = dataPath;
        this.storagePath = storagePath;
        this.engineType = engineType;
        this.batchSize = batchSize;
        this.spilloverConfig = spilloverConfig;
        this.partitionedTables = partitionedTables;
    }

    // Getters...
}
```

### SpilloverConfig

```java
public class SpilloverConfig {
    private final long memoryThreshold;
    private final String spillDirectory;
    private final boolean enabled;
    private final int maxSpillFiles;
    private final boolean compressionEnabled;
    private final String cleanupStrategy;

    public SpilloverConfig(long memoryThreshold, String spillDirectory, boolean enabled,
                          int maxSpillFiles, boolean compressionEnabled, String cleanupStrategy) {
        this.memoryThreshold = memoryThreshold;
        this.spillDirectory = spillDirectory;
        this.enabled = enabled;
        this.maxSpillFiles = maxSpillFiles;
        this.compressionEnabled = compressionEnabled;
        this.cleanupStrategy = cleanupStrategy;
    }

    // Getters...
}
```

## Advanced Features

### Multiple Schema Support

```java
private void addConfiguredSchemas(MaterializedViewConfig config, Properties info) {
    // Support configuration like:
    // schema.sales.data_path=/data/sales
    // schema.sales.storage_path=/mnt/sales_mv
    // schema.analytics.data_path=/data/analytics
    // schema.analytics.storage_path=/mnt/analytics_mv

    Set<String> schemaNames = new HashSet<>();

    // Find all schema.* properties
    for (Object key : info.keySet()) {
        String keyStr = key.toString();
        if (keyStr.startsWith("schema.") && keyStr.contains(".")) {
            String[] parts = keyStr.split("\\.", 3);
            if (parts.length >= 2) {
                schemaNames.add(parts[1]);
            }
        }
    }

    // Create configuration for each schema
    for (String schemaName : schemaNames) {
        String dataPath = info.getProperty("schema." + schemaName + ".data_path");
        String storagePath = info.getProperty("schema." + schemaName + ".storage_path");
        String engine = info.getProperty("schema." + schemaName + ".engine", "parquet");

        if (dataPath != null && storagePath != null) {
            // Extract spillover configuration per schema
            long memoryThreshold = Long.parseLong(info.getProperty("schema." + schemaName + ".memory_threshold", "67108864"));
            String spillDirectory = info.getProperty("schema." + schemaName + ".spill_directory",
                                                   System.getProperty("java.io.tmpdir") + "/calcite_spill_" + schemaName);

            SpilloverConfig spilloverConfig = new SpilloverConfig(memoryThreshold, spillDirectory, true, 100, true, "auto");
            SchemaConfig schemaConfig = new SchemaConfig(schemaName, dataPath, storagePath, engine, 2048, spilloverConfig);
            config.addSchema(schemaConfig);
        }
    }
}
```

### Deduplication and Caching

```java
public class MaterializedViewJdbcDriver extends Driver {
    // Track initialized schemas to avoid duplicate setup
    private static final Set<String> INITIALIZED_SCHEMAS = ConcurrentHashMap.newKeySet();

    private void setupSchema(SchemaPlus rootSchema, SchemaConfig schemaConfig) {
        String schemaKey = schemaConfig.getName() + ":" + schemaConfig.getDataPath();

        // Avoid duplicate initialization
        if (INITIALIZED_SCHEMAS.contains(schemaKey)) {
            LOGGER.info("Schema already initialized: " + schemaConfig.getName());
            return;
        }

        // ... setup logic ...

        INITIALIZED_SCHEMAS.add(schemaKey);
    }
}
```

### Error Handling and Logging

```java
private void setupSchemasAndRecoverMaterializedViews(CalciteConnection connection,
                                                    String url, Properties info) {
    try {
        // ... setup logic ...
    } catch (IOException e) {
        LOGGER.severe("I/O error during schema setup: " + e.getMessage());
        throw new SQLException("Failed to read schema configuration", e);
    } catch (IllegalArgumentException e) {
        LOGGER.severe("Invalid configuration: " + e.getMessage());
        throw new SQLException("Invalid driver configuration", e);
    } catch (Exception e) {
        LOGGER.severe("Unexpected error during schema setup: " + e.getMessage());
        throw new SQLException("Schema setup failed", e);
    }
}
```

## Driver Version Information

```java
@Override
protected DriverVersion createDriverVersion() {
    return new DriverVersion(
        "Your Materialized View JDBC Driver",
        "1.0.0",                              // Your driver version
        "Your Organization",
        "1.41.0-SNAPSHOT",                    // Calcite version
        true,                                 // JDBC compliant
        1, 0,                                 // Driver major/minor
        1, 41                                 // Calcite major/minor
    );
}
```

## Testing Your Driver

### Unit Tests

```java
@Test
public void testDriverRegistration() throws SQLException {
    Driver driver = DriverManager.getDriver("jdbc:calcite:schema=file");
    assertNotNull(driver);
    assertTrue(driver instanceof MaterializedViewJdbcDriver);
}

@Test
public void testConnectionCreation() throws SQLException {
    String url = "jdbc:calcite:schema=file;data_path=/tmp/test;storage_path=/tmp/mv";
    try (Connection conn = DriverManager.getConnection(url)) {
        assertNotNull(conn);
        assertTrue(conn.unwrap(CalciteConnection.class) != null);
    }
}

@Test
public void testSpilloverConfiguration() throws SQLException {
    String url = "jdbc:calcite:schema=file;data_path=/tmp/test;storage_path=/tmp/mv;"
               + "engine=parquet;memory_threshold=8388608;spill_directory=/tmp/test_spill";
    try (Connection conn = DriverManager.getConnection(url)) {
        // Verify spillover is properly configured
        CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
        SchemaPlus schema = calciteConn.getRootSchema().getSubSchema("files");
        assertNotNull(schema);

        // Test with a query that would trigger spillover
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM large_dataset");
        assertTrue(rs.next());
        assertTrue(rs.getLong(1) > 0);
    }
}
```

### Integration Tests

```java
@Test
public void testMaterializedViewRecovery() throws SQLException {
    // Create test materialized view files
    createTestMaterializedViews();

    String url = "jdbc:calcite:schema=file;data_path=" + testDataDir + ";storage_path=" + testMvDir;
    try (Connection conn = DriverManager.getConnection(url)) {
        // Verify materialized views are available
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet tables = metaData.getTables(null, null, null, new String[]{"TABLE"});

        Set<String> tableNames = new HashSet<>();
        while (tables.next()) {
            tableNames.add(tables.getString("TABLE_NAME"));
        }

        assertTrue(tableNames.contains("sales_summary"));
    }
}
```

## Production Considerations

### Performance

```java
// Use connection pooling
HikariConfig config = new HikariConfig();
config.setDriverClassName("com.yourcompany.MaterializedViewJdbcDriver");
config.setJdbcUrl(jdbcUrl);
config.setMaximumPoolSize(20);
```

### Monitoring

```java
// Add metrics collection
private void logSchemaSetupMetrics(SchemaConfig config, long setupTimeMs) {
    LOGGER.info("Schema setup completed: {} in {}ms", config.getName(), setupTimeMs);

    // Send to your monitoring system
    Metrics.timer("schema.setup.time")
           .tag("schema", config.getName())
           .tag("engine", config.getEngineType())
           .record(setupTimeMs, TimeUnit.MILLISECONDS);
}
```

### Security

```java
// Validate paths to prevent directory traversal
private void validatePath(String path) throws SQLException {
    try {
        Path normalizedPath = Paths.get(path).normalize();
        if (!normalizedPath.isAbsolute()) {
            throw new SQLException("Path must be absolute: " + path);
        }
        // Additional security checks...
    } catch (InvalidPathException e) {
        throw new SQLException("Invalid path: " + path, e);
    }
}
```

### Remote File Refresh

The JDBC driver supports efficient remote file refresh for HTTP/HTTPS/S3/FTP sources:

```java
// Configuration for remote file refresh
String url = "jdbc:calcite:schema=file;data_path=/tmp/data;"
           + "refresh_interval=5m;enable_remote_refresh=true";

Properties props = new Properties();
props.setProperty("tables", "[{"
    + "\"name\":\"api_data\","
    + "\"url\":\"https://api.example.com/data.csv\","
    + "\"refresh_interval\":\"30s\""
    + "}]");

try (Connection conn = DriverManager.getConnection(url, props)) {
    // The driver will automatically check for updates
    // using HEAD requests (HTTP) or metadata checks (S3)
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM api_data");

    // Wait for refresh interval...
    Thread.sleep(31000);

    // Next query will check if remote file changed
    // Only re-downloads if ETag/Last-Modified changed
    rs = stmt.executeQuery("SELECT * FROM api_data");
}
```

**Benefits:**
- Reduces bandwidth usage by checking metadata instead of downloading
- Supports HTTP ETag and Last-Modified headers
- Works with S3 object metadata
- Configurable refresh intervals per table or schema-wide

## Materialized Views with Spillover

### Impact on Materialized View Design

The spillover functionality fundamentally changes materialized view capabilities:

#### ✅ **New Capabilities**

```java
// 1. Unlimited Materialized View Size
// Can create materialized views from TB+ datasets
CREATE MATERIALIZED VIEW sales_summary AS
SELECT region, product_category, SUM(amount) as total_sales
FROM huge_sales_data  -- 1TB+ CSV file
GROUP BY region, product_category;

// 2. Multiple Large Views Simultaneously
// Each view uses only 8-64MB RAM regardless of source size
CREATE MATERIALIZED VIEW customer_analytics AS ...;  -- From 500GB dataset
CREATE MATERIALIZED VIEW product_metrics AS ...;     -- From 800GB dataset
CREATE MATERIALIZED VIEW financial_summary AS ...;   -- From 1.2TB dataset
```

#### 🔄 **Storage Strategy Recommendations**

```java
// For Large Materialized Views - use CSV format
String largeViewConfig = "jdbc:calcite:schema=file;"
    + "storage_path=/mnt/ssd/materialized_views;"
    + "engine=parquet;"
    + "format=csv;"                    // Streaming-compatible
    + "batch_size=10000;"
    + "memory_threshold=67108864";     // 64MB per view

// For Analytical Materialized Views - use Parquet format
String analyticsConfig = "jdbc:calcite:schema=file;"
    + "storage_path=/mnt/nvme/analytics_views;"
    + "engine=parquet;"
    + "format=parquet;"                // Columnar compression
    + "batch_size=8192;"
    + "memory_threshold=134217728";    // 128MB per view
```

#### 🏗️ **Architecture Patterns**

**Pattern 1: Spillover as Materialized View Storage**
```java
// Use spillover mechanism as the storage layer
public class SpilloverMaterializedView {
    private final ParquetEnumerator enumerator;
    private final Path persistentSpillPath;

    public void persist() {
        // Convert spillover files to persistent storage
        moveSpillFiles(enumerator.getSpillDirectory(), persistentSpillPath);
    }
}
```

**Pattern 2: Tiered Materialized View Storage**
```java
// Hot/Warm/Cold data tiers
public class TieredMaterializedView {
    private final Map<String, Object> hotData;      // In memory
    private final Path warmDataPath;                // Spillover files
    private final Path coldDataPath;                // Persistent storage

    public ResultSet query(String sql) {
        // Check hot data first, then warm, then cold
    }
}
```

**Pattern 3: Cross-Session Shared Views**
```java
// Share spillover files across JDBC connections
String sharedConfig = "jdbc:calcite:schema=file;"
    + "spill_directory=/shared/materialized_views;"  // Shared location
    + "shared_spillover=true;"                       // Enable sharing
    + "cleanup_on_close=false";                      // Preserve for other sessions
```

#### 📊 **Format Selection Guide**

| Materialized View Type | Recommended Format | Spillover Behavior | Best For |
|----------------------|------------------|------------------|----------|
| Large Aggregations | CSV | True streaming | Multi-TB source data |
| Analytics Cubes | Parquet | Columnar batches | Complex analytics |
| Small Lookups | JSON | In-memory batches | Reference data |
| Real-time Updates | Arrow | Vector streaming | Live dashboards |

#### ⚡ **Performance Optimizations**

```java
// 1. Pre-warm frequently accessed materialized views
public void prewarmMaterializedViews(List<String> viewNames) {
    for (String viewName : viewNames) {
        // Load first batch into memory
        ParquetEnumerator enum = getMaterializedView(viewName);
        enum.moveNext(); // Triggers first batch load
    }
}

// 2. Background spillover cleanup
@Scheduled(fixedRate = 300000) // Every 5 minutes
public void cleanupOldSpillFiles() {
    for (MaterializedView view : activeMaterializedViews) {
        view.getEnumerator().cleanupOldSpillFiles(MAX_SPILL_FILES);
    }
}

// 3. Spillover monitoring and alerting
public void monitorSpilloverHealth() {
    for (MaterializedView view : activeMaterializedViews) {
        StreamingStats stats = view.getEnumerator().getStreamingStats();
        if (stats.getSpillRatio() > 0.8) { // 80% spilled
            logger.warn("High spillover ratio for view {}: {}",
                       view.getName(), stats.getSpillRatio());
        }
    }
}
```

## Partitioned Table Support

### Configuration

```java
// Example URL with partitioned table and refresh configuration
String url = "jdbc:calcite:schema=file;"
    + "data_path=/data/warehouse;"
    + "engine=parquet;"
    + "refresh_interval=5 minutes;"        // Schema-level refresh interval
    + "partitioned_tables=[{"
    + "'name':'sales',"
    + "'pattern':'sales/**/*.parquet',"
    + "'partitions':{"
    + "'style':'hive'"  // Auto-detect year=2024/month=01 patterns
    + "}}"]";
```

### Partition Styles

1. **Hive-Style (Auto-detected)**
```json
{
  "name": "sales",
  "pattern": "sales/**/*.parquet"
  // Automatically detects key=value directory patterns
}
```

2. **Directory-Based**
```json
{
  "name": "events",
  "pattern": "events/**/*.parquet",
  "partitions": {
    "style": "directory",
    "columns": ["year", "month", "day"]
  }
}
```

3. **Typed Partitions**
```json
{
  "name": "metrics",
  "pattern": "metrics/**/*.parquet",
  "partitions": {
    "style": "directory",
    "columns": [
      {"name": "year", "type": "INTEGER"},
      {"name": "month", "type": "INTEGER"}
    ]
  }
}
```

### Performance Impact

| Query Type | Without Partitions | With Partitions | Improvement |
|------------|-------------------|------------------|-------------|
| Full scan (1000 files) | 12.5s | 12.5s | No change |
| Year filter | 12.5s | 0.42s | **30x faster** |
| Year+Month filter | 12.5s | 0.08s | **156x faster** |

## Refresh Functionality

The JDBC driver supports automatic table refresh through URL configuration:

### Configuration Options

Add refresh intervals to your JDBC URL:

```java
// Schema-level refresh (applies to all tables)
String url = "jdbc:calcite:schema=file;"
    + "data_path=/data/warehouse;"
    + "refresh_interval=5 minutes";     // All tables refresh every 5 minutes

// Table-specific refresh in URL parameters
String url = "jdbc:calcite:schema=file;"
    + "data_path=/data/warehouse;"
    + "refresh_interval=10 minutes;"    // Schema default
    + "table_refresh=products:1 hour,customers:30 minutes"; // Table overrides
```

### Custom Regex Partitions

For non-Hive naming conventions, configure custom partition extraction:

```java
// Extract year/month from filenames like "sales_2024_01.parquet"
String url = "jdbc:calcite:schema=file;"
    + "data_path=/data/warehouse;"
    + "partitioned_tables=[{"
    + "'name':'sales',"
    + "'pattern':'sales/*.parquet',"
    + "'partitions':{"
    + "'style':'custom',"
    + "'regex':'sales_(\\\\d{4})_(\\\\d{2})\\\\.parquet$',"
    + "'columnMappings':[{"
    + "'name':'year','group':1,'type':'INTEGER'"
    + "},{"
    + "'name':'month','group':2,'type':'INTEGER'"
    + "}]}"
    + "}]"

### Supported Intervals

- `"30 seconds"`, `"1 minute"`, `"5 minutes"`
- `"1 hour"`, `"2 hours"`, `"12 hours"`
- `"1 day"`, `"7 days"`

### Refresh Behaviors

- **CSV/JSON Files**: Checks modification time, reloads if changed
- **Directory Scans**: Reloads data changes in existing files (but no new files or schema changes)
- **Partitioned Tables**: Discovers new partitions automatically
- **Materialized Views**: Re-executes queries and updates cache

### Example Implementation

```java
@Override
public Connection connect(String url, Properties info) throws SQLException {
    Connection connection = super.connect(url, info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

    // Parse refresh configuration from URL
    String refreshInterval = parseRefreshInterval(url);
    Map<String, String> tableRefreshRates = parseTableRefreshRates(url);

    // Configure schema with refresh support
    setupRefreshableSchema(calciteConnection, refreshInterval, tableRefreshRates);

    return connection;
}
```

## Identifier Casing Configuration

The file adapter supports configurable identifier casing for both table and column names, which is particularly useful for database compatibility.

### Supported Casing Options

- **UPPER**: Converts identifiers to uppercase (default for table names)
- **LOWER**: Converts identifiers to lowercase (PostgreSQL-style)
- **UNCHANGED**: Preserves original case (default for column names)

### JDBC URL Configuration

```java
// PostgreSQL-style configuration (lowercase identifiers)
String pgUrl = "jdbc:calcite:schema=file;"
    + "data_path=/data/warehouse;"
    + "table_name_casing=LOWER;"      // Tables: sales_data instead of SALES_DATA
    + "column_name_casing=LOWER";      // Columns: customer_id instead of CUSTOMER_ID

// Oracle-style configuration (uppercase identifiers)
String oracleUrl = "jdbc:calcite:schema=file;"
    + "data_path=/data/warehouse;"
    + "table_name_casing=UPPER;"       // Tables: SALES_DATA (default)
    + "column_name_casing=UPPER";       // Columns: CUSTOMER_ID

// Case-sensitive configuration (preserve original)
String exactUrl = "jdbc:calcite:schema=file;"
    + "data_path=/data/warehouse;"
    + "table_name_casing=UNCHANGED;"   // Tables: Sales_Data
    + "column_name_casing=UNCHANGED";   // Columns: Customer_Id (default)
```

### Properties Configuration

```java
Properties info = new Properties();
info.setProperty("tableNameCasing", "LOWER");
info.setProperty("columnNameCasing", "LOWER");

Connection conn = DriverManager.getConnection(
    "jdbc:calcite:schema=file;data_path=/data", info);
```

### Use Cases

**PostgreSQL Compatibility:**
```java
// PostgreSQL expects lowercase unquoted identifiers
String url = "jdbc:calcite:schema=file;data_path=/data;"
    + "table_name_casing=LOWER;column_name_casing=LOWER";

// Now you can use PostgreSQL-style queries:
// SELECT customer_id FROM sales_data WHERE amount > 1000
```

**Legacy System Migration:**
```java
// Match existing Oracle system conventions
String url = "jdbc:calcite:schema=file;data_path=/data;"
    + "table_name_casing=UPPER;column_name_casing=UPPER";

// Queries use uppercase: SELECT CUSTOMER_ID FROM SALES_DATA
```

**File Name Preservation:**
```java
// Keep original file names as table names
String url = "jdbc:calcite:schema=file;data_path=/data;"
    + "table_name_casing=UNCHANGED;column_name_casing=UNCHANGED";

// File "Sales_Report_2024.csv" becomes table "Sales_Report_2024"
```

## Best Practices

1. **Always extend the base Calcite Driver** to inherit connection management
2. **Use static registration** for automatic driver discovery
3. **Implement proper error handling** with meaningful error messages
4. **Support both URL and Properties configuration** for flexibility
5. **Cache schema initialization** to avoid duplicate work
6. **Add comprehensive logging** for troubleshooting
7. **Validate configuration** before attempting schema creation
8. **Use thread-safe collections** for multi-threaded environments
9. **Test with various configuration scenarios** including edge cases
10. **Document your URL format and configuration options** clearly
11. **🆕 Configure spillover appropriately** for your materialized view sizes
12. **🆕 Monitor spillover statistics** in production environments
13. **🆕 Use CSV format for very large materialized views** to enable streaming
14. **🆕 Test with PARQUET engine** for best performance (328ms vs 538ms CSV)
15. **🆕 Consider partitioned tables** for large datasets with natural hierarchies
16. **🆕 Use typed partition columns** for numeric comparisons and aggregations
17. **🆕 Configure refresh intervals** for tables that change frequently
18. **🆕 Use custom regex partitions** for non-standard naming schemes
19. **🆕 Enable auto-discovery** for Excel and HTML table extraction
20. **🆕 Monitor refresh behavior** with RefreshableTable interface

## Common Pitfalls

- **Not calling super.connect()** - Always delegate to parent for base connection
- **Missing URL pattern validation** - Implement acceptsURL() correctly
- **Ignoring Properties configuration** - Support both URL and Properties
- **Not handling initialization failures** - Wrap exceptions appropriately
- **Duplicate schema registration** - Use caching to avoid conflicts
- **Missing driver version info** - Implement createDriverVersion() properly
- **🆕 Incorrect spillover configuration** - Ensure memory thresholds appropriate for dataset sizes
- **🆕 Not monitoring spillover activity** - Track spill ratios and performance impact
- **🆕 Using wrong engine for use case** - PARQUET for analytics, VECTORIZED for batch processing

## Resources

- **Calcite JDBC Driver Source**: Study the base Driver implementation
- **EnhancedFileSchema**: Reference implementation for materialized view support
- **ExecutionEngineConfig**: Configuration class for engine parameters
- **JDBC Specification**: Follow JDBC standards for compatibility

---

This guide provides the foundation for creating robust JDBC drivers with materialized view support. Adapt the examples to your specific requirements and test thoroughly in your target environment.
