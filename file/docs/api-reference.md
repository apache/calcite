# API Reference

This document provides comprehensive API reference for programmatic usage of the Apache Calcite File Adapter.

## Core Classes

### FileSchemaFactory

Main factory class for creating File Adapter schemas.

```java
public class FileSchemaFactory implements SchemaFactory {
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand);
}
```

**Configuration Parameters:**

| Parameter | Type | Description | Environment Variable Support |
|-----------|------|-------------|------------------------------|
| `directory` | String | Base directory path | ✅ Yes |
| `recursive` | Boolean | Enable recursive scanning | ✅ Yes |
| `executionEngine` | String | Execution engine selection (`linq4j`, `arrow`, `vectorized`, `parquet`, `duckdb`) | ✅ Yes |
| `batchSize` | Integer | Batch size for columnar engines (default: 2048) | ✅ Yes |
| `memoryThreshold` | Long | Memory threshold before spillover in bytes (default: 64MB) | ✅ Yes |
| `directoryPattern` | String | Directory pattern for glob-based discovery | ✅ Yes |
| `storageType` | String | Storage provider type (`local`, `s3`, `http`, `sharepoint`) | ✅ Yes |
| `storageConfig` | Map | Storage provider-specific configuration | ✅ Yes (nested values) |
| `refreshInterval` | String | Table refresh interval for dynamic sources | ✅ Yes |
| `tableNameCasing` | String | Table name casing transformation (`UPPER`, `LOWER`, `SMART_CASING`) | ✅ Yes |
| `columnNameCasing` | String | Column name casing transformation (`UPPER`, `LOWER`, `SMART_CASING`) | ✅ Yes |
| `primeCache` | Boolean | Whether to prime statistics cache on startup (default: true) | ✅ Yes |
| `ephemeralCache` | Boolean | Use temporary cache directory (default: false) | ✅ Yes |
| `duckdbConfig` | Map | DuckDB-specific configuration options | ✅ Yes (nested values) |
| `csvTypeInference` | Map | CSV type inference configuration | ✅ Yes (nested values) |

**Note:** All string, numeric, and boolean configuration values support environment variable substitution using `${VAR_NAME}` or `${VAR_NAME:default}` syntax.

**Example Usage:**

```java
Map<String, Object> operand = new HashMap<>();
operand.put("directory", "/path/to/data");
operand.put("recursive", true);
operand.put("executionEngine", "parquet");
operand.put("tableNameCasing", "SMART_CASING");
operand.put("columnNameCasing", "SMART_CASING");
operand.put("primeCache", true);

// DuckDB configuration example
Map<String, Object> duckdbConfig = new HashMap<>();
duckdbConfig.put("memory_limit", "8GB");
duckdbConfig.put("threads", 16);
operand.put("duckdbConfig", duckdbConfig);

FileSchemaFactory factory = new FileSchemaFactory();
Schema schema = factory.create(parentSchema, "FILES", operand);
```

**Example with Environment Variables:**

```java
// Model JSON with environment variables
String modelJson = """
{
  "version": "1.0",
  "defaultSchema": "${SCHEMA_NAME:files}",
  "schemas": [{
    "name": "${SCHEMA_NAME:files}",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "${DATA_DIR:/data}",
      "executionEngine": "${ENGINE_TYPE:PARQUET}",
      "batchSize": "${BATCH_SIZE:2048}",
      "ephemeralCache": "${USE_TEMP_CACHE:false}",
      "storageType": "${STORAGE_TYPE:local}",
      "storageConfig": {
        "bucketName": "${S3_BUCKET}",
        "region": "${AWS_REGION:us-east-1}"
      }
    }
  }]
}
""";

// Set environment variables (or use System.setProperty for testing)
System.setProperty("DATA_DIR", "/production/data");
System.setProperty("ENGINE_TYPE", "DUCKDB");
System.setProperty("BATCH_SIZE", "5000");
System.setProperty("STORAGE_TYPE", "s3");
System.setProperty("S3_BUCKET", "my-data-bucket");

// Create connection - variables are automatically substituted
Properties info = new Properties();
info.setProperty("model", "inline:" + modelJson);
Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
```

### FileSchema

Main schema implementation containing discovered tables.

```java
public class FileSchema extends AbstractSchema {
    public Map<String, Table> getTableMap();
    public Map<String, Function> getFunctionMap();
    public Expression getExpression(SchemaPlus parentSchema, String name);
}
```

**Methods:**

```java
// Get all discovered tables
Map<String, Table> tables = schema.getTableMap();

// Check if table exists
boolean exists = schema.getTableMap().containsKey("customers");

// Get specific table
Table customerTable = schema.getTable("customers");
```

## Table Implementations

### CsvTable

Table implementation for CSV files with type inference.

```java
public class CsvTable extends AbstractTable implements ScannableTable {
    public CsvTable(Source source, RelProtoDataType protoRowType);
    public Enumerable<Object[]> scan(DataContext root);
    public RelDataType getRowType(RelDataTypeFactory typeFactory);
}
```

**Configuration:**

```java
CsvTable table = new CsvTable(
    Sources.of(file),
    null,  // Auto-detect schema
    CsvEnumerator.identityList(columnCount),
    CsvEnumerator.identityList(fieldTypes)
);
```

### JsonTable

Table implementation for JSON files with nested data support.

```java
public class JsonTable extends AbstractTable implements ScannableTable {
    public JsonTable(Source source, RelProtoDataType protoRowType);
    public Enumerable<Object[]> scan(DataContext root);
}
```

**Multi-table extraction:**

```java
JsonMultiTableFactory factory = new JsonMultiTableFactory();
Map<String, Table> tables = factory.createTables(jsonSource, jsonConfig);
```

### ParquetTable

High-performance table implementation for Parquet files.

```java
public class ParquetScannableTable extends AbstractTable 
    implements ScannableTable, FilterableTable {
    
    public Enumerable<Object[]> scan(DataContext root);
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters);
}
```

**Features:**
- Column pruning
- Filter pushdown
- Statistics utilization
- Predicate pushdown

## Execution Engines

### ExecutionEngineConfig

Configure execution engine selection and parameters.

```java
public class ExecutionEngineConfig {
    public enum EngineType { PARQUET, DUCKDB, ARROW, LINQ4J }
    
    public void setEngineType(EngineType type);
    public void setEngineOptions(Map<String, Object> options);
}
```

**Usage:**

```java
ExecutionEngineConfig config = new ExecutionEngineConfig();
config.setEngineType(ExecutionEngineConfig.EngineType.PARQUET);

Map<String, Object> options = new HashMap<>();
options.put("compression", "snappy");
options.put("enableStatistics", true);
config.setEngineOptions(options);
```

### DuckDBExecutionEngine

High-performance analytical execution engine.

```java
public class DuckDBExecutionEngine {
    public void initialize(Map<String, Object> config);
    public ResultSet executeQuery(String sql);
    public void close();
}
```

**Configuration:**

```java
Map<String, Object> duckdbConfig = new HashMap<>();
duckdbConfig.put("memoryLimit", "4GB");
duckdbConfig.put("threads", 8);
duckdbConfig.put("enableOptimizations", true);

DuckDBExecutionEngine engine = new DuckDBExecutionEngine();
engine.initialize(duckdbConfig);
```

### VectorizedArrowExecutionEngine

Arrow-based vectorized execution engine.

```java
public class VectorizedArrowExecutionEngine {
    public void setBatchSize(int batchSize);
    public void setVectorizationEnabled(boolean enabled);
    public ColumnBatch processColumnBatch(ColumnBatch input);
}
```

**Advanced vectorization features:**

```java
// Configure column batch processing
AdaptiveColumnBatch batch = new AdaptiveColumnBatch();
batch.setCompressionEnabled(true);
batch.setSIMDOptimizations(true);

// SIMD-accelerated operations
SIMDColumnBatch simdBatch = new SIMDColumnBatch(batch);
simdBatch.enableSIMDOperations();
```

## Storage Providers

### StorageProvider Interface

Base interface for all storage providers.

```java
public interface StorageProvider {
    void initialize(Map<String, Object> config);
    List<StorageProviderFile> listFiles(String path);
    InputStream openFile(StorageProviderFile file);
    boolean fileExists(String path);
    void close();
}
```

### S3StorageProvider

Amazon S3 storage provider implementation.

```java
public class S3StorageProvider implements StorageProvider {
    public void setBucketName(String bucket);
    public void setRegion(String region);
    public void setCredentials(AWSCredentials credentials);
}
```

**Usage:**

```java
S3StorageProvider s3Provider = new S3StorageProvider();

Map<String, Object> config = new HashMap<>();
config.put("bucket", "my-data-bucket");
config.put("region", "us-east-1");
config.put("accessKey", System.getenv("AWS_ACCESS_KEY"));
config.put("secretKey", System.getenv("AWS_SECRET_KEY"));

s3Provider.initialize(config);
```

### HttpStorageProvider

HTTP/HTTPS storage provider for REST APIs.

```java
public class HttpStorageProvider implements StorageProvider {
    public void setBaseUrl(String baseUrl);
    public void setAuthenticationType(AuthType authType);
    public void setHeaders(Map<String, String> headers);
}
```

**Authentication configuration:**

```java
HttpStorageProvider httpProvider = new HttpStorageProvider();

Map<String, Object> config = new HashMap<>();
config.put("baseUrl", "https://api.example.com/data");
config.put("authType", "bearer");
config.put("authToken", System.getenv("API_TOKEN"));

Map<String, String> headers = new HashMap<>();
headers.put("User-Agent", "Calcite-File-Adapter/1.0");
headers.put("Accept", "application/json");
config.put("headers", headers);

httpProvider.initialize(config);
```

## Type System and Converters

### CsvTypeInferrer

Automatic type inference for CSV data.

```java
public class CsvTypeInferrer {
    public List<SqlTypeName> inferTypes(List<String[]> sampleRows);
    public void setNullStrings(List<String> nullStrings);
    public void setSampleSize(int sampleSize);
}
```

**Usage:**

```java
CsvTypeInferrer inferrer = new CsvTypeInferrer();
inferrer.setNullStrings(Arrays.asList("", "null", "NULL", "N/A"));
inferrer.setSampleSize(1000);

List<SqlTypeName> types = inferrer.inferTypes(sampleData);
```

### FileRowConverter

Convert file data to Calcite row format.

```java
public class FileRowConverter {
    public Object[] convertRow(String[] csvRow, List<SqlTypeName> types);
    public Object convertValue(String value, SqlTypeName type);
}
```

### JsonFlattener

Flatten nested JSON structures for table representation.

```java
public class JsonFlattener {
    public Map<String, Object> flatten(JsonNode jsonNode);
    public void setDelimiter(String delimiter);
    public void setMaxDepth(int maxDepth);
}
```

**Example:**

```java
JsonFlattener flattener = new JsonFlattener();
flattener.setDelimiter("_");
flattener.setMaxDepth(5);

JsonNode nestedJson = objectMapper.readTree(jsonString);
Map<String, Object> flattened = flattener.flatten(nestedJson);
```

## Statistics and Optimization

### HyperLogLogSketch

Cardinality estimation for query optimization.

```java
public class HyperLogLogSketch {
    public void add(Object value);
    public long estimate();
    public void merge(HyperLogLogSketch other);
    public byte[] serialize();
    public static HyperLogLogSketch deserialize(byte[] data);
}
```

**Usage:**

```java
HyperLogLogSketch sketch = new HyperLogLogSketch();

// Add values during data processing
for (Object value : columnValues) {
    sketch.add(value);
}

// Get cardinality estimate
long distinctCount = sketch.estimate();
```

### StatisticsProvider

Collect and manage column statistics.

```java
public class StatisticsProvider {
    public ColumnStatistics getColumnStatistics(String tableName, String columnName);
    public void updateStatistics(String tableName, String columnName, ColumnStatistics stats);
    public TableStatistics getTableStatistics(String tableName);
}
```

### StatisticsCache

Cache statistics for performance optimization.

```java
public class StatisticsCache {
    public void put(String key, ColumnStatistics stats);
    public ColumnStatistics get(String key);
    public void invalidate(String key);
    public void enablePersistence(String directory);
}
```

## Materialized Views

### MaterializedViewTable

Pre-computed query results with automatic refresh.

```java
public class MaterializedViewTable extends AbstractTable {
    public MaterializedViewTable(String sql, RefreshInterval interval);
    public void refresh();
    public boolean isStale();
    public Timestamp getLastRefresh();
}
```

**Usage:**

```java
String sql = "SELECT region, SUM(sales) as total FROM sales_data GROUP BY region";
RefreshInterval interval = RefreshInterval.hours(1);

MaterializedViewTable mvTable = new MaterializedViewTable(sql, interval);
```

### RefreshableMaterializedViewTable

Materialized view with automatic refresh capabilities.

```java
public class RefreshableMaterializedViewTable extends MaterializedViewTable {
    public void setRefreshSchedule(ScheduledExecutorService executor);
    public void enableAutoRefresh();
    public void disableAutoRefresh();
}
```

## Cache Management

### ConcurrentSpilloverManager

Manage memory and disk spillover for large datasets.

```java
public class ConcurrentSpilloverManager {
    public void setMemoryThreshold(long bytes);
    public void setSpilloverDirectory(String directory);
    public void enableCompression(boolean enabled);
}
```

**Configuration:**

```java
ConcurrentSpilloverManager manager = new ConcurrentSpilloverManager();
manager.setMemoryThreshold(512 * 1024 * 1024); // 512MB
manager.setSpilloverDirectory("/tmp/calcite-spill");
manager.enableCompression(true);
```

### HLLSketchCache

Cache HyperLogLog sketches for performance.

```java
public class HLLSketchCache {
    public void put(String key, HyperLogLogSketch sketch);
    public HyperLogLogSketch get(String key);
    public void enableRedisBackend(String redisHost, int redisPort);
}
```

## Configuration Builders

### FileSchemaConfigBuilder

Fluent API for building File Adapter configurations.

```java
public class FileSchemaConfigBuilder {
    public static FileSchemaConfigBuilder create();
    public FileSchemaConfigBuilder directory(String directory);
    public FileSchemaConfigBuilder recursive(boolean recursive);
    public FileSchemaConfigBuilder executionEngine(String engine);
    public FileSchemaConfigBuilder storageProvider(StorageProvider provider);
    public Map<String, Object> build();
}
```

**Usage:**

```java
Map<String, Object> config = FileSchemaConfigBuilder.create()
    .directory("/data")
    .recursive(true)
    .executionEngine("parquet")
    .csvConfig(csvConfig -> csvConfig
        .enableTypeInference(true)
        .nullStrings(Arrays.asList("", "null", "N/A")))
    .build();
```

## Event Handling and Monitoring

### FileAdapterEventListener

Listen to adapter events for monitoring and debugging.

```java
public interface FileAdapterEventListener {
    void onFileDiscovered(String filePath);
    void onTableCreated(String tableName, String filePath);
    void onQueryExecuted(String sql, long executionTimeMs);
    void onCacheHit(String cacheKey);
    void onCacheMiss(String cacheKey);
    void onError(Exception error, String context);
}
```

**Implementation:**

```java
FileAdapterEventListener listener = new FileAdapterEventListener() {
    @Override
    public void onQueryExecuted(String sql, long executionTimeMs) {
        logger.info("Query executed in {}ms: {}", executionTimeMs, sql);
    }
    
    @Override
    public void onError(Exception error, String context) {
        logger.error("Error in {}: {}", context, error.getMessage());
    }
};

fileSchema.addEventListener(listener);
```

## Utility Classes

### SmartCasing

Handle table and column name casing consistently.

```java
public class SmartCasing {
    public static String normalize(String name);
    public static String toSnakeCase(String camelCase);
    public static String toCamelCase(String snake_case);
}
```

### ComparableArrayList

List implementation for maintaining sorted order.

```java
public class ComparableArrayList<T extends Comparable<T>> extends ArrayList<T> {
    public void insertSorted(T element);
    public boolean containsSorted(T element);
}
```

## Exception Handling

### FileReaderException

Specific exception for file reading errors.

```java
public class FileReaderException extends SQLException {
    public FileReaderException(String message);
    public FileReaderException(String message, Throwable cause);
    public FileReaderException(String message, String sqlState);
}
```

**Common error codes:**

| SQL State | Description |
|-----------|-------------|
| `42000` | Syntax error in configuration |
| `42S02` | Table not found |
| `HY000` | General file access error |
| `08001` | Unable to connect to storage |
| `22018` | Data type conversion error |

## Performance Metrics

### FileAdapterMetrics

Collect performance and usage metrics.

```java
public class FileAdapterMetrics {
    public long getQueriesExecuted();
    public long getTotalExecutionTime();
    public double getAverageQueryTime();
    public long getCacheHitRate();
    public Map<String, Long> getTableAccessCounts();
}
```

**Usage:**

```java
FileAdapterMetrics metrics = fileSchema.getMetrics();

System.out.println("Queries executed: " + metrics.getQueriesExecuted());
System.out.println("Average query time: " + metrics.getAverageQueryTime() + "ms");
System.out.println("Cache hit rate: " + metrics.getCacheHitRate() + "%");
```

## Integration Examples

### JDBC Integration

```java
// Create connection with File Adapter
Properties props = new Properties();
props.put("model", "file-model.json");

Connection connection = DriverManager.getConnection(
    "jdbc:calcite:", props);

// Execute queries
Statement stmt = connection.createStatement();
ResultSet rs = stmt.executeQuery("SELECT * FROM files.customers");

while (rs.next()) {
    System.out.println(rs.getString("name"));
}
```

### Spring Integration

```java
@Configuration
public class FileAdapterConfig {
    
    @Bean
    public DataSource fileAdapterDataSource() {
        Map<String, Object> config = FileSchemaConfigBuilder.create()
            .directory("/data")
            .executionEngine("parquet")
            .build();
            
        return new CalciteDataSource(config);
    }
    
    @Bean
    public JdbcTemplate fileAdapterJdbcTemplate() {
        return new JdbcTemplate(fileAdapterDataSource());
    }
}
```

### Programmatic Usage

```java
// Create schema programmatically
SchemaPlus rootSchema = Frameworks.createRootSchema(true);

Map<String, Object> operand = new HashMap<>();
operand.put("directory", "/data");
operand.put("executionEngine", "parquet");

FileSchemaFactory factory = new FileSchemaFactory();
Schema fileSchema = factory.create(rootSchema, "FILES", operand);
rootSchema.add("FILES", fileSchema);

// Create connection and execute queries
CalciteConnection connection = DriverManager
    .getConnection("jdbc:calcite:")
    .unwrap(CalciteConnection.class);
connection.setRootSchema(rootSchema);

Statement stmt = connection.createStatement();
ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM FILES.customers");
```