# Design Ideas

## JSONL Support with Incremental Refresh

### Overview
Extend the file adapter to support JSONL (JSON Lines) format with incremental refresh capability that only processes new rows appended to the file.

### Motivation
- JSONL is a common format for log files, streaming data exports, event streams
- Current JSON support requires reading entire file on each refresh
- JSONL files are typically append-only, making incremental reads efficient
- Reduces memory overhead and processing time for large files

### Design Approach

#### 1. Minimal Extension to Existing JSON Support
- Leverage existing Jackson ObjectMapper infrastructure
- Add JSONL detection based on `.jsonl` extension
- Reuse schema deduction from JsonEnumerator

#### 2. Core Implementation Changes

**JsonEnumerator Enhancement:**
```java
// Detect JSONL format
if (source.path().endsWith(".jsonl")) {
    return parseJsonLines(reader);
}

// Line-by-line parsing using Jackson's MappingIterator
ObjectReader reader = mapper.readerFor(Map.class);
try (MappingIterator<Map<String,Object>> it = reader.readValues(source.reader())) {
    while (it.hasNext()) {
        list.add(it.next());
    }
}
```

#### 3. Incremental Refresh with Metadata Sidecar

**Metadata File Structure (.jsonl.meta):**
```json
{
  "version": "1.0",
  "sourceFile": "data.jsonl",
  "lastReadPosition": 1048576,
  "lastLineNumber": 5000,
  "fileChecksum": "sha256:abc123...",
  "lastModified": 1234567890,
  "schema": {...}
}
```

**Incremental Read Logic:**
```java
public class RefreshableJsonlTable extends AbstractRefreshableTable {
    private JsonlMetadataManager metadataManager;
    
    protected void doRefresh() {
        long lastPosition = metadataManager.getLastPosition();
        File file = source.file();
        
        // Check file changes
        if (file.length() < lastPosition) {
            // File truncated/replaced - full refresh
            performFullRefresh();
        } else if (file.length() > lastPosition) {
            // New data appended - incremental refresh
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                raf.seek(lastPosition);
                appendNewRecords(raf);
                metadataManager.updatePosition(file.length());
            }
        }
    }
}
```

#### 4. Key Benefits
- **Efficient incremental updates** - Only read new lines on refresh
- **Memory efficient** - Line-by-line parsing, no need to load entire file
- **Schema reuse** - After initial detection, schema is known for fast parsing
- **Simple integration** - Minimal changes to existing JSON infrastructure

#### 5. Configuration
```json
{
  "format": "jsonl",
  "refresh": {
    "enabled": true,
    "interval": "5m",
    "incremental": true,
    "metadataDir": ".calcite-meta"
  }
}
```

### Implementation Phases
1. **Phase 1:** Basic JSONL reading (full file)
2. **Phase 2:** Metadata sidecar file management
3. **Phase 3:** Incremental refresh support
4. **Phase 4:** Performance optimization and testing

### Use Cases
- Application logs that grow continuously
- Event streams exported from message queues
- API audit logs
- Time-series data in JSONL format
- OpenTelemetry traces exported as JSONL

### Open Questions
- Should metadata files be stored alongside source files or in a central directory?
- How to handle schema evolution in append-only files?
- Should we support compressed JSONL (.jsonl.gz) with incremental refresh?

## Materialized View Dependency Tracking and Cascade Refresh

### Overview
Enhance materialized views with explicit dependency tracking and intelligent cascade refresh to ensure data consistency when source tables update.

### Current Limitations
- MVs only refresh based on simple timers, not actual data changes
- No awareness when source tables are updated
- Can serve stale data even after sources refresh
- No coordination between dependent MVs

### Design Approach

#### 1. Explicit Dependency Declaration in Model File
```json
{
  "materializedViews": [
    {
      "name": "sales_summary",
      "sql": "SELECT region, SUM(amount) FROM sales_data GROUP BY region",
      "storage": "sales_summary.parquet",
      "dependencies": ["sales_data"],  // Explicit dependencies
      "refresh": {
        "interval": "5m",
        "cascadeFrom": true,  // Refresh when dependencies refresh
        "imperative": true    // Allow manual refresh calls
      }
    },
    {
      "name": "quarterly_report",
      "sql": "SELECT * FROM sales_summary JOIN products ...",
      "storage": "quarterly.parquet",
      "dependencies": ["sales_summary", "products"],  // Can depend on other MVs
      "refresh": {
        "cascadeFrom": true
      }
    }
  ]
}
```

#### 2. Refresh Coordinator with Debouncing
```java
public class RefreshCoordinator {
    private final Map<String, Set<String>> dependencyGraph;  // table -> dependent MVs
    private final ScheduledExecutorService executor;
    private final Map<String, ScheduledFuture<?>> pendingRefreshes;
    private final Duration debounceWindow = Duration.ofSeconds(5);
    
    // Register when a table refreshes
    public void notifyTableRefreshed(String tableName) {
        Set<String> dependents = dependencyGraph.get(tableName);
        if (dependents != null) {
            for (String mvName : dependents) {
                scheduleDebounced(mvName);
            }
        }
    }
    
    // Debounced scheduling - if multiple dependencies refresh quickly
    private void scheduleDebounced(String mvName) {
        // Cancel existing scheduled refresh
        ScheduledFuture<?> existing = pendingRefreshes.get(mvName);
        if (existing != null && !existing.isDone()) {
            existing.cancel(false);
        }
        
        // Schedule new refresh after debounce window
        ScheduledFuture<?> future = executor.schedule(
            () -> refreshMV(mvName),
            debounceWindow.toMillis(),
            TimeUnit.MILLISECONDS
        );
        pendingRefreshes.put(mvName, future);
    }
}
```

#### 3. Debouncing Benefits
```
Without debouncing:
10:00:00 - table1 refreshes → triggers MV refresh
10:00:01 - table2 refreshes → triggers ANOTHER MV refresh  
10:00:02 - table3 refreshes → triggers THIRD MV refresh
= MV refreshes 3 times wastefully

With debouncing (5 sec window):
10:00:00 - table1 refreshes → schedules MV refresh at 10:00:05
10:00:01 - table2 refreshes → reschedules to 10:00:06
10:00:02 - table3 refreshes → reschedules to 10:00:07
10:00:07 - MV refreshes ONCE with all updates
```

#### 4. Imperative Refresh API
```sql
-- SQL interface
CALL sys.refresh_materialized_view('my_schema.my_mv');
CALL sys.refresh_materialized_view('my_schema.my_mv', true);  -- with cascade
CALL sys.refresh_dependent_views('my_schema.source_table');

-- Java API
RefreshableTable table = (RefreshableTable) schema.getTable("my_mv");
table.forceRefresh();
MaterializedViewManager.refreshWithDependents("my_mv");
```

#### 5. Dependency Graph Management
- Build graph from model file declarations at startup
- Validate all dependencies exist
- Detect circular dependencies with topological sort
- Order refreshes correctly (dependencies before dependents)

### Key Benefits
- **Data consistency** - MVs refresh when sources change
- **Resource efficiency** - Debouncing prevents redundant refreshes
- **Explicit control** - Dependencies declared in configuration
- **Flexibility** - Both automatic cascade and manual refresh

### Implementation Considerations
- Need to hook into existing RefreshableTable.doRefresh()
- Coordinator should be singleton per schema
- Queue size limits to prevent memory issues
- Concurrent refresh limits for resource control

### Alternative: Simple Staleness Detection
Instead of cascade refresh, just detect staleness:
```java
public boolean isStale() {
    for (String source : dependencies) {
        Table sourceTable = schema.getTable(source);
        if (sourceTable instanceof RefreshableTable) {
            RefreshableTable rt = (RefreshableTable) sourceTable;
            if (rt.getLastRefreshTime().isAfter(this.lastRefreshTime)) {
                return true;  // Source refreshed after us
            }
        }
    }
    return false;
}
```

### Open Questions
- Should refresh be synchronous or asynchronous?
- How to handle partial refresh failures in a cascade?
- Should we support priority levels for refresh ordering?
- Maximum depth for cascade refresh to prevent runaway refreshes?

## gRPC Support - SQL over Microservices

### Overview
Enable SQL queries over gRPC services by treating RPC methods as virtual tables, bridging the gap between microservices and analytical workloads.

### Motivation
- Organizations have valuable data locked in microservices
- No easy way to perform SQL analytics on service data
- ETL pipelines add latency and complexity
- Real-time queries across services require custom code
- gRPC's streaming and schema features align well with SQL semantics

### Design Approach

#### 1. gRPC as a Storage Provider
```json
{
  "tables": [{
    "name": "users",
    "type": "grpc",
    "options": {
      "endpoint": "grpc://api.example.com:50051",
      "service": "UserService", 
      "method": "StreamUsers",
      "proto": "schemas/user_service.proto",
      "timeout": "30s",
      "maxMessageSize": "4MB"
    }
  }]
}
```

#### 2. Proto to SQL Schema Mapping
```protobuf
// Proto definition
message User {
  int64 id = 1;
  string name = 2;
  google.protobuf.Timestamp created_at = 3;
  repeated string tags = 4;
  Status status = 5;
}

// Maps to SQL schema:
// id: BIGINT
// name: VARCHAR
// created_at: TIMESTAMP
// tags: ARRAY<VARCHAR>
// status: VARCHAR (enum name)
```

#### 3. Predicate Pushdown Protocol
```protobuf
service DataService {
  // Server-side streaming for large results
  rpc QueryTable(QueryRequest) returns (stream Row);
  
  // Bidirectional for interactive queries
  rpc InteractiveQuery(stream QueryRequest) returns (stream Row);
}

message QueryRequest {
  repeated string columns = 1;      // SELECT columns
  string filter_expression = 2;     // WHERE clause
  repeated OrderBy order_by = 3;    // ORDER BY
  int32 limit = 4;                 // LIMIT
  int32 offset = 5;                // OFFSET
  repeated string group_by = 6;     // GROUP BY
}

message Row {
  repeated google.protobuf.Value values = 1;
}
```

#### 4. Implementation Architecture
```java
public class GrpcTable extends AbstractTable implements FilterableTable, ProjectableFilterableTable {
    private final ManagedChannel channel;
    private final ServiceDescriptor serviceDescriptor;
    private final MethodDescriptor streamMethod;
    
    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        // Build gRPC request with pushed predicates
        QueryRequest.Builder request = QueryRequest.newBuilder();
        
        // Convert Calcite filters to gRPC filter expression
        if (filters != null) {
            String filterExpr = convertFiltersToExpression(filters);
            request.setFilterExpression(filterExpr);
        }
        
        // Add projection
        if (projects != null) {
            request.addAllColumns(getColumnNames(projects));
        }
        
        // Stream results
        Iterator<Row> stream = stub.queryTable(request.build());
        return Linq4j.asEnumerable(() -> new GrpcEnumerator(stream, rowType));
    }
}
```

#### 5. Advanced Features

**Connection Pooling:**
```java
public class GrpcConnectionManager {
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final ChannelPool pool;
    
    public ManagedChannel getChannel(String endpoint) {
        return channels.computeIfAbsent(endpoint, e -> 
            ManagedChannelBuilder.forTarget(e)
                .usePlaintext()
                .maxInboundMessageSize(maxMessageSize)
                .keepAliveTime(keepAlive)
                .build());
    }
}
```

**Streaming Semantics:**
```java
public class GrpcEnumerator implements Enumerator<Object[]> {
    private final Iterator<Row> stream;
    private final BlockingQueue<Row> buffer;
    private final StreamObserver<Row> observer;
    
    public GrpcEnumerator(ClientCall<QueryRequest, Row> call) {
        // Handle backpressure with buffering
        this.buffer = new ArrayBlockingQueue<>(bufferSize);
        
        // Async streaming with flow control
        call.start(new ClientCall.Listener<Row>() {
            @Override
            public void onMessage(Row row) {
                buffer.offer(row);
            }
        });
    }
}
```

### Use Cases

1. **Federated Queries Across Services:**
```sql
SELECT 
    u.name,
    COUNT(o.id) as order_count,
    SUM(p.amount) as total_spent
FROM grpc.user_service.users u
JOIN grpc.order_service.orders o ON u.id = o.user_id  
JOIN grpc.payment_service.payments p ON o.id = p.order_id
WHERE u.created_at > CURRENT_DATE - INTERVAL '30' DAY
GROUP BY u.name
```

2. **Real-time Analytics:**
```sql
-- Live dashboard from production services
SELECT 
    status,
    COUNT(*) as count,
    AVG(processing_time) as avg_time
FROM grpc.api_gateway.requests
WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '5' MINUTE
GROUP BY status
```

3. **Service Mesh Observability:**
```sql
-- Query Envoy/Istio metrics via gRPC
SELECT 
    source_service,
    destination_service,
    p99_latency,
    error_rate
FROM grpc.envoy.metrics
WHERE error_rate > 0.01
```

### Benefits
- **Zero ETL** - Query microservices directly
- **Real-time** - No data staleness
- **Federation** - JOIN across services
- **Schema-aware** - Protobuf provides strong typing
- **Efficient** - HTTP/2 multiplexing and streaming
- **Pushdown** - Reduce data transfer with server-side filtering

### Implementation Phases

1. **Phase 1: Basic gRPC Table**
   - Simple unary RPC calls
   - Full table scans
   - Proto to RelDataType conversion

2. **Phase 2: Streaming Support**
   - Server-side streaming
   - Backpressure handling
   - Connection pooling

3. **Phase 3: Predicate Pushdown**
   - Filter expression protocol
   - Projection pushdown
   - Limit/offset support

4. **Phase 4: Advanced Features**
   - Bidirectional streaming
   - Aggregation pushdown
   - Join pushdown (for services that support it)
   - Authentication/TLS

### Technical Challenges
- **Schema Discovery** - How to handle dynamic schemas?
- **Error Handling** - Network failures, timeouts, retries
- **Performance** - Buffering strategies for large streams
- **Security** - TLS, authentication tokens, mTLS
- **Type Mapping** - Complex proto types (oneof, maps, Any)

### Open Questions
- Should we support client-side load balancing?
- How to handle service discovery (Consul, K8s, etc.)?
- Should filter expressions use SQL syntax or a custom DSL?
- How to handle schema evolution in proto files?
- Support for gRPC reflection for dynamic schema discovery?

## Delta Lake Support - ACID Transactions for Data Lakes

### Overview
Add support for Delta Lake format to enable ACID transactions, time travel queries, and schema evolution on data lake storage.

### Motivation
- Delta Lake is becoming the de facto standard for data lakes
- Provides ACID guarantees missing in raw Parquet
- Time travel enables querying historical data states
- Schema evolution without breaking downstream consumers
- Automatic file compaction and Z-ordering optimization
- Wide adoption in Databricks, AWS, Azure ecosystems

### Design Approach

#### 1. Delta Table Configuration
```json
{
  "tables": [{
    "name": "transactions",
    "type": "delta",
    "options": {
      "path": "s3://bucket/delta/transactions",
      "versionAsOf": 10,  // Optional: specific version
      "timestampAsOf": "2024-01-01T00:00:00Z",  // Optional: time travel
      "readOptions": {
        "mergeSchema": true,
        "ignoreDeletes": false
      }
    }
  }]
}
```

#### 2. Core Implementation
```java
public class DeltaTable extends AbstractTable implements TranslatableTable {
    private final String tablePath;
    private final DeltaLog deltaLog;
    private final Snapshot snapshot;
    
    public DeltaTable(String path, Map<String, Object> options) {
        this.tablePath = path;
        this.deltaLog = DeltaLog.forTable(new Configuration(), path);
        
        // Handle time travel options
        if (options.containsKey("versionAsOf")) {
            Long version = (Long) options.get("versionAsOf");
            this.snapshot = deltaLog.getSnapshotForVersionAsOf(version);
        } else if (options.containsKey("timestampAsOf")) {
            String timestamp = (String) options.get("timestampAsOf");
            this.snapshot = deltaLog.getSnapshotForTimestampAsOf(
                Instant.parse(timestamp).toEpochMilli());
        } else {
            this.snapshot = deltaLog.snapshot();
        }
    }
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        // Convert Delta schema to Calcite RelDataType
        StructType deltaSchema = snapshot.getMetadata().getSchema();
        return convertDeltaSchema(deltaSchema, typeFactory);
    }
}
```

#### 3. Reading Delta Tables
```java
public class DeltaEnumerator implements Enumerator<Object[]> {
    private final Iterator<Row> rows;
    
    public DeltaEnumerator(Snapshot snapshot, List<String> projectedColumns) {
        // Get active files (considering deletes)
        List<AddFile> files = snapshot.getAllFiles()
            .filter(f -> !f.isDeleted())
            .collect(Collectors.toList());
        
        // Read Parquet files with Delta metadata
        Dataset<Row> df = spark.read()
            .format("parquet")
            .load(files.stream()
                .map(AddFile::getPath)
                .toArray(String[]::new));
        
        // Apply deletion vectors if present
        if (snapshot.getMetadata().isDeletionVectorsEnabled()) {
            df = applyDeletionVectors(df, snapshot);
        }
        
        // Project columns
        if (projectedColumns != null) {
            df = df.select(projectedColumns.toArray(new String[0]));
        }
        
        this.rows = df.toLocalIterator();
    }
}
```

#### 4. Time Travel Queries
```sql
-- Query specific version
SELECT * FROM delta_table VERSION AS OF 10;

-- Query by timestamp
SELECT * FROM delta_table TIMESTAMP AS OF '2024-01-01T00:00:00Z';

-- Compare versions
SELECT 
    curr.*,
    prev.*
FROM delta_table VERSION AS OF 100 curr
JOIN delta_table VERSION AS OF 99 prev
ON curr.id = prev.id
WHERE curr.amount != prev.amount;
```

#### 5. Change Data Feed (CDC) Support
```java
public class DeltaCDCTable extends DeltaTable {
    public Enumerable<Object[]> readChangeData(long startVersion, long endVersion) {
        // Read CDC data between versions
        Dataset<Row> changes = deltaLog.createDataFrame(
            deltaLog.getChanges(startVersion, endVersion),
            snapshot.getMetadata().getSchema()
        );
        
        // Include CDC metadata columns
        changes = changes
            .withColumn("_change_type", col("_change_type"))
            .withColumn("_commit_version", col("_commit_version"))
            .withColumn("_commit_timestamp", col("_commit_timestamp"));
        
        return new SparkDatasetEnumerable(changes);
    }
}
```

```sql
-- Query CDC data
SELECT 
    *,
    _change_type,
    _commit_timestamp
FROM TABLE(delta_cdc('transactions', 10, 20))
WHERE _change_type IN ('insert', 'update_postimage');
```

#### 6. Predicate Pushdown & Column Pruning
```java
public class DeltaTableScan extends TableScan {
    @Override
    public RelNode optimize(RelOptPlanner planner) {
        // Push filters to Delta
        List<Filter> deltaFilters = convertCalciteFilters(filters);
        
        // Use Delta statistics for partition pruning
        Snapshot prunedSnapshot = snapshot.filesForScan(deltaFilters);
        
        // Z-order optimization for better data skipping
        if (snapshot.getMetadata().getZOrderColumns() != null) {
            return optimizeWithZOrder(prunedSnapshot, filters);
        }
        
        return this;
    }
}
```

#### 7. Schema Evolution Support
```java
public class EvolvableDeltaTable extends DeltaTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (options.get("mergeSchema") == Boolean.TRUE) {
            // Merge schemas across all versions
            StructType mergedSchema = deltaLog.snapshot()
                .getMetadata()
                .getSchema();
            
            // Include columns from all versions
            for (long v = 0; v <= deltaLog.snapshot().version(); v++) {
                StructType versionSchema = deltaLog
                    .getSnapshotForVersionAsOf(v)
                    .getMetadata()
                    .getSchema();
                mergedSchema = mergeSchemas(mergedSchema, versionSchema);
            }
            
            return convertDeltaSchema(mergedSchema, typeFactory);
        }
        return super.getRowType(typeFactory);
    }
}
```

### Integration with Existing Features

#### 1. Refresh Support
```java
public class RefreshableDeltaTable extends DeltaTable implements RefreshableTable {
    @Override
    public void refresh() {
        // Update to latest snapshot
        deltaLog.update();
        this.snapshot = deltaLog.snapshot();
        
        // Clear any cached data
        clearCache();
    }
}
```

#### 2. Materialized Views on Delta
```sql
-- Create MV on specific Delta version
CREATE MATERIALIZED VIEW sales_summary AS
SELECT region, SUM(amount) 
FROM delta_sales VERSION AS OF 100
GROUP BY region;
```

### Benefits
- **ACID Transactions** - Consistent reads even during writes
- **Time Travel** - Query any historical version
- **Schema Evolution** - Add/modify columns without breaking readers
- **Performance** - Z-ordering and data skipping optimizations
- **CDC Support** - Track changes between versions
- **Compatibility** - Works with existing Parquet tools

### Implementation Considerations

#### Dependencies
```xml
<dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-core_2.12</artifactId>
    <version>2.4.0</version>
</dependency>
```

#### Alternative: Delta Standalone (No Spark)
```java
// Use Delta Standalone for lighter weight implementation
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;

public class StandaloneDeltaTable {
    // Lighter implementation without Spark dependency
    private final DeltaLog deltaLog;
    
    public CloseableIterator<RowRecord> scan() {
        return snapshot.open();
    }
}
```

### Challenges
- **Spark Dependency** - Delta Core requires Spark (standalone is limited)
- **Transaction Log** - Managing _delta_log directory
- **Deletion Vectors** - Complex to implement correctly
- **Z-Order** - Requires understanding of data skipping indexes
- **Compatibility** - Different Delta protocol versions

### Open Questions
- Use Delta Core (with Spark) or Delta Standalone (limited features)?
- Support write operations or read-only initially?
- How to handle concurrent writes from other systems?
- Support Delta Sharing protocol for cross-organization access?
- Integrate with Unity Catalog for governance?

## ORC Support - Optimized Row Columnar Format

### Overview
Add support for Apache ORC (Optimized Row Columnar) format, a highly efficient columnar storage format optimized for big data workloads.

### Motivation
- Better compression than Parquet for many workloads (up to 70% smaller)
- Native support for ACID transactions in Hive
- Excellent performance for ETL and data warehouse queries
- Built-in indexes (bloom filters, min/max) for fast filtering
- Wide adoption in Hadoop/Hive ecosystems
- Self-describing with rich metadata

### Design Approach

#### 1. ORC Table Configuration
```json
{
  "tables": [{
    "name": "sales",
    "type": "orc",
    "options": {
      "file": "sales.orc",
      "columns": ["id", "amount", "region"],  // Optional column pruning
      "searchArgument": "amount > 100"  // Optional pushdown filter
    }
  }]
}
```

#### 2. Core Implementation
```java
public class OrcTable extends AbstractTable implements FilterableTable, ProjectableFilterableTable {
    private final Source source;
    private final Reader orcReader;
    private final TypeDescription schema;
    
    public OrcTable(Source source, Map<String, Object> options) {
        this.source = source;
        Configuration conf = new Configuration();
        
        // Open ORC file
        Path path = new Path(source.path());
        this.orcReader = OrcFile.createReader(path, 
            OrcFile.readerOptions(conf));
        this.schema = orcReader.getSchema();
    }
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return convertOrcSchema(schema, typeFactory);
    }
    
    @Override
    public Enumerable<Object[]> scan(DataContext root, 
                                     List<RexNode> filters, 
                                     int[] projects) {
        // Build ORC SearchArgument from Calcite filters
        SearchArgument sarg = buildSearchArgument(filters, schema);
        
        // Configure reader with pushdowns
        Reader.Options options = orcReader.options()
            .searchArgument(sarg, new String[]{})
            .include(buildIncludedColumns(projects));
        
        RecordReader rows = orcReader.rows(options);
        return new OrcEnumerable(rows, schema);
    }
}
```

#### 3. Leveraging ORC Features
```java
public class OrcPushdownOptimizer {
    // Use ORC's built-in indexes
    public SearchArgument buildSearchArgument(List<RexNode> filters) {
        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        
        for (RexNode filter : filters) {
            if (filter instanceof RexCall) {
                RexCall call = (RexCall) filter;
                // Convert to ORC SearchArgument
                convertToOrcPredicate(call, builder);
            }
        }
        
        return builder.build();
    }
    
    // Leverage bloom filters for point lookups
    public boolean canUseBLoomFilter(RexNode filter) {
        // Check if column has bloom filter index
        ColumnStatistics[] stats = orcReader.getStatistics();
        // Use bloom filter for equality/IN predicates
    }
}
```

### Benefits
- **Superior Compression** - Often 50-70% smaller than Parquet
- **Fast Filtering** - Built-in indexes reduce I/O
- **ACID Support** - Transactional tables in Hive
- **Predicate Pushdown** - SearchArgument API for efficient filtering
- **Column Encryption** - Built-in column-level encryption

## Avro Support - Schema Evolution and Streaming

### Overview  
Add support for Apache Avro format, focusing on schema evolution capabilities and streaming data scenarios.

### Motivation
- Excellent schema evolution support (backward/forward compatibility)
- Compact binary format with JSON schema
- Standard format for Kafka and streaming pipelines
- Row-oriented format good for write-heavy workloads
- Self-describing with embedded schema
- Splittable for parallel processing

### Design Approach

#### 1. Avro Table Configuration
```json
{
  "tables": [{
    "name": "events",
    "type": "avro",
    "options": {
      "file": "events.avro",
      "schema": "schemas/event.avsc",  // Optional external schema
      "readerSchema": "schemas/event_v2.avsc"  // Schema evolution
    }
  }]
}
```

#### 2. Core Implementation
```java
public class AvroTable extends AbstractTable implements ScannableTable {
    private final Source source;
    private final Schema avroSchema;
    private final Schema readerSchema;
    
    public AvroTable(Source source, Map<String, Object> options) {
        this.source = source;
        
        // Get schema (from file or external)
        if (options.containsKey("schema")) {
            this.avroSchema = new Schema.Parser()
                .parse(new File((String) options.get("schema")));
        } else {
            // Read embedded schema from Avro file
            try (DataFileReader<GenericRecord> reader = 
                new DataFileReader<>(source.file(), 
                    new GenericDatumReader<>())) {
                this.avroSchema = reader.getSchema();
            }
        }
        
        // Handle schema evolution
        this.readerSchema = options.containsKey("readerSchema")
            ? new Schema.Parser().parse(new File((String) options.get("readerSchema")))
            : avroSchema;
    }
    
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new AvroEnumerable(source, avroSchema, readerSchema);
    }
}
```

#### 3. Schema Evolution Support
```java
public class AvroSchemaEvolution {
    // Handle schema compatibility
    public RelDataType mergeSchemas(Schema writerSchema, 
                                   Schema readerSchema,
                                   RelDataTypeFactory typeFactory) {
        // Avro handles missing fields with defaults
        // New fields are ignored if not in reader schema
        
        List<RelDataType> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        
        for (Schema.Field field : readerSchema.getFields()) {
            // Check if field exists in writer schema
            Schema.Field writerField = writerSchema.getField(field.name());
            
            if (writerField != null) {
                // Use writer's type
                types.add(convertAvroType(writerField.schema(), typeFactory));
            } else if (field.hasDefaultValue()) {
                // Use default value
                types.add(convertAvroType(field.schema(), typeFactory));
            }
            names.add(field.name());
        }
        
        return typeFactory.createStructType(types, names);
    }
}
```

### Benefits
- **Schema Evolution** - Backward/forward compatibility
- **Compact Size** - Efficient binary encoding
- **Streaming-Friendly** - Row-oriented for fast writes
- **Self-Describing** - Schema travels with data
- **Kafka Integration** - Native format for Kafka/Confluent

## S3 Select Support - Push Queries to Cloud Storage

### Overview
Integrate S3 Select to push SQL queries directly to S3, reducing data transfer and processing costs.

### Motivation
- Reduce data transfer costs by 80-90%
- Lower compute requirements (S3 does the filtering)
- Faster query response for selective queries
- Support for CSV, JSON, and Parquet in S3
- Similar services: Azure Blob Query, GCS BigQuery
- Seamless integration with existing S3 storage

### Design Approach

#### 1. S3 Select Configuration
```json
{
  "tables": [{
    "name": "logs",
    "type": "s3select",
    "options": {
      "bucket": "my-bucket",
      "key": "logs/2024/*.csv",
      "format": "CSV",
      "compression": "GZIP",
      "headerInfo": "USE",
      "pushdownEnabled": true
    }
  }]
}
```

#### 2. Core Implementation
```java
public class S3SelectTable extends AbstractTable implements FilterableTable {
    private final S3Client s3Client;
    private final String bucket;
    private final String key;
    private final InputSerialization inputFormat;
    
    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        // Convert Calcite filters to S3 Select SQL
        String s3SelectQuery = buildS3SelectQuery(filters, projections);
        
        // Build S3 Select request
        SelectObjectContentRequest request = SelectObjectContentRequest.builder()
            .bucket(bucket)
            .key(key)
            .expression(s3SelectQuery)
            .expressionType(ExpressionType.SQL)
            .inputSerialization(inputFormat)
            .outputSerialization(OutputSerialization.builder()
                .json(JSONOutput.builder().recordDelimiter("\n").build())
                .build())
            .build();
        
        // Stream results
        SelectObjectContentResponseHandler handler = 
            new SelectObjectContentResponseHandler();
        CompletableFuture<Void> future = s3Client
            .selectObjectContent(request, handler);
        
        return new S3SelectEnumerable(handler);
    }
}
```

#### 3. Query Pushdown Translation
```java
public class S3SelectQueryBuilder {
    public String buildS3SelectQuery(List<RexNode> filters, 
                                    int[] projections,
                                    RelDataType rowType) {
        StringBuilder sql = new StringBuilder("SELECT ");
        
        // Build projection
        if (projections != null) {
            String cols = Arrays.stream(projections)
                .mapToObj(i -> "s." + rowType.getFieldList().get(i).getName())
                .collect(Collectors.joining(", "));
            sql.append(cols);
        } else {
            sql.append("*");
        }
        
        sql.append(" FROM S3Object s");
        
        // Build WHERE clause
        if (filters != null && !filters.isEmpty()) {
            sql.append(" WHERE ");
            sql.append(convertFiltersToS3SQL(filters));
        }
        
        return sql.toString();
    }
    
    // Handle S3 Select limitations
    public boolean canPushdown(RexNode filter) {
        // S3 Select supports basic operators
        // No JOINs, limited functions
        return isSupportedByS3Select(filter);
    }
}
```

#### 4. Cost-Based Optimization
```java
public class S3SelectCostModel {
    public boolean shouldUseS3Select(RelOptPlanner planner, 
                                    double selectivity,
                                    long fileSize) {
        // Estimate costs
        double s3SelectCost = calculateS3SelectCost(fileSize, selectivity);
        double downloadCost = calculateFullDownloadCost(fileSize);
        
        // Use S3 Select if:
        // - Selectivity < 20% (filtering out 80%+ of data)
        // - File size > 100MB
        // - Simple predicates that S3 can handle
        
        return s3SelectCost < downloadCost * 0.5;  // 50% cost threshold
    }
}
```

#### 5. Multi-File Support
```java
public class S3SelectMultiFile {
    public Enumerable<Object[]> scanMultiple(List<String> keys) {
        // Parallel S3 Select on multiple files
        List<CompletableFuture<List<Object[]>>> futures = 
            keys.stream()
                .map(key -> CompletableFuture.supplyAsync(() -> 
                    selectFromFile(key)))
                .collect(Collectors.toList());
        
        // Combine results
        return new CompositeEnumerable(futures);
    }
}
```

### Benefits
- **Cost Reduction** - Pay only for data scanned, not transferred
- **Performance** - Faster for selective queries
- **Scalability** - S3 handles the compute
- **Simple Integration** - Works with existing S3 data

### Limitations to Handle
- No JOINs (single object only)
- Limited SQL functions
- 256KB result record size limit
- Cannot update/delete data
- Costs can add up for frequent queries

### Open Questions
- Support for Glacier Select (archived data)?
- Handle S3 Select quotas and throttling?
- Integrate with S3 Batch Operations for large-scale queries?
- Support similar services (Azure Blob Query, GCS)?

## Imperative Materialized View Refresh - Simple Manual Control

### Overview
Add simple SQL commands to manually refresh materialized views on-demand, without complex dependency tracking or automatic cascading.

### Motivation
- Users know when their source data changes (after ETL, imports, etc.)
- Current timer-based refresh can serve stale data
- Complex dependency tracking is overkill for most use cases
- Need simple, scriptable refresh mechanism
- 90% of use cases just need "refresh this MV now"

### Design Approach

#### 1. SQL Commands
```sql
-- Refresh single materialized view
CALL sys.refresh_mv('sales_summary');

-- Refresh multiple MVs
CALL sys.refresh_mv('sales_summary', 'product_summary');

-- Refresh all MVs in schema
CALL sys.refresh_all_mvs();
CALL sys.refresh_all_mvs('my_schema');

-- Check MV staleness
SELECT * FROM sys.mv_status;
-- Returns: mv_name, last_refresh, is_stale, source_tables
```

#### 2. Core Implementation
```java
public class MaterializedViewManager {
    
    public static void registerSystemProcedures(SchemaPlus schema) {
        schema.add("refresh_mv", new RefreshMVProcedure());
        schema.add("refresh_all_mvs", new RefreshAllMVsProcedure());
        schema.add("mv_status", new MVStatusTableFunction());
    }
    
    private static class RefreshMVProcedure implements Procedure {
        public void execute(String... mvNames) {
            for (String mvName : mvNames) {
                refreshSingleMV(mvName);
            }
        }
        
        private void refreshSingleMV(String mvName) {
            Table table = schema.getTable(mvName);
            
            if (table instanceof MaterializedViewTable) {
                MaterializedViewTable mv = (MaterializedViewTable) table;
                
                // Simple refresh - delete parquet and reset flag
                if (mv.parquetFile.exists()) {
                    boolean deleted = mv.parquetFile.delete();
                    if (!deleted) {
                        throw new RuntimeException("Failed to delete MV cache: " + mvName);
                    }
                }
                
                // Reset materialized flag to force regeneration
                mv.materialized.set(false);
                
                // Log the refresh
                LOGGER.info("Materialized view '{}' marked for refresh", mvName);
                
            } else if (table instanceof RefreshableTable) {
                // Handle other refreshable tables
                ((RefreshableTable) table).forceRefresh();
                
            } else {
                throw new IllegalArgumentException("Table '" + mvName + 
                    "' is not a materialized view");
            }
        }
    }
}
```

#### 3. Integration with JDBC
```java
// Use from Java application
try (Connection conn = DriverManager.getConnection("jdbc:calcite:");
     CallableStatement stmt = conn.prepareCall("{CALL sys.refresh_mv(?)}")) {
    
    stmt.setString(1, "sales_summary");
    stmt.execute();
}

// Or with Statement
try (Statement stmt = conn.createStatement()) {
    stmt.execute("CALL sys.refresh_mv('sales_summary')");
}
```

#### 4. Batch Refresh with Status
```java
public class BatchRefreshProcedure {
    public ResultSet execute(String... mvNames) {
        List<RefreshResult> results = new ArrayList<>();
        
        for (String mvName : mvNames) {
            long startTime = System.currentTimeMillis();
            
            try {
                refreshSingleMV(mvName);
                long duration = System.currentTimeMillis() - startTime;
                
                results.add(new RefreshResult(
                    mvName, 
                    "SUCCESS", 
                    duration,
                    null
                ));
                
            } catch (Exception e) {
                results.add(new RefreshResult(
                    mvName,
                    "FAILED",
                    0,
                    e.getMessage()
                ));
            }
        }
        
        return convertToResultSet(results);
    }
}
```

#### 5. MV Status Table Function
```java
public class MVStatusTableFunction implements TableFunction {
    public Table eval() {
        List<MVStatus> statuses = new ArrayList<>();
        
        for (String tableName : schema.getTableNames()) {
            Table table = schema.getTable(tableName);
            
            if (table instanceof MaterializedViewTable) {
                MaterializedViewTable mv = (MaterializedViewTable) table;
                
                statuses.add(new MVStatus(
                    tableName,
                    mv.parquetFile.exists(),
                    mv.parquetFile.exists() ? 
                        new Timestamp(mv.parquetFile.lastModified()) : null,
                    mv.parquetFile.length(),
                    extractSourceTables(mv.sql)  // Simple regex extraction
                ));
            }
        }
        
        return new MVStatusTable(statuses);
    }
}
```

### Use Cases

#### 1. Post-ETL Refresh
```bash
# In ETL script
python load_sales_data.py
sqlline -e "CALL sys.refresh_mv('sales_summary', 'regional_summary')"
```

#### 2. Scheduled Refresh
```sql
-- In cron job or scheduler
CALL sys.refresh_all_mvs();
```

#### 3. Conditional Refresh
```sql
-- Check staleness and refresh if needed
SELECT mv_name 
FROM sys.mv_status 
WHERE last_refresh < CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- Then refresh those MVs
CALL sys.refresh_mv('old_mv_1', 'old_mv_2');
```

#### 4. Integration with Applications
```python
# Python example
import jaydebeapi

conn = jaydebeapi.connect(
    "org.apache.calcite.jdbc.Driver",
    "jdbc:calcite:model=model.json"
)

# After data load
cursor = conn.cursor()
cursor.execute("CALL sys.refresh_mv('sales_summary')")
cursor.close()
```

### Benefits
- **Dead Simple** - One command to refresh
- **Scriptable** - Easy to integrate with ETL/workflows
- **Transparent** - Users control when refresh happens
- **Minimal Code** - ~100 lines to implement
- **No Side Effects** - No automatic cascading surprises

### Implementation Effort
- **Estimated:** 4-8 hours
- **Code Changes:**
  - Add system procedure registration
  - Implement refresh procedures (delete + flag reset)
  - Add status table function
  - Update documentation

### Future Enhancements (Optional)
- Async refresh with progress tracking
- Refresh statistics/metrics
- Warm cache after refresh
- Partial refresh for partitioned MVs

### Open Questions
- Should refresh be synchronous or return immediately?
- Add dry-run mode to show what would be refreshed?
- Support refresh with different options (e.g., keep cache, rebuild only)?
- Add refresh history/audit log?

### Simplified Implementation Plan (45 minutes total)

#### Step 1: Create the Refresh Function (30 minutes)
```java
// In file/src/main/java/org/apache/calcite/adapter/file/functions/RefreshMaterializedViewFunction.java
public class RefreshMaterializedViewFunction {
    public static void refreshMV(String mvName) {
        // Get current schema context from DataContext
        CalciteConnection connection = getConnectionFromContext();
        SchemaPlus schema = connection.getRootSchema();
        
        Table table = schema.getTable(mvName);
        if (table instanceof MaterializedViewTable) {
            MaterializedViewTable mv = (MaterializedViewTable) table;
            
            // Simple refresh - same pattern as RefreshableMaterializedViewTable
            if (mv.parquetFile.exists()) {
                mv.parquetFile.delete();  // Delete cached parquet
            }
            mv.materialized.set(false);  // Force regeneration on next access
            
            LOGGER.info("Materialized view '{}' marked for refresh", mvName);
        } else {
            throw new IllegalArgumentException("Table '" + mvName + "' is not a materialized view");
        }
    }
    
    // Overloaded for multiple MVs
    public static void refreshMV(String... mvNames) {
        for (String mvName : mvNames) {
            refreshMV(mvName);
        }
    }
}
```

#### Step 2: Register Function in FileSchema (15 minutes)
```java
// In FileSchema.java, override getFunctionMap():
@Override 
protected Map<String, Function> getFunctionMap() {
    Map<String, Function> functions = new HashMap<>();
    
    // Register refresh function
    functions.put("REFRESH_MV", 
        ScalarFunctionImpl.create(RefreshMaterializedViewFunction.class, "refreshMV"));
    
    return functions;
}
```

#### Step 3: Immediate Usage (0 minutes)
```sql
-- Single MV refresh
SELECT REFRESH_MV('sales_summary');

-- Multiple MV refresh  
SELECT REFRESH_MV('sales_summary', 'product_summary');

-- Use in scripts/ETL
SELECT REFRESH_MV('daily_report') AS result;
```

#### Key Implementation Notes
- **Reuses existing pattern**: Same logic as `RefreshableMaterializedViewTable.refresh()`
- **No new infrastructure**: Just register a function in the schema
- **Immediate effect**: `parquetFile.delete()` + `materialized.set(false)` = instant refresh
- **Error handling**: Function validates table exists and is an MV
- **Logging**: Standard debug logging for operations

#### Why This is So Simple
1. **MaterializedViewTable already has the fields we need**: `parquetFile`, `materialized`
2. **Refresh pattern already exists**: RefreshableMaterializedViewTable shows us exactly what to do
3. **Calcite function registration is straightforward**: Just override `getFunctionMap()`
4. **No state management needed**: The MV handles its own regeneration on next access

## Cache Management SQL Functions

### Overview
SQL functions to control Parquet cache and statistics cache for immediate performance tuning.

### Implementation
```sql
SELECT CLEAR_PARQUET_CACHE();           -- Clear all
SELECT CLEAR_PARQUET_CACHE('table');    -- Clear specific
SELECT REFRESH_STATISTICS('table');     -- Refresh HLL stats
```

### ROI Assessment
- **Effort:** 30 minutes each
- **Impact:** Essential for debugging/performance
- **ROI Score:** 9/10

---

## File Discovery SQL Functions

### Overview
SQL functions to explore files and schemas without creating tables.

### Implementation
```sql
SELECT * FROM TABLE(TEST_GLOB_PATTERN('*.csv'));
SELECT * FROM TABLE(INFER_SCHEMA('data.json'));
SELECT * FROM TABLE(FIND_RECENT_FILES('/path', '2024-01-01'));
```

### ROI Assessment
- **Effort:** 30-60 minutes each
- **Impact:** Saves hours during development
- **ROI Score:** 8.5/10

---

## Dynamic Table Creation from File Discovery

### Overview
Enable CREATE TABLE directly from file discovery results, automating schema creation from file system exploration.

### Motivation
- Currently must manually create table definitions after discovering files
- File discovery shows us the files but doesn't let us use them
- Natural workflow: discover → create → query

### Design Approach

#### 1. CREATE TABLE AS from Discovery
```sql
-- Discover and create single table
CREATE TABLE sales AS 
SELECT * FROM TABLE(DISCOVER_FILE('data/sales.csv'));

-- Discover and create multiple tables from pattern
CREATE TABLES FROM TABLE(DISCOVER_PATTERN('data/*.csv'));

-- Create with inferred schema
CREATE TABLE events AS
SELECT * FROM TABLE(INFER_AND_LOAD('logs/events.jsonl'));
```

#### 2. Implementation Pattern
```java
public class DiscoveryFunctions {
    
    // Returns a scannable table directly
    public static Table discoverFile(String path) {
        Source source = Sources.of(new File(path));
        
        if (path.endsWith(".csv")) {
            return new CsvTranslatableTable(source, null);
        } else if (path.endsWith(".json") || path.endsWith(".jsonl")) {
            return new JsonTable(source);
        } else if (path.endsWith(".parquet")) {
            return new ParquetTranslatableTable(new File(path));
        }
        // Auto-detect format and return appropriate table
    }
    
    // Batch discovery and registration
    public static void createTablesFromPattern(String pattern) {
        List<File> files = glob(pattern);
        
        for (File file : files) {
            String tableName = deriveTableName(file);
            Table table = discoverFile(file.getPath());
            schema.add(tableName, table);
        }
    }
}
```

#### 3. Advanced Usage Patterns

**Auto-discovery with type inference:**
```sql
-- Create all CSV files as tables with inferred types
CALL CREATE_TABLES_FROM_PATTERN('data/*.csv', 
    OPTIONS => 'inferTypes=true,sampleRows=1000');

-- Create partitioned table from multiple files
CREATE TABLE sales PARTITION BY (year, month) AS
SELECT * FROM TABLE(DISCOVER_PARTITIONED('data/sales/year=*/month=*/*.parquet'));
```

**Schema evolution support:**
```sql
-- Merge schemas from multiple files
CREATE TABLE events WITH (mergeSchema=true) AS
SELECT * FROM TABLE(DISCOVER_AND_MERGE('logs/*.jsonl'));
```

#### 4. Integration with Existing Discovery Functions

```sql
-- First explore
SELECT * FROM TABLE(TEST_GLOB_PATTERN('*.csv'));
-- Returns: file_path, size, modified, estimated_rows

-- Then create what you want
CREATE TABLE sales AS 
SELECT * FROM TABLE(LOAD_FILE(
    SELECT file_path FROM TABLE(TEST_GLOB_PATTERN('*sales*.csv')) 
    ORDER BY modified DESC LIMIT 1
));
```

### Benefits
- **Zero-config table creation** - No JSON model needed
- **Dynamic schema evolution** - Discover and adapt to new files
- **Batch operations** - Create dozens of tables in one command
- **Type safety** - Infer and validate schemas automatically

### Implementation Approach

#### Phase 1: Basic Discovery (1-2 hours) - Ship This First!
```java
// Simple: discover file → create table → done
public static Table loadFile(String path) {
    return FileSchema.createTableForFile(path);
}
```
- No refresh configuration
- No persistence  
- Just dynamic table creation
- **This alone provides 80% of the value!**

#### Phase 2: Refresh & Persistence (2-3 days) - Later Enhancement
- Add refresh configuration options
- Persist dynamically created tables
- Handle schema evolution
- Add `CREATE REFRESHABLE TABLE` syntax

#### Phase 3: Advanced Features (1 week) - Future
- Pattern-based batch creation
- Schema merging across files
- Partitioned table detection
- Global configuration defaults

### ROI Assessment
- **Effort:** 2-4 hours (builds on existing infrastructure)
- **Impact:** Eliminates manual table definition
- **ROI Score:** 9/10
- **Why:** Natural extension that saves massive time

### Examples of Power

```sql
-- One command to load entire directory structure
CALL DISCOVER_AND_CREATE_ALL('/data', 
    OPTIONS => 'recursive=true, formats=csv,json,parquet');

-- Result: All files are now queryable tables!

-- Then just query
SELECT * FROM sales_2024_01;
SELECT * FROM products;
SELECT * FROM customers_backup;
```

### The Key Insight
File discovery functions already find the files and understand their structure. We just need to bridge the final gap: **turn that discovery into actual tables**. This transforms the file adapter from "configure then query" to "discover and query".

### Important Considerations

#### Refresh Behavior
Dynamically created tables wouldn't inherit automatic refresh configuration from model.json. Options:

1. **No automatic refresh by default** - Safer, predictable
```sql
-- Create without refresh
CREATE TABLE sales AS SELECT * FROM TABLE(DISCOVER_FILE('sales.csv'));

-- Manually refresh when needed
SELECT REFRESH_TABLE('sales');
```

2. **Explicit refresh configuration**
```sql
-- Create with refresh interval
CREATE TABLE sales WITH (refreshInterval='5m') AS 
SELECT * FROM TABLE(DISCOVER_FILE('sales.csv'));
```

3. **Global default for discovered tables**
```sql
-- Set default for session
SET file.discovery.defaultRefreshInterval = '10m';

-- Now all discovered tables get this interval
CREATE TABLE sales AS SELECT * FROM TABLE(DISCOVER_FILE('sales.csv'));
```

#### Persistence Considerations
- Dynamically created tables should persist (like views/MVs)
- Store configuration in metadata including refresh settings
- On restart, recreate with same configuration

#### Schema Evolution
- File might change between discovery and next session
- Need to handle schema mismatches gracefully
- Option to "re-discover" if file structure changed

### Recommended Approach
Default to **no automatic refresh** for safety, with explicit opt-in:
```sql
-- Safe by default
CREATE TABLE sales AS SELECT * FROM TABLE(DISCOVER_FILE('sales.csv'));

-- Explicit refresh if wanted
CREATE REFRESHABLE TABLE sales WITH (interval='5m') AS 
SELECT * FROM TABLE(DISCOVER_FILE('sales.csv'));
```

This avoids unexpected behavior while maintaining flexibility.

---

## Schema Export/Import - Portable File Database Definitions

### Overview
Export the entire file adapter schema (tables, views, MVs, configurations) as a shareable package that others can import and use immediately.

### Motivation
- Model.json already does this but is static
- No way to export dynamically created tables/views
- Teams want to share "database" configurations
- Enable "file database as code" workflows

### Design Approach

#### 1. Export Current Schema
```sql
-- Export everything to a model file
SELECT EXPORT_SCHEMA('/path/to/my_database.json');

-- Export with data samples for documentation
SELECT EXPORT_SCHEMA('/path/to/my_database.json', 
    OPTIONS => 'includeSamples=true, includeStats=true');
```

Generated file:
```json
{
  "name": "sales_analytics",
  "version": "1.0",
  "exported": "2024-01-15T10:00:00Z",
  "source_info": {
    "base_directory": "/data/sales",
    "original_connection": "jdbc:calcite:model=..."
  },
  "schemas": [{
    "name": "FILES",
    "tables": [
      {
        "name": "sales_2024",
        "file": "sales/2024/*.parquet",
        "format": "parquet",
        "discovered": true,
        "statistics": {
          "rowCount": 1000000,
          "sizeBytes": 50000000
        }
      }
    ],
    "views": [
      {
        "name": "sales_summary",
        "sql": "SELECT region, SUM(amount) FROM sales_2024 GROUP BY region",
        "created": "2024-01-10T09:00:00Z"
      }
    ],
    "materializedViews": [
      {
        "name": "daily_report",
        "sql": "SELECT date, COUNT(*) FROM sales_2024 GROUP BY date",
        "refreshInterval": "1h"
      }
    ]
  }]
}
```

#### 2. Import Schema Package
```sql
-- Import someone else's schema
SELECT IMPORT_SCHEMA('/path/to/their_database.json');

-- Import with path remapping
SELECT IMPORT_SCHEMA('/path/to/their_database.json',
    OPTIONS => 'basePath=/my/data, readOnly=true');
```

#### 3. Schema Packages with Relative Paths
```json
{
  "portable": true,
  "data_layout": {
    "structure": "year/month/day",
    "formats": ["parquet", "csv"],
    "relative_paths": true
  },
  "tables": [
    {
      "name": "events",
      "path": "./events/${year}/${month}/*.parquet",
      "description": "Event data partitioned by date"
    }
  ]
}
```

#### 4. Git-Friendly Schema Versioning
```bash
# Track schema changes in git
git add my_database.json
git commit -m "Added sales_summary view"

# Share with team
git push

# Team member uses it
SELECT IMPORT_SCHEMA('https://github.com/company/schemas/sales.json');
```

### Advanced Features

#### Schema Templates
```sql
-- Export as template (no absolute paths)
SELECT EXPORT_SCHEMA_TEMPLATE('template.json');

-- Others instantiate with their paths
SELECT INSTANTIATE_TEMPLATE('template.json', 
    BASE_PATH => '/their/data/location');
```

#### Schema Validation
```sql
-- Validate schema before import
SELECT VALIDATE_SCHEMA('their_database.json');
-- Returns: missing files, incompatible formats, etc.

-- Dry run import
SELECT IMPORT_SCHEMA('their_database.json', DRY_RUN => true);
```

#### Schema Comparison
```sql
-- Compare current schema with file
SELECT * FROM TABLE(COMPARE_SCHEMAS('production.json'));
-- Returns: tables_added, tables_removed, views_changed, etc.
```

### Implementation (Simple Version - 2-3 hours)

```java
public class SchemaExporter {
    public static void exportSchema(String path) {
        FileSchema schema = getCurrentSchema();
        
        Map<String, Object> export = new HashMap<>();
        export.put("version", "1.0");
        export.put("exported", Instant.now());
        
        // Export tables
        List<Map<String, Object>> tables = new ArrayList<>();
        for (Entry<String, Table> entry : schema.getTableMap().entrySet()) {
            tables.add(serializeTable(entry.getKey(), entry.getValue()));
        }
        export.put("tables", tables);
        
        // Export views (already in memory)
        export.put("views", serializeViews());
        
        // Write JSON
        mapper.writeValue(new File(path), export);
    }
}
```

### Benefits
- **Team Collaboration** - Share "databases" via git
- **Environment Promotion** - Dev → Test → Prod schemas
- **Backup/Recovery** - Export before changes
- **Documentation** - Self-documenting data layouts
- **Templates** - Reusable schema patterns

### ROI Assessment
- **Effort:** 2-3 hours (simple version)
- **Impact:** Enables team workflows
- **ROI Score:** 8/10
- **Why:** Multiplies value across teams

### The Key Insight
The file adapter with export/import becomes a **shareable, versionable database definition**. Teams can collaborate on schema design just like code, making the file adapter a "database as code" solution.

### Round-Trip Workflow: Dynamic Exploration to Shareable Model

#### Overview
Enable complete round-trip workflow where dynamic schema exploration can be exported back to a model.json file, allowing teams to share discovered configurations.

#### Workflow Pattern
1. **Explore** - Developer uses discovery functions to find and test files
2. **Create** - Dynamically create tables, views, and MVs during exploration
3. **Export** - Export the working configuration to model.json
4. **Share** - Commit to git, share with team
5. **Import** - Others use the exported model directly

#### Implementation
```sql
-- Phase 1: Dynamic exploration and creation
SELECT * FROM TABLE(TEST_GLOB_PATTERN('data/*.csv'));
CREATE TABLE sales AS SELECT * FROM TABLE(DISCOVER_FILE('data/sales.csv'));
CREATE VIEW sales_summary AS SELECT region, SUM(amount) FROM sales GROUP BY region;
CREATE MATERIALIZED VIEW daily_report AS SELECT date, COUNT(*) FROM sales GROUP BY date;

-- Phase 2: Export discovered schema
SELECT EXPORT_SCHEMA('discovered_model.json', 
    OPTIONS => 'includeDiscovered=true, format=model');

-- Result: Standard model.json that others can use
```

Generated model.json:
```json
{
  "version": "1.0",
  "defaultSchema": "FILES",
  "schemas": [{
    "name": "FILES",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "data",
      "tables": [
        {
          "name": "sales",
          "file": "sales.csv",
          "discovered": true,
          "discoveredAt": "2024-01-15T10:00:00Z"
        }
      ],
      "views": [
        {
          "name": "sales_summary",
          "sql": "SELECT region, SUM(amount) FROM sales GROUP BY region"
        }
      ],
      "materializations": [
        {
          "name": "daily_report",
          "sql": "SELECT date, COUNT(*) FROM sales GROUP BY date",
          "storage": ".calcite/cache/daily_report.parquet"
        }
      ]
    }
  }]
}
```

#### Benefits
- **Zero to Production** - From exploration to deployable configuration
- **Self-Documenting** - Discovered schemas include metadata
- **Team Onboarding** - New developers get working configuration immediately
- **Version Control** - Track schema evolution in git

#### Use Cases

**Data Science Workflow:**
```sql
-- Data scientist explores and creates working environment
CALL DISCOVER_AND_CREATE_ALL('/data/lake', OPTIONS => 'formats=parquet,csv');
CREATE VIEW training_data AS SELECT ... ;
CREATE VIEW test_data AS SELECT ... ;

-- Export for team
SELECT EXPORT_SCHEMA('ml_pipeline.json');
```

**ETL Development:**
```sql
-- Developer builds ETL views interactively
CREATE TABLE raw_events AS SELECT * FROM TABLE(DISCOVER_FILE('events.jsonl'));
CREATE VIEW cleaned_events AS SELECT ... FROM raw_events WHERE ...;
CREATE MATERIALIZED VIEW aggregates AS SELECT ... FROM cleaned_events;

-- Export for production
SELECT EXPORT_SCHEMA('etl_config.json', OPTIONS => 'production=true');
```

### DuckDB Catalog Comparison

This design essentially creates a **persistent catalog system** similar to DuckDB's catalog, where:

1. **All resources are views on base tables** - Just like DuckDB where everything is ultimately a view/query on data
2. **Catalog persistence** - DuckDB saves catalog metadata; we save schema definitions
3. **Dynamic schema evolution** - Both support CREATE/ALTER/DROP at runtime
4. **Layered architecture**:
   - Base tables = raw files (CSV, JSON, Parquet)
   - Views = SQL transformations
   - Materialized Views = Cached/optimized results

The key difference: DuckDB's catalog is internal to the database, while our approach makes it **portable and shareable** as JSON configurations.

### Comparison to DuckDB Architecture

| Feature | DuckDB Catalog | File Adapter with Export |
|---------|---------------|-------------------------|
| Base Storage | Tables in database files | Raw files (CSV/JSON/Parquet) |
| Views | Internal catalog entries | Exportable JSON definitions |
| Persistence | Binary database format | Human-readable JSON |
| Sharing | Copy database file | Git-friendly JSON |
| Schema Evolution | ALTER commands | Dynamic + exportable |
| Portability | DuckDB-specific | Any Calcite deployment |

### The Unified Vision

With all these features combined, the file adapter becomes:
- **A virtual database** over files (like DuckDB over its storage)
- **A catalog system** for schema management
- **A collaboration platform** via shareable models
- **A development environment** with dynamic exploration

This transforms raw files into a **managed, versionable, shareable database system** without the overhead of a traditional database server.

---

## Local Development with Production Promotion Workflow

### Overview
Create a complete development-to-production workflow where developers work in isolated local environments and promote tested SQL objects (tables, views, MVs) to shared production through peer review.

### Motivation
- Currently no safe way to develop and test analytics before production
- Direct production changes break dashboards and reports  
- No version control or audit trail for schema changes
- Teams need software engineering workflows for analytics
- Local development eliminates fear of breaking production

### Design Approach

#### 1. Core Principle: Startup Immutable, Runtime Personal
```java
public class ImmutableStartupSchema extends FileSchema {
    private final Set<String> startupObjects = new HashSet<>();
    private final Path personalDir = Paths.get(System.getProperty("user.home"), ".calcite");
    
    @Override
    protected void init() {
        super.init();
        // Everything from model.json is read-only
        startupObjects.addAll(getTableNames());
        startupObjects.addAll(getViewNames()); 
        startupObjects.addAll(getMaterializedViewNames());
    }
    
    @Override
    public void createTable(String name, Table table) {
        // New objects ALWAYS go to personal space
        Path personalPath = personalDir.resolve("tables").resolve(name);
        super.createTable(name, new PersonalTable(table, personalPath));
    }
    
    @Override
    public void dropTable(String name) {
        if (startupObjects.contains(name)) {
            throw new SecurityException("Cannot modify startup objects");
        }
        super.dropTable(name);
    }
}
```

#### 2. Directory Structure
```
/shared/production/             # Shared, immutable
├── model.json                  # Production contract
├── data/
│   ├── sales.parquet          # Read-only data
│   └── customers.csv          # Read-only data
└── cache/
    └── daily_summary.parquet  # Shared MVs (read-only)

~/.calcite/                    # Personal, mutable
├── tables/
│   └── my_analysis.parquet   # Personal tables
├── cache/
│   └── my_cache.parquet      # Personal MVs
└── metadata/
    └── session.json           # Personal objects
```

#### 3. Development Workflow
```sql
-- Start with production model (read-only)
SELECT * FROM sales;           -- Production table
SELECT * FROM revenue_summary; -- Production view

-- Create experimental objects locally
CREATE TABLE my_test AS SELECT * FROM sales WHERE year = 2024;
CREATE VIEW my_analysis AS SELECT region, SUM(amount) FROM my_test GROUP BY region;
CREATE MATERIALIZED VIEW my_dashboard AS SELECT * FROM my_analysis;

-- Test and validate locally
SELECT * FROM my_dashboard;

-- Promote to production when ready
SELECT PROMOTE_TO_MODEL('my_dashboard', 
    CREATE_PR => true,
    DESCRIPTION => 'New regional dashboard for 2024'
);
```

#### 4. Promotion System
```java
public class PromotionManager {
    
    public void promoteToModel(String objectName, Map<String, Object> options) {
        // 1. Validate object exists and is personal
        if (startupObjects.contains(objectName)) {
            throw new IllegalArgumentException("Already in production");
        }
        
        // 2. Analyze dependencies
        Set<String> deps = analyzeDependencies(objectName);
        for (String dep : deps) {
            if (!startupObjects.contains(dep) && !options.get("include_deps")) {
                throw new IllegalStateException("Depends on personal object: " + dep);
            }
        }
        
        // 3. Generate model.json changes
        ModelUpdate update = generateModelUpdate(objectName);
        
        // 4. Create git branch and PR
        if (options.get("create_pr")) {
            String branch = "promote-" + objectName;
            git.checkoutNewBranch(branch);
            updateModelJson(update);
            git.commit("Promote " + objectName + " to production");
            git.push(branch);
            
            PullRequest pr = github.createPR(
                branch,
                "Promote " + objectName,
                generatePRDescription(objectName, update)
            );
            
            System.out.println("Created PR: " + pr.getUrl());
        }
    }
}
```

#### 5. Git Integration
```yaml
# .github/workflows/validate-promotion.yml
name: Validate SQL Promotion
on: [pull_request]

jobs:
  validate:
    steps:
      - name: Check SQL Syntax
        run: calcite --validate-syntax $CHANGED_FILES
      
      - name: Check Naming Convention
        run: calcite --check-naming $CHANGED_FILES
      
      - name: Estimate Query Cost
        run: calcite --explain-cost $CHANGED_FILES
      
      - name: Test Compatibility
        run: calcite --test-compatibility $CHANGED_FILES
```

#### 6. Configuration
```json
{
  "version": "2.0",
  "mode": "development",
  "startup": {
    "immutable": true,
    "source": "model.json"
  },
  "personal": {
    "path": "~/.calcite",
    "persist": true,
    "cleanup": "manual"
  },
  "promotion": {
    "require_review": true,
    "auto_create_pr": true,
    "checks": ["syntax", "naming", "performance"]
  }
}
```

### Use Cases

#### 1. Data Scientist Workflow
```sql
-- Monday: Explore and develop
CREATE TABLE experiments AS SELECT * FROM production.sales SAMPLE 10000;
CREATE VIEW feature_engineering AS SELECT ...;

-- Tuesday: Test model inputs
CREATE MATERIALIZED VIEW training_data AS SELECT ...;

-- Wednesday: Validate results
SELECT * FROM training_data WHERE ...;

-- Thursday: Promote to production
SELECT PROMOTE_TO_MODEL('training_data', 'feature_engineering');

-- Friday: Team uses production version
```

#### 2. Analytics Engineer Workflow
```bash
# Clone analytics repo
git clone company/analytics
cd analytics

# Start development session
calcite --model model.json --dev

# Develop new dashboard
SQL> CREATE VIEW regional_metrics AS ...;
SQL> CREATE MATERIALIZED VIEW exec_dashboard AS ...;

# Test locally
SQL> SELECT * FROM exec_dashboard;

# Promote with PR
SQL> SELECT PROMOTE('exec_dashboard', JIRA => 'DATA-123');

# Review and merge
# Everyone gets tested, reviewed analytics
```

### Benefits
- **Zero Fear Development** - Can't break production
- **Peer Review** - SQL changes reviewed like code
- **Version Control** - Full git history of schema evolution
- **Rollback** - Just revert the commit
- **CI/CD** - Automated validation of SQL changes
- **Audit Trail** - Know who changed what and why

### Implementation Effort
- **Core immutable/personal separation**: 1-2 days
- **Promotion system**: 2-3 days
- **Git integration**: 1-2 days
- **CI/CD templates**: 1 day
- **Total**: 1-1.5 weeks

### ROI Assessment
- **Effort**: 1-1.5 weeks
- **Impact**: Transforms analytics development
- **ROI Score**: 10/10
- **Why**: Solves the biggest pain point in analytics - safe development

---

## The Read-Only Query Engine Philosophy

### Overview
Embrace the file adapter's nature as a read-only query engine over immutable files, with mutations happening through external data pipelines - not trying to be a database.

### Motivation
- File adapter is not a database and shouldn't pretend to be
- Read-only design eliminates entire classes of problems (ACID, locks, permissions)
- Mutations through external tools (Python, Spark) are more appropriate for bulk operations
- Clear separation of concerns: Query (SQL) vs Transform (ETL)
- File system permissions naturally handle access control

### Design Principles

#### 1. Explicit Read-Only Contract
```java
public class ReadOnlyFileAdapter {
    // These methods don't exist
    // No insert(), update(), delete(), alter()
    
    // Only these exist
    public Table select(String query) { ... }
    public View createView(String sql) { ... }      // Derived, not mutation
    public MaterializedView cache(String sql) { ... } // Cached, not mutation
}
```

#### 2. The Architecture Stack
```
┌─────────────────────────────────────────────┐
│           SQL Query Interface               │  <- File Adapter
├─────────────────────────────────────────────┤
│  Views & MVs (Derived Data)    │ READ-WRITE │  <- Personal sandbox
├─────────────────────────────────────────────┤
│  Base Tables (Source Files)    │ READ-ONLY  │  <- Immutable via SQL
├─────────────────────────────────────────────┤
│  File System / Object Storage  │ READ-WRITE │  <- ETL pipelines update
└─────────────────────────────────────────────┘
```

#### 3. Clear Boundaries
```yaml
What SQL CAN Do:
- Read any supported file format
- Create derived views
- Cache results as MVs
- Join across formats
- Push down predicates
- Optimize query execution

What SQL CANNOT Do (by design):
- INSERT INTO tables
- UPDATE records
- DELETE rows
- ALTER TABLE structure
- TRUNCATE data
- Modify source files

What Happens Outside SQL:
- ETL pipelines write new files
- Python/Spark process data
- Schedulers orchestrate updates
- Version control manages schemas
```

#### 4. Mutation Patterns
```python
# Traditional Database Anti-Pattern
for row in millions_of_rows:
    cursor.execute("UPDATE sales SET processed = true WHERE id = ?", row.id)
# Slow, locks, transaction logs, etc.

# File Adapter Pattern
# Python/Spark processes in bulk
df = pd.read_parquet('sales.parquet')
df['processed'] = True
df.to_parquet('sales_processed.parquet')

# Atomic swap
os.rename('sales.parquet', 'sales_old.parquet')
os.rename('sales_processed.parquet', 'sales.parquet')

# SQL immediately sees new data
# No downtime, no locks, no transaction overhead
```

#### 5. Permission Model Simplicity
```bash
# File permissions ARE database permissions
-r--r--r-- /data/sales.parquet      # Everyone can SELECT
-rw-r--r-- /staging/temp.csv        # Owner can "modify"
drwx------ ~/.calcite/cache/        # Personal MV space

# No need for
GRANT SELECT ON sales TO user;      # File system handles this
REVOKE UPDATE ON sales FROM user;   # Can't UPDATE anyway
```

### Benefits

#### 1. **Simplicity**
- No transaction manager
- No lock manager  
- No WAL/redo logs
- No vacuum/analyze
- No permission system
- Just reads files

#### 2. **Performance**
- No ACID overhead
- No lock contention
- Bulk operations stay bulk
- Native tool performance (Pandas, Polars, DuckDB)

#### 3. **Reliability**
- Can't corrupt data via SQL
- Source files remain pristine
- ETL failures don't affect queries
- No "database corruption"

#### 4. **Compatibility**
- Works with any tool that writes files
- Spark, Python, R, Julia all work
- No proprietary format lock-in
- Use the best tool for each job

### Implementation Guidelines

#### 1. Make Read-Only Explicit
```sql
-- Clear error messages
UPDATE sales SET amount = 100;
ERROR: File adapter does not support UPDATE. 
       Modify source files through your ETL pipeline.

DELETE FROM sales;
ERROR: File adapter does not support DELETE.
       Create a filtered view or new table instead.
```

#### 2. Document the Philosophy
```markdown
# File Adapter Philosophy

The file adapter is a **read-only query engine**, not a database.

- ✅ Use it for: Analytics, reporting, exploration
- ❌ Don't use it for: OLTP, mutations, transactions

Mutations happen through your data pipeline (Airflow, Python, etc.)
Queries happen through SQL (File Adapter)
```

#### 3. Provide Migration Patterns
```python
# Instead of UPDATE
def update_sales():
    df = pd.read_parquet('sales.parquet')
    df.loc[df['region'] == 'US', 'tax_rate'] = 0.08
    df.to_parquet('sales_updated.parquet')
    atomic_swap('sales.parquet', 'sales_updated.parquet')

# Instead of DELETE  
def delete_old_records():
    df = pd.read_parquet('events.parquet')
    df_filtered = df[df['date'] >= '2024-01-01']
    df_filtered.to_parquet('events_cleaned.parquet')
    atomic_swap('events.parquet', 'events_cleaned.parquet')
```

### ROI Assessment
- **Effort**: 0 (already how it works!)
- **Impact**: Clarity of purpose, reduced complexity
- **ROI Score**: 10/10
- **Why**: Embracing constraints leads to better design

---

## Single-Machine Analytics Excellence

### Overview
Optimize the file adapter for single-machine performance, acknowledging that 99% of analytical workloads don't need distributed computing.

### Motivation
- Modern machines can handle TB-scale data (64-256GB RAM, NVMe SSDs)
- Single-machine eliminates network overhead and serialization costs
- DuckDB/Polars can process billions of rows on a laptop
- Distributed systems add complexity for marginal benefit in most cases
- Most "big data" is actually <100GB of active data

### Design Approach

#### 1. Single-Machine Optimization First
```java
public class SingleMachineOptimizer {
    // Optimize for local execution
    private final long AVAILABLE_MEMORY = Runtime.getRuntime().maxMemory();
    private final int AVAILABLE_CORES = Runtime.getRuntime().availableProcessors();
    
    public ExecutionPlan optimize(Query query) {
        // Choose engine based on local resources
        if (canFitInMemory(query)) {
            return new InMemoryPlan(query);  // Fastest
        } else if (canUseDuckDB(query)) {
            return new DuckDBPlan(query);    // Out-of-core processing
        } else {
            return new StreamingPlan(query); // Streaming aggregation
        }
    }
}
```

#### 2. Intelligent Engine Selection
```yaml
Query Routing:
  Small (<1GB): 
    → In-memory vectorized execution
  
  Medium (1GB-100GB):
    → DuckDB with local NVMe
  
  Large (100GB-1TB):
    → DuckDB with memory mapping
    → Streaming aggregations
    → Partition pruning
  
  Huge (>1TB):
    → Document: "Consider Spark/Trino for this workload"
```

#### 3. Local Performance Features
```sql
-- Automatic local caching
SELECT * FROM sales WHERE year = 2024;  -- First: 2s, reads from disk
SELECT * FROM sales WHERE year = 2024;  -- Second: 0.1s, from cache

-- Smart memory management
SET max_memory = '32GB';
SET temp_directory = '/fast/nvme/temp';
SET parallel_threads = 16;

-- Adaptive execution
SELECT /*+ USE_MEMORY_IF_FITS */ * FROM large_table;
```

#### 4. Performance Benchmarks to Document
```markdown
## Single Machine Performance Guide

### What Fits on Modern Hardware

| Data Size | Hardware | Query Time | vs Spark Cluster |
|-----------|----------|------------|------------------|
| 1GB | Laptop (8GB RAM) | <1s | 30s (startup overhead) |
| 10GB | Laptop (16GB RAM) | 2-5s | 35s |
| 100GB | Workstation (64GB) | 10-30s | 45s |
| 1TB | Server (256GB) | 1-5min | 3-5min (similar!) |

### When You DON'T Need Distribution
- Daily/weekly aggregations (<100GB)
- Customer analytics (<50GB)  
- Financial reports (<10GB)
- Marketing analytics (<5GB)
- Data exploration (<100GB)
- Feature engineering (<50GB)

### When You DO Need Distribution  
- Training on ImageNet (>1TB)
- Processing all logs (>10TB active)
- Graph algorithms on social networks
- Genomics (petabytes)
```

#### 5. Configuration for Single Machine
```json
{
  "execution": {
    "mode": "single-machine",
    "memory": {
      "max": "75%",  // Leave room for OS
      "spill_to_disk": true,
      "temp_path": "/nvme/temp"
    },
    "parallelism": {
      "threads": "auto",  // Use all cores
      "vectorize": true,
      "simd": true
    },
    "caching": {
      "enabled": true,
      "size": "25%",  // Of available memory
      "eviction": "lru"
    }
  }
}
```

### Benefits

#### 1. **Simplicity**
- No cluster management
- No network configuration
- No serialization overhead
- No coordination overhead
- Just run queries

#### 2. **Performance**
- Memory bandwidth: 50-100 GB/s (vs 1-10 Gbps network)
- No serialization/deserialization
- CPU cache efficiency
- Vectorized execution
- SIMD instructions

#### 3. **Cost**
```yaml
Spark Cluster (unnecessary):
- 10 nodes × $500/month = $5,000/month
- DevOps engineer time = $10,000/month
- Total: $15,000/month

Single Machine (sufficient):
- 1 workstation = $5,000 one-time
- No DevOps needed
- Total: $5,000 once
```

#### 4. **Development Speed**
```bash
# Distributed development
- Start cluster: 5 minutes
- Deploy code: 2 minutes  
- Run query: 30 seconds
- Debug failure: 30 minutes (logs across nodes)
Total: ~40 minutes per iteration

# Single machine development
- Start: instant
- Run query: 2 seconds
- Debug: 1 minute (local logs)
Total: ~1 minute per iteration
```

### Implementation Recommendations

#### 1. Default to Single Machine
```java
public class FileSchemaFactory {
    public Schema create(Map<String, Object> config) {
        // Default to single-machine optimization
        if (!config.containsKey("distributed")) {
            config.put("execution", "single-machine");
            config.put("engine", "duckdb");  // Best single-machine engine
        }
        return new FileSchema(config);
    }
}
```

#### 2. Add Performance Warnings
```sql
SELECT * FROM massive_table;  -- 10TB
WARNING: Query will process 10TB of data.
         Consider filtering or sampling.
         For full processing, consider distributed engine.
         Continue with single-machine execution? [y/N]
```

#### 3. Document Sweet Spots
```markdown
## File Adapter Sweet Spots

### Perfect For (99% of cases):
- Interactive analytics (<100GB)
- Development/testing (any size - use sampling)
- Dashboards (<10GB active data)
- Reports (<50GB)
- Data science exploration (<100GB)

### Consider Alternatives For:
- Petabyte processing → Use Spark
- Real-time streaming → Use Flink/Kafka
- Graph algorithms → Use Neo4j/Spark GraphX
- Deep learning on huge datasets → Use dedicated ML infrastructure
```

### ROI Assessment
- **Effort**: Mostly documentation and defaults (1-2 days)
- **Impact**: 10-100x performance for common cases
- **ROI Score**: 9.5/10
- **Why**: Optimizing for the 99% case is always the right choice

---

## Horizontal Scalability Through Shared Storage

### Overview
The file adapter naturally supports horizontal scaling through shared storage, allowing multiple server instances to safely read the same data while maintaining cache coherency through appropriate file locking.

### Motivation
- Need to scale read capacity without distributed computing complexity
- Kubernetes/container deployments require horizontal scaling
- Shared storage (NFS, EFS, S3) is ubiquitous in cloud environments
- Multiple readers with coordinated caching is a sweet spot
- File locking for cache writes prevents corruption

### Design Approach

#### 1. Multi-Reader Architecture
```
┌─────────────────────────────────────────────┐
│         Load Balancer / Kubernetes          │
├─────────────────────────────────────────────┤
│  Server 1  │  Server 2  │  Server 3  │ ... │  <- Horizontal pods
├─────────────────────────────────────────────┤
│          File Adapter Instances             │  <- Same model.json
├─────────────────────────────────────────────┤
│         Shared Storage (NFS/EFS/S3)         │  <- Single source
└─────────────────────────────────────────────┘
```

#### 2. Safe Concurrent Access Pattern
```java
public class ConcurrentFileAdapter {
    // Multiple readers, no problem
    public Table readTable(String path) {
        // Just read - file system handles concurrent reads
        return new ParquetTable(path);
    }
    
    // Cache writes use file locking
    public void writeMaterializedView(String name, Data data) {
        Path cachePath = getCachePath(name);
        Path tempPath = cachePath.resolveSibling(name + ".tmp." + UUID.randomUUID());
        
        // Write to temp file first
        writeToFile(tempPath, data);
        
        // Atomic rename with lock
        try (FileLock lock = acquireLock(cachePath + ".lock")) {
            Files.move(tempPath, cachePath, StandardCopyOption.ATOMIC_MOVE);
        }
    }
    
    // Cache reads handle concurrent updates
    public Table readMaterializedView(String name) {
        Path cachePath = getCachePath(name);
        
        // Check if being updated
        if (Files.exists(cachePath + ".lock")) {
            // Read from source instead
            return computeFromSource(name);
        }
        
        return readCachedTable(cachePath);
    }
}
```

#### 3. Kubernetes Deployment Example
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: calcite-file-adapter
spec:
  replicas: 10  # Scale horizontally
  template:
    spec:
      containers:
      - name: calcite
        image: calcite-file-adapter:latest
        env:
        - name: MODEL_PATH
          value: /shared/config/model.json
        volumeMounts:
        - name: shared-data
          mountPath: /shared/data
          readOnly: true  # Data is read-only
        - name: shared-cache
          mountPath: /shared/cache
          readOnly: false  # Cache is read-write
      volumes:
      - name: shared-data
        persistentVolumeClaim:
          claimName: efs-data  # AWS EFS
      - name: shared-cache
        persistentVolumeClaim:
          claimName: efs-cache
```

#### 4. Cache Coordination Strategies

**Option A: Shared Cache with Locking**
```java
public class SharedCacheManager {
    private final Path cacheDir = Paths.get("/shared/cache");
    
    public void refreshMV(String mvName) {
        Path lockFile = cacheDir.resolve(mvName + ".lock");
        
        try (RandomAccessFile raf = new RandomAccessFile(lockFile.toFile(), "rw");
             FileLock lock = raf.getChannel().lock()) {
            
            // Only one instance refreshes at a time
            if (needsRefresh(mvName)) {
                computeAndWriteMV(mvName);
            }
        }
    }
}
```

**Option B: Instance-Affinity Caching**
```java
public class AffinityCacheManager {
    private final String instanceId = System.getenv("HOSTNAME");
    
    public Path getCachePath(String mvName) {
        // Each instance has its own cache
        return Paths.get("/shared/cache", instanceId, mvName + ".parquet");
    }
    
    // Load balancer uses session affinity to route
    // same queries to same instance for cache hits
}
```

**Option C: Read-Through Shared Cache**
```java
public class ReadThroughCache {
    public Table getMV(String mvName) {
        Path cachePath = getCachePath(mvName);
        
        // All instances read same cache
        if (Files.exists(cachePath) && !isStale(cachePath)) {
            return readCache(cachePath);
        }
        
        // First instance to detect staleness refreshes
        if (tryAcquireRefreshLock(mvName)) {
            refreshCache(mvName);
        } else {
            // Others compute from source while refresh happens
            return computeFromSource(mvName);
        }
    }
}
```

#### 5. Cloud Storage Optimizations

**S3/Object Storage**
```java
public class S3OptimizedAdapter {
    // S3 doesn't support file locking, use different strategy
    
    public void writeMV(String mvName, Data data) {
        String key = "cache/" + mvName + "/" + System.currentTimeMillis() + ".parquet";
        s3.putObject(bucket, key, data);
        
        // Write marker for latest version
        s3.putObject(bucket, "cache/" + mvName + "/LATEST", key);
    }
    
    public Table readMV(String mvName) {
        // Read marker to find latest version
        String latestKey = s3.getObject(bucket, "cache/" + mvName + "/LATEST");
        return readParquet(s3.getObject(bucket, latestKey));
    }
}
```

**NFS/EFS Optimizations**
```yaml
# NFS mount options for performance
nfs_options:
  - hard        # Retry indefinitely
  - intr        # Allow interruption
  - rsize=1048576  # 1MB read buffer
  - wsize=1048576  # 1MB write buffer
  - timeo=600   # 60 second timeout
  - retrans=2   # Retry twice
  - noresvport  # Don't use reserved ports
  - async       # Async writes for cache
```

### Benefits

#### 1. **Linear Read Scalability**
- Add more containers = more query capacity
- No coordination needed for reads
- Perfect for read-heavy analytics

#### 2. **Simple Operations**
- Just deploy more pods/containers
- No complex distributed system
- Standard Kubernetes patterns

#### 3. **Cost Effective**
- Shared storage is cheap (especially reads)
- No data duplication across nodes
- Efficient resource utilization

#### 4. **High Availability**
```yaml
# Natural HA through multiple instances
Instance-1: Crashes
Instance-2: Still serving
Instance-3: Still serving
Load Balancer: Routes around failure
```

### Real-World Deployment Patterns

#### 1. **Read-Heavy Analytics Service**
```
- 20 container instances
- Shared EFS with data files
- Each instance: 4 CPU, 16GB RAM
- Handles 1000 concurrent queries
- Total cost: $2000/month
- Alternative (Redshift): $10,000/month
```

#### 2. **Development Platform**
```
- 5 instances per environment (dev/test/prod)
- Shared NFS mount
- Personal caches in ~/.calcite
- Shared production caches in /shared/cache
```

#### 3. **Hybrid Edge/Cloud**
```
- Edge locations: Local file adapter instances
- Cloud: S3 backend storage
- Edge caches frequently accessed data
- Cloud has complete dataset
```

### Configuration Examples

#### Production HA Configuration
```json
{
  "deployment": {
    "mode": "horizontal",
    "instances": "auto",  // Kubernetes HPA
    "storage": {
      "data": {
        "type": "efs",
        "mount": "/shared/data",
        "readonly": true
      },
      "cache": {
        "type": "efs",
        "mount": "/shared/cache",
        "locking": "file-based",
        "ttl": "1h"
      }
    },
    "coordination": {
      "strategy": "shared-cache-with-locking",
      "lock_timeout": "30s",
      "stale_while_revalidate": true
    }
  }
}
```

#### Development Cluster Configuration
```json
{
  "deployment": {
    "mode": "horizontal",
    "instances": 3,
    "storage": {
      "data": {
        "type": "nfs",
        "server": "nfs.internal",
        "path": "/data"
      },
      "cache": {
        "strategy": "instance-local",
        "path": "/tmp/calcite-cache"
      }
    }
  }
}
```

### Implementation Considerations

#### 1. **File Locking Limitations**
```java
// Not all file systems support locking
public boolean supportsLocking(Path path) {
    try {
        // Test lock support
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
             FileLock lock = raf.getChannel().tryLock()) {
            return lock != null;
        }
    } catch (Exception e) {
        return false;  // S3, some NFS configs
    }
}
```

#### 2. **Cache Coherency Options**
- **Eventually Consistent**: Accept stale reads for performance
- **Strong Consistency**: Always check freshness (slower)
- **Time-Based**: TTL-based cache invalidation
- **Event-Based**: Watch for file changes (inotify/FSEvents)

#### 3. **Performance Tuning**
```java
public class PerformanceOptimizedReader {
    // Connection pooling for cloud storage
    private final S3ConnectionPool s3Pool;
    
    // Read-ahead for sequential access
    private final int readAheadBuffer = 10 * 1024 * 1024; // 10MB
    
    // Parallel reads for large files
    private final ExecutorService readExecutor = 
        Executors.newFixedThreadPool(10);
    
    // Local cache for hot data
    private final LoadingCache<String, Table> hotCache;
}
```

### ROI Assessment
- **Effort**: 0 (already works this way!)
- **Impact**: Enables production deployments at scale
- **ROI Score**: 10/10
- **Why**: Critical for real-world usage, already implemented

### The Key Insight

The file adapter's **stateless design** makes horizontal scaling trivial:
- No session state to coordinate
- No transaction logs to synchronize
- No cluster membership to manage
- Just multiple readers of shared files

This is actually **better than traditional database clustering** because:
- No split-brain problems
- No consensus protocols needed
- No replication lag
- No failover complexity

It's the **simplest possible horizontal scaling**: run more copies!

---

## Storage Provider SQL Functions

### Overview
SQL functions for testing and debugging remote storage connections.

### Implementation
```sql
SELECT TEST_STORAGE_CONNECTION('s3://bucket/');
SELECT * FROM TABLE(LIST_REMOTE_FILES('s3://bucket/', '*.parquet'));
```

### ROI Assessment
- **Effort:** 1-2 hours
- **Impact:** Critical for cloud deployments
- **ROI Score:** 7/10

---

## Performance Monitoring SQL Functions

### Overview
SQL functions to monitor query performance and engine usage.

### Implementation
```sql
SELECT * FROM TABLE(ENGINE_USAGE_STATS());
SELECT * FROM TABLE(SLOW_QUERY_LOG(1000));
```

### ROI Assessment
- **Effort:** 2-3 hours
- **Impact:** Production monitoring
- **ROI Score:** 5/10

---

## Additional SQL Functions for File Adapter (Original Full List)

#### 1. Parquet Cache Control
```sql
-- Clear all parquet cache files
SELECT CLEAR_PARQUET_CACHE();

-- Clear cache for specific table
SELECT CLEAR_PARQUET_CACHE('sales_data');

-- Show cache status
SELECT * FROM TABLE(PARQUET_CACHE_STATUS());
-- Returns: table_name, cache_file, size_mb, last_modified, hit_ratio
```

#### 2. Statistics Cache Management
```sql
-- Refresh HLL statistics for table
SELECT REFRESH_STATISTICS('sales_data');

-- Prime statistics cache for all tables
SELECT PRIME_STATISTICS_CACHE();

-- Show statistics status
SELECT * FROM TABLE(STATISTICS_STATUS());
-- Returns: table_name, stats_available, cardinality_estimate, last_updated
```

### File Discovery Functions

#### 3. File Pattern Analysis
```sql
-- Test glob patterns without creating tables
SELECT * FROM TABLE(TEST_GLOB_PATTERN('data/*.csv'));
-- Returns: file_path, size, last_modified, estimated_rows

-- Discover schema from files
SELECT * FROM TABLE(INFER_SCHEMA('logs/*.jsonl'));
-- Returns: column_name, data_type, nullable, sample_values

-- Find files modified since timestamp
SELECT * FROM TABLE(FIND_RECENT_FILES('/data', '2024-01-01'));
```

### Storage Provider Functions

#### 4. Remote Storage Operations
```sql
-- Test storage connectivity
SELECT TEST_STORAGE_CONNECTION('s3://my-bucket/data/');

-- List remote files
SELECT * FROM TABLE(LIST_REMOTE_FILES('s3://my-bucket/', '*.parquet'));

-- Download file metadata without reading
SELECT * FROM TABLE(REMOTE_FILE_INFO('https://api.com/data.json'));
-- Returns: url, size, content_type, last_modified, response_time
```

### Data Quality Functions

#### 5. File Validation
```sql
-- Validate file integrity
SELECT VALIDATE_FILE('data.parquet') AS is_valid;

-- Check schema compatibility
SELECT SCHEMA_COMPATIBLE('old_data.csv', 'new_data.csv') AS compatible;

-- Estimate query cost
SELECT * FROM TABLE(ESTIMATE_QUERY_COST('SELECT * FROM big_table WHERE date > ?', '2024-01-01'));
-- Returns: estimated_rows, estimated_io_mb, execution_engine, cost_score
```

### Performance Monitoring Functions

#### 6. Query Performance Insights
```sql
-- Show execution engine usage
SELECT * FROM TABLE(ENGINE_USAGE_STATS());
-- Returns: engine, queries_executed, avg_time_ms, cache_hits

-- Table access patterns
SELECT * FROM TABLE(TABLE_ACCESS_STATS());
-- Returns: table_name, access_count, avg_scan_time, cache_hit_ratio

-- Show slow queries
SELECT * FROM TABLE(SLOW_QUERY_LOG(1000)); -- queries > 1000ms
```

### Development/Debug Functions

#### 7. Debugging Utilities
```sql
-- Show table metadata
SELECT * FROM TABLE(DESCRIBE_TABLE_DETAIL('sales_data'));
-- Returns: property, value (file_path, format, engine, cache_status, etc.)

-- Test predicate pushdown
SELECT * FROM TABLE(EXPLAIN_PUSHDOWN('SELECT * FROM sales WHERE amount > 100'));
-- Returns: filter_pushed, engine_used, estimated_selectivity

-- Force engine selection for query
SELECT * FROM TABLE(WITH_ENGINE('duckdb', 'SELECT COUNT(*) FROM big_table'));
```

### Implementation Pattern (Same for All)

```java
// Simple function class
public class CacheManagementFunctions {
    public static String clearParquetCache() {
        // Find all .parquet files in cache directory
        // Delete them
        return "Cache cleared";
    }
    
    public static String clearParquetCache(String tableName) {
        // Find cache file for specific table
        // Delete it
        return "Cache cleared for " + tableName;
    }
}

// Registration in FileSchema
@Override 
protected Map<String, Function> getFunctionMap() {
    Map<String, Function> functions = new HashMap<>();
    
    functions.put("CLEAR_PARQUET_CACHE", 
        ScalarFunctionImpl.create(CacheManagementFunctions.class, "clearParquetCache"));
    
    return functions;
}
```

### Benefits by Category

**Cache Management**: Direct control over performance-critical caches
**File Discovery**: Explore data without creating formal tables  
**Storage Operations**: Debug remote connectivity issues
**Data Quality**: Validate data before processing
**Performance**: Monitor and optimize query execution
**Debugging**: Troubleshoot adapter behavior

### ROI Assessment

**Highest Value (30 min each)**:
1. `CLEAR_PARQUET_CACHE` - Immediate performance control
2. `TEST_GLOB_PATTERN` - Saves time during schema design
3. `VALIDATE_FILE` - Catch corruption early
4. `REFRESH_STATISTICS` - Performance tuning

**Medium Value (1-2 hours each)**:
5. `PARQUET_CACHE_STATUS` - Operational visibility  
6. `INFER_SCHEMA` - Automate table creation
7. `TEST_STORAGE_CONNECTION` - Remote debugging

**Nice to Have (2-4 hours each)**:
8. Performance monitoring functions
9. Debug utilities
10. Engine selection functions

### Implementation Strategy
Start with cache management functions (immediate user pain points), then add file discovery (development productivity), then monitoring/debug functions (operational maturity).

All follow the same simple pattern: static method + function registration = powerful SQL capability!

## Updated Comprehensive ROI Ranking (All 20+ Design Ideas)

### 🥇 **Tier 1: "Friday Afternoon" Quick Wins (< 2 hours each)**

**1. Imperative MV Refresh Function**
- **Effort:** 45 minutes
- **ROI Score:** 10/10
- **Why:** Desperately needed, trivial implementation

**2. DDL Persistence (Make CREATE VIEW/MV Permanent)**
- **Effort:** 1-2 hours
- **ROI Score:** 9.5/10
- **Why:** Calcite already has DDL, just need to save/restore

**3. CLEAR_PARQUET_CACHE Function**
- **Effort:** 30 minutes
- **ROI Score:** 9.5/10
- **Why:** Everyone needs this constantly

**4. TEST_GLOB_PATTERN Function**
- **Effort:** 30 minutes
- **ROI Score:** 9/10
- **Why:** Eliminates trial-and-error table creation

**5. REFRESH_STATISTICS Function**
- **Effort:** 30 minutes
- **ROI Score:** 9/10
- **Why:** Direct performance control

**6. INFER_SCHEMA Function**
- **Effort:** 1 hour
- **ROI Score:** 8.5/10
- **Why:** Automates table creation

### 🥈 **Tier 2: High-Value Features (2-3 days)**

**7. JSONL with Incremental Refresh**
- **Effort:** 2-3 days
- **ROI Score:** 8/10
- **Why:** Common use case, builds on JSON

**8. Avro Support**
- **Effort:** 1-2 days
- **ROI Score:** 7.5/10
- **Why:** Kafka ecosystem standard

**9. Storage Provider SQL Functions**
- **Effort:** 1-2 hours
- **ROI Score:** 7/10
- **Why:** Critical for cloud debugging

**10. ORC Support**
- **Effort:** 2-3 days
- **ROI Score:** 7/10
- **Why:** Hadoop ecosystem

**11. S3 Select Support**
- **Effort:** 3-4 days
- **ROI Score:** 7/10
- **Why:** Cost savings but S3-specific

### 🥉 **Tier 3: Nice-to-Have (Variable effort)**

**12. File Discovery Functions (remaining)**
- **Effort:** 2-3 hours total
- **ROI Score:** 6/10
- **Why:** Helpful but not critical

**13. Performance Monitoring Functions**
- **Effort:** 2-3 hours
- **ROI Score:** 5/10
- **Why:** Production-only value

**14. Delta Lake Support**
- **Effort:** 1-2 weeks
- **ROI Score:** 5/10
- **Why:** Complex, competes with Iceberg

### **Tier 4: Complex/Experimental**

**15. MV Dependency Tracking (Full)**
- **Effort:** 1-2 weeks
- **ROI Score:** 4/10
- **Why:** Complex for marginal benefit

**16. gRPC Support**
- **Effort:** 2-3 weeks
- **ROI Score:** 3.5/10
- **Why:** Innovative but niche

**17. DDL with SQL Rewriting**
- **Effort:** 2-3 hours
- **ROI Score:** 3/10 (redundant)
- **Why:** Not needed since Calcite has DDL

## Complete ROI Ranking of All Design Ideas (Original)

### 🥇 **Tier 1: Quick Wins (Hours of effort, immediate value)**

**1. Imperative MV Refresh** 
- **Effort:** 45 minutes
- **Impact:** Solves immediate user pain point
- **ROI Score:** 10/10
- **Why:** Trivial implementation, desperately needed

**2. CLEAR_PARQUET_CACHE Function**
- **Effort:** 30 minutes  
- **Impact:** Essential for debugging/development
- **ROI Score:** 9.5/10
- **Why:** Everyone needs this, constantly

**3. TEST_GLOB_PATTERN Function**
- **Effort:** 30 minutes
- **Impact:** Saves hours during schema design
- **ROI Score:** 9/10
- **Why:** Eliminates trial-and-error table creation

**4. REFRESH_STATISTICS Function**
- **Effort:** 30 minutes
- **Impact:** Direct performance control
- **ROI Score:** 8.5/10
- **Why:** Immediate performance tuning capability

### 🥈 **Tier 2: High Value Features (Days of effort, broad impact)**

**5. JSONL with Incremental Refresh**
- **Effort:** 2-3 days
- **Impact:** Solves common log/streaming use case
- **ROI Score:** 8/10
- **Why:** Small extension to existing JSON, widely needed

**6. Avro Support**
- **Effort:** 1-2 days
- **Impact:** Kafka/streaming ecosystem
- **ROI Score:** 7.5/10
- **Why:** Excellent libraries, enterprise standard

**7. ORC Support**
- **Effort:** 2-3 days
- **Impact:** Hadoop/Hive users
- **ROI Score:** 7/10
- **Why:** Can reuse Parquet infrastructure

**8. S3 Select Support**
- **Effort:** 3-4 days
- **Impact:** 80-90% cost reduction for cloud users
- **ROI Score:** 7/10 (was higher but effort increased after analysis)
- **Why:** High value but only for S3 users

### 🥉 **Tier 3: Nice-to-Have Features (Days of effort, targeted impact)**

**9. INFER_SCHEMA Function**
- **Effort:** 2 hours
- **Impact:** Development productivity
- **ROI Score:** 6.5/10
- **Why:** Helpful but not critical

**10. File Validation Functions Suite**
- **Effort:** 2-3 hours total
- **Impact:** Data quality assurance
- **ROI Score:** 6/10
- **Why:** Good practice but not blocking

**11. Performance Monitoring Functions**
- **Effort:** 4-6 hours total
- **Impact:** Operational insights
- **ROI Score:** 5.5/10
- **Why:** Valuable for production, not for development

### **Tier 4: Complex/Niche Features (Weeks of effort)**

**12. Delta Lake Support**
- **Effort:** 1-2 weeks
- **Impact:** Modern data lake users
- **ROI Score:** 5/10
- **Why:** Great features but Spark dependency is heavy

**13. MV Dependency Tracking (Complex)**
- **Effort:** 1-2 weeks
- **Impact:** Advanced use cases only
- **ROI Score:** 4/10
- **Why:** Complex for marginal benefit over imperative refresh

**14. gRPC Support**
- **Effort:** 2-3 weeks
- **Impact:** Microservices architectures
- **ROI Score:** 3.5/10
- **Why:** Innovative but very niche, high complexity

## Recommended Implementation Order

### Phase 1: "Friday Afternoon" Features (4 hours total)
1. Imperative MV Refresh (45 min)
2. CLEAR_PARQUET_CACHE (30 min)
3. TEST_GLOB_PATTERN (30 min)
4. REFRESH_STATISTICS (30 min)
**Result:** Immediate productivity boost for all users

### Phase 2: Format Support (1 week)
5. JSONL Support (2-3 days)
6. Avro Support (1-2 days)
**Result:** Cover 90% of enterprise data formats

### Phase 3: Cloud Optimization (1 week)
7. S3 Select (3-4 days)
8. ORC Support (2-3 days)
**Result:** Cost savings and Hadoop ecosystem support

### Phase 4: Advanced Features (2-4 weeks)
9. Additional SQL Functions (as needed)
10. Delta Lake (if resources allow)
**Result:** Feature completeness

## Key Insights

**The "One Hour Wonder" Pattern:**
Functions that take <1 hour to implement often provide 80% of the value of features that take weeks. The imperative MV refresh and cache management functions are perfect examples.

**Format vs Feature:**
Adding file formats (JSONL, Avro, ORC) is straightforward engineering with predictable ROI. Adding features (gRPC, Delta Lake) involves architectural decisions and ongoing maintenance.

**SQL Functions Rule:**
Any operational capability exposed as a SQL function has multiplicative value because it's:
- Discoverable in SQL tools
- Scriptable in ETL pipelines  
- Usable without Java knowledge
- Composable with other SQL

**Recommendation:**
Implement all Phase 1 features immediately (one afternoon), then evaluate user feedback before proceeding to Phase 2.

## DDL Persistence - Make CREATE VIEW/MV Permanent

### Overview
Add persistence to Calcite's existing CREATE VIEW and CREATE MATERIALIZED VIEW commands so they survive session restarts.

### Motivation
- Calcite already supports CREATE VIEW/MV but they're transient
- Views disappear when connection closes
- One-line fix to add massive value

### Implementation (1-2 hours)
```java
public class PersistentFileSchema extends FileSchema {
    @Override
    public void add(String name, Table table) {
        super.add(name, table);
        // Just save what Calcite created
        if (table instanceof ViewTableMacro || table instanceof MaterializedViewTable) {
            metadataStore.persist(name, table);
        }
    }
}
```

### ROI Assessment
- **Effort:** 1-2 hours
- **Impact:** Transforms file adapter into stateful system
- **ROI Score:** 9.5/10

---

## DDL Support for Views and Materialized Views (Full Implementation)

### Overview
Enable CREATE/ALTER/DROP operations for views and materialized views through SQL DDL commands, making the file adapter behave more like a traditional database.

### Motivation
- Currently views/MVs must be defined in static model.json
- No way to create views dynamically during a session
- Can't modify or drop existing views without editing JSON
- Users expect database-like DDL capabilities
- Would enable dynamic schema evolution

### Design Approach

#### 1. Metadata Persistence Strategy
```json
// .calcite/metadata/views.json
{
  "views": [
    {
      "name": "sales_summary",
      "sql": "SELECT region, SUM(amount) FROM sales GROUP BY region",
      "created": "2024-01-15T10:00:00Z",
      "modified": "2024-01-15T10:00:00Z"
    }
  ],
  "materializedViews": [
    {
      "name": "daily_report",
      "sql": "SELECT date, COUNT(*) FROM events GROUP BY date",
      "storage": ".calcite/cache/daily_report.parquet",
      "created": "2024-01-15T10:00:00Z"
    }
  ]
}
```

#### 2. DDL Implementation
```java
public class FileSchemaWithDDL extends FileSchema {
    private final MetadataStore metadataStore;
    
    @Override
    public void execute(SqlNode node, CalcitePrepare.Context context) {
        if (node instanceof SqlCreateView) {
            handleCreateView((SqlCreateView) node);
        } else if (node instanceof SqlDropView) {
            handleDropView((SqlDropView) node);
        } else if (node instanceof SqlCreateMaterializedView) {
            handleCreateMaterializedView((SqlCreateMaterializedView) node);
        }
    }
    
    private void handleCreateView(SqlCreateView create) {
        String viewName = create.getViewName();
        String viewSql = create.getQuery().toString();
        
        // Add to runtime schema
        ViewTable view = new ViewTable(viewSql, this);
        addTable(viewName, view);
        
        // Persist to metadata
        metadataStore.addView(viewName, viewSql);
        metadataStore.save();
    }
    
    private void handleCreateMaterializedView(SqlCreateMaterializedView create) {
        String mvName = create.getViewName();
        String mvSql = create.getQuery().toString();
        File parquetFile = new File(".calcite/cache/" + mvName + ".parquet");
        
        // Create MV table
        MaterializedViewTable mv = new MaterializedViewTable(
            this, schemaName, mvName, mvSql, parquetFile, getTables()
        );
        addTable(mvName, mv);
        
        // Persist to metadata
        metadataStore.addMaterializedView(mvName, mvSql, parquetFile.getPath());
        metadataStore.save();
    }
}
```

#### 3. SQL Syntax Support
```sql
-- Create regular view
CREATE VIEW sales_by_region AS 
SELECT region, SUM(amount) as total 
FROM sales 
GROUP BY region;

-- Create or replace view
CREATE OR REPLACE VIEW sales_by_region AS
SELECT region, SUM(amount) as total, COUNT(*) as count
FROM sales 
GROUP BY region;

-- Create materialized view
CREATE MATERIALIZED VIEW daily_summary AS
SELECT date, COUNT(*) as events, SUM(amount) as total
FROM transactions
GROUP BY date;

-- Drop view
DROP VIEW sales_by_region;
DROP VIEW IF EXISTS sales_by_region;

-- Drop materialized view
DROP MATERIALIZED VIEW daily_summary;

-- Alter view (redefine)
ALTER VIEW sales_by_region AS
SELECT region, product, SUM(amount) as total
FROM sales
GROUP BY region, product;
```

#### 4. Metadata Store Implementation
```java
public class MetadataStore {
    private final File metadataDir;
    private ViewMetadata metadata;
    
    public MetadataStore(File baseDir) {
        this.metadataDir = new File(baseDir, ".calcite/metadata");
        this.metadataDir.mkdirs();
        this.metadata = load();
    }
    
    private ViewMetadata load() {
        File metadataFile = new File(metadataDir, "views.json");
        if (metadataFile.exists()) {
            return mapper.readValue(metadataFile, ViewMetadata.class);
        }
        return new ViewMetadata();
    }
    
    public void save() {
        File metadataFile = new File(metadataDir, "views.json");
        mapper.writerWithDefaultPrettyPrinter()
            .writeValue(metadataFile, metadata);
    }
    
    // Merge with model.json views on startup
    public void mergeWithModel(List<Map<String, Object>> modelViews) {
        // Model.json views take precedence (backward compatibility)
        // Runtime-created views supplement them
    }
}
```

#### 5. Schema Lifecycle Management
```java
public class PersistentFileSchema extends FileSchema {
    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tables = super.getTableMap();
        
        // Load persisted views
        MetadataStore store = new MetadataStore(baseDirectory);
        
        // Add persisted views to schema
        for (ViewDef view : store.getViews()) {
            tables.put(view.name, new ViewTable(view.sql, this));
        }
        
        // Add persisted MVs to schema
        for (MaterializedViewDef mv : store.getMaterializedViews()) {
            tables.put(mv.name, new MaterializedViewTable(...));
        }
        
        return tables;
    }
}
```

### Implementation Challenges

#### 1. Parser Extension
- Need to extend Calcite's parser to recognize CREATE VIEW in file adapter context
- May need custom SqlNode implementations

#### 2. Transaction Semantics
- File system operations aren't transactional
- Need to handle partial failures gracefully

#### 3. Concurrency
- Multiple connections might try to modify metadata
- Need file locking or optimistic concurrency control

#### 4. Schema Refresh
- How to notify other connections about schema changes?
- May need schema versioning

### Benefits
- **Dynamic Schema Evolution** - Create views without restarting
- **SQL-Native Management** - No JSON editing required
- **Session Persistence** - Views survive restarts
- **Familiar Syntax** - Standard SQL DDL
- **Incremental Development** - Build schema interactively

### Implementation Effort
- **Parser Integration:** 2-3 days
- **Metadata Store:** 1-2 days  
- **DDL Handlers:** 2-3 days
- **Testing:** 2-3 days
- **Total:** 1-2 weeks

### ROI Assessment
- **Effort:** 1-2 weeks
- **Impact:** Major usability improvement
- **ROI Score:** 6.5/10
- **Why:** High value but non-trivial implementation

### Alternative: Simpler Function-Based Approach
```sql
-- Use functions instead of DDL (much simpler)
SELECT CREATE_VIEW('sales_summary', 'SELECT * FROM sales WHERE amount > 100');
SELECT DROP_VIEW('sales_summary');
SELECT CREATE_MATERIALIZED_VIEW('daily_report', 'SELECT date, COUNT(*) FROM events GROUP BY date');
```

This would reuse the SQL function pattern (45 minutes to implement) vs full DDL support (1-2 weeks).

### Optimal Approach: SQL Rewriting (2-3 hours!)

Instead of extending the parser, intercept and rewrite DDL statements to function calls:

```java
public class DDLRewritingHook implements Hook {
    @Override
    public String rewrite(String sql) {
        // Simple regex-based rewriting
        if (sql.matches("(?i)CREATE\\s+VIEW\\s+.*")) {
            return rewriteCreateView(sql);
        }
        if (sql.matches("(?i)DROP\\s+VIEW\\s+.*")) {
            return rewriteDropView(sql);
        }
        if (sql.matches("(?i)CREATE\\s+MATERIALIZED\\s+VIEW\\s+.*")) {
            return rewriteCreateMaterializedView(sql);
        }
        return sql; // Pass through non-DDL
    }
    
    private String rewriteCreateView(String sql) {
        // CREATE VIEW sales_summary AS SELECT * FROM sales
        // becomes:
        // SELECT CREATE_VIEW('sales_summary', 'SELECT * FROM sales')
        
        Pattern p = Pattern.compile(
            "(?i)CREATE\\s+(?:OR\\s+REPLACE\\s+)?VIEW\\s+(\\w+)\\s+AS\\s+(.+)",
            Pattern.DOTALL
        );
        Matcher m = p.matcher(sql);
        if (m.matches()) {
            String viewName = m.group(1);
            String viewSql = m.group(2);
            return String.format("SELECT CREATE_VIEW('%s', '%s')", 
                viewName, viewSql.replace("'", "''"));
        }
        return sql;
    }
    
    private String rewriteDropView(String sql) {
        // DROP VIEW sales_summary
        // becomes:
        // SELECT DROP_VIEW('sales_summary')
        
        Pattern p = Pattern.compile(
            "(?i)DROP\\s+VIEW\\s+(?:IF\\s+EXISTS\\s+)?(\\w+)"
        );
        Matcher m = p.matcher(sql);
        if (m.matches()) {
            String viewName = m.group(1);
            return String.format("SELECT DROP_VIEW('%s')", viewName);
        }
        return sql;
    }
}

// Install the hook in FileSchema or connection
public class FileSchemaWithDDL extends FileSchema {
    @Override
    public CalciteConnection wrapConnection(CalciteConnection conn) {
        conn.addStatementHook(new DDLRewritingHook());
        return conn;
    }
}
```

#### Benefits of SQL Rewriting Approach:
1. **Natural SQL Syntax** - Users write standard DDL
2. **Simple Implementation** - Just string manipulation
3. **No Parser Changes** - Works with existing Calcite
4. **Immediate Compatibility** - Tools that generate DDL just work
5. **Gradual Enhancement** - Add more DDL commands over time

#### Implementation Steps (2-3 hours total):
1. **Create rewriting hook** (1 hour)
   - Regex patterns for CREATE/DROP/ALTER VIEW
   - Transform to function calls
   
2. **Implement backing functions** (30 minutes)
   - CREATE_VIEW, DROP_VIEW, etc.
   - Reuse metadata store pattern
   
3. **Install hook in connection** (30 minutes)
   - Add to FileSchema initialization
   - Test with various DDL patterns

#### What Users See:
```sql
-- They write normal DDL
CREATE VIEW regional_sales AS 
SELECT region, SUM(amount) FROM sales GROUP BY region;

-- It gets rewritten behind the scenes to:
SELECT CREATE_VIEW('regional_sales', 'SELECT region, SUM(amount) FROM sales GROUP BY region');

-- But they see:
View 'regional_sales' created successfully
```

#### This Approach Wins Because:
- **User Experience:** Natural SQL DDL syntax
- **Implementation:** Dead simple (regex + functions)
- **Time to Market:** 2-3 hours vs 1-2 weeks
- **Maintenance:** No parser modifications to maintain
- **Compatibility:** Works with any SQL tool

### Open Questions
- Should views be schema-specific or global?
- How to handle view dependencies when dropping?

### Important Discovery: Calcite Already Supports DDL!

After investigation, Calcite already has full support for:
- `CREATE VIEW` - Works via `SqlCreateView` and `ServerDdlExecutor`
- `CREATE MATERIALIZED VIEW` - Works via `SqlCreateMaterializedView` and `MaterializationService`
- `DROP VIEW` / `DROP MATERIALIZED VIEW` - Works via `SqlDropObject`

**The only missing piece:** These views/MVs don't persist between sessions.

### Simplified Implementation: Just Add Persistence (1-2 hours!)

```java
public class PersistentFileSchema extends FileSchema {
    private final MetadataStore metadataStore = new MetadataStore(".calcite/metadata");
    
    @Override
    public void add(String name, Table table) {
        super.add(name, table);  // Let Calcite do all the work
        
        // Just persist what Calcite created
        if (table instanceof ViewTableMacro) {
            metadataStore.saveView(name, extractSql(table));
        } else if (table instanceof MaterializedViewTable) {
            metadataStore.saveMaterializedView(name, extractSql(table));
        }
    }
    
    @Override
    protected void init() {
        super.init();
        // On startup, replay the DDL commands
        for (ViewDef view : metadataStore.loadViews()) {
            execute("CREATE VIEW " + view.name + " AS " + view.sql);
        }
        for (MVDef mv : metadataStore.loadMVs()) {
            execute("CREATE MATERIALIZED VIEW " + mv.name + " AS " + mv.sql);
        }
    }
}
```

### Engine Compatibility

**CREATE VIEW** - Works with ALL engines:
- Just stores SQL query definition
- Executes using whatever engine handles the underlying tables
- ✅ Works with: CSV, JSON, Parquet, DuckDB, Linq4j, Excel, etc.

**CREATE MATERIALIZED VIEW** - Limited to Parquet/DuckDB for storage:
- Source tables can be ANY format (CSV, JSON, etc.)
- Query executes using source table's engine
- Results stored as Parquet file
- MV queries served via Parquet or DuckDB engine
- This matches current `MaterializedViewTable` behavior

Example:
```sql
-- Source is CSV, but MV stored as Parquet
CREATE MATERIALIZED VIEW sales_summary AS 
SELECT region, SUM(amount) FROM csv_sales GROUP BY region;

-- View works with any engine
CREATE VIEW filtered_sales AS
SELECT * FROM csv_sales WHERE amount > 100;
```

### Updated ROI Assessment
- **Effort:** 1-2 hours (just add persistence!)
- **Impact:** Major usability improvement
- **ROI Score:** 9/10 (was 6.5/10 with full implementation)
- **Why:** Calcite does the hard work, we just save/restore

### The Key Insight
We don't need to implement DDL support - Calcite already has it! We just need to make it persistent. This changes a 1-2 week project into a 1-2 hour enhancement.
- Support view security/permissions?
- Allow cross-schema views?
- How to handle schema evolution (view becomes invalid)?