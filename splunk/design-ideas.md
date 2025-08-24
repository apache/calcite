# Design Ideas for Splunk Adapter

## Parallelization Framework for Query Performance Optimization

**Problem**: The Splunk adapter processes search results sequentially, which creates bottlenecks for large result sets that users query repeatedly throughout the day. While schema discovery is slow, it only happens once at startup. The real performance pain is in query execution - parsing results, type conversion, and row processing all happen single-threaded, impacting every single query response.

**Proposed Solution**: Focus parallelization efforts on query-time operations that happen constantly, providing 2-5x performance improvements for search result processing, with secondary optimization for one-time startup operations.

### Critical Priority: Query-Time Parallelization

#### 1. Search Result Processing (Happens on EVERY Query) 🔥🔥🔥
**Current State**: Single-threaded CSV/JSON parsing in `SplunkConnectionImpl.java`
```java
// Current: Sequential result processing - happens on EVERY query!
BufferedReader reader = new BufferedReader(new InputStreamReader(is));
String line;
while ((line = reader.readLine()) != null) {
    Object[] row = parseLine(line);  // Parse CSV/JSON
    row = convertTypes(row);          // Type conversion
    listener.processRow(row);         // Row processing
}
```

**This is the #1 performance bottleneck** - every query suffers from this sequential processing!

**Parallel Implementation with Stream Processing**:
```java
public class ParallelSplunkResultProcessor {
    private static final int BATCH_SIZE = 500;  // Smaller batches for lower latency
    private static final int PROCESSOR_THREADS = 4;
    
    public void processQueryResults(InputStream is, SearchResultListener listener, 
                                   RelDataType schema) {
        // Use ring buffer for low-latency processing
        Disruptor<ResultEvent> disruptor = new Disruptor<>(
            ResultEvent::new, 
            1024,  // Ring buffer size
            DaemonThreadFactory.INSTANCE);
        
        // Parallel processing pipeline
        disruptor.handleEventsWithWorkerPool(
            createWorkers(PROCESSOR_THREADS, schema, listener));
        
        RingBuffer<ResultEvent> ringBuffer = disruptor.start();
        
        // Producer - reads and publishes to ring buffer
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                long sequence = ringBuffer.next();
                ResultEvent event = ringBuffer.get(sequence);
                event.setLine(line);
                ringBuffer.publish(sequence);
            }
        } finally {
            disruptor.shutdown();
        }
    }
    
    private WorkHandler<ResultEvent>[] createWorkers(int count, RelDataType schema, 
                                                     SearchResultListener listener) {
        WorkHandler<ResultEvent>[] workers = new WorkHandler[count];
        for (int i = 0; i < count; i++) {
            workers[i] = event -> {
                // Parse line
                Object[] row = parseLine(event.getLine());
                
                // Type conversion based on schema
                if (schema != null) {
                    row = convertTypes(row, schema);
                }
                
                // Process row
                listener.processRow(row);
            };
        }
        return workers;
    }
}
```

**Alternative: Streaming with CompletableFuture for Lower Overhead**:
```java
public class StreamingResultProcessor {
    private final ForkJoinPool customThreadPool = new ForkJoinPool(4);
    
    public void processResults(InputStream is, SearchResultListener listener, 
                              RelDataType schema) {
        try (Stream<String> lines = new BufferedReader(
                new InputStreamReader(is)).lines()) {
            
            // Process in parallel while maintaining some order
            lines.parallel()
                 .map(line -> CompletableFuture.supplyAsync(() -> {
                     Object[] row = parseLine(line);
                     if (schema != null) {
                         row = convertTypes(row, schema);
                     }
                     return row;
                 }, customThreadPool))
                 .map(CompletableFuture::join)
                 .forEach(listener::processRow);
        }
    }
}
```

**Expected Speedup**: 
- **Small queries (< 1000 rows)**: 1.5-2x faster
- **Medium queries (1K-10K rows)**: 2-3x faster  
- **Large queries (> 10K rows)**: 3-4x faster
- **Impact**: EVERY query benefits, thousands of times per day!

**Risk**: Low-Medium (need order preservation for some queries)
**Memory Impact**: Controlled by ring buffer/batch size

#### 2. Type Conversion Pipeline (Also EVERY Query)
**Current State**: Sequential type conversion in `SimpleTypeConverter`
```java
// Current: Convert each field sequentially
for (int i = 0; i < row.length; i++) {
    Object value = row[i];
    SqlTypeName targetType = schema.getFieldList().get(i).getType();
    row[i] = convertValue(value, targetType);  // String->Integer, String->Timestamp, etc.
}
```

**Parallel Implementation with SIMD-style Processing**:
```java
public class VectorizedTypeConverter {
    // Process multiple rows at once for better CPU cache utilization
    public Object[][] convertBatch(Object[][] rows, RelDataType schema) {
        int numRows = rows.length;
        int numCols = schema.getFieldCount();
        
        // Parallel column-wise conversion (better for cache locality)
        IntStream.range(0, numCols).parallel().forEach(col -> {
            SqlTypeName targetType = schema.getFieldList().get(col).getType();
            for (int row = 0; row < numRows; row++) {
                rows[row][col] = convertValue(rows[row][col], targetType);
            }
        });
        
        return rows;
    }
}
```

**Expected Speedup**: 2-3x for wide tables (many columns)
**Impact**: Every query with type conversion benefits

### One-Time Startup Optimizations (Lower Priority)

While these operations are slow, they only happen once at startup, not on every query:

#### 3. Data Model Discovery (One-time at Startup)
**Current State**: Sequential fetching and processing of data models
```java
// Happens once at startup
for (DataModelInfo model : models) {
    Table table = createTableForDataModel(typeFactory, model);
    tables.put(tableName, table);
}
```

**Parallel Implementation**:
```java
// Nice to have, but only saves time once at startup
Map<String, Table> tables = models.parallelStream()
    .collect(Collectors.toConcurrentMap(
        model -> normalizeTableName(model.name),
        model -> createTableForDataModel(typeFactory, model)
    ));
```

**Expected Speedup**: 3-5x for initial startup
**Risk**: Low
**Priority**: LOW - only happens once

#### 4. Multiple App Context Discovery (One-time at Startup)
**Current State**: Sequential discovery across multiple app contexts

**Parallel Implementation**:
```java
public class ParallelAppDiscovery {
    private final ExecutorService executorService = 
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    
    public Map<String, Table> discoverAcrossApps(String[] appContexts, 
                                                  RelDataTypeFactory typeFactory) {
        List<CompletableFuture<Map<String, Table>>> futures = 
            Arrays.stream(appContexts)
                .map(app -> CompletableFuture.supplyAsync(() -> {
                    try {
                        DataModelDiscovery discovery = new DataModelDiscovery(
                            createConnectionForApp(app), app, cacheTtl);
                        return discovery.discoverDataModels(typeFactory, modelFilter);
                    } catch (Exception e) {
                        LOGGER.error("Failed to discover models in app '{}': {}", 
                            app, e.getMessage());
                        return new HashMap<>();
                    }
                }, executorService))
                .collect(Collectors.toList());
        
        // Merge results with duplicate handling
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (v1, v2) -> {
                        LOGGER.warn("Duplicate table name '{}' across apps", 
                            ((Table) v1).getTableName());
                        return v1; // Keep first occurrence
                    },
                    LinkedHashMap::new)))
            .join();
    }
}
```

**Expected Speedup**: Linear with number of apps (up to connection pool size)
**Risk**: Medium - requires connection pooling
**Prerequisites**: Connection pool implementation

#### 3. Metadata Schema Population
**Current State**: Sequential iteration through schemas and tables for metadata
```java
// Current in SplunkInformationSchema.java
for (String schemaName : rootSchema.subSchemas().getNames()) {
    Schema subSchema = rootSchema.getSubSchema(schemaName);
    for (String tableName : subSchema.tables().getNames()) {
        // Process each table sequentially
        rows.add(buildTableRow(schemaName, tableName));
    }
}
```

**Parallel Implementation**:
```java
// Parallel metadata collection with thread-safe row addition
List<Object[]> rows = Collections.synchronizedList(new ArrayList<>());

rootSchema.subSchemas().getNames().parallelStream()
    .flatMap(schemaName -> {
        Schema subSchema = rootSchema.getSubSchema(schemaName);
        return subSchema.tables().getNames().stream()
            .map(tableName -> buildTableMetadata(schemaName, tableName, subSchema));
    })
    .forEach(rows::add);

// Alternative with better ordering preservation
ConcurrentSkipListMap<String, List<Object[]>> orderedResults = 
    new ConcurrentSkipListMap<>();

rootSchema.subSchemas().getNames().parallelStream()
    .forEach(schemaName -> {
        Schema subSchema = rootSchema.getSubSchema(schemaName);
        List<Object[]> schemaRows = subSchema.tables().getNames().stream()
            .map(tableName -> buildTableMetadata(schemaName, tableName, subSchema))
            .collect(Collectors.toList());
        orderedResults.put(schemaName, schemaRows);
    });

// Flatten in order
List<Object[]> rows = orderedResults.values().stream()
    .flatMap(List::stream)
    .collect(Collectors.toList());
```

**Expected Speedup**: 2-4x for large catalogs
**Risk**: Low - read-only operations
**Memory Impact**: Minimal - metadata is small

### Medium-Impact Opportunities

#### 4. Field Discovery for Multiple Tables
**Current State**: Tables discover their fields one at a time via REST API
```java
// Current: Sequential field discovery
for (Table table : tables) {
    Map<String, RelDataType> fields = discoverFields(table);
    table.setFields(fields);
}
```

**Parallel Implementation with Rate Limiting**:
```java
public class RateLimitedFieldDiscovery {
    private final RateLimiter rateLimiter = RateLimiter.create(10.0); // 10 requests/sec
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    
    public void discoverFieldsParallel(List<Table> tables) {
        List<CompletableFuture<Void>> futures = tables.stream()
            .map(table -> CompletableFuture.runAsync(() -> {
                rateLimiter.acquire(); // Rate limit API calls
                try {
                    Map<String, RelDataType> fields = discoverFields(table);
                    table.setFields(fields);
                } catch (Exception e) {
                    LOGGER.error("Failed to discover fields for table '{}': {}", 
                        table.getName(), e.getMessage());
                }
            }, executor))
            .collect(Collectors.toList());
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }
}
```

**Expected Speedup**: 3x for initial schema loading
**Risk**: Medium - Splunk API rate limiting
**Mitigation**: Configurable rate limiter

#### 5. Search Result Processing
**Current State**: Single-threaded CSV result parsing
```java
// Current: Sequential CSV parsing in SplunkConnectionImpl
BufferedReader reader = new BufferedReader(new InputStreamReader(is));
String line;
while ((line = reader.readLine()) != null) {
    Object[] row = parseLine(line);
    listener.processRow(row);
}
```

**Parallel Implementation with Batching**:
```java
public class ParallelResultProcessor {
    private static final int BATCH_SIZE = 1000;
    private static final int PROCESSOR_THREADS = 4;
    
    public void processResults(InputStream is, SearchResultListener listener) {
        BlockingQueue<List<String>> batchQueue = 
            new LinkedBlockingQueue<>(PROCESSOR_THREADS * 2);
        ExecutorService processors = Executors.newFixedThreadPool(PROCESSOR_THREADS);
        AtomicBoolean producerDone = new AtomicBoolean(false);
        
        // Producer thread - reads lines into batches
        CompletableFuture<Void> producer = CompletableFuture.runAsync(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8))) {
                List<String> batch = new ArrayList<>(BATCH_SIZE);
                String line;
                while ((line = reader.readLine()) != null) {
                    batch.add(line);
                    if (batch.size() >= BATCH_SIZE) {
                        batchQueue.offer(new ArrayList<>(batch));
                        batch.clear();
                    }
                }
                // Don't forget the last partial batch
                if (!batch.isEmpty()) {
                    batchQueue.offer(batch);
                }
            } catch (IOException e) {
                LOGGER.error("Error reading search results", e);
            } finally {
                producerDone.set(true);
            }
        });
        
        // Consumer threads - parse batches in parallel
        List<CompletableFuture<Void>> consumers = IntStream.range(0, PROCESSOR_THREADS)
            .mapToObj(i -> CompletableFuture.runAsync(() -> {
                while (!producerDone.get() || !batchQueue.isEmpty()) {
                    List<String> batch = batchQueue.poll();
                    if (batch != null) {
                        batch.stream()
                            .map(this::parseLine)
                            .forEach(listener::processRow);
                    } else if (!producerDone.get()) {
                        // Producer still working, wait a bit
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }, processors))
            .collect(Collectors.toList());
        
        // Wait for completion
        producer.join();
        CompletableFuture.allOf(consumers.toArray(new CompletableFuture[0])).join();
        processors.shutdown();
    }
}
```

**Expected Speedup**: 2-3x for large result sets
**Risk**: Medium - order preservation may be needed
**Memory Impact**: Controlled by batch size and queue limits

### Quick Win Opportunities

#### 6. Custom Table Configuration Processing
**Current State**: Sequential processing of custom table definitions
```java
// Current in CustomTableConfigProcessor.java
for (String tableName : tableNames) {
    Map<String, Object> config = loadTableConfig(tableName);
    Table table = createTable(config);
    tables.put(tableName, table);
}
```

**Parallel Implementation**:
```java
// Simple parallel processing for independent table configs
Map<String, Table> tables = tableNames.parallelStream()
    .collect(Collectors.toConcurrentMap(
        tableName -> tableName,
        tableName -> {
            try {
                Map<String, Object> config = loadTableConfig(tableName);
                return createTable(config);
            } catch (Exception e) {
                LOGGER.error("Failed to create table '{}': {}", 
                    tableName, e.getMessage());
                return null;
            }
        }
    ))
    .entrySet().stream()
    .filter(e -> e.getValue() != null)
    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
```

**Expected Speedup**: 2x for many custom tables
**Risk**: Very low - completely independent operations

#### 7. Cache Warming
**Current State**: No pre-warming of data model cache
**Parallel Implementation**:
```java
public class SplunkCacheWarmer {
    private final ScheduledExecutorService scheduler = 
        Executors.newScheduledThreadPool(2);
    private final Set<String> frequentlyUsedModels = ConcurrentHashMap.newKeySet();
    
    public void startWarmingCycle(SplunkConnection connection, long intervalMinutes) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                warmCache(connection);
            } catch (Exception e) {
                LOGGER.error("Cache warming failed", e);
            }
        }, 0, intervalMinutes, TimeUnit.MINUTES);
    }
    
    private void warmCache(SplunkConnection connection) {
        frequentlyUsedModels.parallelStream()
            .forEach(model -> {
                try {
                    DataModelDiscovery discovery = new DataModelDiscovery(
                        connection, model, -1); // Permanent cache
                    discovery.discoverDataModels(typeFactory, null, true);
                    LOGGER.debug("Warmed cache for model: {}", model);
                } catch (Exception e) {
                    LOGGER.warn("Failed to warm cache for model '{}': {}", 
                        model, e.getMessage());
                }
            });
    }
    
    public void trackModelUsage(String modelName) {
        frequentlyUsedModels.add(modelName);
        // Keep only top N models
        if (frequentlyUsedModels.size() > 20) {
            // Implement LRU eviction
        }
    }
}
```

**Expected Impact**: Eliminates first-query latency
**Risk**: Low - background process

### Implementation Architecture

#### Connection Pool Management
```java
public class SplunkConnectionPool {
    private final Queue<SplunkConnection> available = new ConcurrentLinkedQueue<>();
    private final Set<SplunkConnection> inUse = ConcurrentHashMap.newKeySet();
    private final Semaphore semaphore;
    private final ConnectionConfig config;
    private final int maxSize;
    
    public SplunkConnectionPool(ConnectionConfig config, int size) {
        this.config = config;
        this.maxSize = size;
        this.semaphore = new Semaphore(size);
        
        // Pre-create connections
        for (int i = 0; i < size; i++) {
            available.offer(createNewConnection());
        }
    }
    
    public SplunkConnection acquire(long timeout, TimeUnit unit) 
            throws InterruptedException, TimeoutException {
        if (!semaphore.tryAcquire(timeout, unit)) {
            throw new TimeoutException("Could not acquire connection within timeout");
        }
        
        SplunkConnection conn = available.poll();
        if (conn == null || !conn.isValid()) {
            conn = createNewConnection();
        }
        
        inUse.add(conn);
        return conn;
    }
    
    public void release(SplunkConnection conn) {
        if (inUse.remove(conn)) {
            if (conn.isValid()) {
                available.offer(conn);
            }
            semaphore.release();
        }
    }
    
    private SplunkConnection createNewConnection() {
        return new SplunkConnectionImpl(
            config.getUrl(),
            config.getUsername(),
            config.getPassword(),
            config.getApp()
        );
    }
}
```

#### Rate Limiting for API Calls
```java
public class SplunkApiRateLimiter {
    private final RateLimiter rateLimiter;
    private final ExecutorService executor;
    private final int maxConcurrent;
    private final Semaphore concurrentLimit;
    
    public SplunkApiRateLimiter(double requestsPerSecond, int maxConcurrent) {
        this.rateLimiter = RateLimiter.create(requestsPerSecond);
        this.maxConcurrent = maxConcurrent;
        this.concurrentLimit = new Semaphore(maxConcurrent);
        this.executor = Executors.newFixedThreadPool(maxConcurrent);
    }
    
    public <T> CompletableFuture<T> submit(Callable<T> task) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                concurrentLimit.acquire();
                rateLimiter.acquire();
                return task.call();
            } catch (Exception e) {
                throw new CompletionException(e);
            } finally {
                concurrentLimit.release();
            }
        }, executor);
    }
}
```

### Configuration Schema

```json
{
  "splunk": {
    "parallelism": {
      "enabled": true,
      "discoveryThreads": 4,
      "metadataThreads": 4,
      "resultProcessorThreads": 4,
      "connectionPoolSize": 10,
      "apiRateLimit": 10.0,
      "maxConcurrentApiCalls": 5,
      "resultBatchSize": 1000,
      "cacheWarmingEnabled": true,
      "cacheWarmingIntervalMinutes": 30
    }
  }
}
```

### Performance Expectations

| Operation | Current Time | Parallel Time | Speedup | Threads |
|-----------|-------------|---------------|---------|---------|
| Discover 20 data models | 10s | 2-3s | 3-5x | 4 |
| Multi-app discovery (3 apps) | 15s | 5s | 3x | 3 |
| Metadata schema load (100 tables) | 5s | 1-2s | 2.5-5x | 4 |
| Large result processing (100K rows) | 20s | 7-10s | 2-3x | 4 |
| Custom table config (10 tables) | 2s | 1s | 2x | 4 |
| Field discovery (50 tables) | 25s | 8s | 3x | 4 |

### Risk Mitigation

**API Rate Limiting**:
- Configurable rate limiter (requests per second)
- Exponential backoff on 429 responses
- Circuit breaker pattern for API failures

**Session Management**:
- Prefer token authentication for parallel requests
- Connection pool with session validation
- Automatic re-authentication on 401 errors

**Memory Management**:
- Bounded queues for result processing
- Configurable batch sizes
- Memory-aware thread pool sizing

**Error Handling**:
- Graceful degradation to sequential on failures
- Per-operation timeout configuration
- Comprehensive error logging with context

### Implementation Phases

**Phase 1 - Foundation** (1 week):
- Connection pool implementation
- Rate limiter setup
- Basic thread pool configuration

**Phase 2 - Discovery Parallelization** (1 week):
- Parallel data model discovery
- Multi-app context support
- Custom table parallel processing

**Phase 3 - Metadata & Results** (1 week):
- Parallel metadata schema population
- Batched result processing
- Field discovery parallelization

**Phase 4 - Optimization** (3 days):
- Cache warming implementation
- Performance tuning
- Monitoring and metrics

### Benefits

1. **Dramatic Startup Improvement**: 3-5x faster schema initialization
2. **Better Multi-App Support**: Linear scaling with app count
3. **Improved Query Response**: 2-3x faster result processing
4. **Enterprise Readiness**: Handles large Splunk deployments efficiently
5. **BI Tool Compatibility**: Faster metadata queries improve tool integration

### Compatibility Notes

- Backward compatible - parallelism can be disabled via configuration
- Token authentication recommended for best parallel performance
- Respects Splunk API limits through configurable rate limiting
- No changes to public APIs or JDBC interface

### Splunk-Specific Considerations

1. **API Limits**: Default to 10 requests/second, configurable
2. **Session Types**: Token auth scales better than session cookies
3. **App Isolation**: Each app context may need separate connection
4. **Result Ordering**: Some queries may require order preservation
5. **Memory Usage**: Large parallel searches need memory limits

This parallelization framework would significantly improve the Splunk adapter's performance, especially for enterprise deployments with multiple apps and many data models. The improvements are most noticeable during initial connection and schema discovery, which currently can take 10-30 seconds in large environments.