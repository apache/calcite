# Performance Tuning

This guide covers optimization strategies and performance tuning for the Apache Calcite File Adapter.

## Execution Engine Selection

### Choosing the Right Engine

Each execution engine is specialized for specific use cases. Choose based on your dataset size and workload characteristics:

| Dataset Size | Workload Type | Recommended Engine | Key Benefit |
|--------------|---------------|-------------------|-------------|
| > 2GB | General analytics | **Parquet** | File-based spillover, persistent cache |
| > 2GB | Complex SQL analytics | **DuckDB** | Native spillover + advanced SQL |
| < 2GB | High-performance analytics | **Arrow** | SIMD vectorization, zero-copy |
| < 100MB | Simple queries | **LINQ4J** | Low overhead, debugging |

### Parquet Engine (Default - Large Datasets)

Specialized for datasets that exceed available memory through automatic spillover.

**Best For:**
- **Large datasets (> 2GB)** that don't fit in memory
- **Mixed file formats** requiring standardization
- **Persistent caching** across JVM restarts
- **Production workloads** needing reliability

**Configuration:**
```json
{
  "executionEngine": "parquet",
  "parquetConfig": {
    "compression": "snappy",
    "pageSize": "1MB",
    "enableStatistics": true,
    "enableDictionary": true,
    "cacheDirectory": ".parquet_cache"
  }
}
```

**Performance Benefits:**
- Up to 1.6x faster than other engines
- Automatic columnar storage conversion
- Built-in compression and statistics
- Efficient column pruning and filter pushdown

### DuckDB Engine (Advanced Analytics with Native Spillover)

High-performance analytical engine with advanced SQL features, SIMD optimizations, and **native spillover support**.

**Best For:**
- **Large analytical workloads** that may exceed memory
- **Complex SQL queries** with window functions and CTEs
- **Join-heavy operations** on large datasets
- **Time-series analysis** with advanced aggregations
- **Production analytics** requiring both speed and scale

**Key Features:**
- **Native spillover to disk** when queries exceed memory_limit
- Built-in SIMD vectorization
- Advanced SQL optimizer
- Parallel execution

**Configuration:**
```json
{
  "executionEngine": "duckdb",
  "duckdbConfig": {
    "memory_limit": "4GB",        // When to trigger spillover
    "temp_directory": "/tmp/duckdb", // Where to spill to disk
    "threads": 8,
    "max_memory": "80%",           // Percentage of system memory
    "enable_progress_bar": false,
    "preserve_insertion_order": true
  }
}
```

**Advanced Vectorization Features:**
- **SIMD Processing**: Utilizes AVX/SSE instructions for parallel operations
- **Vectorized Execution**: Processes data in batches for better CPU cache utilization
- **Column-oriented Processing**: Optimized for analytical workloads
- **Advanced Join Algorithms**: Hash joins with SIMD acceleration
```

### Arrow Engine (In-Memory High Performance)

Specialized for in-memory columnar processing with advanced SIMD vectorization and ColumnBatch optimizations.

**Best For:**
- **Datasets that fit in memory (< 2GB)** 
- **Real-time analytics** requiring microsecond latency
- **SIMD-accelerated operations** (aggregations, filters)
- **Interactive dashboards** with fast response times
- **Development and testing** with immediate feedback

**Key Advantages:**
- Zero-copy data access
- Native SIMD vectorization
- No serialization overhead
- Optimal CPU cache utilization

**Configuration:**
```json
{
  "executionEngine": "arrow",
  "arrowConfig": {
    "batchSize": 10000,
    "enableVectorization": true,
    "memoryPool": "system",
    "columnBatch": {
      "enableAdaptive": true,
      "enableSIMD": true,
      "enableCompression": true,
      "adaptiveThreshold": 1000000,
      "compressionAlgorithm": "lz4"
    },
    "vectorization": {
      "enableSIMDOperations": true,
      "vectorSize": 2048,
      "enableParallelProcessing": true
    }
  }
}
```

**Advanced ColumnBatch Features:**
- **AdaptiveColumnBatch**: Automatically selects optimal processing strategy based on data characteristics
- **SIMDColumnBatch**: Hardware-accelerated operations using SIMD instructions
- **CompressedColumnBatch**: In-memory compression to reduce memory footprint
- **ArrowComputeColumnBatch**: Native Arrow compute operations for maximum performance
- **UniversalDataBatchAdapter**: Seamless conversion between different batch formats
```

### LINQ4J Engine (Simple Row Processing)

Traditional row-based processing engine for simple workloads.

**Best For:**
- **Small datasets (< 100MB)**
- **Simple SELECT queries** without complex analytics
- **Compatibility testing** with legacy systems
- **Debugging** with straightforward execution model

## Engine Selection Decision Matrix

### Quick Decision Guide

```
Dataset Size?
├─> Greater than 2GB or might grow?
│   ├─> Need complex SQL (window functions, CTEs)?
│   │   └─> Use DUCKDB (native spillover + advanced SQL)
│   └─> General analytics, need persistent cache?
│       └─> Use PARQUET (file-based spillover + cache)
├─> Less than 2GB, need maximum speed?
│   └─> Use ARROW (SIMD vectorization, zero-copy)
└─> Small files, simple queries?
    └─> Use LINQ4J (minimal overhead)
```

### Feature Comparison

| Feature | Parquet | Arrow | DuckDB | LINQ4J |
|---------|---------|-------|---------|---------|
| **Max Dataset Size** | Unlimited | ~2GB | Unlimited* | ~500MB |
| **Spillover Support** | ✅ Yes | ❌ No | ✅ Yes* | ❌ No |
| **SIMD Vectorization** | ❌ No | ✅ Yes | ✅ Yes | ❌ No |
| **Zero-Copy Access** | ❌ No | ✅ Yes | ❌ No | ❌ No |
| **Persistent Cache** | ✅ Yes | ❌ No | ❌ No | ❌ No |
| **Complex SQL** | ✅ Good | ✅ Good | ✅ Excellent | ⚠️ Basic |
| **Startup Time** | Medium | Fast | Medium | Fast |
| **Memory Efficiency** | ✅ Excellent | ⚠️ Good | ⚠️ Good | ❌ Poor |

*Note: DuckDB has native spillover built into its query engine. Configure with `temp_directory` and `memory_limit`.

### Performance Characteristics

| Metric | Parquet | Arrow | DuckDB | LINQ4J |
|--------|---------|-------|---------|---------|
| **Throughput (GB/s)** | 1.2 | 2.5 | 2.0 | 0.3 |
| **Latency (ms)** | 10-50 | 1-5 | 5-20 | 5-10 |
| **Memory Usage** | Low-Medium | High | Medium-High | Low |
| **CPU Utilization** | 60-80% | 90-100% | 80-95% | 30-50% |

### Spillover Mechanism Comparison

**Parquet Engine Spillover:**
- Converts all file formats to Parquet files in `.parquet_cache/`
- Spillover is file-based and persistent across restarts
- Managed by the File Adapter's ConcurrentSpilloverManager
- Best for: Mixed file formats, persistent caching needs

**DuckDB Engine Spillover:**
- Native spillover built into DuckDB's query execution engine
- Temporary files created in `temp_directory` during query execution
- Automatically managed by DuckDB based on `memory_limit`
- Best for: Complex SQL queries on large datasets

**Key Differences:**
- Parquet: File-level spillover (entire files cached/spilled)
- DuckDB: Operation-level spillover (intermediate query results spilled)
- Parquet cache persists; DuckDB spillover is query-scoped

## Memory Management

### Heap Size Configuration

Set appropriate JVM heap size based on dataset size:

```bash
# For large datasets (> 10GB)
-Xmx8g -Xms4g

# For medium datasets (1-10GB)  
-Xmx4g -Xms2g

# For small datasets (< 1GB)
-Xmx2g -Xms1g
```

### Spillover Configuration

Configure disk spillover for datasets larger than memory:

```json
{
  "memoryConfig": {
    "enableSpillover": true,
    "spilloverThreshold": "512MB",
    "spilloverDirectory": "/tmp/calcite-spill",
    "compressionEnabled": true,
    "maxSpillSize": "10GB"
  }
}
```

### Garbage Collection Tuning

Recommended GC settings for different workloads:

```bash
# For throughput-focused workloads
-XX:+UseG1GC -XX:MaxGCPauseMillis=200

# For latency-sensitive workloads  
-XX:+UseZGC -XX:UnlockExperimentalVMOptions

# For mixed workloads
-XX:+UseParallelGC -XX:ParallelGCThreads=8
```

## Caching Strategies

### Parquet Cache Configuration

Optimize the automatic Parquet conversion cache:

```json
{
  "cacheConfig": {
    "directory": "/fast-storage/.parquet_cache",
    "maxSize": "50GB",
    "ttl": "7 days",
    "compressionLevel": 6,
    "enablePruning": true,
    "pruningInterval": "1 hour"
  }
}
```

### Statistics Cache

Configure HyperLogLog statistics caching:

```json
{
  "statisticsConfig": {
    "enableCache": true,
    "cacheSize": "1GB",
    "refreshInterval": "24 hours",
    "persistToDisk": true,
    "diskCacheDirectory": ".stats_cache"
  }
}
```

### Distributed Caching with Redis

For multi-node deployments:

```json
{
  "distributedCache": {
    "type": "redis",
    "host": "redis-cluster.example.com",
    "port": 6379,
    "database": 0,
    "ttl": "24 hours",
    "keyPrefix": "calcite-file:",
    "enableCompression": true
  }
}
```

## Query Optimization

### Column Pruning

Automatically enabled - only requested columns are read:

```sql
-- Efficient: Only reads 'name' and 'email' columns
SELECT name, email FROM customers;

-- Less efficient: Reads all columns then selects
SELECT * FROM customers WHERE name = 'John';
```

### Filter Pushdown

Push filters to the storage level:

```json
{
  "optimizationRules": {
    "enableFilterPushdown": true,
    "supportedOperators": ["=", "!=", "<", ">", "<=", ">=", "IN", "LIKE"],
    "pushdownThreshold": 0.1
  }
}
```

### Join Optimization

Configure join reordering and optimization:

```json
{
  "joinConfig": {
    "enableReordering": true,
    "costBasedOptimization": true,
    "preferHashJoins": true,
    "broadcastThreshold": "10MB"
  }
}
```

### Statistics-Based Optimization

Enable HyperLogLog statistics for better query planning:

```json
{
  "statisticsConfig": {
    "enableHLL": true,
    "sampleSize": 10000,
    "accuracy": 0.05,
    "autoUpdate": true
  }
}
```

## File Organization

### Partitioning

Organize files for efficient partition pruning:

```
data/
├── year=2023/
│   ├── month=01/
│   │   ├── sales_01.parquet
│   │   └── sales_02.parquet
│   └── month=02/
│       └── sales_03.parquet
└── year=2024/
    └── month=01/
        └── sales_04.parquet
```

Configuration:
```json
{
  "partitioning": {
    "enabled": true,
    "partitionColumns": ["year", "month"],
    "partitionPattern": "year={year}/month={month}/*.parquet"
  }
}
```

Query optimization:
```sql
-- Efficient: Only scans January 2024 partition
SELECT * FROM sales 
WHERE year = 2024 AND month = 1;
```

### File Size Optimization

Optimal file sizes for different use cases:

| Use Case | Recommended Size | Reason |
|----------|------------------|---------|
| Analytical queries | 128MB - 512MB | Balance between parallelism and overhead |
| Transactional queries | 32MB - 128MB | Faster individual file access |
| Archive/cold storage | 512MB - 1GB | Minimize storage costs |
| Real-time ingestion | 8MB - 32MB | Lower latency for recent data |

### Compression

Choose appropriate compression based on workload:

```json
{
  "compressionConfig": {
    "algorithm": "snappy",  // Fast compression/decompression
    "level": 6,             // Balance size vs speed
    "enableDictionary": true // Better compression for repeated values
  }
}
```

Compression comparison:

| Algorithm | Compression Ratio | Speed | CPU Usage |
|-----------|------------------|--------|-----------|
| uncompressed | 1.0x | Fastest | Lowest |
| snappy | 2.0x | Fast | Low |
| lz4 | 2.5x | Fast | Low |
| gzip | 3.0x | Medium | Medium |
| brotli | 3.5x | Slow | High |

## Network Optimization

### Connection Pooling

For remote storage providers:

```json
{
  "connectionPool": {
    "maxConnections": 50,
    "maxConnectionsPerRoute": 10,
    "connectionTimeout": "30s",
    "socketTimeout": "60s",
    "keepAliveTimeout": "300s"
  }
}
```

### Parallel Processing

Configure concurrent file processing:

```json
{
  "parallelism": {
    "maxConcurrentFiles": 8,
    "maxConcurrentOperations": 16,
    "threadPoolSize": 32,
    "queueSize": 1000
  }
}
```

### Batch Operations

Optimize batch operations for cloud storage:

```json
{
  "batchConfig": {
    "batchSize": 100,
    "maxBatchWaitTime": "1s",
    "enableBatching": true,
    "preferBatchOperations": true
  }
}
```

## Monitoring and Profiling

### Performance Metrics

Enable detailed performance monitoring:

```json
{
  "monitoring": {
    "enableMetrics": true,
    "metricsInterval": "30s",
    "enableProfiling": true,
    "profileSampling": 0.1,
    "metricsExport": {
      "type": "prometheus",
      "endpoint": "/metrics",
      "port": 9090
    }
  }
}
```

### Key Metrics to Monitor

**Query Performance:**
- Query execution time
- Rows processed per second
- Data throughput (MB/s)
- Cache hit ratio

**Resource Utilization:**
- Memory usage and GC behavior
- CPU utilization
- Disk I/O rates
- Network bandwidth

**Storage Metrics:**
- File access patterns
- Cache effectiveness
- Storage latency
- Error rates

### Logging Configuration

Configure detailed logging for performance analysis:

```json
{
  "logging": {
    "level": "INFO",
    "enablePerformanceLogging": true,
    "slowQueryThreshold": "5s",
    "logQueries": true,
    "logFileAccess": false,
    "appenders": [
      {
        "type": "file",
        "file": "/logs/calcite-performance.log",
        "pattern": "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n"
      }
    ]
  }
}
```

## Benchmarking

### Performance Testing Framework

Use consistent benchmarking for optimization:

```sql
-- Test query performance
EXPLAIN PLAN FOR
SELECT region, SUM(sales) as total_sales
FROM sales_data 
WHERE year = 2024
GROUP BY region
ORDER BY total_sales DESC;

-- Measure execution time
SELECT 
  query_id,
  execution_time_ms,
  rows_processed,
  bytes_processed
FROM system.query_history
WHERE query_text LIKE '%sales_data%'
ORDER BY start_time DESC
LIMIT 10;
```

### Load Testing

Configure realistic load tests:

```json
{
  "loadTest": {
    "concurrentUsers": 10,
    "queriesPerUser": 100,
    "testDuration": "10 minutes",
    "queryMix": {
      "simple_select": 0.4,
      "aggregation": 0.3,
      "join": 0.2,
      "complex": 0.1
    }
  }
}
```

## Troubleshooting Performance Issues

### Common Performance Problems

**Slow Query Execution:**
1. Check if statistics are up-to-date
2. Verify filter pushdown is working
3. Examine join order and strategy
4. Monitor memory usage and GC behavior

**High Memory Usage:**
1. Increase heap size or enable spillover
2. Optimize batch sizes
3. Check for memory leaks in long-running processes
4. Consider using more memory-efficient engines

**Poor Cache Performance:**
1. Verify cache directory has fast storage
2. Check cache hit ratios
3. Adjust cache size and TTL settings
4. Monitor cache eviction patterns

### Performance Tuning Checklist

- [ ] Selected appropriate execution engine for workload
- [ ] Configured adequate memory allocation
- [ ] Enabled disk spillover for large datasets
- [ ] Optimized file organization and partitioning
- [ ] Configured appropriate compression settings
- [ ] Enabled statistics collection and caching
- [ ] Set up monitoring and alerting
- [ ] Conducted performance benchmarking
- [ ] Documented configuration and performance baselines

## Best Practices Summary

1. **Use Parquet engine** for most workloads
2. **Partition large datasets** by commonly filtered columns
3. **Size files appropriately** (128MB-512MB for analytics)
4. **Enable caching** for frequently accessed data
5. **Monitor key metrics** continuously
6. **Test configuration changes** with realistic workloads
7. **Document performance baselines** for comparison
8. **Scale storage and compute** independently as needed