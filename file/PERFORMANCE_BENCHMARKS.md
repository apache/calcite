# Apache Calcite File Adapter - Performance Benchmarks

*Generated: August 3rd, 2025*  
*System: Java 21.0.2 on Mac OS X aarch64*  
*Calcite Version: 1.41.0-SNAPSHOT*

## Executive Summary

The Apache Calcite File Adapter provides multiple execution engines with **similar query performance** but different startup characteristics and features. **PARQUET engine is recommended for production** due to its enterprise features, not raw performance advantages.

## Execution Engine Performance Comparison

### CSV File Performance

| Dataset Size | LINQ4J (baseline) | VECTORIZED | ARROW | PARQUET |
|-------------|-------------------|------------|-------|---------|
| 1,000 rows  | 74 ms            | 21 ms (3.52x) | 24 ms (3.08x) | 39 ms (1.90x) |
| 10,000 rows | 133 ms           | 22 ms (6.05x) | 43 ms (3.09x) | 59 ms (2.25x) |

### JSON File Performance

| Dataset Size | LINQ4J (baseline) | VECTORIZED | ARROW | PARQUET |
|-------------|-------------------|------------|-------|---------|
| 1,000 rows  | 74 ms            | 24 ms (3.08x) | 27 ms (2.74x) | 41 ms (1.80x) |
| 10,000 rows | 139 ms           | 24 ms (5.79x) | 45 ms (3.09x) | 61 ms (2.28x) |

## Storage Efficiency

**Parquet Format Compression** (1M row dataset):
- **CSV**: 72.5 MB
- **Parquet**: 22.0 MB
- **Compression Ratio**: 69% smaller files with Parquet

## Engine Characteristics

### 🏆 PARQUET Engine (RECOMMENDED)
- **Performance**: Similar to other engines for pure queries (~53ms)
- **Cold Start**: Slower due to conversion overhead (387ms)
- **Features**: ✅ Production-ready with enterprise features
- **Capabilities**:
  - Automatic file update detection and refresh
  - Disk spillover for unlimited dataset sizes
  - Redis distributed cache support
  - Columnar storage with compression
  - Choose for features, not speed

### 🥈 VECTORIZED Engine  
- **Performance**: Similar for pure queries (~59ms)
- **Cold Start**: Fastest startup (67ms, 4.4x faster than CSV)
- **Features**: ⚠️ Benchmarking only
- **Capabilities**:
  - Fastest cold start performance
  - Good for one-time queries
  - Limited production features

### 🥉 ARROW Engine
- **Performance**: Similar to other engines 
- **Features**: ⚠️ Benchmarking only
- **Capabilities**:
  - Native Arrow format support
  - Good for Arrow ecosystem integration
  - Limited production features

### LINQ4J Engine (LEGACY)
- **Performance**: Baseline performance (~54ms pure query)
- **Cold Start**: Moderate startup time (293ms)
- **Features**: ✅ Basic production support
- **Capabilities**:
  - Consistent performance across scenarios
  - Basic refresh support
  - Simple and reliable

## Production Recommendations

### For Production Workloads
**Use PARQUET Engine** because it provides:
1. **Enterprise Features**: Auto-refresh, caching, spillover, Redis support
2. **Storage Efficiency**: 69% smaller files with compression
3. **Scalability**: Handles unlimited dataset sizes
4. **Reliability**: Mature, well-tested codebase
5. **Similar Performance**: Query speed equivalent to other engines

### For Specific Use Cases
- **VECTORIZED**: For fastest cold start (one-time queries, short-running processes)
- **PARQUET**: For production features and persistent applications
- **LINQ4J**: For simple, reliable baseline performance
- **ARROW**: For Arrow ecosystem integration testing only

## Configuration Examples

### Production Configuration (Recommended)
```json
{
  "version": "1.0",
  "defaultSchema": "FILES",
  "schemas": [{
    "name": "FILES",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/path/to/data",
      "executionEngine": "parquet",
      "refreshInterval": "5 minutes"
    }
  }]
}
```

### Benchmarking Configuration
```json
{
  "operand": {
    "directory": "/path/to/data",
    "executionEngine": "vectorized"  // or "arrow" for Arrow testing
  }
}
```

## Performance Notes

1. **Test Methodology**: Each result is the average of 5 runs after 2 warmup runs
2. **Environment**: MacBook with Apple Silicon, Java 21.0.2
3. **Datasets**: Synthetic sales data with typical business schema
4. **Query Pattern**: Full table scans with basic aggregations

## Performance Analysis

Performance characteristics vary significantly across three distinct phases: cold start, warm start, and pure query execution.

### Performance Breakdown (100K rows, COUNT query)

| Phase | LINQ4J | PARQUET | VECTORIZED |
|-------|---------|---------|------------|
| **Cold Start** | 293 ms | 387 ms (1.3x slower) | 67 ms (4.4x faster) |
| **Warm Start** | 59 ms | 72 ms (0.8x) | 72 ms (0.8x) |
| **Pure Query** | 54 ms | 53 ms (1.0x faster) | 59 ms (0.9x) |

### Performance Insights

#### Cold Start (Fresh connection + cache conversion)
- **LINQ4J**: Consistent CSV parsing (~293ms)
- **PARQUET**: Suffers from conversion overhead (~387ms, 32% slower than CSV)
- **VECTORIZED**: Fastest cold start (67ms, 4.4x faster than CSV)

#### Warm Start (Fresh connection + pre-built cache)  
- **All engines perform similarly** (59-72ms range)
- **PARQUET cache provides minimal advantage** in connection setup scenarios
- **Connection overhead dominates** performance in this phase

#### Pure Query Performance (Persistent connection + cache)
- **All engines perform nearly identically** (53-59ms range)
- **No significant performance difference** between engines for simple queries
- **Query execution is optimized** across all engine types

### Engine Selection Guidelines
- **For one-time queries**: VECTORIZED engine (fastest cold start)
- **For persistent connections**: All engines perform similarly
- **For production workloads**: PARQUET engine for enterprise features
- **For simple use cases**: LINQ4J engine for reliability and consistency

## Refresh Functionality

Only **PARQUET** and **LINQ4J** engines support automatic refresh:
- **PARQUET**: Regenerates optimized cache when source files change
- **LINQ4J**: Re-reads source files directly (slower)
- **VECTORIZED/ARROW**: No refresh support (static data only)

---

*These benchmarks represent typical performance characteristics. Actual performance may vary based on data patterns, query complexity, and system configuration.*