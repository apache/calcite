# Apache Calcite File Adapter - Performance Test Results

**Generated:** Mon Aug 11 13:55:16 EDT 2025
**Test Configuration:**
- Each engine runs in isolated JVM
- PreparedStatements used (execution time only, no planning)
- Minimum time from 10 runs after 3 warmup runs

**Engines tested:** linq4j, parquet, parquet+hll, parquet+vec, parquet+all, arrow, vectorized
**Data sizes:** [1000]

## Complex Join

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 1K | 14 ms | 14 ms | 14 ms | 18 ms (1.4x slower) | 14 ms | 14 ms | **13 ms** |

**Best performers:**
- 1K: VECTORIZED (13 ms)

## Count Distinct Multiple

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 1K | **8 ms** | **8 ms** | **8 ms** | **8 ms** | 10 ms (1.3x slower) | **8 ms** | **8 ms** |

**Best performers:**
- 1K: LINQ4J (8 ms)

## Count Distinct Single

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 1K | **7 ms** | **7 ms** | 8 ms (1.1x slower) | **7 ms** | 8 ms (1.1x slower) | 9 ms (1.3x slower) | 8 ms (1.1x slower) |

**Best performers:**
- 1K: LINQ4J (7 ms)

## Filtered Aggregation

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 1K | **7 ms** | 8 ms (1.1x slower) | 8 ms (1.1x slower) | 8 ms (1.1x slower) | 8 ms (1.1x slower) | 10 ms (1.4x slower) | 8 ms (1.1x slower) |

**Best performers:**
- 1K: LINQ4J (7 ms)

## Group By Count Distinct

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 1K | **8 ms** | **8 ms** | **8 ms** | 9 ms (1.1x slower) | **8 ms** | **8 ms** | **8 ms** |

**Best performers:**
- 1K: LINQ4J (8 ms)

## Simple Aggregation

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 1K | 10 ms (1.3x slower) | 9 ms (1.1x slower) | **8 ms** | **8 ms** | **8 ms** | **8 ms** | **8 ms** |

**Best performers:**
- 1K: PARQUET + HLL (8 ms)

## Summary

❌ **HLL optimization is NOT working** - COUNT(DISTINCT) queries are scanning data instead of using pre-computed sketches

**Key Findings:**
- Each engine ran in complete isolation (separate JVM)
- File caches were cleared between each test
- Results show execution time only (planning time excluded via PreparedStatement)
- **CRITICAL: HLL should provide < 1ms performance for COUNT(DISTINCT) but is not working**

**Engine Descriptions:**
- **LINQ4J**: Default row-by-row processing engine
- **PARQUET**: Native Parquet reader with basic optimizations
- **PARQUET+HLL**: Parquet with HyperLogLog sketches for COUNT(DISTINCT)
- **PARQUET+VEC**: Parquet with vectorized/columnar batch reading
- **PARQUET+ALL**: All optimizations combined
- **ARROW**: Apache Arrow columnar format
- **VECTORIZED**: Legacy vectorized engine
