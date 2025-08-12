# Apache Calcite File Adapter - Performance Test Results

**Generated:** Mon Aug 11 16:02:30 EDT 2025
**Test Configuration:**
- Each engine runs in isolated JVM
- PreparedStatements used (execution time only, no planning)
- Minimum time from 10 runs after 3 warmup runs

**Engines tested:** linq4j, parquet, parquet+hll, parquet+vec, parquet+all, arrow, vectorized
**Data sizes:** [50000]

## Complex Join

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 50K | 40 ms | 42 ms (1.1x slower) | 39 ms | **38 ms** | **38 ms** | 44 ms (1.2x slower) | 40 ms |

**Best performers:**
- 50K: PARQUET + VEC (38 ms)

## Count Distinct Multiple

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 50K | 28 ms | 27 ms | **0 ms** | 29 ms | **0 ms** | 28 ms | 40 ms |

**Best performers:**
- 50K: PARQUET + HLL (0 ms)

## Count Distinct Single

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 50K | 24 ms | 23 ms | **0 ms** | 24 ms | **0 ms** | 23 ms | 24 ms |

**Best performers:**
- 50K: PARQUET + HLL (0 ms)

## Filtered Aggregation

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 50K | 24 ms | 23 ms | 23 ms | 23 ms | **22 ms** | 23 ms | **22 ms** |

**Best performers:**
- 50K: PARQUET + ALL (22 ms)

## Group By Count Distinct

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 50K | 31 ms (1.1x slower) | 34 ms (1.2x slower) | 32 ms (1.1x slower) | 32 ms (1.1x slower) | 32 ms (1.1x slower) | 32 ms (1.1x slower) | **28 ms** |

**Best performers:**
- 50K: VECTORIZED (28 ms)

## Simple Aggregation

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 50K | 22 ms | 27 ms (1.3x slower) | **21 ms** | 24 ms (1.1x slower) | **21 ms** | 22 ms | 24 ms (1.1x slower) |

**Best performers:**
- 50K: PARQUET + HLL (21 ms)

## Summary

✅ **HLL optimization is working** - sub-millisecond COUNT(DISTINCT) performance achieved

**Key Findings:**
- Each engine ran in complete isolation (separate JVM)
- File caches were cleared between each test
- Results show execution time only (planning time excluded via PreparedStatement)

**Engine Descriptions:**
- **LINQ4J**: Default row-by-row processing engine
- **PARQUET**: Native Parquet reader with basic optimizations
- **PARQUET+HLL**: Parquet with HyperLogLog sketches for COUNT(DISTINCT)
- **PARQUET+VEC**: Parquet with vectorized/columnar batch reading
- **PARQUET+ALL**: All optimizations combined
- **ARROW**: Apache Arrow columnar format
- **VECTORIZED**: Legacy vectorized engine
