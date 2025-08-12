# Apache Calcite File Adapter - Performance Test Results

**Generated:** Mon Aug 11 16:18:46 EDT 2025
**Test Configuration:**
- Each engine runs in isolated JVM
- PreparedStatements used (execution time only, no planning)
- Minimum time from 10 runs after 3 warmup runs

**Engines tested:** linq4j, parquet, parquet+hll, parquet+vec, parquet+all, arrow, vectorized
**Data sizes:** [500000]

## Complex Join

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 500K | 303 ms | 307 ms | 298 ms | 297 ms | 317 ms | 299 ms | **290 ms** |

**Best performers:**
- 500K: VECTORIZED (290 ms)

## Count Distinct Multiple

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 500K | 213 ms | 210 ms | **0 ms** | 248 ms | **0 ms** | 219 ms | 228 ms |

**Best performers:**
- 500K: PARQUET + HLL (0 ms)

## Count Distinct Single

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 500K | 171 ms | 250 ms | **0 ms** | 165 ms | **0 ms** | 163 ms | 159 ms |

**Best performers:**
- 500K: PARQUET + HLL (0 ms)

## Filtered Aggregation

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 500K | 150 ms | **148 ms** | 170 ms (1.1x slower) | 152 ms | 171 ms (1.2x slower) | 150 ms | 152 ms |

**Best performers:**
- 500K: PARQUET (148 ms)

## Group By Count Distinct

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 500K | **294 ms** | **294 ms** | 305 ms | 296 ms | 326 ms (1.1x slower) | 308 ms | 310 ms |

**Best performers:**
- 500K: LINQ4J (294 ms)

## Simple Aggregation

| Rows | LINQ4J | PARQUET | PARQUET + HLL | PARQUET + VEC | PARQUET + ALL | ARROW | VECTORIZED |
|------|--------|--------|--------|--------|--------|--------|--------|
| 500K | 252 ms (1.7x slower) | 153 ms | 156 ms | 181 ms (1.2x slower) | 187 ms (1.2x slower) | **151 ms** | 171 ms (1.1x slower) |

**Best performers:**
- 500K: ARROW (151 ms)

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
