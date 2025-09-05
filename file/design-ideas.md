# Design Ideas for File Adapter (Prioritized by ROI)

## JSON Schema Companion Files for Type Fidelity

### Problem Statement
JSON files lack type information, leading to:
- Type inference errors (e.g., "1000000" becoming STRING instead of INTEGER)
- Loss of precision/scale for decimal values
- No distinction between INTEGER vs BIGINT
- No way to specify VARCHAR lengths
- Semantic information loss (units, constraints, etc.)

### Proposed Solution: JSON Schema Companion Files
Use the JSON Schema standard with a companion file naming convention: `{filename-root}.schema.json`

#### File Structure
```
data/
├── financial_data.json           # The data file
├── financial_data.schema.json    # The companion schema
├── company_info.json
├── company_info.schema.json
└── transactions.json             # No schema = use type inference
```

#### Example Schema (financial_data.schema.json)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "ticker": {
        "type": "string",
        "maxLength": 10,
        "description": "Stock ticker symbol"
      },
      "revenue": {
        "type": "number",
        "multipleOf": 0.0001,
        "x-sql-type": "DECIMAL(19,4)",
        "x-sql-unit": "USD",
        "description": "Annual revenue in USD"
      },
      "shares_outstanding": {
        "type": "integer",
        "minimum": 0,
        "x-sql-type": "BIGINT",
        "x-sql-unit": "shares"
      },
      "market_cap": {
        "type": "number",
        "multipleOf": 0.01,
        "x-sql-type": "DECIMAL(22,2)",
        "x-sql-unit": "USD"
      },
      "gross_margin": {
        "type": "number",
        "minimum": 0,
        "maximum": 1,
        "multipleOf": 0.0001,
        "x-sql-type": "DECIMAL(5,4)",
        "description": "Gross margin as decimal (0.4531 = 45.31%)"
      },
      "filing_date": {
        "type": "string",
        "format": "date",
        "description": "SEC filing date"
      },
      "fiscal_period_end": {
        "type": "string",
        "format": "date",
        "description": "End of fiscal period"
      }
    },
    "required": ["ticker", "filing_date"]
  }
}
```

### Type Mapping Strategy

#### Standard JSON Schema → SQL Type Mappings
| JSON Schema | SQL Type | Notes |
|-------------|----------|-------|
| `type: "string", format: "date"` | DATE | ISO 8601 date |
| `type: "string", format: "date-time"` | TIMESTAMP | ISO 8601 datetime |
| `type: "string", format: "time"` | TIME | ISO 8601 time |
| `type: "string", maxLength: n` | VARCHAR(n) | Length-constrained string |
| `type: "string"` | VARCHAR or TEXT | Unlimited string |
| `type: "integer"` | INTEGER | 32-bit by default |
| `type: "number"` | DOUBLE | Floating point by default |
| `type: "boolean"` | BOOLEAN | True/false |
| `type: "null"` | NULL | Null value |

#### Using multipleOf for Decimal Scale Inference
- `multipleOf: 1` → INTEGER
- `multipleOf: 0.01` → DECIMAL with scale 2
- `multipleOf: 0.0001` → DECIMAL with scale 4
- `multipleOf: 0.00001` → DECIMAL with scale 5

#### SQL Extension Keywords (x-sql-* prefix)
- `x-sql-type`: Explicit SQL type (e.g., "DECIMAL(19,4)", "BIGINT", "VARCHAR(255)")
- `x-sql-precision`: Total digits for DECIMAL
- `x-sql-scale`: Decimal places for DECIMAL
- `x-sql-length`: Character length for VARCHAR/CHAR
- `x-sql-unit`: Semantic unit information (e.g., "USD", "shares", "percent")
- `x-sql-nullable`: Override nullable constraint

### Implementation Approach

#### Phase 1: Basic JSON Schema Support
1. **Schema Detection**
   - Check for `{filename-root}.schema.json` companion file
   - Load and parse JSON Schema if found
   - Cache parsed schema for performance

2. **Type Resolution**
   - Map standard JSON Schema types to SQL types
   - Support format specifiers (date, date-time, time)
   - Use maxLength for VARCHAR sizing
   - Apply multipleOf for decimal scale detection

3. **Backward Compatibility**
   - If no schema file exists, use current type inference
   - Allow schema override via configuration

#### Phase 2: Advanced Features
1. **SQL Extensions**
   - Support `x-sql-type` for precise type specification
   - Handle `x-sql-precision` and `x-sql-scale` for DECIMAL
   - Support `x-sql-length` for string types

2. **Validation**
   - Validate data against JSON Schema
   - Report schema violations as warnings
   - Optional strict mode to reject invalid data

3. **Performance Optimization**
   - Cache compiled schemas
   - Skip row scanning when schema provides types
   - Reuse schema across related files

#### Phase 3: Extended Capabilities
1. **Nested Structure Support**
   - Handle nested objects as JSON columns
   - Support arrays with JSON array types
   - Flatten nested structures based on schema hints

2. **Schema References**
   - Support `$ref` for shared definitions
   - Handle external schema files
   - Support schema composition with `allOf`/`anyOf`

3. **Auto-Generation Tools**
   - Tool to generate schema from existing Parquet files
   - Tool to infer schema from JSON samples
   - Schema migration utilities

### Benefits

1. **Type Fidelity**: Preserves exact SQL types including precision/scale
2. **Standards-Based**: Uses JSON Schema draft-07, not proprietary format
3. **Tool Support**: IDEs and validators understand JSON Schema
4. **Self-Documenting**: Schema serves as data documentation
5. **Validation**: Can validate data against schema
6. **Performance**: No need to scan rows for type inference
7. **Extensible**: Can add domain-specific extensions with x- prefix
8. **Backward Compatible**: Existing code continues to work

### Use Cases

1. **Financial Data**: Preserve decimal precision for monetary values
2. **Scientific Data**: Maintain numeric precision for measurements
3. **API Responses**: Define types for REST API JSON responses
4. **Data Exports**: Specify types for exported JSON data
5. **Configuration Files**: Type-safe configuration with validation
6. **XBRL Data**: Preserve rich type information from XBRL sources (see XBRL adapter integration)

### XBRL Adapter Integration
When the file adapter supports JSON Schema companion files, the XBRL adapter can leverage this for lossless type preservation:

1. **XBRL → JSON + Schema**: Generate both data and schema files
2. **Schema preserves XBRL types**: Map XBRL data types to SQL types via schema
3. **Enables JSON intermediate**: Solves the type fidelity problem for XBRL→JSON→Parquet pipeline
4. **XBRL-specific extensions**: Use `x-xbrl-*` properties for XBRL metadata

Example XBRL-generated schema:
```json
{
  "properties": {
    "us-gaap:Revenue": {
      "type": "number",
      "x-sql-type": "DECIMAL(19,4)",
      "x-xbrl-type": "monetaryItemType",
      "x-xbrl-unit": "USD",
      "x-xbrl-context": "instant"
    },
    "us-gaap:CommonStockSharesOutstanding": {
      "type": "integer",
      "x-sql-type": "BIGINT",
      "x-xbrl-type": "sharesItemType",
      "x-xbrl-unit": "shares"
    }
  }
}
```

### Configuration Options

```json
{
  "name": "financial",
  "type": "custom",
  "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
  "operand": {
    "files": ["financial_data.json"],
    "schemaFile": "custom_schema.json",  // Optional: override companion file
    "schemaMode": "strict",              // Optional: strict validation
    "inferIfNoSchema": true               // Optional: fallback behavior
  }
}
```

### Related Standards
- JSON Schema Draft-07: https://json-schema.org/draft-07/json-schema-validation
- JSON Schema Draft 2020-12: Latest version with additional features
- OpenAPI Schema Object: Similar approach for API specifications

### Future Considerations
- Support for JSON Schema Draft 2020-12 features
- Integration with Apache Avro schemas
- Support for Protocol Buffers schema
- Automatic schema evolution tracking
- Schema registry integration for centralized management

# Design Ideas for File Adapter (Prioritized by ROI)

## 🔥🔥🔥 HIGH ROI - Quick Wins (Days to Implement, Immediate Value)

### ✅ 1. Environment Variable Substitution in Model Files - **COMPLETED**

**ROI**: Effort: 1-2 days | Impact: High | Risk: Low | **Completed**: 2024-08-24 | **Commit**: e8513d288

~~**Problem**: Model files (JSON format) don't support environment variable substitution, making it difficult to configure execution engines and other settings dynamically across test suites and deployments.~~

~~**Proposed Solution**: Add support for environment variable substitution in model JSON files using syntax like `${VAR_NAME}` or `${VAR_NAME:default_value}`.~~

### ✅ IMPLEMENTED SOLUTION:
- **Core Utility**: `EnvironmentVariableSubstitutor` class in `core.util` package
- **Integration**: ModelHandler performs substitution before JSON/YAML parsing
- **Syntax Support**: Full `${VAR}` and `${VAR:default}` patterns
- **Type Conversion**: Automatic JSON type conversion for numbers/booleans
- **Fallback**: Environment variables → System properties → Defaults → Error
- **Testing**: 15 unit tests + integration tests with 100% coverage
- **Documentation**: Complete user guide with examples and best practices

### Implemented Usage (matches original proposal):
```json
{
  "version": "1.0",
  "defaultSchema": "${SCHEMA_NAME:MY_SCHEMA}",
  "schemas": [
    {
      "name": "${SCHEMA_NAME:MY_SCHEMA}",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "${DATA_DIR:/data}",
        "executionEngine": "${CALCITE_FILE_ENGINE_TYPE:PARQUET}",
        "batchSize": "${BATCH_SIZE:1000}",
        "ephemeralCache": "${USE_TEMP_CACHE:true}"
      }
    }
  ]
}
```

### Production Benefits Achieved:
- ✅ CI/CD pipeline support with environment-specific configs
- ✅ Docker/Kubernetes integration via environment variables  
- ✅ Secure credential management (no hardcoded values)
- ✅ Test automation with dynamic configuration
- ✅ Multi-environment deployments (dev/staging/prod)

### 2. Multi-URL Pattern-Based Web Scraping

**ROI**: Effort: 1 week | Impact: Very High | Risk: Low

**Problem**: Users often need to scrape the same data structure from multiple URLs that follow a pattern - like fetching stock summaries for different tickers, product details for different SKUs, or weather data for different cities. Currently, each URL requires a separate table definition, leading to repetitive configuration and maintenance overhead.

**Proposed Solution**: A lightweight extension to the existing HTML table extraction that supports URL patterns with variable substitution and batch fetching, perfect for aggregating structured data from multiple similar web pages.

### ✅ 3. Duplicate Schema Name Detection - **COMPLETED**

**ROI**: Effort: 2-3 hours | Impact: Medium | Risk: None | **Completed**: 2024-08-25 | **Commit**: 074ccc4da

~~**Problem**: Multiple schemas with the same name within a single connection can cause conflicts and unpredictable behavior.~~

~~**Proposed Solution**: Add validation when adding schemas to detect and prevent duplicate schema names within the same connection/root schema.~~

**✅ IMPLEMENTATION COMPLETE:**
- Schema name uniqueness validation in FileSchemaFactory.create() 
- Clear IllegalArgumentException with existing schema lists for troubleshooting
- Comprehensive test coverage with updated MultipleSchemaTest (4 test cases)
- Complete documentation in troubleshooting guide and configuration reference
- Breaking change properly documented with migration guidance
- Early error detection prevents configuration mistakes and silent replacement issues

## Delta Lake Support with Time Travel

**Problem**: The file adapter currently supports Iceberg for versioned table access, but lacks support for Delta Lake, another popular open table format with versioning and time travel capabilities.

**Proposed Solution**: Add Delta Lake support to the file adapter, following the existing Iceberg implementation patterns while accommodating Delta Lake's unique features.

### Key Features:
- **Time Travel Queries**: Support both version-based (`VERSION AS OF`) and timestamp-based (`TIMESTAMP AS OF`) queries
- **Transaction History**: Access to Delta Lake's transaction log and commit history
- **Schema Evolution**: Handle schema changes across versions
- **Change Data Capture**: Support for CDC reads when enabled
- **Metadata Tables**: System tables for history, files, commits, and vacuum operations

### Example Usage:
```json
{
  "version": "1.0",
  "defaultSchema": "delta_test",
  "schemas": [
    {
      "name": "delta_test",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "storageType": "delta",
        "storageConfig": {
          "tablePath": "s3://bucket/delta-table",
          "versionAsOf": 10,
          "timestampAsOf": "2024-01-15T10:30:00Z",
          "readChangeFeed": true,
          "ignoreDeletes": false
        }
      }
    }
  ]
}
```

### SQL Time Travel Examples:
```sql
-- Version-based time travel
SELECT * FROM delta_table VERSION AS OF 5;

-- Timestamp-based time travel  
SELECT * FROM delta_table TIMESTAMP AS OF '2024-01-15T10:30:00Z';

-- Query metadata tables
SELECT version, timestamp, operation FROM delta_table$history;
SELECT path, size_bytes FROM delta_table$files;
```

### Implementation Components:

#### Core Classes (package: `org.apache.calcite.adapter.file.delta`):
- **DeltaTable**: Main table implementation with time travel support
- **DeltaStorageProvider**: Storage abstraction for Delta Lake catalogs
- **DeltaCatalogManager**: Manages Delta table instances and transaction logs
- **DeltaEnumerator**: Handles data scanning with deletion vector support
- **DeltaTimeRangeTable**: Multi-version queries across time ranges
- **DeltaMetadataTables**: System tables for metadata access

#### Key Features:
1. **Transaction Log Reading**: Parse JSON transaction logs from `_delta_log/` directory
2. **Version Resolution**: Map timestamps to specific versions using commit metadata
3. **Deletion Vectors**: Handle GDPR-compliant row deletions without rewriting files
4. **Column Mapping**: Support both name-based and position-based column mapping
5. **Partition Pruning**: Leverage Delta's partition statistics for optimization
6. **Statistics-Based Skipping**: Use min/max stats for predicate pushdown

#### Dependencies:
```gradle
implementation("io.delta:delta-standalone_2.12:3.1.0")
implementation("io.delta:delta-storage:3.1.0")
```

#### Configuration Parameters:
- `tablePath`: Path to Delta table root directory
- `versionAsOf`: Specific version number for time travel
- `timestampAsOf`: ISO-8601 timestamp for time travel
- `readChangeFeed`: Enable CDC reading
- `ignoreDeletes`: Skip deletion vectors for performance
- `mergeSchema`: Handle schema evolution across versions

### Integration with Existing Architecture:
- Follows same patterns as Iceberg implementation
- Integrates with StorageProviderFactory and FileSchema
- Supports all existing storage backends (local, S3, Azure, GCS)
- Compatible with existing caching and metadata catalog systems

### Future Enhancements:
- **Z-Order Support**: Recognize and optimize for Z-ordered tables
- **Delta Sharing**: Support for remote Delta tables via REST API
- **Unity Catalog**: Integration with Databricks Unity Catalog
- **Liquid Clustering**: Support for Databricks' liquid clustering feature

## Text Similarity Search

**Problem**: Traditional SQL queries rely on exact matching or pattern matching (LIKE, REGEXP), which miss semantically similar content. Users need the ability to find documents, records, or text fields that are conceptually similar rather than literally matching.

**Proposed Solution**: Add vector-based text similarity search capabilities to the file adapter, enabling semantic search across text columns in various file formats.

### Key Features:
- **Embedding Generation**: Convert text to dense vector representations using configurable models
- **Similarity Functions**: Support multiple distance metrics (cosine, euclidean, dot product)
- **Index Support**: Optional vector indexes for performance (HNSW, IVF, etc.)
- **Hybrid Search**: Combine traditional filters with similarity scoring
- **Multi-modal Support**: Extend to image/document similarity in supported formats

### Example Usage:
```json
{
  "version": "1.0",
  "defaultSchema": "semantic_search",
  "schemas": [
    {
      "name": "semantic_search",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/documents",
        "textSimilarity": {
          "enabled": true,
          "embeddingModel": "sentence-transformers/all-MiniLM-L6-v2",
          "modelType": "local",
          "vectorCache": "/tmp/vectors",
          "columns": ["content", "description", "title"],
          "indexType": "hnsw",
          "indexParams": {
            "m": 16,
            "ef_construction": 200
          }
        }
      }
    }
  ]
}
```

### SQL Examples:
```sql
-- Find similar documents using SIMILARITY function
SELECT title, content, SIMILARITY(content, 'machine learning algorithms') AS score
FROM documents
WHERE SIMILARITY(content, 'machine learning algorithms') > 0.7
ORDER BY score DESC
LIMIT 10;

-- Combine with traditional filters
SELECT * FROM products
WHERE category = 'electronics'
  AND SIMILARITY(description, 'wireless noise cancelling headphones') > 0.6
ORDER BY price;

-- K-nearest neighbor search
SELECT * FROM articles
ORDER BY VECTOR_DISTANCE(embedding, EMBED('quantum computing breakthroughs'))
FETCH FIRST 5 ROWS ONLY;

-- Cross-column similarity
SELECT a.id, b.id, SIMILARITY(a.abstract, b.content) AS relevance
FROM papers a, articles b
WHERE SIMILARITY(a.abstract, b.content) > 0.8;
```

### Implementation Components:

#### Core Classes (package: `org.apache.calcite.adapter.file.similarity`):
- **TextEmbeddingProvider**: Interface for embedding generation
  - LocalEmbeddingProvider (ONNX models)
  - RemoteEmbeddingProvider (OpenAI, Cohere, etc.)
  - CachedEmbeddingProvider (with persistent cache)
- **VectorIndex**: Abstract base for similarity indexes
  - HNSWIndex: Hierarchical Navigable Small World
  - FaissIndex: Facebook AI Similarity Search wrapper
  - BruteForceIndex: Simple linear search
- **SimilarityTable**: Enhanced table with similarity capabilities
- **SimilarityOperator**: Custom SQL operators for similarity functions
- **VectorCache**: Persistent storage for computed embeddings

#### SQL Functions:
- `SIMILARITY(text1, text2)`: Cosine similarity between texts
- `VECTOR_DISTANCE(vec1, vec2, metric)`: Distance between vectors
- `EMBED(text)`: Generate embedding for text
- `KNN(column, query, k)`: K-nearest neighbor search
- `SEMANTIC_JOIN`: Join based on similarity threshold

#### Configuration Parameters:
- `embeddingModel`: Model identifier or path
- `modelType`: local, openai, cohere, huggingface
- `vectorDimensions`: Embedding vector size (default: auto-detect)
- `batchSize`: Batch size for embedding generation
- `similarityThreshold`: Default minimum similarity score
- `maxCacheSize`: Maximum size of vector cache
- `precomputeEmbeddings`: Generate embeddings on schema load
- `updateStrategy`: lazy, eager, or scheduled

### Integration Approaches:

#### 1. Pushdown to Vector Databases:
```java
// Detect and pushdown to specialized vector stores
if (storageType.equals("pinecone") || storageType.equals("weaviate")) {
    return new VectorStorePushdown(query);
}
```

#### 2. Hybrid with DuckDB:
```sql
-- Use DuckDB's VSS extension for in-memory similarity
INSTALL vss;
LOAD vss;
SELECT * FROM duckdb_similarity_search(?, ?, ?);
```

#### 3. Parquet Metadata Extension:
- Store embeddings as Parquet column metadata
- Enable zero-copy similarity operations
- Leverage Arrow's compute functions

### Performance Optimizations:
- **Incremental Indexing**: Update indexes for new/modified files only
- **Quantization**: Reduce memory with product quantization
- **Pruning**: Use metadata to skip irrelevant files/partitions
- **Parallel Processing**: Multi-threaded embedding generation
- **GPU Acceleration**: Optional CUDA support for large-scale operations

### Use Cases:
1. **Document Search**: Find similar reports, contracts, or articles
2. **Data Discovery**: Locate related datasets based on descriptions
3. **Duplicate Detection**: Identify near-duplicate records
4. **Recommendation**: Suggest similar items based on text attributes
5. **Question Answering**: Match queries to relevant passages
6. **Cross-lingual Search**: Find similar content across languages

### Dependencies:
```gradle
// Embedding models
implementation("ai.djl:api:0.27.0")
implementation("ai.djl.huggingface:tokenizers:0.27.0")
implementation("ai.djl.onnxruntime:onnxruntime-engine:0.27.0")

// Vector operations
implementation("com.github.jbellis:jvector:2.0.0")
implementation("org.apache.lucene:lucene-core:9.10.0")

// Optional: External vector stores
implementation("io.pinecone:pinecone-client:1.0.0")
implementation("io.weaviate:client:4.5.0")
```

### Future Enhancements:
- **Multi-modal Embeddings**: Support image and audio similarity
- **Fine-tuning Support**: Custom embeddings for domain-specific data
- **Streaming Similarity**: Real-time similarity on streaming data
- **Federated Search**: Distributed similarity across multiple sources
- **Explanation API**: Show why documents are considered similar
- **Active Learning**: Improve embeddings based on user feedback

## Pluggable Data Pipeline Framework

**Problem**: The file adapter currently handles static file conversion, but many use cases require continuous data ingestion, transformation, and materialization from external sources. Adding each new data source requires modifying core adapter code, making it difficult to extend and maintain.

**Proposed Solution**: Create a pluggable data pipeline framework that allows adding new data sources as plugins/modules to the file adapter, each capable of materializing and maintaining tables from external sources.

### Core Concept

Transform the file adapter from a static file reader into a data materialization platform where plugins can:
- Fetch data from external sources (APIs, databases, streams)
- Transform and normalize data
- Materialize as Parquet/Iceberg tables
- Manage incremental updates
- Handle scheduling and orchestration

### Architecture

```
File Adapter
├── Core Engine (existing file handling)
└── Pipeline Framework
    ├── Pipeline Registry
    ├── Scheduler
    ├── State Manager
    └── Plugins/
        ├── SECEdgarPipeline
        ├── TwitterPipeline
        ├── WeatherPipeline
        ├── GitHubPipeline
        └── CustomPipeline...
```

### Pipeline Plugin Interface

```java
public interface DataPipeline {
    // Pipeline metadata
    String getName();
    String getDescription();
    PipelineConfig getDefaultConfig();
    
    // Lifecycle
    void initialize(PipelineContext context);
    void validate(PipelineConfig config);
    void shutdown();
    
    // Execution
    PipelineResult fetch(FetchRequest request);
    DataFrame transform(DataFrame input, TransformConfig config);
    void materialize(DataFrame data, MaterializationTarget target);
    
    // Schema definition
    RelDataType getSchema(SchemaContext context);
    List<String> getTableNames();
    
    // Scheduling
    Schedule getSchedule();
    boolean shouldRunNow(PipelineState state);
    
    // State management
    PipelineState getState();
    void saveState(PipelineState state);
}
```

### Configuration Example

```json
{
  "version": "1.0",
  "defaultSchema": "pipeline_data",
  "schemas": [{
    "name": "pipeline_data",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/data/pipelines",
      "pipelines": [
        {
          "type": "sec-edgar",
          "name": "financial_statements",
          "config": {
            "tickers": ["AAPL", "MSFT", "GOOGL"],
            "filingTypes": ["10-K", "10-Q"],
            "startDate": "2020-01-01",
            "outputFormat": "iceberg"
          },
          "schedule": "0 0 * * *"  // Daily at midnight
        },
        {
          "type": "rest-api",
          "name": "weather_data",
          "config": {
            "url": "https://api.weather.gov/stations/${station}/observations",
            "stations": ["KNYC", "KLAX", "KORD"],
            "authentication": {
              "type": "bearer",
              "token": "${WEATHER_API_TOKEN}"
            },
            "transform": {
              "mapping": {
                "temperature": "$.properties.temperature.value",
                "humidity": "$.properties.relativeHumidity.value",
                "timestamp": "$.properties.timestamp"
              }
            }
          },
          "schedule": "*/15 * * * *"  // Every 15 minutes
        },
        {
          "type": "github",
          "name": "repository_metrics",
          "config": {
            "repos": ["apache/calcite", "duckdb/duckdb"],
            "metrics": ["stars", "forks", "issues", "prs"],
            "includeHistory": true
          },
          "schedule": "0 */6 * * *"  // Every 6 hours
        }
      ]
    }
  }]
}
```

### Built-in Pipeline Types

#### 1. REST API Pipeline
```java
public class RestApiPipeline implements DataPipeline {
    // Generic REST API fetcher with:
    // - Authentication (OAuth, API Key, Bearer)
    // - Rate limiting
    // - Pagination handling
    // - JSON/XML parsing
    // - Response transformation
}
```

#### 2. Database CDC Pipeline
```java
public class DatabaseCDCPipeline implements DataPipeline {
    // Change Data Capture from databases:
    // - Connect via JDBC
    // - Track changes using timestamps/sequences
    // - Handle schema evolution
    // - Incremental updates only
}
```

#### 3. File Watch Pipeline
```java
public class FileWatchPipeline implements DataPipeline {
    // Monitor directories for new files:
    // - Watch for new files
    // - Process and archive
    // - Handle various formats
    // - Deduplication
}
```

#### 4. Stream Pipeline
```java
public class StreamPipeline implements DataPipeline {
    // Consume from streaming sources:
    // - Kafka topics
    // - AWS Kinesis
    // - Google Pub/Sub
    // - WebSockets
}
```

### Pipeline Execution Framework

```java
public class PipelineExecutor {
    private final ScheduledExecutorService scheduler;
    private final StateManager stateManager;
    private final MaterializationEngine materializationEngine;
    
    public void registerPipeline(DataPipeline pipeline) {
        // Validate pipeline
        pipeline.validate(pipeline.getDefaultConfig());
        
        // Schedule based on configuration
        Schedule schedule = pipeline.getSchedule();
        scheduler.scheduleAtFixedRate(
            () -> executePipeline(pipeline),
            schedule.getInitialDelay(),
            schedule.getPeriod(),
            schedule.getTimeUnit()
        );
    }
    
    private void executePipeline(DataPipeline pipeline) {
        try {
            // Check if should run
            PipelineState state = stateManager.getState(pipeline);
            if (!pipeline.shouldRunNow(state)) {
                return;
            }
            
            // Fetch data
            FetchRequest request = buildFetchRequest(state);
            PipelineResult result = pipeline.fetch(request);
            
            // Transform
            DataFrame transformed = pipeline.transform(
                result.getData(), 
                state.getTransformConfig()
            );
            
            // Materialize
            MaterializationTarget target = materializationEngine
                .getTarget(pipeline.getName());
            pipeline.materialize(transformed, target);
            
            // Update state
            state.setLastRun(Instant.now());
            state.setLastResult(result);
            stateManager.saveState(pipeline, state);
            
        } catch (Exception e) {
            handlePipelineError(pipeline, e);
        }
    }
}
```

### Materialization Strategies

#### Append-Only (Iceberg)
```java
public class IcebergMaterializer {
    public void materialize(DataFrame data, IcebergTable table) {
        // Append new data to Iceberg table
        // Maintains full history
        // Supports time travel
    }
}
```

#### Merge/Upsert (Delta Lake)
```java
public class DeltaMaterializer {
    public void materialize(DataFrame data, DeltaTable table) {
        // Merge based on keys
        // Update existing records
        // Insert new records
    }
}
```

#### Full Refresh (Parquet)
```java
public class ParquetMaterializer {
    public void materialize(DataFrame data, ParquetTarget target) {
        // Overwrite entire dataset
        // Good for small reference data
        // Simple and fast
    }
}
```

### State Management

```sql
-- Pipeline execution state
CREATE TABLE pipeline_state (
    pipeline_name VARCHAR PRIMARY KEY,
    last_run_time TIMESTAMP,
    last_success_time TIMESTAMP,
    last_error VARCHAR,
    records_processed BIGINT,
    state_json JSON,  -- Pipeline-specific state
    checkpoint JSON   -- For incremental processing
);

-- Pipeline history
CREATE TABLE pipeline_history (
    pipeline_name VARCHAR,
    run_id VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR,
    records_fetched INTEGER,
    records_written INTEGER,
    error_message VARCHAR
);
```

### Monitoring and Observability

```sql
-- Monitor pipeline health
SELECT 
    pipeline_name,
    last_run_time,
    CURRENT_TIMESTAMP - last_run_time as time_since_run,
    CASE 
        WHEN last_error IS NOT NULL THEN 'ERROR'
        WHEN CURRENT_TIMESTAMP - last_run_time > INTERVAL '1 day' THEN 'STALE'
        ELSE 'HEALTHY'
    END as status
FROM pipeline_state;

-- Pipeline performance metrics
SELECT 
    pipeline_name,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration_seconds,
    SUM(records_written) as total_records,
    COUNT(*) as run_count,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as error_count
FROM pipeline_history
WHERE start_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY pipeline_name;
```

### Plugin Development

#### Example: Twitter Pipeline
```java
@PipelinePlugin(name = "twitter", version = "1.0")
public class TwitterPipeline implements DataPipeline {
    
    @Override
    public PipelineResult fetch(FetchRequest request) {
        // Use Twitter API v2
        TwitterClient client = new TwitterClient(config.getApiKey());
        
        // Get tweets since last checkpoint
        String sinceId = request.getCheckpoint().get("since_id");
        List<Tweet> tweets = client.searchRecent(
            config.getQuery(),
            sinceId,
            config.getMaxResults()
        );
        
        // Convert to DataFrame
        return PipelineResult.builder()
            .data(tweetsToDataFrame(tweets))
            .checkpoint(Map.of("since_id", getMaxId(tweets)))
            .build();
    }
    
    @Override
    public RelDataType getSchema(SchemaContext context) {
        return context.getTypeFactory().builder()
            .add("tweet_id", SqlTypeName.VARCHAR)
            .add("text", SqlTypeName.VARCHAR)
            .add("author_id", SqlTypeName.VARCHAR)
            .add("created_at", SqlTypeName.TIMESTAMP)
            .add("retweet_count", SqlTypeName.INTEGER)
            .add("like_count", SqlTypeName.INTEGER)
            .add("sentiment_score", SqlTypeName.DECIMAL)
            .build();
    }
}
```

### Benefits

**Extensibility:**
- Add new data sources without modifying core code
- Community can contribute pipelines
- Plugin marketplace potential

**Reusability:**
- Common patterns (REST, CDC, streaming) as base classes
- Shared authentication, rate limiting, error handling
- Standardized configuration

**Maintainability:**
- Isolated pipeline logic
- Versioned plugins
- Independent deployment

**Operations:**
- Centralized scheduling and monitoring
- Consistent state management
- Unified error handling

### Use Cases

**Financial Data:**
- SEC filings pipeline
- Stock price pipeline
- Economic indicators pipeline

**Social Media:**
- Twitter sentiment pipeline
- Reddit discussions pipeline
- News aggregation pipeline

**DevOps:**
- GitHub metrics pipeline
- CI/CD metrics pipeline
- Cloud cost pipeline

**IoT/Sensors:**
- Weather station pipeline
- Traffic sensor pipeline
- Energy meter pipeline

### Implementation Priority

**Phase 1: Core Framework**
- Pipeline interface and registry
- Basic scheduler
- State management
- Parquet materialization

**Phase 2: Built-in Pipelines**
- REST API pipeline
- File watch pipeline
- Basic transformations

**Phase 3: Advanced Features**
- Iceberg/Delta materialization
- Stream processing
- Complex transformations
- Pipeline dependencies

**Phase 4: Ecosystem**
- Plugin repository
- Pipeline templates
- Visual pipeline builder
- Monitoring dashboard

This framework would transform the file adapter into a powerful data ingestion platform while maintaining backward compatibility with existing file-based functionality.

## Caffeine-based Parquet Batch Caching

**Problem**: The file adapter repeatedly reads and decodes Parquet files for each query, even when accessing the same data batches. This leads to unnecessary I/O operations, CPU overhead for decompression/decoding, and poor performance for analytical workloads that repeatedly query the same datasets.

**Proposed Solution**: Integrate the Caffeine caching library to cache decoded Parquet batches at the schema level, with configurable memory management, refresh strategies, and seamless spillover support for large datasets.

### Architecture Analysis: Per-Schema vs Global vs Hybrid Cache

#### Per-Schema Cache Approach

**Advantages:**
- **Isolation**: Different schemas can have different cache policies (e.g., sales data cached longer than temp data)
- **Resource Control**: Each schema gets its own memory budget - prevents one schema from monopolizing cache
- **Configuration Flexibility**: Different refresh strategies per data source (e.g., real-time vs batch data)
- **Fault Isolation**: Cache corruption/issues in one schema don't affect others
- **Lifecycle Management**: Clean shutdown per schema without affecting others
- **Multi-tenancy**: Better for multi-tenant scenarios where different tenants have different SLAs

**Disadvantages:**
- **Memory Fragmentation**: Fixed memory per schema may lead to underutilization (one schema uses 10%, another needs 150%)
- **Duplicate Caching**: Same file accessed through different schemas gets cached multiple times
- **Complex Configuration**: More configuration overhead for administrators
- **Cross-Schema Queries**: No benefit when joining across schemas
- **Overhead**: Multiple cache instances mean more overhead (threads, management structures)

#### Global Cache Approach

**Advantages:**
- **Memory Efficiency**: Single pool adapts to actual usage patterns dynamically
- **Deduplication**: Same file/batch cached once regardless of access path
- **Simpler Configuration**: One set of cache settings to tune
- **Cross-Schema Benefits**: Joins across schemas benefit from shared cache
- **Better Statistics**: Single point for monitoring cache effectiveness
- **Hot Data Optimization**: Most frequently accessed data stays cached regardless of schema

**Disadvantages:**
- **No Isolation**: One misbehaving schema can evict everyone's data
- **Single Policy**: Can't have different TTLs or refresh strategies per data source
- **Resource Contention**: All schemas compete for same cache space
- **Complex Eviction**: Harder to implement fair eviction across schemas
- **Security Concerns**: Potential data leakage across schema boundaries (though data is already in same JVM)

#### Recommended Hybrid Approach

A hybrid design that combines the benefits of both approaches while mitigating their disadvantages:

```java
public class ParquetBatchCacheManager {
    // Single Caffeine cache instance for memory efficiency
    private final Cache<ParquetBatchCacheKey, CachedBatch> globalCache;
    
    // Per-schema configuration and quotas
    private final Map<String, SchemaConfig> schemaConfigs;
    private final Map<String, AtomicLong> schemaUsage;
    
    // Weighted eviction that considers schema quotas and priorities
    private final Weigher<ParquetBatchCacheKey, CachedBatch> schemaAwareWeigher;
}
```

### Hybrid Cache Implementation Design

#### Cache Key Structure
```java
public class ParquetBatchCacheKey {
    private final String schemaName;      // For schema-aware operations
    private final String filePath;        // Absolute path to parquet file
    private final int rowGroupIndex;      // Row group within file
    private final long batchOffset;       // Offset within row group
    private final int batchSize;          // Number of rows in batch
    private final long fileModTime;       // File modification timestamp
    
    // Proper equals/hashCode for cache lookups
    // Include schema name for schema-aware eviction
}
```

#### Schema-Level Configuration
```json
{
  "version": "1.0",
  "defaultSchema": "analytics",
  "schemas": [{
    "name": "analytics",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "data/analytics",
      "parquetBatchCache": {
        "enabled": true,
        "priority": 8,                    // 1-10, higher = more important
        "softQuotaMb": 256,               // Soft limit for this schema
        "hardQuotaMb": 512,               // Hard limit (can't exceed)
        "expireAfterWriteMinutes": 30,   // TTL for cached batches
        "expireAfterAccessMinutes": 10,  // Idle timeout
        "refreshAfterWriteMinutes": 15,  // Background refresh
        "spilloverEnabled": true,        // Allow spillover to disk
        "spilloverThresholdMb": 10,      // Batch size to trigger spillover
        "recordStats": true              // Track cache statistics
      }
    }
  }]
}
```

### Cache Manager Architecture

#### Core Components

**1. Caffeine Cache Configuration:**
```java
public class CacheBuilder {
    public Cache<ParquetBatchCacheKey, CachedBatch> buildCache(GlobalConfig config) {
        return Caffeine.newBuilder()
            .maximumWeight(config.getMaxGlobalMemoryBytes())
            .weigher(new SchemaAwareWeigher(schemaConfigs))
            .expireAfter(new SchemaAwareExpiry())
            .removalListener(new SpilloverRemovalListener())
            .recordStats()
            .build();
    }
}
```

**2. Schema-Aware Weigher:**
```java
public class SchemaAwareWeigher implements Weigher<ParquetBatchCacheKey, CachedBatch> {
    public int weigh(ParquetBatchCacheKey key, CachedBatch batch) {
        SchemaConfig config = schemaConfigs.get(key.getSchemaName());
        int baseWeight = batch.getMemorySize();
        
        // Apply schema priority as weight multiplier
        // Higher priority = lower weight = less likely to evict
        double priorityMultiplier = 1.0 / (config.getPriority() / 5.0);
        
        return (int)(baseWeight * priorityMultiplier);
    }
}
```

**3. Spillover Integration:**
```java
public class SpilloverBatchLoader implements CacheLoader<ParquetBatchCacheKey, CachedBatch> {
    @Override
    public CachedBatch load(ParquetBatchCacheKey key) throws Exception {
        // First check if spilled to disk
        File spillFile = spilloverManager.getSpillFile(key);
        if (spillFile != null && spillFile.exists()) {
            return loadFromSpillover(spillFile);
        }
        
        // Otherwise load from parquet file
        VectorizedParquetReader reader = new VectorizedParquetReader(
            key.getFilePath(),
            cacheManager  // Pass cache manager for coordination
        );
        
        List<Object[]> batch = reader.readBatch(
            key.getRowGroupIndex(),
            key.getBatchOffset(),
            key.getBatchSize()
        );
        
        // Check if should spill immediately
        CachedBatch cached = new CachedBatch(batch);
        if (cached.getMemorySize() > config.getSpilloverThresholdBytes()) {
            spilloverManager.spillToDisk(key, cached);
            cached.setSpilled(true);
        }
        
        return cached;
    }
}
```

### Integration Points

#### 1. VectorizedParquetReader Enhancement
```java
public class VectorizedParquetReader {
    private final ParquetBatchCacheManager cacheManager;
    
    public List<Object[]> readBatch() throws IOException {
        // Generate cache key
        ParquetBatchCacheKey key = new ParquetBatchCacheKey(
            schemaName,
            filePath,
            currentRowGroupIndex,
            currentRowInGroup,
            batchSize,
            fileModTime
        );
        
        // Try cache first
        if (cacheManager != null) {
            CachedBatch cached = cacheManager.get(key);
            if (cached != null) {
                cacheStats.recordHit();
                return cached.getData();
            }
            cacheStats.recordMiss();
        }
        
        // Read from file
        List<Object[]> batch = readBatchFromFile();
        
        // Cache for future use
        if (cacheManager != null && shouldCache(batch)) {
            cacheManager.put(key, new CachedBatch(batch));
        }
        
        return batch;
    }
}
```

#### 2. FileSchema Integration
```java
public class FileSchema extends AbstractSchema {
    private final ParquetBatchCacheManager cacheManager;
    
    public FileSchema(File baseDirectory, Map<String, Object> operand) {
        // Parse cache configuration
        Map<String, Object> cacheConfig = 
            (Map<String, Object>) operand.get("parquetBatchCache");
        
        if (cacheConfig != null && Boolean.TRUE.equals(cacheConfig.get("enabled"))) {
            this.cacheManager = ParquetBatchCacheManager.getInstance()
                .registerSchema(this.name, cacheConfig);
        }
    }
    
    @Override
    public void close() {
        if (cacheManager != null) {
            cacheManager.evictSchema(this.name);
        }
    }
}
```

### Memory Management Strategies

#### 1. Adaptive Memory Allocation
```java
public class AdaptiveMemoryManager {
    public void adjustQuotas() {
        // Monitor actual usage patterns
        Map<String, Double> usageRatios = calculateUsageRatios();
        
        // Adjust soft quotas based on demand
        for (String schema : schemaConfigs.keySet()) {
            double ratio = usageRatios.get(schema);
            if (ratio > 0.9) {  // Schema needs more memory
                increaseSoftQuota(schema);
            } else if (ratio < 0.3) {  // Schema underutilizing
                decreaseSoftQuota(schema);
            }
        }
    }
}
```

#### 2. Memory Pressure Response
```java
public class MemoryPressureMonitor {
    public void onMemoryPressure() {
        // Progressive response to memory pressure
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        double usage = memory.getHeapMemoryUsage().getUsed() / 
                      (double) memory.getHeapMemoryUsage().getMax();
        
        if (usage > 0.9) {
            // Critical: Spill largest batches
            spillLargestBatches(10);
        } else if (usage > 0.8) {
            // High: Evict low-priority schemas
            evictLowPriorityData();
        } else if (usage > 0.7) {
            // Moderate: Stop prefetching
            disablePrefetching();
        }
    }
}
```

### Cache Statistics and Monitoring

```java
public class CacheStatistics {
    // Per-schema statistics
    private final Map<String, SchemaStats> schemaStats;
    
    public static class SchemaStats {
        private long hits;
        private long misses;
        private long evictions;
        private long spillovers;
        private long bytesInCache;
        private long bytesSpilled;
        
        public double getHitRate() {
            return hits / (double)(hits + misses);
        }
        
        public long getMemoryUsage() {
            return bytesInCache + bytesSpilled;
        }
    }
    
    // JMX exposure for monitoring
    @MXBean
    public interface CacheStatisticsMXBean {
        Map<String, Double> getHitRatesBySchema();
        long getTotalMemoryUsage();
        long getSpilledBytes();
        Map<String, Long> getEvictionsBySchema();
    }
}
```

### Benefits of Hybrid Approach

1. **Memory Efficiency**: Single cache pool adapts to actual usage patterns
2. **Schema Awareness**: Different policies and priorities per schema
3. **Spillover Support**: Seamless handling of large datasets
4. **Fair Resource Sharing**: Soft quotas with ability to exceed when space available
5. **Performance**: Dramatic speedup for repeated queries
6. **Monitoring**: Comprehensive statistics for tuning

### Performance Optimizations

1. **Batch Prefetching**: Predictively load next batches based on access patterns
2. **Compression**: Keep compressed batches in cache, decompress on demand
3. **Tiered Caching**: Hot data in memory, warm data spilled, cold data on disk
4. **Async Loading**: Background loading of likely-needed batches
5. **NUMA Awareness**: Pin cache regions to NUMA nodes for better locality

### Future Enhancements

1. **Distributed Caching**: Share cache across multiple file adapter instances
2. **Persistent Cache**: Survive JVM restarts with memory-mapped files
3. **Smart Eviction**: ML-based prediction of future access patterns
4. **Column-Level Caching**: Cache individual columns for better memory efficiency
5. **Query-Aware Caching**: Cache computed results of common sub-queries

## Pluggable Cache Provider Pattern

**Problem**: Different deployment environments have different caching requirements. Some need in-memory caching (Caffeine), others need distributed caching (Redis/Hazelcast), and some need custom solutions (S3-backed, etc.). Hardcoding a single caching solution limits flexibility and forces users to accept trade-offs that may not fit their use case.

**Proposed Solution**: Implement a pluggable cache provider pattern that allows users to choose or implement their own caching strategy through configuration, without modifying the core file adapter code.

### Provider Interface

```java
public interface ParquetCacheProvider {
    // Lifecycle
    void initialize(CacheConfig config);
    void close();
    
    // Basic operations
    CompletableFuture<CachedBatch> get(ParquetBatchCacheKey key);
    CompletableFuture<Void> put(ParquetBatchCacheKey key, CachedBatch batch);
    CompletableFuture<Void> evict(ParquetBatchCacheKey key);
    CompletableFuture<Void> clear();
    
    // Bulk operations
    CompletableFuture<Map<ParquetBatchCacheKey, CachedBatch>> getAll(Set<ParquetBatchCacheKey> keys);
    CompletableFuture<Void> putAll(Map<ParquetBatchCacheKey, CachedBatch> entries);
    
    // Management
    CacheStatistics getStatistics();
    long getMemoryUsage();
    void setEvictionPolicy(EvictionPolicy policy);
    
    // Provider metadata
    String getName();
    Set<String> getSupportedFeatures();
    boolean supportsAsync();
    boolean supportsDistributed();
}
```

### Configuration-Driven Selection

```json
{
  "parquetBatchCache": {
    "enabled": true,
    "provider": "tiered",  // caffeine, redis, hazelcast, disk, tiered, or custom class
    "config": {
      // Provider-specific configuration
      "tiers": [
        {
          "name": "l1-memory",
          "provider": "caffeine",
          "maxSizeMb": 256,
          "expireAfterAccessMinutes": 5,
          "recordStats": true
        },
        {
          "name": "l2-redis", 
          "provider": "redis",
          "connection": "${REDIS_URL:redis://localhost:6379}",
          "maxSizeGb": 10,
          "serializer": "kryo",
          "compression": "lz4"
        },
        {
          "name": "l3-disk",
          "provider": "disk",
          "directory": "${CACHE_DIR:/tmp/parquet-cache}",
          "maxSizeGb": 100,
          "blockSize": 4096
        }
      ]
    }
  }
}
```

### Built-in Providers

#### 1. Caffeine Provider (Local In-Memory)
**Best for**: Single JVM, low latency, small-medium datasets
```java
public class CaffeineCacheProvider implements ParquetCacheProvider {
    private Cache<ParquetBatchCacheKey, CachedBatch> cache;
    
    @Override
    public Set<String> getSupportedFeatures() {
        return Set.of("local", "fast", "ttl", "size-eviction", "weight-eviction");
    }
}
```

#### 2. Redis Provider (Distributed)
**Best for**: Multi-JVM, large datasets, shared cache
```java
public class RedisCacheProvider implements ParquetCacheProvider {
    private RedissonClient redisson;
    private RMapCache<String, byte[]> cache;
    
    @Override
    public Set<String> getSupportedFeatures() {
        return Set.of("distributed", "persistent", "shared", "large-capacity", 
                     "pub-sub", "atomic-ops");
    }
}
```

#### 3. Hazelcast Provider (In-Memory Data Grid)
**Best for**: Distributed computing, near-cache support
```java
public class HazelcastCacheProvider implements ParquetCacheProvider {
    private HazelcastInstance hazelcast;
    private IMap<ParquetBatchCacheKey, CachedBatch> cache;
    
    @Override
    public Set<String> getSupportedFeatures() {
        return Set.of("distributed", "near-cache", "replicated", "queryable", 
                     "entry-processor", "continuous-query");
    }
}
```

#### 4. Apache Ignite Provider (Compute Grid)
**Best for**: Collocated compute, SQL queries on cache
```java
public class IgniteCacheProvider implements ParquetCacheProvider {
    private Ignite ignite;
    private IgniteCache<ParquetBatchCacheKey, CachedBatch> cache;
    
    @Override
    public Set<String> getSupportedFeatures() {
        return Set.of("distributed", "compute-grid", "sql-queries", 
                     "collocated-processing", "persistence");
    }
}
```

#### 5. Disk Provider (File-Based Spillover)
**Best for**: Large datasets, persistence, spillover
```java
public class DiskCacheProvider implements ParquetCacheProvider {
    private Path cacheDir;
    private ConcurrentHashMap<ParquetBatchCacheKey, Path> index;
    
    @Override
    public Set<String> getSupportedFeatures() {
        return Set.of("persistent", "spillover", "unlimited-size", "low-memory");
    }
}
```

#### 6. Tiered Provider (Composite)
**Best for**: Combining multiple cache levels
```java
public class TieredCacheProvider implements ParquetCacheProvider {
    private List<ParquetCacheProvider> tiers;
    
    @Override
    public CompletableFuture<CachedBatch> get(ParquetBatchCacheKey key) {
        // Waterfall through tiers
        return getTiered(key, 0);
    }
    
    private CompletableFuture<CachedBatch> getTiered(ParquetBatchCacheKey key, int tier) {
        if (tier >= tiers.size()) {
            return CompletableFuture.completedFuture(null);
        }
        
        return tiers.get(tier).get(key)
            .thenCompose(batch -> {
                if (batch != null) {
                    promoteToHigherTiers(key, batch, tier);
                    return CompletableFuture.completedFuture(batch);
                }
                return getTiered(key, tier + 1);
            });
    }
}
```

### Custom Provider Implementation

Users can implement their own providers for specific requirements:

```java
// Example: S3-backed cache for serverless deployments
public class S3CacheProvider implements ParquetCacheProvider {
    private S3Client s3Client;
    private String bucket;
    private String prefix;
    
    @Override
    public void initialize(CacheConfig config) {
        this.s3Client = S3Client.builder()
            .region(Region.of(config.getString("region", "us-east-1")))
            .build();
        this.bucket = config.getString("bucket");
        this.prefix = config.getString("prefix", "parquet-cache/");
    }
    
    @Override
    public CompletableFuture<CachedBatch> get(ParquetBatchCacheKey key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(prefix + key.toS3Key())
                    .build();
                    
                ResponseBytes<GetObjectResponse> response = 
                    s3Client.getObjectAsBytes(request);
                    
                return deserialize(response.asByteArray());
            } catch (NoSuchKeyException e) {
                return null;
            }
        });
    }
    
    @Override
    public Set<String> getSupportedFeatures() {
        return Set.of("distributed", "persistent", "serverless", 
                     "unlimited-size", "s3-native");
    }
}
```

### Provider Factory and Registration

```java
public class CacheProviderFactory {
    private static final Map<String, Class<? extends ParquetCacheProvider>> PROVIDERS = 
        new ConcurrentHashMap<>();
    
    static {
        // Register built-in providers
        registerProvider("caffeine", CaffeineCacheProvider.class);
        registerProvider("redis", RedisCacheProvider.class);
        registerProvider("hazelcast", HazelcastCacheProvider.class);
        registerProvider("ignite", IgniteCacheProvider.class);
        registerProvider("disk", DiskCacheProvider.class);
        registerProvider("tiered", TieredCacheProvider.class);
        registerProvider("ehcache", EhcacheCacheProvider.class);
    }
    
    public static void registerProvider(String name, 
                                       Class<? extends ParquetCacheProvider> providerClass) {
        PROVIDERS.put(name.toLowerCase(), providerClass);
    }
    
    public static ParquetCacheProvider create(String type, CacheConfig config) {
        // Check if it's a class name (custom provider)
        if (type.contains(".")) {
            return createCustomProvider(type, config);
        }
        
        // Built-in provider
        Class<? extends ParquetCacheProvider> providerClass = PROVIDERS.get(type.toLowerCase());
        if (providerClass == null) {
            throw new IllegalArgumentException("Unknown cache provider: " + type);
        }
        
        try {
            ParquetCacheProvider provider = providerClass.getDeclaredConstructor().newInstance();
            provider.initialize(config);
            return provider;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create cache provider: " + type, e);
        }
    }
}
```

### Advanced Provider Features

#### Cache Warming
```java
public interface WarmableCacheProvider extends ParquetCacheProvider {
    CompletableFuture<Void> warmCache(List<ParquetBatchCacheKey> keys);
    void setWarmingStrategy(WarmingStrategy strategy);
    void scheduleWarming(String cronExpression);
}
```

#### Cache Coherence
```java
public interface CoherentCacheProvider extends ParquetCacheProvider {
    void addInvalidationListener(InvalidationListener listener);
    void invalidateAcrossNodes(ParquetBatchCacheKey key);
    void setCoherenceMode(CoherenceMode mode);
    CompletableFuture<Void> synchronize();
}
```

#### Observable Cache
```java
public interface ObservableCacheProvider extends ParquetCacheProvider {
    void addCacheListener(CacheListener listener);
    Stream<CacheEvent> getEventStream();
    void enableMetrics(MeterRegistry registry);
    void enableTracing(Tracer tracer);
}
```

### Provider Selection Matrix

| Provider | Latency | Capacity | Distributed | Persistent | Use Case |
|----------|---------|----------|-------------|------------|----------|
| Caffeine | <1μs | GB | No | No | Single JVM, hot data |
| Redis | 1-2ms | TB | Yes | Yes | Multi-JVM, shared cache |
| Hazelcast | <1ms | TB | Yes | Optional | Compute grid |
| Ignite | <1ms | TB | Yes | Yes | Collocated compute |
| Disk | 10ms | Unlimited | No | Yes | Large spillover |
| S3 | 50-200ms | Unlimited | Yes | Yes | Serverless |
| Tiered | Variable | Unlimited | Optional | Optional | Mixed workloads |

### Integration Example

```java
public class FileSchema extends AbstractSchema {
    private ParquetCacheProvider cacheProvider;
    
    public FileSchema(File baseDirectory, Map<String, Object> operand) {
        Map<String, Object> cacheConfig = 
            (Map<String, Object>) operand.get("parquetBatchCache");
        
        if (cacheConfig != null && Boolean.TRUE.equals(cacheConfig.get("enabled"))) {
            String providerType = (String) cacheConfig.getOrDefault("provider", "caffeine");
            
            // Create provider through factory
            this.cacheProvider = CacheProviderFactory.create(
                providerType, 
                new CacheConfig(cacheConfig)
            );
            
            // Optional: warm cache on startup
            if (cacheProvider instanceof WarmableCacheProvider) {
                ((WarmableCacheProvider) cacheProvider)
                    .setWarmingStrategy(WarmingStrategy.ON_STARTUP);
            }
        }
    }
}
```

### Benefits of Provider Pattern

1. **Flexibility**: Choose the right cache for your deployment
2. **No Lock-in**: Switch providers without code changes
3. **Extensibility**: Add custom providers for specific needs
4. **Testing**: Easy to mock/stub providers
5. **Configuration-Driven**: Change behavior via config
6. **Best of Breed**: Use specialized caches for their strengths
7. **Future-Proof**: New cache technologies can be added as providers
8. **Composability**: Combine providers (e.g., tiered caching)

### Implementation Priority

**Phase 1**: Core provider interface and factory
**Phase 2**: Caffeine and Disk providers (local options)
**Phase 3**: Redis provider (distributed option)
**Phase 4**: Tiered provider (composition)
**Phase 5**: Additional providers based on demand

## Enhanced Web Table Extraction

**Problem**: The current web/HTML feature extracts all HTML tables from a single URL, which works well but is limited in scope. Users often need to extract specific tables from multiple sources, target tables by ID or class, or extract tables matching certain patterns.

**Current Implementation**: 
- Single URL per schema
- Extracts all `<table>` elements from the page
- Clean and simple - tables are obviously tabular data

**Proposed Enhancement**: Extend the current table extraction with multi-URL support and CSS selector-based targeting while maintaining the simplicity of "tables are tabular data".

### Multi-URL and Selector Configuration

```json
{
  "type": "web",
  "urls": [
    "https://example.com/page1.html",
    "https://example.com/page2.html",
    "https://example.com/reports/*/daily.html"  // Glob pattern support
  ],
  "selectors": {
    "sales_table": "table#sales-data",           // Specific table by ID
    "product_tables": "table.product-listing",   // All tables with class
    "nested_data": "div.container > table",      // Tables within specific container
    "monthly_reports": "table[data-period='monthly']",  // Attribute selector
    "price_table": {
      "selector": "table",
      "contains_text": "Price List",            // Table containing specific text
      "has_headers": ["Product", "Price"],      // Required column headers
      "min_rows": 5                             // Minimum row count
    }
  }
}
```

### Selector Strategies

#### CSS Selectors
Standard CSS selector support for precise table targeting:
```json
{
  "selectors": {
    "by_id": "#data-table",
    "by_class": ".financial-data",
    "by_attribute": "table[data-source='bloomberg']",
    "nested": "div#reports table.quarterly",
    "nth_table": "table:nth-of-type(2)",
    "after_heading": "h2:contains('Sales') + table"
  }
}
```

#### Content-Based Selection
Select tables based on their content:
```json
{
  "selectors": {
    "financial_tables": {
      "selector": "table",
      "header_pattern": ".*Q[1-4] 20\\d{2}.*",  // Regex for headers
      "data_pattern": "\\$[\\d,]+\\.\\d{2}",     // Contains currency
      "contains_text": "Revenue",
      "min_rows": 10,
      "max_rows": 1000
    }
  }
}
```

### URL Pattern Support

#### Static Lists
```json
{
  "urls": [
    "https://site.com/data1.html",
    "https://site.com/data2.html"
  ]
}
```

#### Date Range Expansion
```json
{
  "urls": {
    "template": "https://site.com/data/{date}/report.html",
    "date_range": {
      "start": "2024-01-01",
      "end": "2024-01-31",
      "format": "yyyy-MM-dd"
    }
  }
}
```

#### Pagination Support
```json
{
  "urls": {
    "template": "https://site.com/list?page={page}",
    "pages": {
      "start": 1,
      "end": 10
    }
  }
}
```

### SQL Interface

```sql
-- Create schema with multiple web sources
CREATE SCHEMA web_data AS WEB('web_config.json');

-- Query specific extracted tables
SELECT * FROM web_data.sales_table;
SELECT * FROM web_data.product_tables_page1;
SELECT * FROM web_data.monthly_reports;

-- Dynamic extraction with selectors
SELECT * FROM web.extract(
  url := 'https://example.com/data.html',
  selector := 'table#specific-data'
);

-- Extract from multiple URLs
SELECT * FROM web.extract_all(
  urls := ARRAY[
    'https://site1.com/data.html',
    'https://site2.com/data.html'
  ],
  selector := 'table.price-list'
);

-- Union tables from multiple sources
WITH combined_prices AS (
  SELECT 'source1' as source, * 
  FROM web.extract('https://competitor1.com', 'table.prices')
  UNION ALL
  SELECT 'source2' as source, *
  FROM web.extract('https://competitor2.com', 'table#price-grid')
)
SELECT * FROM combined_prices;
```

### Implementation Components

#### WebTableExtractor Enhancement
```java
public class WebTableExtractor {
    
    public Map<String, DataFrame> extractTables(WebConfig config) {
        Map<String, DataFrame> tables = new HashMap<>();
        
        for (String urlPattern : config.getUrls()) {
            List<String> urls = expandUrlPattern(urlPattern);
            
            for (String url : urls) {
                Document doc = fetchDocument(url);
                
                for (Entry<String, SelectorConfig> entry : config.getSelectors().entrySet()) {
                    Elements selected = selectTables(doc, entry.getValue());
                    
                    if (!selected.isEmpty()) {
                        String tableName = generateTableName(entry.getKey(), url);
                        tables.put(tableName, convertToDataFrame(selected));
                    }
                }
            }
        }
        return tables;
    }
    
    private Elements selectTables(Document doc, SelectorConfig config) {
        Elements tables = doc.select(config.getSelector());
        
        // Apply filters
        if (config.getContainsText() != null) {
            tables = filterByText(tables, config.getContainsText());
        }
        
        if (config.getRequiredHeaders() != null) {
            tables = filterByHeaders(tables, config.getRequiredHeaders());
        }
        
        if (config.getMinRows() != null) {
            tables = filterByRowCount(tables, config.getMinRows(), config.getMaxRows());
        }
        
        return tables;
    }
}
```

### Use Cases

#### Financial Data Aggregation
```json
{
  "name": "market_data",
  "urls": [
    "https://finance.yahoo.com/quote/AAPL",
    "https://finance.yahoo.com/quote/GOOGL",
    "https://finance.yahoo.com/quote/MSFT"
  ],
  "selectors": {
    "historical_prices": "table[data-test='historical-prices']",
    "key_statistics": "table:contains('Valuation Measures')",
    "financials": "div.financials table"
  }
}
```

#### Government Data Collection
```json
{
  "name": "census_tables",
  "urls": [
    "https://census.gov/data/tables/2020/*.html",
    "https://census.gov/data/tables/2021/*.html"
  ],
  "selectors": {
    "population_data": {
      "selector": "table",
      "contains_text": "Population",
      "has_headers": ["State", "Population", "Change"]
    },
    "economic_data": {
      "selector": "table.economic-indicators",
      "min_rows": 10
    }
  }
}
```

#### Competitive Intelligence
```json
{
  "name": "competitor_monitoring",
  "urls": [
    "https://competitor1.com/pricing",
    "https://competitor2.com/products",
    "https://competitor3.com/catalog"
  ],
  "selectors": {
    "price_tables": {
      "selector": "table",
      "contains_text": ["$", "USD", "Price"],
      "data_pattern": "\\$[\\d,]+\\.\\d{2}"
    }
  },
  "refresh_interval": "daily"
}
```

### Advanced Features

#### Table Merging Strategies
```json
{
  "merge": {
    "strategy": "union_all",  // or "join", "concatenate"
    "key_columns": ["product_id", "date"],
    "deduplicate": true,
    "conflict_resolution": "latest"  // or "first", "merge"
  }
}
```

#### Schema Validation
```json
{
  "validation": {
    "expect_columns": ["id", "name", "price"],
    "column_types": {
      "id": "integer",
      "price": "decimal",
      "date": "date"
    },
    "fail_on_mismatch": false,
    "coerce_types": true
  }
}
```

#### Change Detection
```sql
-- Monitor tables for changes
CREATE MATERIALIZED VIEW price_changes AS
WITH current_prices AS (
  SELECT * FROM web.extract('https://competitor.com', 'table#prices')
),
previous_prices AS (
  SELECT * FROM price_changes_history
)
SELECT 
  c.*,
  p.price as previous_price,
  c.price - p.price as price_change,
  CURRENT_TIMESTAMP as detected_at
FROM current_prices c
LEFT JOIN previous_prices p ON c.product_id = p.product_id
WHERE c.price != p.price OR p.price IS NULL;
```

### Benefits

1. **Maintains Simplicity**: Still just extracting HTML tables - no complex content analysis
2. **Precise Targeting**: Extract exactly the tables you need
3. **Multi-Source Support**: Aggregate data from multiple pages/sites
4. **Predictable**: CSS selectors are well-understood and documented
5. **Flexible**: Can start simple and gradually add more sophisticated selectors
6. **Efficient**: Only extract and store the tables you actually need

### Implementation Priority

**Phase 1**: Multi-URL support with basic CSS selectors
**Phase 2**: Content-based filtering (contains_text, has_headers)
**Phase 3**: Pattern matching and regex support
**Phase 4**: URL templating and pagination
**Phase 5**: Change detection and monitoring

## Multi-URL Pattern-Based Web Scraping (Focused Feature)

**Problem**: Users often need to scrape the same data structure from multiple URLs that follow a pattern - like fetching stock summaries for different tickers, product details for different SKUs, or weather data for different cities. Currently, each URL requires a separate table definition, leading to repetitive configuration and maintenance overhead.

**Proposed Solution**: A lightweight extension to the existing HTML table extraction that supports URL patterns with variable substitution and batch fetching, perfect for aggregating structured data from multiple similar web pages.

### Core Concept

Enable pattern-based URL definitions with variable substitution using an array of variable dictionaries:

```json
{
  "name": "stock_summaries",
  "type": "custom",
  "operand": {
    "urls": "https://finance.example.com/quote/{ticker}/summary",
    "variables": [
      {"ticker": "AAPL", "company": "Apple"},
      {"ticker": "GOOGL", "company": "Google"},
      {"ticker": "MSFT", "company": "Microsoft"},
      {"ticker": "AMZN", "company": "Amazon"},
      {"ticker": "TSLA", "company": "Tesla"}
    ],
    "selector": "table.summary-data",
    "addSourceColumns": ["ticker", "company"]  // Which variables to include as columns
  }
}
```

This single configuration would fetch and aggregate data from 5 different URLs, adding source columns to track which ticker and company each row came from.

### Use Cases

#### Stock Market Data Aggregation
```sql
-- Single query across multiple tickers
SELECT ticker, price, volume, market_cap, pe_ratio
FROM stock_summaries
WHERE pe_ratio < 25
ORDER BY market_cap DESC;
```

#### E-commerce Price Monitoring
```json
{
  "name": "product_prices",
  "type": "custom",
  "operand": {
    "urls": [
      "https://store1.com/product/{sku}",
      "https://store2.com/item/{sku}",
      "https://store3.com/p/{sku}"
    ],
    "variables": [
      {"sku": "ABC123", "product_name": "Widget A", "category": "widgets"},
      {"sku": "DEF456", "product_name": "Gadget B", "category": "gadgets"},
      {"sku": "GHI789", "product_name": "Tool C", "category": "tools"}
    ],
    "selector": "div.price-info table",
    "addSourceColumns": ["sku", "product_name", "category", "_url_index"],
    "addTimestampColumn": true
  }
}
```

#### Geographic Data Collection
```json
{
  "name": "city_weather",
  "type": "custom",
  "operand": {
    "urls": "https://weather.api.com/{country}/{city}",
    "variables": [
      {"country": "us", "city": "new-york", "timezone": "EST", "lat": 40.7128, "lon": -74.0060},
      {"country": "uk", "city": "london", "timezone": "GMT", "lat": 51.5074, "lon": -0.1278},
      {"country": "ca", "city": "toronto", "timezone": "EST", "lat": 43.6532, "lon": -79.3832}
    ],
    "selector": "#current-conditions",
    "addSourceColumns": ["city", "country", "timezone"],
    "refreshInterval": "30 minutes"
  }
}
```

#### Real Estate Listings with Metadata
```json
{
  "name": "property_listings",
  "type": "custom",
  "operand": {
    "urls": "https://realestate.com/listing/{mls_id}",
    "variables": [
      {"mls_id": "ML81234567", "neighborhood": "Downtown", "listing_agent": "Jane Smith"},
      {"mls_id": "ML81234568", "neighborhood": "Westside", "listing_agent": "John Doe"},
      {"mls_id": "ML81234569", "neighborhood": "Eastside", "listing_agent": "Jane Smith"}
    ],
    "selector": "div.property-details table",
    "addSourceColumns": ["mls_id", "neighborhood", "listing_agent"]
  }
}
```

### Implementation Details

#### URL Expansion Logic
```java
public class UrlPatternExpander {
    // Now takes an array of variable dictionaries instead of cartesian product
    public List<ExpandedUrl> expandUrls(String pattern, List<Map<String, Object>> variables) {
        List<ExpandedUrl> expandedUrls = new ArrayList<>();
        
        for (Map<String, Object> variableSet : variables) {
            String url = pattern;
            // Replace each variable in the pattern
            for (Entry<String, Object> var : variableSet.entrySet()) {
                url = url.replace("{" + var.getKey() + "}", String.valueOf(var.getValue()));
            }
            
            // Store the URL along with its variable values for source tracking
            expandedUrls.add(new ExpandedUrl(url, variableSet));
        }
        
        return expandedUrls;
    }
    
    // For multiple URL patterns (like different stores with same SKU)
    public List<ExpandedUrl> expandMultiplePatterns(List<String> patterns, 
                                                     List<Map<String, Object>> variables) {
        List<ExpandedUrl> allUrls = new ArrayList<>();
        
        for (String pattern : patterns) {
            for (Map<String, Object> variableSet : variables) {
                String url = pattern;
                for (Entry<String, Object> var : variableSet.entrySet()) {
                    url = url.replace("{" + var.getKey() + "}", String.valueOf(var.getValue()));
                }
                
                // Add pattern index for tracking which URL pattern was used
                Map<String, Object> enrichedVars = new HashMap<>(variableSet);
                enrichedVars.put("_url_pattern_index", patterns.indexOf(pattern));
                allUrls.add(new ExpandedUrl(url, enrichedVars));
            }
        }
        
        return allUrls;
    }
}

class ExpandedUrl {
    final String url;
    final Map<String, Object> variables;
    
    ExpandedUrl(String url, Map<String, Object> variables) {
        this.url = url;
        this.variables = variables;
    }
}
```

#### Batch Fetching with Rate Limiting
```java
public class BatchWebTableFetcher {
    private final RateLimiter rateLimiter = RateLimiter.create(2.0); // 2 requests/second
    private final ExecutorService executor = Executors.newFixedThreadPool(3);
    
    public List<TableData> fetchTables(List<String> urls, String selector) {
        List<CompletableFuture<TableData>> futures = urls.stream()
            .map(url -> CompletableFuture.supplyAsync(() -> {
                rateLimiter.acquire();
                return fetchSingleTable(url, selector);
            }, executor))
            .collect(Collectors.toList());
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()))
            .join();
    }
}
```

#### Source Tracking
```java
public class SourceAwareTable extends AbstractTable {
    private final List<String> sourceColumns;
    private final boolean addTimestamp;
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        
        // Add original columns from scraped table
        for (RelDataTypeField field : baseRowType.getFieldList()) {
            builder.add(field);
        }
        
        // Add requested source tracking columns from variables
        for (String columnName : sourceColumns) {
            builder.add(columnName, SqlTypeName.VARCHAR);
        }
        
        // Add metadata columns
        if (addTimestamp) {
            builder.add("_fetched_at", SqlTypeName.TIMESTAMP);
        }
        builder.add("_source_url", SqlTypeName.VARCHAR);
        
        return builder.build();
    }
    
    // Enrich each row with source variable values
    public Object[] enrichRow(Object[] originalRow, Map<String, Object> variables, String url) {
        List<Object> enrichedRow = new ArrayList<>(Arrays.asList(originalRow));
        
        // Add variable values as columns
        for (String columnName : sourceColumns) {
            enrichedRow.add(variables.get(columnName));
        }
        
        // Add metadata
        if (addTimestamp) {
            enrichedRow.add(new Timestamp(System.currentTimeMillis()));
        }
        enrichedRow.add(url);
        
        return enrichedRow.toArray();
    }
}
```

### Advanced Example: Variable Selectors

You could even use variables in the selector pattern:

```json
{
  "name": "competitor_prices",
  "type": "custom",
  "operand": {
    "urls": "{base_url}/product/{sku}",
    "selector": "{selector}",
    "variables": [
      {
        "base_url": "https://amazon.com",
        "sku": "B08N5WRWNW",
        "selector": "span.a-price-whole",
        "competitor": "Amazon",
        "product": "Echo Dot"
      },
      {
        "base_url": "https://bestbuy.com",
        "sku": "6430060",
        "selector": "div.pricing-price__regular-price",
        "competitor": "Best Buy",
        "product": "Echo Dot"
      },
      {
        "base_url": "https://walmart.com",  
        "sku": "735005303",
        "selector": "span[itemprop='price']",
        "competitor": "Walmart",
        "product": "Echo Dot"
      }
    ],
    "addSourceColumns": ["competitor", "product", "sku"]
  }
}
```

This gives you complete flexibility - different URLs, different selectors, all in one unified table!

### Configuration Options

```json
{
  "multiUrlConfig": {
    "urls": "string or array",           // URL pattern(s) with {variables}
    "variables": [],                     // Array of variable dictionaries
    "selector": "string",                // CSS selector (can also use {variables})
    "addSourceColumn": true,             // Track source URL
    "addTimestampColumn": true,          // Track fetch time
    "failureStrategy": "skip|fail",      // How to handle failed fetches
    "maxConcurrent": 3,                  // Max parallel fetches
    "requestsPerSecond": 2,              // Rate limiting
    "timeout": 10000,                    // Timeout per request (ms)
    "retries": 2,                       // Retry failed requests
    "cacheMinutes": 60,                 // Cache fetched data
    "userAgent": "custom",              // Custom user agent
    "headers": {}                       // Additional HTTP headers
  }
}
```

### Benefits

1. **Massive Configuration Reduction**: One table definition instead of dozens
2. **Automatic Aggregation**: Built-in data combining from multiple sources
3. **Source Tracking**: Know where each row came from
4. **Rate Limiting**: Respect server limits automatically
5. **Parallel Fetching**: Fast data collection
6. **Cache Management**: Avoid redundant requests

### Example Query Patterns

```sql
-- Compare prices across stores
SELECT sku, MIN(price) as best_price, 
       ARRAY_AGG(DISTINCT _source_url) as available_at
FROM product_prices
GROUP BY sku;

-- Track stock movements
SELECT ticker, 
       price as current_price,
       LAG(price) OVER (PARTITION BY ticker ORDER BY _fetched_at) as previous_price,
       price - LAG(price) OVER (PARTITION BY ticker ORDER BY _fetched_at) as change
FROM stock_summaries;

-- Find anomalies across locations
SELECT city, temperature, humidity
FROM city_weather
WHERE temperature > (
    SELECT AVG(temperature) + 2 * STDDEV(temperature) 
    FROM city_weather
);
```

### ROI Analysis

**Effort**: 1 week (building on existing HTML extraction)
**Impact**: High - enables entire new class of use cases
**Risk**: Low - extends existing functionality
**Priority**: HIGH - small effort, big user value

This focused feature would make the file adapter incredibly powerful for web data aggregation scenarios like:
- Financial data monitoring
- E-commerce price tracking  
- Weather/environmental monitoring
- Sports statistics aggregation
- Real estate listings compilation
- Job posting aggregation

The beauty is its simplicity - it's just a pattern expander on top of the existing HTML table extraction, but it unlocks massive value for users who need to aggregate structured data from multiple similar sources.

## Parallelization Framework for Performance Optimization

**Problem**: The file adapter currently processes most operations sequentially - table discovery, file reading, statistics generation, and data conversion all happen one at a time. This leaves significant performance on the table, especially for multi-core systems and I/O-bound operations that could overlap.

**Proposed Solution**: Implement a comprehensive parallelization framework that identifies and parallelizes independent operations throughout the file adapter, providing 3-10x performance improvements for common operations.

### High-Impact Parallelization Opportunities

#### 1. Schema Discovery & Table Loading
**Current State**: Tables are discovered and loaded sequentially during schema initialization
```java
// Current: Sequential processing
for (File file : files) {
    Table table = createTable(file);
    tables.put(name, table);
}
```

**Parallel Implementation**:
```java
// Parallel table creation using ForkJoinPool
Map<String, Table> tables = files.parallelStream()
    .collect(Collectors.toConcurrentMap(
        file -> deriveTableName(file),
        file -> createTable(file)
    ));
```

**Expected Speedup**: 5-10x for schemas with many tables
**Risk**: Low - table creation is independent

#### 2. Multi-File Queries (Glob/Partitioned Tables)
**Current State**: Multiple Parquet files in partitioned tables are read sequentially
```java
// Current: Sequential partition reading
List<Object[]> allResults = new ArrayList<>();
for (String partition : partitions) {
    allResults.addAll(readPartition(partition));
}
```

**Parallel Implementation**:
```java
// Parallel partition reading with CompletableFuture
CompletableFuture<List<Object[]>>[] futures = partitions.stream()
    .map(partition -> CompletableFuture.supplyAsync(
        () -> readPartition(partition), 
        ioExecutor))
    .toArray(CompletableFuture[]::new);

List<Object[]> allResults = CompletableFuture.allOf(futures)
    .thenApply(v -> Arrays.stream(futures)
        .map(CompletableFuture::join)
        .flatMap(List::stream)
        .collect(Collectors.toList()))
    .join();
```

**Expected Speedup**: Linear with number of files (up to I/O bandwidth)
**Risk**: Medium - needs memory management for large result sets

#### 3. Statistics Generation (HLL Sketches)
**Current State**: HyperLogLog sketches are built sequentially for each column
```java
// Current: Sequential HLL sketch generation
Map<String, HyperLogLogPlus> sketches = new HashMap<>();
for (String column : columns) {
    sketches.put(column, buildHLLSketch(column));
}
```

**Parallel Implementation**:
```java
// Parallel column statistics computation
Map<String, HyperLogLogPlus> sketches = columns.parallelStream()
    .collect(Collectors.toConcurrentMap(
        column -> column,
        column -> buildHLLSketch(column)
    ));
```

**Expected Speedup**: 3-5x for wide tables (many columns)
**Risk**: Low - column processing is independent

### Medium-Impact Opportunities

#### 4. Batch Processing with Read-Ahead
**Current State**: VectorizedParquetReader reads batches synchronously
```java
// Current: Synchronous batch reading
public List<Object[]> readBatch() {
    // Read batch, block until complete
    return readNextBatchFromFile();
}
```

**Parallel Implementation**:
```java
public class PrefetchingParquetReader {
    private final BlockingQueue<CompletableFuture<List<Object[]>>> batchQueue;
    private final ExecutorService prefetchExecutor;
    
    public PrefetchingParquetReader() {
        this.batchQueue = new ArrayBlockingQueue<>(prefetchSize);
        // Start background prefetching
        startPrefetching();
    }
    
    private void startPrefetching() {
        prefetchExecutor.submit(() -> {
            while (hasMoreData()) {
                CompletableFuture<List<Object[]>> future = 
                    CompletableFuture.supplyAsync(this::readNextBatchFromFile);
                batchQueue.offer(future);
            }
        });
    }
    
    public List<Object[]> readBatch() {
        // Return already-fetched batch
        return batchQueue.poll().join();
    }
}
```

**Expected Speedup**: 20-30% for I/O bound queries
**Risk**: Low - well-established pattern

#### 5. Parallel File Format Conversion
**Current State**: Files are converted to Parquet one at a time
```java
// Current: Sequential conversion
for (File csvFile : csvFiles) {
    File parquetFile = convertToParquet(csvFile);
    parquetFiles.add(parquetFile);
}
```

**Parallel Implementation**:
```java
// Parallel conversion with rate limiting
ExecutorService conversionPool = Executors.newFixedThreadPool(
    config.getConversionThreads());
    
List<CompletableFuture<File>> conversions = csvFiles.stream()
    .map(csv -> CompletableFuture.supplyAsync(
        () -> convertToParquet(csv), 
        conversionPool))
    .collect(Collectors.toList());

List<File> parquetFiles = CompletableFuture.allOf(
    conversions.toArray(new CompletableFuture[0]))
    .thenApply(v -> conversions.stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList()))
    .join();
```

**Expected Speedup**: 3-4x for initial load
**Risk**: Medium - I/O contention possible

#### 6. Storage Provider Parallel Downloads
**Current State**: Files downloaded sequentially from S3/SharePoint/HTTP
```java
// Current: Sequential downloads
for (String key : s3Keys) {
    downloadFile(bucket, key, localPath);
}
```

**Parallel Implementation**:
```java
// Parallel downloads with S3 TransferManager
S3TransferManager transferManager = S3TransferManager.builder()
    .s3Client(s3Client)
    .maxConcurrency(10)
    .build();

List<CompletedFileDownload> downloads = s3Keys.parallelStream()
    .map(key -> {
        DownloadFileRequest request = DownloadFileRequest.builder()
            .getObjectRequest(req -> req.bucket(bucket).key(key))
            .destination(Paths.get(localPath, key))
            .build();
        return transferManager.downloadFile(request).completionFuture();
    })
    .map(CompletableFuture::join)
    .collect(Collectors.toList());
```

**Expected Speedup**: 5-10x for remote files
**Risk**: Low - standard practice for cloud storage

### Quick Win Opportunities

#### 7. CSV Type Inference Parallelization
```java
// Parallel column type inference
Map<String, SqlTypeName> columnTypes = columns.parallelStream()
    .collect(Collectors.toConcurrentMap(
        column -> column,
        column -> inferTypeFromSamples(columnSamples.get(column))
    ));
```
**Expected Speedup**: 2x for wide CSVs

#### 8. Parallel Join Scanning
```java
// Scan both sides of join in parallel
CompletableFuture<List<Row>> leftScan = 
    CompletableFuture.supplyAsync(() -> scanTable(leftTable));
CompletableFuture<List<Row>> rightScan = 
    CompletableFuture.supplyAsync(() -> scanTable(rightTable));

JoinResult result = CompletableFuture.allOf(leftScan, rightScan)
    .thenApply(v -> performJoin(leftScan.join(), rightScan.join()))
    .join();
```
**Expected Speedup**: 2x for join operations

### Implementation Architecture

#### Thread Pool Management
```java
public class FileAdapterExecutors {
    // CPU-bound tasks (parsing, type inference, HLL computation)
    private static final ForkJoinPool CPU_POOL = new ForkJoinPool(
        Runtime.getRuntime().availableProcessors(),
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        null, // Default exception handler
        true  // Async mode for better throughput
    );
    
    // I/O-bound tasks (file reading, network operations)
    private static final ExecutorService IO_POOL = new ThreadPoolExecutor(
        4,     // Core pool size
        32,    // Maximum pool size
        60L, TimeUnit.SECONDS, // Keep-alive time
        new LinkedBlockingQueue<>(100), // Bounded queue
        new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("FileAdapter-IO-" + counter.incrementAndGet());
                return t;
            }
        },
        new ThreadPoolExecutor.CallerRunsPolicy() // Backpressure strategy
    );
    
    // Scheduled tasks (cache refresh, statistics update)
    private static final ScheduledExecutorService SCHEDULED_POOL = 
        Executors.newScheduledThreadPool(2);
}
```

#### Memory Management Strategies
```java
public class ParallelMemoryManager {
    private final Semaphore memoryPermits;
    private final long maxMemoryBytes;
    
    public ParallelMemoryManager(long maxMemoryBytes) {
        this.maxMemoryBytes = maxMemoryBytes;
        // Each permit represents 1MB of memory
        this.memoryPermits = new Semaphore((int)(maxMemoryBytes / (1024 * 1024)));
    }
    
    public <T> CompletableFuture<T> executeWithMemoryLimit(
            Supplier<T> task, long estimatedMemoryBytes) {
        int permits = (int)(estimatedMemoryBytes / (1024 * 1024));
        return CompletableFuture.supplyAsync(() -> {
            try {
                memoryPermits.acquire(permits);
                return task.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                memoryPermits.release(permits);
            }
        });
    }
}
```

#### Configuration Schema
```json
{
  "parallelism": {
    "enabled": true,
    "schemaDiscovery": {
      "threads": 4,
      "enabled": true
    },
    "fileReading": {
      "threads": 8,
      "prefetchBatches": 2,
      "enabled": true
    },
    "conversion": {
      "threads": 2,
      "maxConcurrent": 4,
      "enabled": true
    },
    "statistics": {
      "threads": 4,
      "enabled": true
    },
    "downloads": {
      "maxConcurrency": 10,
      "connectionPoolSize": 20,
      "enabled": true
    },
    "memory": {
      "maxParallelMemoryMb": 1024,
      "backpressureThreshold": 0.8
    }
  }
}
```

### Performance Expectations

| Operation | Current Time | Parallel Time | Speedup | Threads |
|-----------|-------------|---------------|---------|---------|
| Schema with 100 tables | 10s | 1-2s | 5-10x | 4 |
| Query 10 partitions | 5s | 0.8-1s | 5-6x | 10 |
| Statistics for 50 columns | 30s | 8-10s | 3-4x | 4 |
| S3 download 20 files | 20s | 2-3s | 7-10x | 10 |
| Convert 10 CSV to Parquet | 15s | 4-5s | 3-4x | 4 |
| Wide table (100 cols) type inference | 8s | 2s | 4x | 8 |
| Join two large tables | 10s | 5-6s | 1.7-2x | 2 |

### Risk Mitigation

**Thread Safety**:
- Use concurrent collections (ConcurrentHashMap, CopyOnWriteArrayList)
- Immutable data structures where possible
- Proper synchronization for shared state

**Resource Management**:
- Bounded queues to prevent memory exhaustion
- Semaphore-based memory limiting
- Graceful degradation under load

**Error Handling**:
- Proper exception propagation in CompletableFuture chains
- Fallback to sequential processing on failure
- Comprehensive logging for debugging

### Implementation Phases

**Phase 1 - Quick Wins** (1 week):
- Parallel schema discovery
- CSV type inference parallelization
- Basic thread pool setup

**Phase 2 - Core Operations** (2 weeks):
- Multi-file parallel queries
- Parallel statistics generation
- Storage provider parallel downloads

**Phase 3 - Advanced Features** (2 weeks):
- Batch prefetching
- Parallel format conversion
- Memory management framework

**Phase 4 - Optimization** (1 week):
- Performance tuning
- Adaptive parallelism based on system load
- Monitoring and metrics

### Benefits

1. **Dramatic Performance Improvement**: 3-10x speedup for common operations
2. **Better Resource Utilization**: Full use of multi-core processors
3. **Improved Responsiveness**: Reduced query latency
4. **Scalability**: Better handling of large schemas and datasets
5. **User Experience**: Faster schema loading and query execution

### Compatibility Notes

- Backward compatible - parallelism can be disabled via configuration
- Thread-safe implementation preserves correctness
- Gradual rollout possible with feature flags
- No changes to public APIs required