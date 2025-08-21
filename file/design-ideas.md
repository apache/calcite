# Design Ideas for File Adapter

## Environment Variable Substitution in Model Files

**Problem**: Model files (JSON format) don't support environment variable substitution, making it difficult to configure execution engines and other settings dynamically across test suites and deployments.

**Proposed Solution**: Add support for environment variable substitution in model JSON files using syntax like `${VAR_NAME}` or `${VAR_NAME:default_value}`.

### Example Usage:
```json
{
  "version": "1.0",
  "defaultSchema": "MY_SCHEMA",
  "schemas": [
    {
      "name": "MY_SCHEMA",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "${DATA_DIR:/data}",
        "executionEngine": "${CALCITE_FILE_ENGINE_TYPE:CSV}",
        "parquetCacheDirectory": "${PARQUET_CACHE_DIR}"
      }
    }
  ]
}
```

### Implementation Notes:
- Parse model JSON and replace `${VAR}` patterns with environment variable values
- Support default values with `${VAR:default}` syntax
- Handle in FileSchemaFactory or model parsing layer
- Would simplify test configuration and deployment scenarios

## Duplicate Schema Name Detection

**Problem**: Multiple schemas with the same name within a single connection can cause conflicts and unpredictable behavior.

**Proposed Solution**: Add validation when adding schemas to detect and prevent duplicate schema names within the same connection/root schema.

### Implementation:
- Check for existing schema with same name when adding to root schema
- Throw descriptive error if duplicate detected
- Helps identify configuration issues early

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