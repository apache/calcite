# Troubleshooting

This guide covers common issues, error messages, and solutions when using the Apache Calcite File Adapter.

## Common Configuration Issues

### Schema Not Found

**Error:**
```
org.apache.calcite.sql.validate.SqlValidatorException: Object 'FILES' not found
```

**Solution:**
1. Verify schema name in model configuration matches query
2. Check that schema factory is correctly specified
3. Ensure directory path exists and is readable

```json
{
  "schemas": [{
    "name": "FILES",  // Must match query: SELECT * FROM FILES.tablename
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/path/to/data"  // Verify this path exists
    }
  }]
}
```

### Table Not Found

**Error:**
```
org.apache.calcite.sql.validate.SqlValidatorException: Object 'tablename' not found within 'FILES'
```

**Causes and Solutions:**

1. **File doesn't exist in directory:**
   ```bash
   # Check files in directory
   ls -la /path/to/data/
   ```

2. **Incorrect table naming:**
   - File: `customer-data.csv` → Table: `customer_data`
   - File: `SALES.JSON` → Table: `sales` (lowercase)

3. **File format not supported:**
   ```json
   {
     "filePatterns": ["*.csv", "*.json", "*.xlsx", "*.parquet"]
   }
   ```

4. **Recursive scanning disabled:**
   ```json
   {
     "recursive": true  // Enable to find files in subdirectories
   }
   ```

### Directory Access Issues

**Error:**
```
java.io.FileNotFoundException: /path/to/data (Permission denied)
```

**Solutions:**

1. **Check directory permissions:**
   ```bash
   ls -ld /path/to/data
   chmod 755 /path/to/data  # If needed
   ```

2. **Verify Java process permissions:**
   ```bash
   # Run as appropriate user
   sudo -u calcite-user java ...
   ```

3. **Configure security settings:**
   ```json
   {
     "security": {
       "allowedPaths": ["/path/to/data"],
       "validatePermissions": false
     }
   }
   ```

## File Format Issues

### CSV Type Inference Problems

**Error:**
```
java.lang.NumberFormatException: For input string: "N/A"
```

**Solution:**
Configure null handling and type inference:

```json
{
  "csvConfig": {
    "enableTypeInference": true,
    "nullStrings": ["", "null", "NULL", "N/A", "n/a", "-"],
    "inferenceOptions": {
      "sampleSize": 1000,
      "strictTypeInference": false
    }
  }
}
```

### JSON Parsing Errors

**Error:**
```
com.fasterxml.jackson.core.JsonParseException: Unexpected character
```

**Solutions:**

1. **Validate JSON format:**
   ```bash
   jq . data.json  # Validate JSON syntax
   ```

2. **Handle malformed JSON:**
   ```json
   {
     "jsonConfig": {
       "ignoreParseErrors": true,
       "skipMalformedRecords": true,
       "logParseErrors": true
     }
   }
   ```

3. **Configure encoding:**
   ```json
   {
     "encoding": "UTF-8"  // or "ISO-8859-1", "UTF-16"
   }
   ```

### Excel File Issues

**Error:**
```
java.io.IOException: Your InputStream was neither an OLE2 stream
```

**Solutions:**

1. **Check file format:**
   ```bash
   file document.xlsx  # Verify file type
   ```

2. **Handle different Excel versions:**
   ```json
   {
     "excelConfig": {
       "supportOldFormat": true,  // Support .xls files
       "strictMode": false
     }
   }
   ```

3. **Memory issues with large Excel files:**
   ```json
   {
     "excelConfig": {
       "streamingMode": true,
       "maxRowsInMemory": 10000
     }
   }
   ```

## Performance Issues

### Slow Query Execution

**Symptoms:**
- Queries taking longer than expected
- High CPU or memory usage
- Frequent garbage collection

**Diagnosis Steps:**

1. **Enable query timing:**
   ```sql
   !set verbose true
   EXPLAIN PLAN FOR SELECT * FROM large_table;
   ```

2. **Check execution engine:**
   ```json
   {
     "executionEngine": "parquet",  // Recommended for performance
     "enableStatistics": true
   }
   ```

3. **Monitor resource usage:**
   ```bash
   jstat -gc -t <pid> 1s    # Monitor GC behavior
   top -p <pid>             # Monitor CPU/memory
   ```

**Solutions:**

1. **Optimize execution engine:**
   ```json
   {
     "executionEngine": "parquet",
     "parquetConfig": {
       "enableStatistics": true,
       "compression": "snappy"
     }
   }
   ```

2. **Enable caching:**
   ```json
   {
     "cacheDirectory": "/fast-storage/.parquet_cache",
     "enableStatistics": true
   }
   ```

3. **Adjust memory settings:**
   ```bash
   -Xmx4g -Xms2g -XX:+UseG1GC
   ```

### Memory OutOfMemory Errors

**Error:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Immediate Solutions:**

1. **Increase heap size:**
   ```bash
   -Xmx8g -Xms4g
   ```

2. **Enable disk spillover:**
   ```json
   {
     "memoryConfig": {
       "enableSpillover": true,
       "spilloverThreshold": "512MB",
       "spilloverDirectory": "/tmp/calcite-spill"
     }
   }
   ```

3. **Reduce batch sizes:**
   ```json
   {
     "batchSize": 1000,
     "maxConcurrentFiles": 2
   }
   ```

**Long-term Solutions:**

1. **Optimize data layout:**
   - Use columnar formats (Parquet)
   - Partition large datasets
   - Compress data appropriately

2. **Tune garbage collection:**
   ```bash
   -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=32m
   ```

### High CPU Usage

**Causes:**
- Inefficient query plans
- Lack of filter pushdown
- Unoptimized joins

**Solutions:**

1. **Enable query optimization:**
   ```json
   {
     "optimizationRules": {
       "enableFilterPushdown": true,
       "enableColumnPruning": true,
       "enableJoinReordering": true
     }
   }
   ```

2. **Use appropriate execution engine:**
   ```json
   {
     "executionEngine": "duckdb",  // For analytical workloads
     "duckdbConfig": {
       "threads": 4,
       "enableOptimizations": true
     }
   }
   ```

## Storage Provider Issues

### Amazon S3 Connection Problems

**Error:**
```
com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied
```

**Solutions:**

1. **Verify credentials:**
   ```bash
   aws s3 ls s3://your-bucket/  # Test AWS CLI access
   ```

2. **Check IAM permissions:**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": ["s3:GetObject", "s3:ListBucket"],
       "Resource": ["arn:aws:s3:::your-bucket/*"]
     }]
   }
   ```

3. **Configure retry settings:**
   ```java
   Properties props = new Properties();
   props.setProperty("httpRetries", "3");  // Simple retry count
   ```

### HTTP Connection Timeouts

**Error:**
```
java.net.SocketTimeoutException: Read timed out
```

**Solutions:**

1. **Increase timeout values:**
   ```json
   {
     "storageProvider": {
       "type": "http",
       "options": {
         "connectionTimeout": "60s",
         "readTimeout": "300s"
       }
     }
   }
   ```

2. **Configure retry settings:**
   ```java
   Properties props = new Properties();
   props.setProperty("httpTimeout", "60000");  // 60 second timeout
   props.setProperty("httpRetries", "5");       // Retry 5 times
   ```
   
   **Note:** The adapter currently only supports simple retry counts, not advanced retry policies with exponential backoff or specific error handling.

3. **Use connection pooling:**
   ```json
   {
     "connectionPool": {
       "maxConnections": 20,
       "keepAliveTimeout": "300s"
     }
   }
   ```

### SharePoint Authentication Issues

**Error:**
```
microsoft.graph.authentication.AuthenticationException: Invalid client secret
```

**Solutions:**

1. **Verify application registration:**
   - Check client ID and secret in Azure portal
   - Ensure correct tenant ID
   - Verify application permissions

2. **Update authentication configuration:**
   ```json
   {
     "authentication": {
       "type": "oauth",
       "clientId": "${CLIENT_ID}",
       "clientSecret": "${CLIENT_SECRET}",
       "tenantId": "${TENANT_ID}",
       "scope": "Files.Read.All Sites.Read.All"
     }
   }
   ```

## Execution Engine Specific Issues

### Parquet Engine Problems

**Issue: Parquet conversion failures**

**Error:**
```
org.apache.parquet.hadoop.ParquetWriter$WriteException
```

**Solutions:**

1. **Check disk space:**
   ```bash
   df -h .parquet_cache/
   ```

2. **Verify permissions:**
   ```bash
   ls -ld .parquet_cache/
   chmod 755 .parquet_cache/
   ```

3. **Configure compression:**
   ```json
   {
     "parquetConfig": {
       "compression": "snappy",  // More reliable than gzip
       "enableValidation": false  // If strict validation causes issues
     }
   }
   ```

### DuckDB Engine Issues

**Issue: DuckDB connection problems**

**Error:**
```
java.sql.SQLException: DuckDB connection failed
```

**Solutions:**

1. **Check native library:**
   ```bash
   # Verify DuckDB native library is available
   java -Djava.library.path=/path/to/duckdb/libs -cp ... YourApp
   ```

2. **Configure memory limits:**
   ```json
   {
     "duckdbConfig": {
       "memoryLimit": "2GB",
       "maxMemoryUsage": 0.8
     }
   }
   ```

3. **Enable vectorized processing:**
   ```json
   {
     "duckdbConfig": {
       "enableVectorization": true,
       "vectorSize": 2048
     }
   }
   ```

### Arrow Engine Memory Issues

**Error:**
```
io.netty.util.internal.OutOfDirectMemoryError
```

**Solutions:**

1. **Increase direct memory:**
   ```bash
   -XX:MaxDirectMemorySize=2g
   ```

2. **Configure Arrow memory management:**
   ```json
   {
     "arrowConfig": {
       "memoryPool": "system",
       "maxAllocationSize": "1GB"
     }
   }
   ```

## Debugging and Logging

### Enable Debug Logging

```json
{
  "logging": {
    "level": "DEBUG",
    "loggers": {
      "org.apache.calcite.adapter.file": "DEBUG",
      "org.apache.calcite.plan": "INFO",
      "org.apache.calcite.sql": "DEBUG"
    }
  }
}
```

### Useful Log Patterns

**Query execution timing:**
```
DEBUG org.apache.calcite.adapter.file - Query execution time: 1234ms
```

**File discovery:**
```
DEBUG org.apache.calcite.adapter.file.FileSchemaFactory - Discovered 45 files in /data
```

**Cache operations:**
```
DEBUG org.apache.calcite.adapter.file.cache - Cache hit for file: data.csv
```

### Profiling Tools

1. **JProfiler/VisualVM** for JVM profiling
2. **JMX monitoring** for runtime metrics
3. **Query plan analysis** with EXPLAIN

```sql
-- Analyze query plan
EXPLAIN PLAN FOR 
SELECT customer_id, SUM(amount) 
FROM sales 
WHERE date_col >= '2024-01-01' 
GROUP BY customer_id;
```

## System Requirements and Compatibility

### Minimum System Requirements

- **Java:** OpenJDK 11 or later
- **Memory:** 2GB RAM minimum, 4GB+ recommended
- **Disk:** 1GB free space for cache
- **CPU:** 2+ cores recommended

### Compatibility Issues

**Java Version:**
```bash
java -version  # Verify Java 11+
```

**Memory Settings:**
```bash
# For Java 11+
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

**Operating System:**
- Linux: Full support
- Windows: Full support  
- macOS: Full support
- Docker: Supported with proper volume mounts

## Getting Help

### Log Collection

When reporting issues, include:

1. **Configuration file** (sanitized of credentials)
2. **Error logs** with full stack traces
3. **System information:**
   ```bash
   java -version
   uname -a
   df -h
   free -h
   ```
4. **Sample data** that reproduces the issue
5. **SQL queries** that cause problems

### Performance Baseline

Establish performance baselines:

```sql
-- Simple performance test
SELECT COUNT(*) FROM large_table;

-- Measure timing
\timing on
SELECT customer_id, SUM(amount) FROM sales GROUP BY customer_id;
```

### Community Resources

- **Apache Calcite Documentation:** https://calcite.apache.org/docs/
- **Mailing Lists:** dev@calcite.apache.org
- **Issue Tracker:** https://issues.apache.org/jira/projects/CALCITE
- **Stack Overflow:** Tag questions with `apache-calcite`

### Creating Minimal Reproducible Examples

When reporting bugs:

1. **Minimal dataset:** Create smallest possible data that shows the issue
2. **Minimal configuration:** Remove unnecessary options
3. **Exact error message:** Include full stack trace
4. **Environment details:** Java version, OS, memory settings
5. **Steps to reproduce:** Clear step-by-step instructions