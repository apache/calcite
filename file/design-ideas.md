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