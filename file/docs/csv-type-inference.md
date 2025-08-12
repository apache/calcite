# CSV Type Inference Documentation

## Overview

The File Adapter supports automatic type inference for CSV files. Instead of treating all columns as VARCHAR, the system can analyze CSV data and automatically detect appropriate types like INTEGER, DOUBLE, DATE, TIME, TIMESTAMP, and BOOLEAN.

## Features

- **Automatic Type Detection**: Analyzes CSV data to identify column types
- **Sampling Strategy**: Configurable sampling rate to balance accuracy vs performance
- **Safe Defaults**: All inferred types are nullable by default
- **Temporal Type Support**: Detects dates, times, and timestamps including RFC-formatted strings
- **Confidence Threshold**: Only uses inferred types when confidence is high enough

## Configuration

### Schema-Level Configuration (model.json)

```json
{
  "version": "1.0",
  "defaultSchema": "CSV",
  "schemas": [
    {
      "name": "CSV",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/path/to/csv/files",
        
        // CSV Type Inference Configuration
        "csvTypeInference": {
          "enabled": true,                    // Enable type inference (default: false)
          "samplingRate": 0.1,                // Sample 10% of rows (default: 0.1)
          "maxSampleRows": 1000,              // Max rows to sample (default: 1000)
          "confidenceThreshold": 0.95,        // Min confidence for type (default: 0.95)
          "makeAllNullable": true,            // Make all types nullable (default: true)
          "nullableThreshold": 0.01,          // If makeAllNullable=false, null ratio threshold
          "inferDates": true,                 // Infer DATE types (default: true)
          "inferTimes": true,                 // Infer TIME types (default: true)
          "inferTimestamps": true             // Infer TIMESTAMP types (default: true)
        }
      }
    }
  ]
}
```

### Table-Level Configuration

You can override type inference settings for specific tables:

```json
{
  "tables": [
    {
      "name": "SALES",
      "type": "csv",
      "path": "sales.csv",
      "csvTypeInference": {
        "enabled": true,
        "samplingRate": 0.5,     // Sample 50% for this critical table
        "maxSampleRows": 5000     // Sample more rows for better accuracy
      }
    },
    {
      "name": "LARGE_LOG",
      "type": "csv", 
      "path": "large_log.csv",
      "csvTypeInference": {
        "enabled": true,
        "samplingRate": 0.01,    // Only sample 1% of this large file
        "maxSampleRows": 10000    // But sample up to 10k rows
      }
    },
    {
      "name": "LEGACY_DATA",
      "type": "csv",
      "path": "legacy.csv",
      "csvTypeInference": {
        "enabled": false          // Disable inference for this table
      }
    }
  ]
}
```

### JDBC URL Configuration

Type inference can also be configured via JDBC URL parameters:

```
jdbc:calcite:model=inline:{
  "schemas": [{
    "name": "CSV",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/data",
      "csv_type_inference_enabled": true,
      "csv_type_inference_sampling_rate": 0.2,
      "csv_type_inference_confidence": 0.9
    }
  }]
}
```

## Type Detection Rules

### Numeric Types
- **INTEGER**: Whole numbers that fit in 32-bit range
- **BIGINT**: Whole numbers outside INTEGER range
- **DOUBLE**: Numbers with decimal points or scientific notation

### Boolean Type
- **BOOLEAN**: Matches true/false, TRUE/FALSE, True/False, 0/1

### Temporal Types
- **DATE**: Common date formats (yyyy-MM-dd, MM/dd/yyyy, dd/MM/yyyy, etc.)
- **TIME**: Time formats (HH:mm:ss, h:mm a, etc.)
- **TIMESTAMP**: Date-time combinations
- **TIMESTAMP WITH TIME ZONE**: RFC-formatted timestamps with timezone info

### String Type
- **VARCHAR**: Default fallback for unrecognized patterns

## Null Handling

The type inferrer recognizes various null representations:
- Empty strings
- NULL, null, Null
- NA, N/A
- NONE, None
- NIL, nil

By default, all inferred types are nullable for safety. This can be configured:

```json
{
  "csvTypeInference": {
    "makeAllNullable": false,      // Don't force nullable
    "nullableThreshold": 0.05      // Make nullable if >5% nulls
  }
}
```

## Performance Considerations

1. **Sampling Rate**: Lower rates (e.g., 0.01) are faster but may miss patterns
2. **Max Sample Rows**: Limits processing time for large files
3. **Caching**: Inferred types are cached with the table schema
4. **First Query Impact**: Type inference happens on first table access

## Example Use Cases

### Financial Data
```json
{
  "csvTypeInference": {
    "enabled": true,
    "samplingRate": 0.5,          // Higher sampling for accuracy
    "confidenceThreshold": 0.99,  // Require very high confidence
    "makeAllNullable": false,     // Some columns may be required
    "nullableThreshold": 0.001    // Very low null tolerance
  }
}
```

### Log Files
```json
{
  "csvTypeInference": {
    "enabled": true,
    "samplingRate": 0.001,        // Very low sampling for huge files
    "maxSampleRows": 10000,       // But sample reasonable amount
    "inferTimestamps": true       // Important for log analysis
  }
}
```

### Mixed Format Data
```json
{
  "csvTypeInference": {
    "enabled": true,
    "confidenceThreshold": 0.8,   // Lower threshold for messy data
    "makeAllNullable": true       // Always nullable for safety
  }
}
```

## Troubleshooting

### Types Not Being Inferred
1. Check if type inference is enabled
2. Verify sampling rate is not too low
3. Check confidence threshold - may be too high
4. Review logs for inference details

### Wrong Types Detected
1. Increase sampling rate for better coverage
2. Increase max sample rows
3. Adjust confidence threshold
4. Consider disabling specific type inference (dates, times, etc.)

### Performance Issues
1. Reduce sampling rate
2. Reduce max sample rows
3. Consider disabling for very large files
4. Use table-specific configuration

## Migration from Legacy Behavior

To maintain backward compatibility, type inference is **disabled by default**. Existing schemas will continue to work unchanged. To enable:

1. Add `csvTypeInference` configuration to your schema
2. Test with a small sampling rate first
3. Adjust configuration based on results
4. Consider table-specific overrides for special cases

## Programmatic Usage

```java
// Create configuration
CsvTypeInferrer.TypeInferenceConfig config = 
    CsvTypeInferrer.TypeInferenceConfig.builder()
        .enabled(true)
        .samplingRate(0.1)
        .maxSampleRows(1000)
        .makeAllNullable(true)
        .build();

// Infer types
List<CsvTypeInferrer.ColumnTypeInfo> types = 
    CsvTypeInferrer.inferTypes(source, config, "UNCHANGED");

// Use inferred types
for (ColumnTypeInfo info : types) {
    System.out.println(info.columnName + ": " + info.inferredType + 
                      " (nullable=" + info.nullable + ")");
}
```