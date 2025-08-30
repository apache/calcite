# JDBC 4.3 Compliant Type Conversions in Splunk Adapter

## Overview

The Splunk adapter now supports JDBC 4.3 compliant type conversions, specifically allowing VARCHAR/CHAR columns containing numeric strings to be read using numeric getter methods like `getInt()`, `getLong()`, `getDouble()`, etc.

## Background

According to the JDBC 4.3 Specification (Section 15.2.3 - "Conversions by ResultSet getter Methods"):

1. **It's explicitly allowed**: The JDBC spec includes a conversion matrix showing that VARCHAR/CHAR columns CAN be read using getInt(), getLong(), getDouble(), etc.

2. **The driver SHOULD attempt conversion**: When you call getInt() on a VARCHAR column containing '123', the driver should:
   - Attempt to parse the string as an integer
   - Throw SQLException if the string isn't numeric

3. **Most major databases support it**:
   - PostgreSQL: ✅ Converts '123' to 123
   - MySQL: ✅ Converts '123' to 123
   - Oracle: ✅ Converts '123' to 123
   - SQL Server: ✅ Converts '123' to 123
   - Splunk JDBC: ✅ **Now supported!**

## Implementation Details

The enhancement was implemented in the `SplunkDataConverter` class by:

1. **Adding VARCHAR to numeric conversion support** in the `convertValue()` method
2. **Following JDBC 4.3 specification** for type conversion behavior
3. **Proper error handling** that throws appropriate exceptions for invalid conversions

### Supported Conversions

The following VARCHAR to numeric conversions are now supported:

- **VARCHAR → INTEGER**: Parses numeric strings to integers, truncates decimals
- **VARCHAR → BIGINT**: Parses numeric strings to longs, truncates decimals
- **VARCHAR → DOUBLE**: Parses numeric strings including scientific notation
- **VARCHAR → FLOAT/REAL**: Parses numeric strings to float values
- **VARCHAR → DECIMAL**: Parses numeric strings to BigDecimal with full precision
- **VARCHAR → BOOLEAN**: Converts "true"/"1"/"yes" to TRUE, "false"/"0"/"no" to FALSE

### Error Handling

- **Null values**: Return null (or 0 for primitive getters per JDBC spec)
- **Empty strings**: Return null
- **Invalid conversions**: Throw `IllegalArgumentException` with descriptive message
- **"null" string literal**: Treated as null value

## Usage Examples

### Basic Usage

```java
// Splunk returns a status code as a string
ResultSet rs = statement.executeQuery("SELECT status FROM web_logs");
rs.next();

// Even if 'status' is VARCHAR containing '200', this now works:
int statusCode = rs.getInt("status");  // Returns 200

// Also works for other numeric types:
long bytesTransferred = rs.getLong("bytes");  // Converts string to long
double responseTime = rs.getDouble("response_time");  // Converts string to double
```

### Real-World Splunk Scenario

Splunk often returns numeric values as strings in its JSON output:

```java
// Splunk query that returns count as string
String query = "SELECT COUNT(*) AS event_count FROM splunk.\"search index=web_logs\"";
ResultSet rs = stmt.executeQuery(query);
rs.next();

// Even if Splunk returns event_count as "12345" (string), this works:
int count = rs.getInt("event_count");  // Successfully converts to 12345
```

## Benefits

1. **Better compatibility**: Aligns with other major JDBC drivers
2. **Simplified code**: No need to manually convert strings to numbers
3. **Handles Splunk's JSON output**: Splunk often returns numbers as strings in JSON
4. **JDBC 4.3 compliant**: Follows the official JDBC specification

## Testing

Two test suites verify the implementation:

1. **Unit Tests** (`SplunkJdbc43ComplianceTest`): Tests the conversion logic directly
2. **Integration Tests** (`SplunkVarcharNumericIntegrationTest`): Tests end-to-end with real Splunk queries

Run tests with:
```bash
./gradlew :splunk:test --tests "SplunkJdbc43ComplianceTest"
```

## Migration Notes

This is a backward-compatible enhancement. Existing code continues to work, and the new functionality is automatically available when numeric getters are called on VARCHAR fields containing numeric values.
