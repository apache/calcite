# SEC Adapter Connection Parameters

The SEC adapter supports configuration through multiple methods, with the following precedence:
1. **Connection URL parameters** (highest priority)
2. **Environment variables**
3. **Model file configuration**
4. **Smart defaults** (lowest priority)

## SEC JDBC Driver (Recommended)

The simplest way to connect is using the dedicated SEC JDBC driver with `jdbc:sec:` URLs:

### Basic SEC Connections

```java
// Minimal - just specify companies
String url = "jdbc:sec:ciks=AAPL";
String url = "jdbc:sec:ciks=AAPL,MSFT,GOOGL";
String url = "jdbc:sec:ciks=MAGNIFICENT7";

// No need to specify these - they're automatic defaults:
// lex = ORACLE
// unquotedCasing = TO_LOWER
// All filing types included
// Years: 2009 to current year
// DuckDB execution engine
```

### Advanced SEC Connections

```java
// With custom date range
String url = "jdbc:sec:ciks=FAANG&startYear=2020&endYear=2023";

// With custom data directory
String url = "jdbc:sec:ciks=AAPL&dataDirectory=/Volumes/T9/sec-data";

// Full configuration
String url = "jdbc:sec:ciks=MAGNIFICENT7" +
             "&startYear=2020" +
             "&endYear=2023" +
             "&dataDirectory=/Volumes/T9/sec-data" +
             "&debug=true";

// Connect with automatic defaults
try (Connection conn = DriverManager.getConnection(url)) {
    // Query financial data - no need to set properties
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(
        "SELECT company_name, fiscal_year, revenue " +
        "FROM financial_line_items " +
        "ORDER BY revenue DESC"
    );
}
```

## Calcite Model-Based Connection (Alternative)

You can also use the traditional Calcite model-based approach:

### Basic Connection with Model File

```java
// Using model file with connection parameters
String url = "jdbc:calcite:model=sec-model.json?ciks=AAPL,MSFT,GOOGL";

// With custom date range
String url = "jdbc:calcite:model=sec-model.json" +
             "?ciks=MAGNIFICENT7" +
             "&startYear=2020" +
             "&endYear=2023";

// With custom data directory
String url = "jdbc:calcite:model=sec-model.json" +
             "?ciks=FAANG" +
             "&dataDirectory=/Volumes/T9/sec-data";
```

### Advanced Configuration

```java
// Full configuration example with short names
String url = "jdbc:calcite:model=sec-model.json" +
             "?ciks=AAPL,MSFT,GOOGL" +                    // Companies
             "&filingTypes=10-K,10-Q,8-K" +               // Specific filing types
             "&startYear=2019" +                          // Start year
             "&endYear=2023" +                            // End year
             "&dataDirectory=/Volumes/T9/sec-data" +     // Data location
             "&embeddingDimension=256" +                  // Vector size
             "&executionEngine=duckdb" +                  // Query engine
             "&debug=true";                                // Enable debug logging
```

### Fully Qualified Names (Optional)

You can also use fully qualified parameter names if needed for clarity or to avoid conflicts:

```java
// Using fully qualified names
String url = "jdbc:calcite:model=sec-model.json" +
             "?calcite.sec.ciks=AAPL" +
             "&calcite.sec.startYear=2020";
```

## Supported Parameters

Both **short names** (recommended) and **fully qualified names** are supported:

| Short Name | Full Name (Optional) | Description | Example | Default |
|------------|---------------------|-------------|---------|---------|
| `ciks` | `calcite.sec.ciks` | Company identifiers | `AAPL`, `FAANG` | Required |
| `filingTypes` | `calcite.sec.filingTypes` | Filing types | `10-K,10-Q,8-K` | All 15 types |
| `startYear` | `calcite.sec.startYear` | Start year | `2019` | 2009 |
| `endYear` | `calcite.sec.endYear` | End year | `2023` | Current year |
| `dataDirectory` | `calcite.sec.dataDirectory` | Data location | `/Volumes/T9/sec-data` | Working dir |
| `embeddingDimension` | `calcite.sec.embeddingDimension` | Vector size | `256` | 128 |
| `executionEngine` | `calcite.sec.executionEngine` | Query engine | `duckdb` | `duckdb` |
| `debug` | `calcite.sec.debug` | Debug logging | `true` | `false` |

## CIK Formats

The `ciks` parameter supports multiple formats:

### Individual Companies
- **Ticker symbols**: `AAPL`, `MSFT`, `GOOGL`
- **CIK numbers**: `0000320193`, `0000789019`

### Company Groups
- **FAANG**: Facebook/Meta, Apple, Amazon, Netflix, Google
- **MAGNIFICENT7**: Top 7 tech stocks
- **BIG_TECH**: Major technology companies
- **BIG_BANKS**: Major financial institutions
- **DOW10**: Top 10 Dow Jones companies
- **FORTUNE10**: Fortune 10 companies

### Mixed Formats
```java
// You can mix tickers, CIKs, and groups
"?calcite.sec.ciks=AAPL,0000789019,FAANG,JPM"
```

## Environment Variable Mapping

Connection parameters can also be set via environment variables:

| Short Param | Environment Variable | Description |
|-------------|---------------------|-------------|
| `ciks` | `EDGAR_CIKS` | Company identifiers |
| `filingTypes` | `EDGAR_FILING_TYPES` | Filing types to download |
| `startYear` | `EDGAR_START_YEAR` | Start year for data |
| `endYear` | `EDGAR_END_YEAR` | End year for data |
| `dataDirectory` | `SEC_DATA_DIRECTORY` | Base data directory |
| `debug` | `SEC_DEBUG` | Enable debug logging |

## Usage Examples

### Java JDBC

```java
import java.sql.*;

public class SecQuery {
    public static void main(String[] args) throws Exception {
        // Connect with SHORT parameter names (recommended)
        String url = "jdbc:calcite:model=sec-model.json" +
                     "?ciks=MAGNIFICENT7" +
                     "&startYear=2020";

        Properties props = new Properties();
        props.setProperty("lex", "ORACLE");
        props.setProperty("unquotedCasing", "TO_LOWER");

        try (Connection conn = DriverManager.getConnection(url, props)) {
            // Query financial data
            String sql = "SELECT company_name, fiscal_year, revenue " +
                        "FROM financial_line_items " +
                        "WHERE fiscal_year >= 2020 " +
                        "ORDER BY revenue DESC";

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                System.out.printf("%s (%d): $%,.0f%n",
                    rs.getString("company_name"),
                    rs.getInt("fiscal_year"),
                    rs.getDouble("revenue"));
            }
        }
    }
}
```

### Python with jaydebeapi

```python
import jaydebeapi

# Connection with parameters
url = ("jdbc:calcite:model=sec-model.json"
       "?calcite.sec.ciks=FAANG"
       "&calcite.sec.startYear=2021"
       "&calcite.sec.endYear=2023")

conn = jaydebeapi.connect(
    "org.apache.calcite.jdbc.Driver",
    url,
    {"lex": "ORACLE", "unquotedCasing": "TO_LOWER"},
    "calcite-sec.jar"
)

cursor = conn.cursor()
cursor.execute("""
    SELECT company_name, fiscal_year, net_income
    FROM financial_line_items
    WHERE net_income > 0
    ORDER BY fiscal_year, net_income DESC
""")

for row in cursor.fetchall():
    print(f"{row[0]} ({row[1]}): ${row[2]:,.0f}")
```

### Command Line Testing

```bash
# Set via environment variables
export EDGAR_CIKS=AAPL,MSFT,GOOGL
export EDGAR_START_YEAR=2020
export EDGAR_END_YEAR=2023
export SEC_DATA_DIRECTORY=/Volumes/T9/sec-data

# Or use connection string in SQL client
sqlline -u "jdbc:calcite:model=sec-model.json?calcite.sec.ciks=MAGNIFICENT7&calcite.sec.startYear=2021"
```

## Priority Order

When the same parameter is specified in multiple places, the following priority order applies:

1. **Connection URL parameters** - Highest priority
2. **Environment variables** - Medium priority
3. **Model file configuration** - Low priority
4. **Smart defaults** - Lowest priority

Example:
```bash
# Environment variable
export EDGAR_START_YEAR=2015

# Model file has startYear: 2010

# Connection URL has calcite.sec.startYear=2020
jdbc:calcite:model=sec-model.json?calcite.sec.startYear=2020

# Result: startYear = 2020 (connection parameter wins)
```
