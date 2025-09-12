# Using the Russell 2000 Registry

The Russell 2000 index contains 2000 small-cap companies. Due to the large number, we provide it as a separate registry file that can be loaded when needed.

## Quick Usage

### Option 1: Use the Built-in Subset
```java
// Top 50 most traded Russell 2000 companies
String url = "jdbc:sec:ciks=RUSSELL2000";

// Specific sectors
String url = "jdbc:sec:ciks=RUSSELL2000_TECH";
String url = "jdbc:sec:ciks=RUSSELL2000_FINANCE";
```

### Option 2: Load Full Russell 2000 Registry
```bash
# Set custom registry to load more companies
export SEC_CIK_REGISTRY=/path/to/calcite/sec/src/main/resources/russell2000-registry.json

# Now you can use all Russell 2000 groups
java -cp ... "jdbc:sec:ciks=RUSSELL2000_TOP100"
```

### Option 3: Combine Multiple Registries
```java
// In your code, load both registries
CikRegistry.setCustomRegistryPath("russell2000-registry.json");

// Or create a combined registry file
```

## Available Russell 2000 Groups

### Main Groups
- `RUSSELL2000` - Top 50 most traded (in main registry)
- `RUSSELL2000_TOP100` - Top 100 by market cap (in russell2000-registry.json)
- `RUSSELL2000_SAMPLE` - 30 company sample

### Sector Groups (in russell2000-registry.json)
- `RUSSELL2000_TECH` - Technology companies (~35)
- `RUSSELL2000_FINANCE` - Financial services (~65)
- `RUSSELL2000_HEALTHCARE` - Healthcare & biotech (~15)
- `RUSSELL2000_CONSUMER` - Consumer discretionary (~20)
- `RUSSELL2000_INDUSTRIAL` - Industrials & materials (~35)
- `RUSSELL2000_ENERGY` - Energy sector (~10)
- `RUSSELL2000_REITS` - Real estate (~8)

## Company Coverage

The russell2000-registry.json includes:
- **200+ individual tickers** with CIK mappings
- **8 sector-specific groups**
- **Top 100 companies** by market capitalization

## Examples

### Analyze Russell 2000 Tech Sector
```java
String url = "jdbc:sec:ciks=RUSSELL2000_TECH&startYear=2022";

try (Connection conn = DriverManager.getConnection(url)) {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(
        "SELECT company_name, fiscal_year, revenue " +
        "FROM financial_line_items " +
        "WHERE filing_type = '10-K' " +
        "ORDER BY revenue DESC"
    );
}
```

### Compare Small-Cap vs Large-Cap
```java
// Small-cap (Russell 2000)
String smallCap = "jdbc:sec:ciks=RUSSELL2000_TOP100&startYear=2023";

// Large-cap (S&P 500)
String largeCap = "jdbc:sec:ciks=SP500&startYear=2023";
```

### Custom Russell 2000 Portfolio
Create your own registry extending Russell 2000:
```json
{
  "groups": {
    "MY_RUSSELL_PICKS": {
      "description": "My selected Russell 2000 stocks",
      "members": ["RUSSELL2000_TECH", "ENPH", "ROKU", "SNAP"]
    }
  }
}
```

## Performance Notes

- Loading 200+ companies will download significant data on first run
- Use sector groups to limit scope when possible
- Consider using `filingTypes=10-K` to reduce data volume
- Cached data is reused (immutable filing strategy)

## Adding More Russell 2000 Companies

To add more Russell 2000 companies to the registry:

1. Find the company ticker and CIK
2. Add to russell2000-registry.json
3. Add to appropriate sector group
4. Set SEC_CIK_REGISTRY to use the extended file

The full Russell 2000 list changes annually. The provided registry focuses on the most actively traded and analyzed companies.
