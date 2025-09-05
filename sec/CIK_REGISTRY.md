# CIK Registry Documentation

The SEC adapter includes a comprehensive CIK (Central Index Key) registry that maps stock tickers and company groups to SEC CIK numbers.

## Default Registry

The default registry is loaded from `/cik-registry.json` and includes:

### Stock Tickers (100+ Companies)
- **Tech Giants**: AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, ORCL, CRM, ADBE, INTC, AMD, CSCO
- **Financial**: JPM, BAC, WFC, C, GS, MS, BRK.A, BRK.B, V, MA, AXP, BLK, SCHW, COF
- **Healthcare**: JNJ, UNH, PFE, ABBV, MRK, LLY, TMO, ABT, BMY, AMGN, GILD, CVS
- **Consumer**: WMT, PG, KO, PEP, HD, MCD, DIS, NKE, COST, TGT, LOW, SBUX
- **Industrial**: BA, CAT, HON, UPS, FDX, LMT, GE, MMM, DE, RTX, NOC
- **Energy**: XOM, CVX, COP, PSX, SLB, HAL, OXY
- **Telecom**: T, VZ, TMUS, CMCSA
- **And 50+ more major companies...**

### Company Groups

| Group | Description | Example Companies |
|-------|-------------|-------------------|
| `FAANG` | Facebook, Apple, Amazon, Netflix, Google | META, AAPL, AMZN, NFLX, GOOGL |
| `MAGNIFICENT7` | Top 7 tech mega-caps | AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA |
| `MAG7` | Alias for MAGNIFICENT7 | Same as above |
| `BIG_TECH` | Major technology companies | 14 tech companies |
| `BIG_BANKS` | Major US banks | JPM, BAC, WFC, C, GS, MS |
| `BIG_GLOBAL_BANKS` | Major global banks (US + International) | US banks + HSBC, TD, RY, BCS, DB, UBS, etc. |
| `DOW10` | Top 10 Dow Jones companies | By market weight |
| `DOW30` | All 30 Dow Jones Industrial Average companies | Complete DJIA |
| `FORTUNE10` | Fortune 10 companies (2024) | Largest US companies |
| `FORTUNE50` | Fortune 50 companies (2024) | Top 50 US companies |
| `FORTUNE100` | Fortune 100 companies (2024) | Top 100 US companies |
| `SP500` | S&P 500 representative sample | Top 50 by weight |
| `RUSSELL2000` | Russell 2000 index (partial) | Top 50 most traded |
| `RUSSELL2000_SAMPLE` | Russell 2000 representative sample | 30 small-cap companies |
| `RUSSELL2000_TOP100` | Top 100 Russell 2000 by market cap | Available in russell2000-registry.json |
| `RUSSELL2000_TECH` | Russell 2000 tech companies | ~35 companies |
| `RUSSELL2000_FINANCE` | Russell 2000 financial companies | ~65 companies |
| `RUSSELL2000_HEALTHCARE` | Russell 2000 healthcare companies | ~15 companies |
| `RUSSELL2000_CONSUMER` | Russell 2000 consumer companies | ~20 companies |
| `RUSSELL2000_INDUSTRIAL` | Russell 2000 industrial companies | ~35 companies |
| `RUSSELL2000_ENERGY` | Russell 2000 energy companies | ~10 companies |
| `RUSSELL2000_REITS` | Russell 2000 REITs | ~8 companies |
| `ENERGY` | Major energy companies | XOM, CVX, and others |
| `HEALTHCARE` | Major healthcare companies | JNJ, UNH, PFE, ABBV, MRK |
| `RETAIL` | Major retail companies | WMT, AMZN, HD, and others |
| `AUTO` | Automotive companies | TSLA, F, GM, and others |
| `SEMICONDUCTORS` | Chip manufacturers | NVDA, INTC, AMD, and others |
| `CLOUD` | Cloud computing providers | MSFT, AMZN, GOOGL, ORCL, CRM |
| `PAYMENT` | Payment processors | V, MA, PYPL, and others |
| `SOCIAL` | Social media companies | META, GOOGL, and others |
| `STREAMING` | Streaming services | NFLX, DIS, and others |
| `DEFENSE` | Defense contractors | LMT, NOC, RTX, BA, LHX |
| `AIRLINES` | Major airlines | DAL, UAL, AAL, LUV |
| `TELECOM` | Telecommunications | T, VZ, TMUS, CMCSA |
| `INSURANCE` | Insurance companies | ALL, PRU, MET, TRV, AIG |
| `REITS` | Real Estate Investment Trusts | SPG, PSA, PLD, WELL |
| `BIOTECH` | Biotechnology companies | AMGN, GILD, REGN, VRTX, ABBV |
| `FINTECH` | Financial technology | SQ, PYPL, V, MA, COF |
| `EV` | Electric vehicles | TSLA, GM, F, RIVN, LCID |
| `AI` | AI-focused companies | NVDA, MSFT, GOOGL, META, AMZN, PANW, CRM |
| `CYBER` | Cybersecurity | PANW, CRWD, ZS, BLK |
| `CHIPS` | Alias for SEMICONDUCTORS | Same as SEMICONDUCTORS |

## Usage Examples

### In Connection URLs

```java
// Single ticker
String url = "jdbc:sec:ciks=AAPL";

// Multiple tickers
String url = "jdbc:sec:ciks=AAPL,MSFT,GOOGL";

// Company group
String url = "jdbc:sec:ciks=MAGNIFICENT7";

// Mixed (ticker + group + CIK)
String url = "jdbc:sec:ciks=AAPL,FAANG,0000789019";
```

### In Model Files

```json
{
  "schemas": [{
    "name": "SEC",
    "operand": {
      "ciks": "MAGNIFICENT7"
    }
  }]
}
```

## Custom Registry Files

You can provide your own CIK registry file to add custom tickers or groups.

### Setting Custom Registry

Three ways to specify a custom registry file:

1. **Environment Variable**:
   ```bash
   export SEC_CIK_REGISTRY=/path/to/my-cik-registry.json
   ```

2. **System Property**:
   ```java
   System.setProperty("sec.cikRegistry", "/path/to/my-cik-registry.json");
   // or
   System.setProperty("cikRegistry", "/path/to/my-cik-registry.json");
   ```

3. **Connection Parameter**:
   ```java
   String url = "jdbc:sec:ciks=MY_CUSTOM_GROUP&cikRegistry=/path/to/my-registry.json";
   ```

### Custom Registry Format

Create a JSON file with this structure. Members can reference:
- **Raw CIK numbers**: `"0000320193"`
- **Ticker symbols**: `"AAPL"`, `"MSFT"`
- **Other groups**: `"FAANG"`, `"BIG_BANKS"`

```json
{
  "tickers": {
    "TICKER1": "0000123456",
    "TICKER2": "0000789012"
  },

  "groups": {
    "MY_PORTFOLIO": {
      "description": "My investment portfolio using mixed references",
      "members": ["AAPL", "MSFT", "GOOGL"]  // Ticker references
    },

    "MY_EXPANDED_PORTFOLIO": {
      "description": "Portfolio including tech and banks",
      "members": ["MY_PORTFOLIO", "BIG_BANKS"]  // Group references
    },

    "MY_WATCHLIST": {
      "description": "Companies I'm watching - mixed format",
      "members": ["TSLA", "0001318605", "NVDA"]  // Mix of ticker and CIK
    },

    "MY_ALIAS": {
      "description": "Alias for another group",
      "alias": "MY_PORTFOLIO"
    }
  },

  "customGroups": {
    "RESEARCH_SET": {
      "members": ["AAPL", "FAANG", "0000789019"]  // Ticker, group, and CIK
    }
  }
}
```

### Extending and Overriding the Default Registry

Your custom registry extends and can override the default registry. The custom registry is loaded AFTER the default, so:

- **New entries are added** to the existing registry
- **Existing entries can be overridden** with new values
- **Groups can be redefined** completely

```json
{
  "tickers": {
    "PRIVATE1": "0009999999",
    "AAPL": "0000999999"  // This overrides the default AAPL CIK
  },

  "groups": {
    "MY_ANALYSIS": {
      "description": "Companies for analysis",
      "members": ["0000320193", "0009999999"]
    },
    "FAANG": {  // This overrides the default FAANG group
      "description": "My custom FAANG definition",
      "members": ["0000320193", "0000789019"]
    }
  }
}
```

## Programmatic Access

```java
import org.apache.calcite.adapter.sec.CikRegistry;

// Resolve any identifier to CIKs
List<String> ciks = CikRegistry.resolveCiks("MAGNIFICENT7");
List<String> ciks = CikRegistry.resolveCiks("AAPL");
List<String> ciks = CikRegistry.resolveCiks("0000320193");

// Get available groups and tickers
List<String> groups = CikRegistry.getAvailableGroups();
List<String> tickers = CikRegistry.getAvailableTickers();

// Set custom registry programmatically
CikRegistry.setCustomRegistryPath("/path/to/my-registry.json");

// Reload registry after changes
CikRegistry.reload();
```

## Registry Statistics

- **100+ Individual Tickers**: Major companies across all sectors
- **25+ Predefined Groups**: Industry sectors, indices, and themes
- **Fortune Rankings**: Fortune 10, 50, and 100 companies
- **Market Indices**: DOW30, S&P 500 sample, Russell 2000 sample
- **Sector Coverage**: Tech, Finance, Healthcare, Energy, Consumer, Industrial, and more
- **Thematic Groups**: AI, EV, Fintech, Cybersecurity, Streaming, Cloud, and more

## Benefits

1. **User-Friendly**: Use familiar tickers instead of CIK numbers
2. **Comprehensive**: 100+ companies and 25+ groups predefined
3. **Flexible**: Mix tickers, CIKs, and groups in any combination
4. **Extensible**: Add your own custom mappings
5. **Efficient**: Automatically expands groups to multiple companies
6. **Maintainable**: Registry in JSON format, easy to update

## Examples

### Analyzing Tech Sector
```java
String url = "jdbc:sec:ciks=BIG_TECH&startYear=2020";
```

### Comparing FAANG Performance
```java
String url = "jdbc:sec:ciks=FAANG&filingTypes=10-K";
```

### Custom Portfolio Analysis
```bash
# Create custom registry
cat > my-portfolio.json << EOF
{
  "groups": {
    "MY_HOLDINGS": {
      "members": ["0000320193", "0000789019", "0001403161"]
    }
  }
}
EOF

# Use it
export SEC_CIK_REGISTRY=my-portfolio.json
String url = "jdbc:sec:ciks=MY_HOLDINGS";
```

### Analyzing Fortune 50 Companies
```java
String url = "jdbc:sec:ciks=FORTUNE50&startYear=2022";
```

### Russell 2000 Small-Cap Analysis
```java
String url = "jdbc:sec:ciks=RUSSELL2000&filingTypes=10-K";
```

### AI Sector Performance
```java
String url = "jdbc:sec:ciks=AI&startYear=2020&endYear=2023";
```
