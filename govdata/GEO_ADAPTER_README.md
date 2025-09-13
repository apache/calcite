# Geographic Data Adapter for Apache Calcite

The Geographic Data Adapter provides SQL access to U.S. government geographic datasets including Census boundaries, demographic data, and ZIP code mappings.

## Quick Start

### 1. Get Your Free API Credentials

All data sources are **completely free** but some require registration:

#### Census Bureau API Key (2 minutes)
1. Go to https://api.census.gov/data/key_signup.html
2. Fill out the simple form
3. Check your email for the API key (instant)

#### HUD USER Account (5 minutes)
1. Go to https://www.huduser.gov/portal/datasets/usps_crosswalk.html
2. Click "Create an Account"
3. Fill out the registration form
4. Login credentials work immediately

### 2. Set Up Your Environment Variables

For testing and development, you have two options:

#### Option A: Use the shared test environment file (recommended)
```bash
# Copy the sample file
cp govdata/.env.test.sample govdata/.env.test

# Edit with your credentials
vi govdata/.env.test

# Fill in your geographic credentials in the GEO section
# (This file is shared with SEC and other govdata adapters)
```

#### Option B: Use shell environment variables
```bash
# Copy the shell template
cp geo-env.sh.template geo-env.sh

# Edit with your credentials
vi geo-env.sh

# Load before running
source geo-env.sh
```

### 3. Connect Using Calcite

```bash
# If using shell variables (Option B), load them first:
source geo-env.sh

# Connect using the model template (Calcite automatically substitutes ${ENV_VAR} placeholders)
java -cp "build/libs/*" org.apache.calcite.jdbc.Driver \
  "jdbc:calcite:model=geo-model-template.json"

# Or use with sqlline
./gradlew :core:sqlline --args="jdbc:calcite:model=geo-model-template.json"

# For tests (credentials from govdata/.env.test are loaded automatically):
./gradlew :govdata:test --tests "*GeoSchemaFactoryTest*"
```

## How It Works

Calcite's `ModelHandler` automatically substitutes environment variables in model files:
- `${ENV_VAR}` - Uses environment variable value, fails if not found
- `${ENV_VAR:default}` - Uses environment variable value, or default if not found

The `geo-model-template.json` uses this syntax:
```json
{
  "operand": {
    "censusApiKey": "${CENSUS_API_KEY:}",
    "hudUsername": "${HUD_USERNAME:}",
    "cacheDir": "${GEO_CACHE_DIR:/tmp/calcite-geo-cache}"
  }
}
```

No preprocessing needed - just set environment variables and connect!

## Available Data Sources

### Without Any Credentials
- **TIGER/Line Boundaries** - States, counties, cities, ZIP codes (ZCTAs)
- **Geographic coordinates** - Centroids and area measurements

### With Census API Key
- **Demographics** - Population, age, gender, race/ethnicity
- **Economics** - Income, employment, poverty rates
- **Housing** - Home values, occupancy, tenure

### With HUD Credentials
- **ZIP to County** - Map ZIP codes to counties with address ratios
- **ZIP to Census Tract** - Fine-grained Census geography mapping
- **ZIP to CBSA** - Metropolitan/Micropolitan Statistical Areas
- **ZIP to Congressional District** - Political boundaries

## Example Queries

```sql
-- Find all California cities
SELECT place_name, population 
FROM geo.places 
WHERE state_abbr = 'CA' 
ORDER BY population DESC;

-- Get demographic data for a ZIP code
SELECT z.zip, d.median_income, d.population
FROM geo.zip_to_tract z
JOIN geo.demographics d ON z.tract = d.geo_id
WHERE z.zip = '94105';

-- Find companies by metropolitan area
SELECT c.cbsa_name, COUNT(DISTINCT s.cik) as company_count
FROM sec.companies s
JOIN geo.zip_to_cbsa z ON s.zip = z.zip
JOIN geo.cbsa_definitions c ON z.cbsa = c.cbsa_code
GROUP BY c.cbsa_name
ORDER BY company_count DESC;
```

## Data Freshness

- **TIGER/Line**: Updated annually (currently 2024 data)
- **Census Demographics**: 5-year American Community Survey (2018-2022)
- **HUD Crosswalk**: Updated quarterly (Q3 2024)

## Cache Management

Downloaded data is cached locally to improve performance:

```bash
# Default cache location
~/.calcite/geo-cache/

# Clear cache to force re-download
rm -rf ~/.calcite/geo-cache/*

# Check cache size
du -sh ~/.calcite/geo-cache/
```

## Troubleshooting

### "Census API key not configured"
- Get your free key at https://api.census.gov/data/key_signup.html
- Add to geo-env.sh: `export CENSUS_API_KEY="your-key"`

### "HUD credentials not configured"
- Register at https://www.huduser.gov/portal/datasets/usps_crosswalk.html
- Add to geo-env.sh: `export HUD_USERNAME="your-username"`

### "No geographic tables found"
- Check your internet connection
- Verify credentials are loaded: `source geo-env.sh`
- Enable auto-download: `export GEO_AUTO_DOWNLOAD="true"`

### Rate Limiting
- Census API: 500 requests/day (very generous)
- HUD API: No published limits
- TIGER downloads: No limits (direct file download)

## Advanced Configuration

### Custom Cache Directory
```bash
export GEO_CACHE_DIR="/path/to/your/cache"
```

### Disable Specific Data Sources
```bash
# Only use TIGER data (no credentials needed)
export GEO_ENABLED_SOURCES="tiger"

# Use TIGER and Census only
export GEO_ENABLED_SOURCES="tiger,census"
```

### Manual Data Download
```bash
export GEO_AUTO_DOWNLOAD="false"
# Then trigger downloads programmatically
```

## Security Notes

- Never commit `geo-env.sh` (it's git-ignored)
- Use `geo-env.sh.template` as a safe template
- Credentials are only stored in environment variables
- HUD token authentication preferred over username/password

## Support

For issues or questions:
- Check the test suite: `./gradlew :govdata:test --tests "*GeoSchemaFactoryTest*"`
- Review logs for credential loading status
- Ensure all environment variables are properly exported