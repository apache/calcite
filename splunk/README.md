<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Splunk JDBC Adapter - User Guide

Connect to Splunk using standard JDBC and query your data with SQL.

## Overview

The Splunk JDBC Adapter allows you to:
- **Connect** to Splunk using standard JDBC drivers with token or username/password authentication
- **Query** Splunk data using familiar SQL syntax with full PostgreSQL-compatible metadata support
- **Discover** data models automatically from any Splunk app context (CIM, vendor TAs, custom apps)
- **Cache** discovered models with production-grade control (permanent, time-based, or disabled)
- **Filter** data models using glob patterns, regex, or exclusion rules
- **Federate** multiple vendor data sources for cross-platform security analytics
- **Customize** table definitions with field mappings and calculated fields

## Prerequisites

- Splunk server with REST API access (port 8089 by default)
- Valid Splunk credentials (username/password or authentication token)
- Java application with JDBC support
- Apache Calcite JDBC driver and Splunk adapter JAR files

## Connection Methods

You can connect to Splunk in multiple ways:

### Method 1: Direct JDBC URL (Recommended)
### Method 2: Properties Object (Alternative)
### Method 3: Calcite Model File (Federation Only)

---

## Quick Reference

### Essential JDBC URL Parameters

| Parameter | Description | Values | Example |
|-----------|-------------|--------|---------|
| `url` | Splunk server URL | `https://host:port` | `https://splunk.com:8089` |
| `user` / `password` | Basic authentication | credentials | `user='admin';password='secret'` |
| `token` | Token authentication (preferred) | auth token | `token='eyJhbGciOiJIUzI1...'` |
| `app` | Splunk app context | app name | `app='Splunk_SA_CIM'` |
| `datamodelFilter` | Filter discovered models | glob/regex | `datamodelFilter='auth*'` |
| `datamodelCacheTtl` | Cache behavior | `-1`=permanent, `0`=none, `>0`=minutes | `datamodelCacheTtl=-1` |
| `refreshDatamodels` | Force cache refresh | `true`/`false` | `refreshDatamodels=true` |

### Environment Variable Support (Production)

| Environment Variable | Description | Maps to Parameter |
|---------------------|-------------|-------------------|
| `SPLUNK_URL` | Splunk server URL | `url` |
| `SPLUNK_TOKEN` | Authentication token | `token` |
| `SPLUNK_USER` | Username | `user` |
| `SPLUNK_PASSWORD` | Password | `password` |
| `SPLUNK_APP` | App context | `app` |

**Priority**: Properties/URL parameters override environment variables

### Production-Ready URL Template
```java
String url = "jdbc:splunk:" +
    "url='https://splunk.company.com:8089';" +
    "token='your_auth_token';" +
    "app='Splunk_SA_CIM';" +
    "datamodelCacheTtl=-1";
```

---

## Method 1: Direct JDBC URL

For simple connections, use a direct JDBC URL with embedded configuration.

### Basic Connection

```java
String url = "jdbc:splunk:url='https://splunk.example.com:8089';user='admin';password='your_password';app='Splunk_SA_CIM';datamodelFilter='authentication'";

Connection conn = DriverManager.getConnection(url);
```

### URL Format

```
jdbc:splunk:url='[splunk_url]';[parameter]='[value]';[parameter]='[value]'...
```

**Core Parameters:**
- `url` - Splunk server URL (required)
- `user` - Splunk username
- `password` - Splunk password
- `token` - Authentication token (alternative to user/password)
- `app` - Splunk app context (e.g., 'Splunk_SA_CIM', 'search')
- `disableSslValidation` - Set to `true` for development (not recommended for production)

**Data Model Parameters:**
- `datamodelFilter` - Filter pattern for dynamic discovery (glob: `auth*`, regex: `/^(auth|web)/`, exclude: `!test_*`)
- `datamodelCacheTtl` - Cache TTL behavior:
  - `> 0`: Cache for specified minutes (default: 60)
  - `0`: No caching - always fetch from API
  - `-1`: Permanent cache - never expires (production recommended)
- `refreshDatamodels` - Force refresh cached models (true/false)
- `calculatedFields` - Add calculated fields to discovered models (e.g., `{"Authentication": [{"name": "action", "splunkField": "action"}]}`)

### Examples

**Dynamic Data Model Discovery (Recommended):**
```java
// Default app context (user's default in Splunk)
String url = "jdbc:splunk:url='https://splunk.company.com:8089';user='analyst';password='secret123'";

// Explicit CIM app context (recommended)
String url = "jdbc:splunk:url='https://splunk.company.com:8089';user='analyst';password='secret123';app='Splunk_SA_CIM'";

// Filter to specific models
String url = "jdbc:splunk:url='https://splunk.company.com:8089';user='analyst';password='secret123';app='Splunk_SA_CIM';datamodelFilter='auth*'";

// Production with permanent cache
String url = "jdbc:splunk:url='https://splunk.company.com:8089';user='analyst';password='secret123';app='Splunk_SA_CIM';datamodelCacheTtl=-1";

// Force cache refresh when needed
String url = "jdbc:splunk:url='https://splunk.company.com:8089';user='analyst';password='secret123';app='Splunk_SA_CIM';refreshDatamodels=true";

// Custom app with exclusions
String url = "jdbc:splunk:url='https://splunk.company.com:8089';user='analyst';password='secret123';app='my_app';datamodelFilter='!test_*,!dev_*'";

// Environment variables for production credentials
// export SPLUNK_URL="https://splunk.company.com:8089"
// export SPLUNK_TOKEN="your_production_token"
// export SPLUNK_APP="Splunk_SA_CIM"
String url = "jdbc:splunk:datamodelCacheTtl=-1"; // Credentials from env vars
```


**Development with SSL Disabled:**
```java
String url = "jdbc:splunk:url='https://localhost:8089';user='admin';password='changeme';app='Splunk_SA_CIM';disableSslValidation='true'";
```

**Production with Environment Variables:**
```bash
# Set environment variables
export SPLUNK_URL="https://prod.splunk.company.com:8089"
export SPLUNK_TOKEN="eyJhbGciOiJIUzI1NiIsIn..."
export SPLUNK_APP="Splunk_SA_CIM"

# Minimal JDBC URL - credentials come from environment
String url = "jdbc:splunk:datamodelCacheTtl=-1";
```

**App Context Behavior:**

| App Context | Discovery Scope | Recommended Use |
|-------------|-----------------|----------------|
| **Not specified** | User's default app in Splunk (usually "search") | ⚠️ Limited data models, depends on user config |
| `Splunk_SA_CIM` | Standard CIM models | ✅ Security analytics, SOC operations |
| `SplunkEnterpriseSecuritySuite` | Enhanced ES models | ✅ Enterprise Security deployments |
| `Splunk_TA_*` | Vendor-specific models | ✅ Single-vendor analysis (requires federation for multi-vendor) |
| Custom app | Organization-specific models | ✅ Custom data model deployments |

**Alternative: Using Properties object:**
```java
Properties props = new Properties();
props.setProperty("url", "https://splunk.example.com:8089");
props.setProperty("user", "admin");
props.setProperty("password", "your_password");
props.setProperty("datamodelFilter", "authentication");

Connection conn = DriverManager.getConnection("jdbc:splunk:", props);
```

---

## Method 2: Properties Object

Alternative to JDBC URL for programmatic configuration:

```java
Properties props = new Properties();
props.setProperty("url", "https://splunk.example.com:8089");
props.setProperty("token", "your_auth_token");
props.setProperty("app", "Splunk_SA_CIM");
props.setProperty("datamodelCacheTtl", "-1");

Connection conn = DriverManager.getConnection("jdbc:splunk:", props);
```

**Benefits:**
- ✅ No factory specification needed
- ✅ Programmatic configuration  
- ✅ Environment variable fallback
- ✅ Same capabilities as JDBC URL

---

## Method 3: Calcite Model File

**⚠️ Only needed for federation** (cross-vendor queries). For single connections, use Method 1 or 2.

**When you need model files:**
- 🚀 **Multi-vendor federation**: Query Cisco + Palo Alto + CrowdStrike in single SQL
- 🚀 **Cross-platform analytics**: Join data from multiple vendor Technology Add-ons

**When you DON'T need model files:**
- ✅ **Single vendor**: Just one Splunk app (use JDBC URL)
- ✅ **Standard CIM**: Only `Splunk_SA_CIM` models (use JDBC URL)
- ✅ **Simple queries**: No cross-vendor joins (use JDBC URL)

For multi-vendor federation, create a `model.json` file:

### Basic Model File

Create a file named `model.json`:

```json
{
  "version": "1.0",
  "defaultSchema": "splunk",
  "schemas": [
    {
      "name": "splunk",
      "operand": {
        "url": "https://splunk.example.com:8089",
        "username": "admin",
        "password": "your_password",
        "datamodelFilter": "authentication"
      }
    }
  ]
}
```


### Connection with Model File

**Option 1: Direct model file**
```java
String url = "jdbc:calcite:model=/path/to/model.json";
Connection conn = DriverManager.getConnection(url);
```

**Option 2: Using SplunkModelHelper**
```java
Connection conn = SplunkModelHelper.connect("/path/to/model.json");

// Or preprocess manually
String preprocessedModel = SplunkModelHelper.preprocessModelFile("/path/to/model.json");
String url = "jdbc:calcite:model=inline:" + preprocessedModel;
Connection conn = DriverManager.getConnection(url);
```

### Advanced Model Examples

#### Dynamic Discovery with Filtering

```json
{
  "version": "1.0",
  "defaultSchema": "splunk",
  "schemas": [
    {
      "name": "splunk",
      "operand": {
        "url": "https://splunk.example.com:8089",
        "token": "your_authentication_token",
        "app": "Splunk_SA_CIM",
        "datamodelFilter": "/^(authentication|network_traffic|web|malware)$/"
      }
    }
  ]
}
```

#### Custom Tables with Field Mapping

```json
{
  "version": "1.0",
  "defaultSchema": "splunk",
  "schemas": [
    {
      "name": "splunk",
      "operand": {
        "url": "https://splunk.example.com:8089",
        "username": "admin",
        "password": "your_password"
      },
      "tables": {
        "security_events": {
          "type": "custom",
          "factory": "org.apache.calcite.adapter.splunk.SplunkTableFactory",
          "operand": {
            "search": "index=security sourcetype=authentication",
            "field_mapping": {
              "event_time": "_time",
              "username": "user",
              "source_ip": "src_ip",
              "login_result": "result"
            }
          },
          "columns": [
            {"name": "event_time", "type": "TIMESTAMP"},
            {"name": "username", "type": "VARCHAR"},
            {"name": "source_ip", "type": "VARCHAR"},
            {"name": "login_result", "type": "VARCHAR"},
            {"name": "_extra", "type": "VARCHAR"}
          ]
        },
        "network_logs": {
          "type": "custom",
          "factory": "org.apache.calcite.adapter.splunk.SplunkTableFactory",
          "operand": {
            "search": "index=network | eval bytes_total=bytes_in+bytes_out",
            "field_mappings": [
              "timestamp:_time",
              "source_host:src",
              "dest_host:dest",
              "total_bytes:bytes_total"
            ]
          },
          "columns": [
            {"name": "timestamp", "type": "TIMESTAMP"},
            {"name": "source_host", "type": "VARCHAR"},
            {"name": "dest_host", "type": "VARCHAR"},
            {"name": "total_bytes", "type": "BIGINT"},
            {"name": "_extra", "type": "VARCHAR"}
          ]
        }
      }
    }
  ]
}
```

**Note:** Table factories still need to be specified explicitly for custom tables.

---

## SQL Query Examples

Once connected, you can run standard SQL queries:

### Authentication Events

```sql
-- Recent successful logins
SELECT user, src, dest, reason, _time
FROM authentication
WHERE action = 'success'
  AND _time > CURRENT_TIMESTAMP - INTERVAL '1' DAY
ORDER BY _time DESC
LIMIT 100;

-- Failed login attempts by user
SELECT user, COUNT(*) as failed_attempts
FROM authentication
WHERE action = 'failure'
  AND _time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY user
HAVING COUNT(*) > 5
ORDER BY failed_attempts DESC;
```

### Network Traffic

```sql
-- Top bandwidth consumers
SELECT src_ip, dest_ip, SUM(bytes) as total_bytes
FROM network_traffic
WHERE _time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY src_ip, dest_ip
ORDER BY total_bytes DESC
LIMIT 10;

-- Protocol distribution
SELECT protocol, COUNT(*) as connection_count, AVG(duration) as avg_duration
FROM network_traffic
WHERE _time > CURRENT_TIMESTAMP - INTERVAL '6' HOUR
GROUP BY protocol
ORDER BY connection_count DESC;
```

### Web Access Logs

```sql
-- HTTP error analysis
SELECT status, COUNT(*) as error_count,
       COUNT(DISTINCT src_ip) as unique_ips
FROM web
WHERE status >= 400
  AND _time > CURRENT_TIMESTAMP - INTERVAL '2' HOUR
GROUP BY status
ORDER BY error_count DESC;

-- Popular pages
SELECT uri_path, COUNT(*) as hits
FROM web
WHERE status = 200
  AND _time > CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY uri_path
ORDER BY hits DESC
LIMIT 20;
```

### Custom Tables

```sql
-- Using custom table with field mappings
SELECT event_time, username, source_ip, login_result
FROM security_events
WHERE login_result = 'failed'
  AND event_time > CURRENT_TIMESTAMP - INTERVAL '30' MINUTE;

-- Complex query with calculations
SELECT source_host, dest_host, total_bytes,
       CASE
         WHEN total_bytes > 1000000 THEN 'High'
         WHEN total_bytes > 100000 THEN 'Medium'
         ELSE 'Low'
       END as traffic_level
FROM network_logs
WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
ORDER BY total_bytes DESC;
```

---

## Configuration Options

### Authentication

**Username/Password:**
```json
{
  "username": "your_username",
  "password": "your_password"
}
```

**Authentication Token:**
```json
{
  "token": "your_auth_token"
}
```

### Connection Settings

**URL Formats:**
```json
{
  "url": "https://splunk.example.com:8089"
}
```

**Or specify components:**
```json
{
  "host": "splunk.example.com",
  "port": 8089,
  "protocol": "https"
}
```

### SSL Configuration

**Production (recommended):**
```json
{
  "url": "https://splunk.example.com:8089"
}
```

**Development only:**
```json
{
  "url": "https://splunk.example.com:8089",
  "disable_ssl_validation": true
}
```

⚠️ **Warning:** Never disable SSL validation in production environments.

### Data Model Cache Configuration

The adapter caches discovered data models to improve performance. Cache behavior is controlled by the `datamodelCacheTtl` parameter:

| TTL Value | Behavior | Use Case |
|-----------|----------|----------|
| `-1` | **Permanent cache** - Never expires until explicitly refreshed | **Production (Recommended)** - Predictable, controlled updates |
| `0` | **No caching** - Always fetch from Splunk API | Development/Testing - Always get latest |
| `> 0` | **Time-based cache** - Expires after specified minutes | General purpose - Balance performance and freshness |

**Production Cache Strategy:**
```java
// Set permanent cache for production stability
String url = "jdbc:splunk:url='https://prod.splunk.com:8089';token='prod_token';app='Splunk_SA_CIM';datamodelCacheTtl=-1";

// When you need to update models (e.g., after Splunk configuration changes)
String refreshUrl = "jdbc:splunk:url='https://prod.splunk.com:8089';token='prod_token';app='Splunk_SA_CIM';refreshDatamodels=true";
```

**Cache Control Parameters:**
- `datamodelCacheTtl=-1` - Permanent cache (production recommended)
- `refreshDatamodels=true` - Force refresh (one-time override)
- Both can be specified in JDBC URL or Properties object

### App Context Configuration

Splunk app context determines which data models are available for discovery. Every Splunk user has a default app context, but you can override it:

**Default App Context (if not specified):**
- Uses the user's default app in Splunk (typically "search")
- May have limited data model visibility
- Depends on user's Splunk configuration

**Explicit App Context (recommended):**
```java
// Use Splunk Common Information Model app
String url = "jdbc:splunk:url='https://splunk.com:8089';user='analyst';password='secret';app='Splunk_SA_CIM'";

// Use custom app with your organization's data models
String url = "jdbc:splunk:url='https://splunk.com:8089';user='analyst';password='secret';app='my_security_app'";

// Use ES (Enterprise Security) app context
String url = "jdbc:splunk:url='https://splunk.com:8089';user='analyst';password='secret';app='SplunkEnterpriseSecuritySuite'";

// Use vendor-specific app contexts
String ciscoUrl = "jdbc:splunk:url='https://splunk.com:8089';user='analyst';password='secret';app='Splunk_TA_cisco-esa'";
String paloAltoUrl = "jdbc:splunk:url='https://splunk.com:8089';user='analyst';password='secret';app='Splunk_TA_paloalto'";
String crowdstrikeUrl = "jdbc:splunk:url='https://splunk.com:8089';user='analyst';password='secret';app='Splunk_TA_crowdstrike'";

// Combine vendor app with filtering
String filteredUrl = "jdbc:splunk:url='https://splunk.com:8089';user='analyst';password='secret';app='Splunk_TA_paloalto';datamodelFilter='threat*'";
```

**App Context Examples:**

| App Context | Description | Typical Data Models |
|-------------|-------------|---------------------|
| `Splunk_SA_CIM` | Common Information Model | authentication, web, network_traffic, malware |
| `search` | Default search app | Limited - depends on configuration |
| `SplunkEnterpriseSecuritySuite` | Enterprise Security | Enhanced CIM models + ES-specific models |
| `Splunk_TA_cisco-esa` | Cisco Email Security | email, cisco_esa_mail_summary |
| `Splunk_TA_paloalto` | Palo Alto Networks | threat, traffic, url, wildfire |
| `Splunk_TA_crowdstrike` | CrowdStrike Falcon | endpoint, process, network_sessions |
| `Splunk_TA_windows` | Windows Technology Add-on | wineventlog, perfmon, windows_audit |
| `Splunk_TA_nix` | Unix/Linux Technology Add-on | cpu, df, interfaces, ps |
| `my_custom_app` | Your organization's app | Custom data models specific to your environment |

**Vendor Technology Add-ons:**

Many security vendors provide Splunk Technology Add-ons (TAs) that include pre-built data models for their products:

- **Cisco**: `Splunk_TA_cisco-esa`, `Splunk_TA_cisco-firepower`
- **Palo Alto Networks**: `Splunk_TA_paloalto` 
- **CrowdStrike**: `Splunk_TA_crowdstrike`
- **Microsoft**: `Splunk_TA_windows`, `Splunk_TA_microsoft-cloudservices`
- **Fortinet**: `Splunk_TA_fortinet-fortigate`
- **Check Point**: `Splunk_TA_checkpoint`

These apps contain vendor-specific data models that extend or complement the standard CIM models.

**Architecture Constraints:**
- 🚫 **One App Context Per Connection**: Each JDBC connection can only access data models from a single Splunk app
- 🚫 **No Mixed App Access**: Cannot query both `Splunk_TA_cisco-esa` and `Splunk_TA_paloalto` from the same schema
- ✅ **Federation Required**: Use multiple schemas (one per vendor app) to enable cross-vendor queries

**Important Notes:**
- App context affects **which data models are discovered**
- Different apps may have different versions of the same model
- Vendor apps often include both CIM-compliant and vendor-specific models
- User must have permissions to access the specified app
- If app doesn't exist or user lacks access, falls back to default

⚠️ **Warning:** Never disable SSL validation in production environments.

### Federation & Multi-App Context Queries

**How Federation Works:**
1. **One JDBC connection** to Calcite: `jdbc:calcite:model=federation.json`
2. **Multiple backend connections**: Calcite creates separate connections to each Splunk app
3. **Cross-schema queries**: SQL joins work across different vendor data sources

**Architecture:**
```
Your Application
       ↓ (Single JDBC connection)
   Calcite Query Engine
       ↓ ↓ ↓ (Multiple backend connections)
   cisco.* ← Splunk_TA_cisco-esa
   paloalto.* ← Splunk_TA_paloalto  
   crowdstrike.* ← Splunk_TA_crowdstrike
```

**Important**: Each schema connects to **one app context**. Federation enables **cross-vendor data correlation** in a single SQL query:

**Multi-Schema Model Configuration:**
```json
{
  "version": "1.0",
  "defaultSchema": "security",
  "schemas": [
    {
      "name": "cisco",
      "operand": {
        "url": "https://splunk.company.com:8089",
        "token": "your_token",
        "app": "Splunk_TA_cisco-esa",
        "datamodelCacheTtl": -1
      }
    },
    {
      "name": "paloalto",
      "operand": {
        "url": "https://splunk.company.com:8089",
        "token": "your_token",
        "app": "Splunk_TA_paloalto",
        "datamodelCacheTtl": -1
      }
    },
    {
      "name": "crowdstrike",
      "operand": {
        "url": "https://splunk.company.com:8089",
        "token": "your_token",
        "app": "Splunk_TA_crowdstrike",
        "datamodelCacheTtl": -1
      }
    },
    {
      "name": "security",
      "operand": {
        "url": "https://splunk.company.com:8089",
        "token": "your_token",
        "app": "Splunk_SA_CIM",
        "datamodelCacheTtl": -1
      }
    }
  ]
}
```


**Programmatic Federation Creation:**
```java
// Create federation model programmatically
String model = SplunkModelHelper.createFederationModel(
    "https://splunk.company.com:8089",
    "your_auth_token",
    "Splunk_SA_CIM",
    "Splunk_TA_cisco-esa", 
    "Splunk_TA_paloalto",
    "Splunk_TA_crowdstrike"
);

// Connect with auto-factory injection
Connection conn = SplunkModelHelper.connect("inline:" + model);

// Or use the convenience method for common security federation
Connection securityConn = SplunkModelHelper.createSecurityFederation(
    "https://splunk.company.com:8089", 
    "your_auth_token"
);
```

**Cross-Vendor Security Analysis Queries:**

```sql
-- Correlate email threats with firewall blocks
SELECT 
    c.recipient_email,
    c.subject,
    c.threat_category,
    p.src_ip,
    p.dest_ip,
    p.action as firewall_action,
    c.time as email_time,
    p.time as firewall_time
FROM cisco.email c
JOIN paloalto.threat p 
  ON c.src_ip = p.src_ip 
  AND ABS(EXTRACT(EPOCH FROM c.time) - EXTRACT(EPOCH FROM p.time)) < 300 -- 5 minute window
WHERE c.threat_category IS NOT NULL
  AND p.action = 'block'
  AND c.time > CURRENT_TIMESTAMP - INTERVAL '1' DAY;

-- Multi-vendor endpoint correlation
SELECT 
    cs.host,
    cs.process_name,
    cs.command_line,
    pa.threat_name,
    sec.user,
    sec.action as auth_result
FROM crowdstrike.endpoint cs
JOIN paloalto.threat pa ON cs.dest_ip = pa.dest_ip
JOIN security.authentication sec ON cs.user = sec.user
WHERE cs.time BETWEEN sec.time - INTERVAL '10' MINUTE 
                  AND sec.time + INTERVAL '10' MINUTE
  AND pa.severity = 'high'
  AND cs.time > CURRENT_TIMESTAMP - INTERVAL '2' HOUR;

-- Cross-platform user behavior analysis
SELECT 
    u.user,
    COUNT(DISTINCT auth.src_ip) as login_ips,
    COUNT(DISTINCT cs.host) as accessed_hosts,
    COUNT(pa.session_id) as firewall_sessions,
    MIN(auth.time) as first_activity,
    MAX(cs.time) as last_activity
FROM security.authentication auth
FULL OUTER JOIN crowdstrike.endpoint cs ON auth.user = cs.user
FULL OUTER JOIN paloalto.traffic pa ON auth.src_ip = pa.src_ip
WHERE auth.time > CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY u.user
HAVING COUNT(DISTINCT auth.src_ip) > 5  -- Users with multiple IPs
ORDER BY last_activity DESC;

-- Threat timeline across all platforms
SELECT 
    'Cisco Email' as source,
    threat_category as threat_type,
    recipient_email as target,
    time as event_time
FROM cisco.email 
WHERE threat_category IS NOT NULL

UNION ALL

SELECT 
    'Palo Alto Firewall' as source,
    threat_name as threat_type,
    dest_ip as target,
    time as event_time
FROM paloalto.threat
WHERE severity IN ('high', 'critical')

UNION ALL

SELECT 
    'CrowdStrike Endpoint' as source,
    detection_name as threat_type,
    host as target,
    time as event_time
FROM crowdstrike.endpoint
WHERE severity >= 70

ORDER BY event_time DESC
LIMIT 100;
```

**Why Federation is Required:**

🚫 **Single Connection Limitation**: Each schema can only access **one app context** at a time  
🚫 **Cannot Mix Apps**: You cannot access both `Splunk_TA_cisco-esa` and `Splunk_TA_paloalto` from the same connection  
✅ **Federation Solution**: Multiple schemas allow cross-vendor queries in a single SQL statement  

**Federation Benefits:**

✅ **Multi-Vendor Access**: Each schema connects to a different vendor app context  
✅ **Cross-Platform Correlation**: Join data models from separate vendor Technology Add-ons  
✅ **Unified Analysis**: Query Cisco + Palo Alto + CrowdStrike data in one SQL statement  
✅ **Consistent Interface**: Standard SQL across all vendor platforms  
✅ **Performance Optimization**: Permanent cache (`-1` TTL) for each vendor schema  
✅ **Operational Efficiency**: Single query interface for multi-vendor SOC operations  

**Real-World Use Cases:**
- **Threat Hunting**: Correlate indicators across email, network, and endpoint platforms
- **Incident Response**: Timeline reconstruction using data from multiple security tools
- **User Behavior Analytics**: Cross-platform analysis of user activities
- **Compliance Reporting**: Unified reports spanning multiple security controls
- **Security Metrics**: KPIs and dashboards combining data from all security tools

⚠️ **Warning:** Never disable SSL validation in production environments.

---

## PostgreSQL-Style Metadata Queries

The Splunk adapter provides PostgreSQL-compatible metadata schemas that allow you to query table and column information using standard SQL.

### Available Metadata Schemas

- `pg_catalog` - PostgreSQL-compatible system catalog
- `information_schema` - SQL standard information schema

### Metadata Tables

**PostgreSQL-compatible tables:**
- `pg_catalog.pg_tables` - List of all tables
- `pg_catalog.pg_namespace` - Schema/namespace information
- `pg_catalog.pg_class` - Relations (tables, indexes, etc.)
- `pg_catalog.pg_attribute` - Column attributes and details
- `information_schema.tables` - SQL standard table information
- `information_schema.columns` - Column metadata
- `information_schema.schemata` - Schema information
- `information_schema.views` - View definitions (empty for Splunk)
- `information_schema.table_constraints` - Table constraints (empty for Splunk)
- `information_schema.key_column_usage` - Key column usage (empty for Splunk)

**Splunk-specific tables:**
- `pg_catalog.splunk_indexes` - Splunk index information
- `pg_catalog.splunk_sources` - Splunk data sources

### Metadata Query Examples

```sql
-- List all tables in the splunk schema
SELECT schemaname, tablename, tableowner
FROM pg_catalog.pg_tables
WHERE schemaname = 'splunk';

-- Get detailed table information
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'splunk';

-- View column details for a specific table
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'splunk' AND table_name = 'web'
ORDER BY ordinal_position;

-- Count columns per table
SELECT t.table_name, COUNT(c.column_name) as column_count
FROM information_schema.tables t
JOIN information_schema.columns c
  ON t.table_schema = c.table_schema
  AND t.table_name = c.table_name
WHERE t.table_schema = 'splunk'
GROUP BY t.table_name
ORDER BY column_count DESC;

-- Query Splunk-specific metadata
SELECT index_name, is_internal
FROM pg_catalog.splunk_indexes
ORDER BY index_name;

-- Advanced PostgreSQL-style queries
-- List schemas with OIDs
SELECT oid, nspname as schema_name
FROM pg_catalog.pg_namespace
ORDER BY nspname;

-- Get detailed table information
SELECT c.relname as table_name, c.relkind as relation_type,
       c.relnatts as column_count, n.nspname as schema_name
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = 'splunk'
ORDER BY c.relname;

-- Get column details with PostgreSQL type information
SELECT a.attname as column_name, a.atttypid as type_oid,
       a.attnum as position, a.attnotnull as not_null
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = 'splunk' AND c.relname = 'web' AND a.attnum > 0
ORDER BY a.attnum;
```

### Schema Structure

The adapter creates these top-level schemas:
- `splunk` - Contains your Splunk data tables (web, authentication, etc.)
- `pg_catalog` - PostgreSQL-compatible system catalog
- `information_schema` - SQL standard metadata views

### Case Sensitivity

The adapter uses PostgreSQL-style lexical conventions:
- **Unquoted identifiers** are converted to lowercase: `table_name` → `table_name`
- **Quoted identifiers** preserve case: `"Table_Name"` → `Table_Name`
- **Case-sensitive matching** after normalization

This means you can use natural lowercase SQL:
```sql
-- All of these work without quotes
SELECT table_name FROM information_schema.tables;
SELECT column_name, data_type FROM information_schema.columns;
SELECT schemaname, tablename FROM pg_catalog.pg_tables;
```

---

## Data Model Discovery

The Splunk adapter automatically discovers data models available in your Splunk instance. Common CIM (Common Information Model) data models include:

- `authentication` - Login/logout events, authentication attempts
- `network_traffic` - Network connections, traffic flows
- `web` - Web server access logs, HTTP requests
- `malware` - Malware detection events, antivirus logs
- `email` - Email server logs, message tracking
- `vulnerability` - Vulnerability scan results, security assessments
- `intrusion_detection` - IDS/IPS alerts, security events
- `change` - System changes, configuration modifications
- `compute_inventory` - Asset inventory, system information
- `performance` - System performance metrics, monitoring data

Each discovered data model automatically includes core Splunk fields (`_time`, `host`, `source`, `sourcetype`, `index`) and CIM-specific calculated fields when applicable.

---

## Troubleshooting

### Connection Issues

**Problem:** "Connection refused" or "Unable to connect"
**Solution:**
- Verify Splunk server is running and accessible
- Check firewall settings (port 8089)
- Confirm SSL/TLS configuration

**Problem:** "Authentication failed"
**Solution:**
- Verify username/password or token
- Check user permissions in Splunk
- Ensure user has REST API access

### Query Issues

**Problem:** "Table not found"
**Solution:**
- Check that the data model exists in Splunk
- Verify the app context (use `app` parameter)
- Ensure user has permissions to access the data model
- Try without filters to see all available models

**Problem:** "Field not found"
**Solution:**
- Check field mappings in custom tables
- Verify field names match Splunk data
- Use `SELECT * FROM table LIMIT 5` to explore available fields

### Performance Issues

**Problem:** Slow query performance
**Solution:**
- Add time range filters (`WHERE _time > ...`)
- Limit result sets (`LIMIT` clause)
- Use specific field lists instead of `SELECT *`
- Consider Splunk search optimization

### Debug Mode

Add to your model.json for debugging:
```json
{
  "operand": {
    "url": "https://splunk.example.com:8089",
    "debug": true
  }
}
```

Or set system property:
```
-Dcalcite.debug=true
```

---

## Best Practices

### Security
- Use authentication tokens instead of passwords when possible
- Enable SSL/TLS in production environments
- Limit user permissions to required indexes only
- Rotate authentication credentials regularly

### Performance
- Always include time range filters in queries
- Use `LIMIT` clauses for exploratory queries
- Select specific fields instead of using `SELECT *`
- Consider creating summary indexes in Splunk for frequently accessed data

### Data Modeling
- Use dynamic discovery to automatically find available data models
- Apply filters to limit discovered models
- Create custom tables for specialized data structures
- Include field mappings to provide meaningful column names
- Add `_extra` field to capture unmapped data

### Query Design
- Start with simple queries and add complexity gradually
- Use aggregation functions (`COUNT`, `SUM`, `AVG`) to reduce result set size
- Test queries with small time windows first
- Consider Splunk search performance when designing complex joins

---

## Support

For issues and questions:
- Check Splunk logs for connection errors
- Verify Splunk REST API functionality directly
- Review Calcite documentation for SQL syntax
- Test connectivity with simple queries first
