# Splunk JDBC Adapter - User Guide

Connect to Splunk using standard JDBC and query your data with SQL.

## Overview

The Splunk JDBC Adapter allows you to:
- Connect to Splunk using standard JDBC drivers
- Query Splunk data using familiar SQL syntax
- Access Splunk Common Information Model (CIM) data
- Create custom table definitions for your specific data

## Prerequisites

- Splunk server with REST API access (port 8089 by default)
- Valid Splunk credentials (username/password or authentication token)
- Java application with JDBC support
- Apache Calcite JDBC driver and Splunk adapter JAR files

## Connection Methods

You can connect to Splunk in two ways:

### Method 1: Direct JDBC URL (Simple)
### Method 2: Calcite Model File (Advanced)

---

## Method 1: Direct JDBC URL

For simple connections, use a direct JDBC URL with embedded configuration.

### Basic Connection

```java
String url = "jdbc:splunk://https://splunk.example.com:8089?" +
             "username=admin&password=your_password&cim_model=authentication";

Connection conn = DriverManager.getConnection(url);
```

### URL Format

```
jdbc:splunk://[splunk_url]?[parameters]
```

**Parameters:**
- `username` - Splunk username
- `password` - Splunk password
- `token` - Authentication token (alternative to username/password)
- `cim_model` - Single CIM model to expose as a table
- `cim_models` - Comma-separated list of CIM models
- `disable_ssl_validation` - Set to `true` for development (not recommended for production)

### Examples

**Single CIM Model:**
```java
String url = "jdbc:splunk://https://splunk.company.com:8089?" +
             "username=analyst&password=secret123&cim_model=authentication";
```

**Multiple CIM Models:**
```java
String url = "jdbc:splunk://https://splunk.company.com:8089?" +
             "token=your_auth_token&cim_models=authentication,network_traffic,web";
```

**Development with SSL Disabled:**
```java
String url = "jdbc:splunk://https://localhost:8089?" +
             "username=admin&password=changeme&cim_model=authentication&disable_ssl_validation=true";
```

---

## Method 2: Calcite Model File

For advanced configurations, create a `model.json` file with detailed table definitions.

### Basic Model File

Create a file named `model.json`:

```json
{
  "version": "1.0",
  "defaultSchema": "splunk",
  "schemas": [
    {
      "name": "splunk",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.splunk.SplunkSchemaFactory",
      "operand": {
        "url": "https://splunk.example.com:8089",
        "username": "admin",
        "password": "your_password",
        "cim_model": "authentication"
      }
    }
  ]
}
```

### Connection with Model File

```java
String url = "jdbc:calcite:model=/path/to/model.json";
Connection conn = DriverManager.getConnection(url);
```

### Advanced Model Examples

#### Multiple CIM Models

```json
{
  "version": "1.0",
  "defaultSchema": "splunk",
  "schemas": [
    {
      "name": "splunk",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.splunk.SplunkSchemaFactory",
      "operand": {
        "url": "https://splunk.example.com:8089",
        "token": "your_authentication_token",
        "cim_models": ["authentication", "network_traffic", "web", "malware"]
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
      "type": "custom",
      "factory": "org.apache.calcite.adapter.splunk.SplunkSchemaFactory",
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

---

## Available CIM Models

You can use these predefined CIM models:

- `authentication` - Login/logout events, authentication attempts
- `network_traffic` - Network connections, traffic flows
- `web` - Web server access logs, HTTP requests
- `malware` - Malware detection events, antivirus logs
- `email` - Email server logs, message tracking
- `vulnerability` - Vulnerability scan results, security assessments
- `intrusion_detection` (or `ids`) - IDS/IPS alerts, security events
- `change` - System changes, configuration modifications
- `inventory` - Asset inventory, system information
- `performance` - System performance metrics, monitoring data

Each CIM model provides a standardized schema with common fields automatically mapped to the appropriate Splunk field names.

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
- Check CIM model name spelling
- Verify data exists in specified indexes
- Confirm user has access to required indexes

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
- Use CIM models when your data fits standard formats
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
