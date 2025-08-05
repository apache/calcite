# Splunk Adapter - Calculated Fields Usage

The Splunk adapter now supports adding calculated fields to dynamically discovered data models. This feature allows you to specify additional fields that exist in your Splunk environment but aren't exposed in the data model metadata.

## Automatic Core Fields

All dynamically discovered models automatically include core Splunk fields:
- `_time` (mapped to `time` in Calcite)
- `host`
- `source`
- `sourcetype`
- `index`

## Automatic CIM Calculated Fields

Known CIM models automatically receive common calculated fields based on their type:

### Authentication Model
- `action`, `app`, `user`, `src`, `src_user`, `dest`
- `is_failure`, `is_success` (boolean fields)

### Web Model
- `action`, `app`, `user`, `src`, `dest`

### Network Traffic Model
- `action`, `app`, `src`, `dest`, `src_ip`, `dest_ip`
- `src_port`, `dest_port` (integer fields)

### Other CIM Models
- `action` field is added to all recognized CIM models

## Custom Calculated Fields

You can specify additional calculated fields for any discovered model using the `calculatedFields` operand:

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
        "url": "https://localhost:8089",
        "username": "admin",
        "password": "password",
        "calculatedFields": {
          "authentication": [
            {"name": "custom_field1", "type": "VARCHAR"},
            {"name": "risk_score", "type": "INTEGER"},
            {"name": "is_critical", "type": "BOOLEAN"}
          ],
          "web": [
            {"name": "response_code", "type": "INTEGER"},
            {"name": "content_length", "type": "BIGINT"}
          ]
        }
      }
    }
  ]
}
```

## Supported Types

The following SQL types are supported for calculated fields:
- `VARCHAR`, `STRING`, `TEXT` → VARCHAR
- `INTEGER`, `INT` → INTEGER
- `BIGINT`, `LONG` → BIGINT
- `DOUBLE`, `FLOAT` → DOUBLE
- `BOOLEAN`, `BOOL` → BOOLEAN
- `TIMESTAMP`, `DATETIME` → TIMESTAMP
- `DATE` → DATE
- `TIME` → TIME

## Example: Adding Custom Fields to Multiple Models

```json
{
  "calculatedFields": {
    "authentication": [
      {"name": "risk_score", "type": "INTEGER"},
      {"name": "geo_country", "type": "VARCHAR"},
      {"name": "geo_city", "type": "VARCHAR"}
    ],
    "network_traffic": [
      {"name": "bytes", "type": "BIGINT"},
      {"name": "packets", "type": "BIGINT"},
      {"name": "protocol_name", "type": "VARCHAR"}
    ],
    "endpoint": [
      {"name": "file_hash", "type": "VARCHAR"},
      {"name": "registry_path", "type": "VARCHAR"},
      {"name": "process_guid", "type": "VARCHAR"}
    ]
  }
}
```

## Notes

1. Calculated fields are added after the model's base fields, so they won't override existing fields
2. Model names in the `calculatedFields` map are case-insensitive
3. All calculated fields are nullable by default
4. This feature works with dynamic discovery only (not with `disableDynamicDiscovery: true`)
