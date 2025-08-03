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

# Calcite SharePoint List Adapter

This adapter provides full read/write access to SharePoint Lists through Apache Calcite SQL queries using the Microsoft Graph API.

## Features

- **Full CRUD Support**: SELECT, INSERT, DELETE operations on SharePoint lists
- **DDL Support**: CREATE TABLE and DROP TABLE for managing SharePoint lists
- **Microsoft Graph API**: Modern API with better performance and reliability
- **Live connection**: Direct access to SharePoint Lists (no data copying)
- **OAuth2 authentication**: Multiple authentication methods via Azure AD
- **Automatic schema discovery**: Lists are automatically discovered as tables
- **Batch operations**: Optimized bulk inserts using Graph API batching
- **All SharePoint column types**: Full support for text, number, boolean, datetime, etc.
- **PostgreSQL metadata support**: Query SharePoint metadata through standard PostgreSQL system catalogs

## Authentication Methods

The adapter supports multiple authentication methods:

1. **Client Credentials** (Service Principal) - Default
2. **Username/Password** (Resource Owner Password)
3. **Certificate** (Service Principal with Certificate)
4. **Device Code** (Interactive browser-based)
5. **Managed Identity** (For Azure-hosted applications)

## Configuration

### Client Credentials Authentication (Default)

```json
{
  "version": "1.0",
  "defaultSchema": "sharepoint",
  "schemas": [
    {
      "name": "sharepoint",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.sharepoint.SharePointListSchemaFactory",
      "operand": {
        "siteUrl": "https://yourcompany.sharepoint.com/sites/yoursite",
        "authType": "CLIENT_CREDENTIALS",
        "clientId": "your-azure-app-client-id",
        "clientSecret": "your-azure-app-client-secret",
        "tenantId": "your-azure-tenant-id"
      }
    }
  ]
}
```

### Username/Password Authentication

```json
{
  "operand": {
    "siteUrl": "https://yourcompany.sharepoint.com/sites/yoursite",
    "authType": "USERNAME_PASSWORD",
    "clientId": "your-azure-app-client-id",
    "tenantId": "your-azure-tenant-id",
    "username": "user@yourcompany.com",
    "password": "user-password"
  }
}
```

### Certificate Authentication

```json
{
  "operand": {
    "siteUrl": "https://yourcompany.sharepoint.com/sites/yoursite",
    "authType": "CERTIFICATE",
    "clientId": "your-azure-app-client-id",
    "tenantId": "your-azure-tenant-id",
    "certificatePath": "/path/to/certificate.pfx",
    "certificatePassword": "certificate-password",
    "thumbprint": "certificate-thumbprint"
  }
}
```

### Device Code Authentication (Interactive)

```json
{
  "operand": {
    "siteUrl": "https://yourcompany.sharepoint.com/sites/yoursite",
    "authType": "DEVICE_CODE",
    "clientId": "your-azure-app-client-id",
    "tenantId": "your-azure-tenant-id"
  }
}
```

### Managed Identity Authentication (Azure)

```json
{
  "operand": {
    "siteUrl": "https://yourcompany.sharepoint.com/sites/yoursite",
    "authType": "MANAGED_IDENTITY",
    "clientId": "optional-user-assigned-identity-client-id"
  }
}
```

### Individual Table Configuration

```json
{
  "version": "1.0",
  "defaultSchema": "sharepoint",
  "schemas": [
    {
      "name": "sharepoint",
      "tables": [
        {
          "name": "Tasks",
          "type": "custom",
          "factory": "org.apache.calcite.adapter.sharepoint.SharePointListTableFactory",
          "operand": {
            "siteUrl": "https://yourcompany.sharepoint.com/sites/yoursite",
            "listName": "Tasks",
            "authType": "CLIENT_CREDENTIALS",
            "clientId": "your-azure-app-client-id",
            "clientSecret": "your-azure-app-client-secret",
            "tenantId": "your-azure-tenant-id"
          }
        }
      ]
    }
  ]
}
```

## Azure AD App Registration

1. Register an app in Azure AD
2. Grant the following Microsoft Graph API permissions:
   - Sites.ReadWrite.All (for full CRUD support)
   - Sites.Manage.All (for CREATE/DROP table support)

   Or for read-only access:
   - Sites.Read.All

3. Create a client secret (for client credentials auth)
4. Use the Application (client) ID, client secret, and Directory (tenant) ID in your configuration

## Naming Convention

The adapter automatically converts SharePoint display names to SQL-friendly lowercase names:
- SharePoint list "Project Tasks" → SQL table `project_tasks`
- Column "Due Date" → SQL column `due_date`
- Column "Is Complete?" → SQL column `is_complete`

When creating lists/columns, SQL names are converted back to proper SharePoint display names:
- SQL table `project_tasks` → SharePoint list "Project Tasks"
- SQL column `due_date` → SharePoint column "Due Date"

## Usage Examples

### Basic Query
```java
Properties info = new Properties();
info.setProperty("model", "path/to/model.json");

Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
Statement statement = connection.createStatement();

// SELECT query - note lowercase table and column names
ResultSet rs = statement.executeQuery("SELECT * FROM sharepoint.tasks WHERE status = 'Active'");
while (rs.next()) {
    System.out.println(rs.getString("title"));
}
```

### Create a New List
```sql
-- SQL names are automatically converted to SharePoint display names
CREATE TABLE sharepoint.project_tasks (
  title VARCHAR(255) NOT NULL,
  description VARCHAR(1000),
  assigned_to VARCHAR(255),
  due_date TIMESTAMP,
  priority INTEGER,
  is_complete BOOLEAN
)
-- Creates SharePoint list "Project Tasks" with columns like "Due Date", "Is Complete"
```

### Insert Data
```sql
-- Single insert (using lowercase column names)
INSERT INTO sharepoint.project_tasks (title, description, priority, is_complete)
VALUES ('Review documentation', 'Review and update API docs', 1, false)

-- Batch insert (automatically optimized)
INSERT INTO sharepoint.project_tasks (title, description, priority, is_complete)
VALUES
  ('Task 1', 'Description 1', 1, false),
  ('Task 2', 'Description 2', 2, false),
  ('Task 3', 'Description 3', 3, true)
```

### Delete Data
```sql
-- Delete by ID
DELETE FROM sharepoint.project_tasks WHERE id = '123'

-- Delete multiple items
DELETE FROM sharepoint.project_tasks WHERE is_complete = true
```

### Drop a List
```sql
DROP TABLE sharepoint.project_tasks
```

## Supported Column Types

SharePoint Type | SQL Type | Notes
----------------|----------|-------
Text, Note | VARCHAR | Single and multi-line text
Choice, MultiChoice | VARCHAR | Single and multiple selection
Number, Currency | DOUBLE | Decimal numbers
Integer, Counter | INTEGER | Whole numbers
Boolean | BOOLEAN | Yes/No fields
DateTime | TIMESTAMP | Date and time values
Lookup, User | VARCHAR | Displays title/name
URL, Hyperlink | VARCHAR | URL fields

## PostgreSQL Metadata Support

The adapter provides PostgreSQL-compatible system catalogs for metadata discovery. This enables SQL tools that expect PostgreSQL system tables to work seamlessly with SharePoint data.

### Available Metadata Schemas

The adapter exposes two metadata schemas at the root level:
- `pg_catalog` - PostgreSQL system catalog compatibility
- `information_schema` - SQL standard information schema

### Metadata Tables

#### PostgreSQL Catalog Tables

##### pg_catalog.pg_tables
```sql
-- List all SharePoint lists as tables
SELECT * FROM pg_catalog.pg_tables;
```

##### pg_catalog.pg_namespace
```sql
-- List all schemas (namespaces)
SELECT * FROM pg_catalog.pg_namespace;
```

##### pg_catalog.pg_class
```sql
-- List all tables, indexes, sequences, views, etc.
SELECT * FROM pg_catalog.pg_class;
```

##### pg_catalog.pg_attribute
```sql
-- List all table columns
SELECT * FROM pg_catalog.pg_attribute;
```

##### pg_catalog.sharepoint_lists
```sql
-- SharePoint-specific metadata
SELECT * FROM pg_catalog.sharepoint_lists;
```

#### SQL Standard Information Schema Tables

##### information_schema.tables
```sql
-- SQL standard table metadata
SELECT * FROM information_schema.tables;
```

##### information_schema.columns
```sql
-- Detailed column information for all lists
SELECT * FROM information_schema.columns;
```

##### information_schema.schemata
```sql
-- Schema information
SELECT * FROM information_schema.schemata;
```

##### information_schema.views
```sql
-- View metadata (currently empty for SharePoint)
SELECT * FROM information_schema.views;
```

##### information_schema.table_constraints
```sql
-- Table constraints (currently empty for SharePoint)
SELECT * FROM information_schema.table_constraints;
```

##### information_schema.key_column_usage
```sql
-- Key column usage (currently empty for SharePoint)
SELECT * FROM information_schema.key_column_usage;
```

### Example Metadata Queries

```sql
-- Find all task lists
SELECT * FROM pg_catalog.sharepoint_lists
WHERE template_type = 'TasksList';

-- Get column details for a specific list
SELECT column_name, data_type, is_nullable, ordinal_position
FROM information_schema.columns
WHERE table_name = 'project_tasks'
ORDER BY ordinal_position;

-- Count columns per list
SELECT table_name, COUNT(*) as column_count
FROM information_schema.columns
GROUP BY table_name
ORDER BY column_count DESC;
```

### Tool Compatibility

This metadata support enables compatibility with:
- PostgreSQL admin tools (pgAdmin, DBeaver, etc.)
- BI tools expecting PostgreSQL metadata
- Schema documentation generators
- Database migration tools

## Performance Optimization

- **Batch Operations**: The adapter automatically batches INSERT operations in groups of 20 (Graph API limit)
- **Efficient Queries**: Uses `$expand` to minimize API calls
- **Pagination**: Handles large result sets automatically

## Limitations

- **UPDATE**: Direct UPDATE statements are not supported (use DELETE + INSERT)
- **Complex Types**: No support for attachments or managed metadata
- **API Limits**: Subject to Microsoft Graph API throttling limits
- **Filtering**: Graph API OData filter syntax limitations apply
- **Name Conversion**: SharePoint lists/columns with null or empty display names use internal names as fallback for SQL identifiers

## Testing

### Unit Tests
Unit tests can run without SharePoint credentials and test CAST operations, query parsing, and schema handling:
```bash
./gradlew :sharepoint-list:test --tests "*SharePointCastProjectionTest*"
```

### Integration Tests
Integration tests require SharePoint credentials. 

**Option 1: Using local properties file (recommended)**
Copy `../file/local-test.properties.sample` to `../file/local-test.properties` and fill in your credentials:

```properties
SHAREPOINT_TENANT_ID=your-tenant-id
SHAREPOINT_CLIENT_ID=your-client-id
SHAREPOINT_CLIENT_SECRET=your-client-secret
SHAREPOINT_SITE_URL=https://yoursite.sharepoint.com
```

**Option 2: Using environment variables**
Set the credentials as environment variables:
```bash
export SHAREPOINT_TENANT_ID=your-tenant-id
export SHAREPOINT_CLIENT_ID=your-client-id
export SHAREPOINT_CLIENT_SECRET=your-client-secret
export SHAREPOINT_SITE_URL=https://yoursite.sharepoint.com
```

**Run integration tests:**
```bash
SHAREPOINT_INTEGRATION_TESTS=true ./gradlew :sharepoint-list:test
```

**Security Note:** Never commit actual credentials to version control. The `local-test.properties` file is in `.gitignore` to prevent accidental commits.
