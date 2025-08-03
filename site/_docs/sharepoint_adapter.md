---
layout: docs
title: SharePoint Lists adapter
permalink: /docs/sharepoint_adapter.html
---
<!--
{% comment %}
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
{% endcomment %}
-->

The SharePoint Lists adapter provides full read/write access to SharePoint Lists through SQL queries using the Microsoft Graph API.

## Features

* **Full CRUD Support**: SELECT, INSERT, DELETE operations on SharePoint lists
* **DDL Support**: CREATE TABLE and DROP TABLE for managing SharePoint lists
* **Microsoft Graph API**: Modern API with better performance and reliability
* **OAuth2 authentication**: Multiple authentication methods via Azure AD
* **Automatic schema discovery**: Lists are automatically discovered as tables
* **Batch operations**: Optimized bulk inserts using Graph API batching
* **PostgreSQL metadata support**: Query SharePoint metadata through standard PostgreSQL system catalogs

## Model

### Schema Factory

The schema factory creates a schema with all accessible SharePoint lists as tables:

{% highlight json %}
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
{% endhighlight %}

### Table Factory

For individual table configuration:

{% highlight json %}
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
{% endhighlight %}

## Authentication

The adapter supports multiple authentication methods:

### Client Credentials (Service Principal)
{% highlight json %}
{
  "authType": "CLIENT_CREDENTIALS",
  "clientId": "your-azure-app-client-id",
  "clientSecret": "your-azure-app-client-secret",
  "tenantId": "your-azure-tenant-id"
}
{% endhighlight %}

### Username/Password
{% highlight json %}
{
  "authType": "USERNAME_PASSWORD",
  "clientId": "your-azure-app-client-id",
  "tenantId": "your-azure-tenant-id",
  "username": "user@yourcompany.com",
  "password": "user-password"
}
{% endhighlight %}

### Certificate
{% highlight json %}
{
  "authType": "CERTIFICATE",
  "clientId": "your-azure-app-client-id",
  "tenantId": "your-azure-tenant-id",
  "certificatePath": "/path/to/certificate.pfx",
  "certificatePassword": "certificate-password",
  "thumbprint": "certificate-thumbprint"
}
{% endhighlight %}

### Device Code (Interactive)
{% highlight json %}
{
  "authType": "DEVICE_CODE",
  "clientId": "your-azure-app-client-id",
  "tenantId": "your-azure-tenant-id"
}
{% endhighlight %}

### Managed Identity (Azure)
{% highlight json %}
{
  "authType": "MANAGED_IDENTITY",
  "clientId": "optional-user-assigned-identity-client-id"
}
{% endhighlight %}

## SQL Operations

### SELECT
{% highlight sql %}
-- Query SharePoint lists
SELECT * FROM sharepoint.tasks WHERE status = 'Active';
SELECT title, due_date FROM sharepoint.project_tasks WHERE priority > 2;
{% endhighlight %}

### INSERT
{% highlight sql %}
-- Single insert
INSERT INTO sharepoint.tasks (title, description, priority)
VALUES ('Review documentation', 'Review and update API docs', 1);

-- Batch insert (automatically optimized)
INSERT INTO sharepoint.tasks (title, description, priority)
VALUES
  ('Task 1', 'Description 1', 1),
  ('Task 2', 'Description 2', 2),
  ('Task 3', 'Description 3', 3);
{% endhighlight %}

### DELETE
{% highlight sql %}
-- Delete by ID
DELETE FROM sharepoint.tasks WHERE id = '123';

-- Delete multiple items
DELETE FROM sharepoint.tasks WHERE is_complete = true;
{% endhighlight %}

### CREATE TABLE
{% highlight sql %}
-- Create a new SharePoint list
CREATE TABLE sharepoint.project_tasks (
  title VARCHAR(255) NOT NULL,
  description VARCHAR(1000),
  assigned_to VARCHAR(255),
  due_date TIMESTAMP,
  priority INTEGER,
  is_complete BOOLEAN
);
{% endhighlight %}

### DROP TABLE
{% highlight sql %}
-- Delete a SharePoint list
DROP TABLE sharepoint.project_tasks;
{% endhighlight %}

## PostgreSQL Metadata Support

The adapter provides PostgreSQL-compatible system catalogs for metadata discovery at the root level.

### Metadata Schemas

* `pg_catalog` - PostgreSQL system catalog compatibility
* `information_schema` - SQL standard information schema

### Metadata Tables

#### PostgreSQL Catalog Tables

##### pg_catalog.pg_tables
{% highlight sql %}
-- List all SharePoint lists as tables
SELECT * FROM pg_catalog.pg_tables;
{% endhighlight %}

##### pg_catalog.pg_namespace
{% highlight sql %}
-- List all schemas (namespaces)
SELECT * FROM pg_catalog.pg_namespace;
{% endhighlight %}

##### pg_catalog.pg_class
{% highlight sql %}
-- List all tables, indexes, sequences, views, etc.
SELECT * FROM pg_catalog.pg_class;
{% endhighlight %}

##### pg_catalog.pg_attribute
{% highlight sql %}
-- List all table columns
SELECT * FROM pg_catalog.pg_attribute;
{% endhighlight %}

##### pg_catalog.sharepoint_lists
{% highlight sql %}
-- SharePoint-specific metadata
SELECT list_id, display_name, template_type, item_count
FROM pg_catalog.sharepoint_lists
WHERE template_type = 'TasksList';
{% endhighlight %}

#### SQL Standard Information Schema Tables

##### information_schema.tables
{% highlight sql %}
-- SQL standard table metadata
SELECT * FROM information_schema.tables;
{% endhighlight %}

##### information_schema.columns
{% highlight sql %}
-- Detailed column information for all lists
SELECT column_name, data_type, is_nullable, ordinal_position
FROM information_schema.columns
WHERE table_name = 'project_tasks'
ORDER BY ordinal_position;
{% endhighlight %}

##### information_schema.schemata
{% highlight sql %}
-- Schema information
SELECT * FROM information_schema.schemata;
{% endhighlight %}

##### information_schema.views
{% highlight sql %}
-- View metadata (currently empty for SharePoint)
SELECT * FROM information_schema.views;
{% endhighlight %}

##### information_schema.table_constraints
{% highlight sql %}
-- Table constraints (currently empty for SharePoint)
SELECT * FROM information_schema.table_constraints;
{% endhighlight %}

##### information_schema.key_column_usage
{% highlight sql %}
-- Key column usage (currently empty for SharePoint)
SELECT * FROM information_schema.key_column_usage;
{% endhighlight %}

## Column Type Mapping

| SharePoint Type | SQL Type | Notes |
|----------------|----------|-------|
| Text, Note | VARCHAR | Single and multi-line text |
| Choice, MultiChoice | VARCHAR | Single and multiple selection |
| Number, Currency | DOUBLE | Decimal numbers |
| Integer, Counter | INTEGER | Whole numbers |
| Boolean | BOOLEAN | Yes/No fields |
| DateTime | TIMESTAMP | Date and time values |
| Lookup, User | VARCHAR | Displays title/name |
| URL, Hyperlink | VARCHAR | URL fields |

## Naming Convention

The adapter automatically converts between SharePoint display names and SQL-friendly names:
* SharePoint list "Project Tasks" → SQL table `project_tasks`
* Column "Due Date" → SQL column `due_date`
* Column "Is Complete?" → SQL column `is_complete`

## Azure AD Configuration

1. Register an app in Azure AD
2. Grant Microsoft Graph API permissions:
   * `Sites.ReadWrite.All` (for full CRUD support)
   * `Sites.Manage.All` (for CREATE/DROP table support)
   * Or `Sites.Read.All` (for read-only access)
3. Create a client secret (for client credentials auth)
4. Use the Application (client) ID, client secret, and Directory (tenant) ID in your configuration

## Limitations

* **UPDATE**: Direct UPDATE statements are not supported (use DELETE + INSERT)
* **Complex Types**: No support for attachments or managed metadata
* **API Limits**: Subject to Microsoft Graph API throttling limits
* **Filtering**: Graph API OData filter syntax limitations apply
* **Name Conversion**: SharePoint lists/columns with null or empty display names use internal names as fallback for SQL identifiers
