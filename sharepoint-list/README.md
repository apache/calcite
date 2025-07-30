# Calcite SharePoint List Adapter

This adapter provides read-only access to SharePoint Lists through Apache Calcite SQL queries.

## Features

- Live connection to SharePoint Lists (no data copying)
- OAuth2 authentication using Azure AD app registration
- Automatic schema discovery
- Support for all standard SharePoint column types
- Pagination support for large lists

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
2. Grant the following API permissions:
   - Microsoft Graph: Sites.Read.All
   - SharePoint: Sites.Read.All
3. Create a client secret
4. Use the Application (client) ID, client secret, and Directory (tenant) ID in your configuration

## Usage Example

```java
Properties info = new Properties();
info.setProperty("model", "path/to/model.json");

Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
Statement statement = connection.createStatement();

ResultSet rs = statement.executeQuery("SELECT * FROM sharepoint.Tasks WHERE Status = 'Active'");
while (rs.next()) {
    System.out.println(rs.getString("Title"));
}
```

## Supported Column Types

- Text, Note, Choice, MultiChoice → VARCHAR
- Number, Currency → DOUBLE
- Integer, Counter → INTEGER
- Boolean → BOOLEAN
- DateTime → TIMESTAMP
- Lookup, User → VARCHAR (displays title/name)
- URL → VARCHAR

## Limitations

- Read-only access
- No support for attachments or complex column types
- Maximum 5000 items per query (SharePoint API limitation)
