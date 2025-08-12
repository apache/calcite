# Storage Providers

The Apache Calcite File Adapter supports multiple storage systems through its pluggable storage provider architecture.

## How Storage Providers Work

### Automatic Detection vs Explicit Configuration

Storage providers work in two different ways:

#### 1. **Explicit Configuration** (Schema-level)
When you configure a schema with a specific `storageType`, ALL files in that schema use that storage provider:

```json
{
  "name": "S3_DATA",
  "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
  "operand": {
    "storageType": "s3",
    "storageConfig": {
      "bucket": "my-bucket",
      "region": "us-east-1",
      "prefix": "data/"
    }
  }
}
```

**Key Points:**
- `storageType: "s3"` forces S3 provider for ALL files
- S3 provider lists files under the specified prefix
- The `directory` field is ignored when `storageType` is set

With this configuration:
- The File Adapter uses S3 storage provider to **list** files in the bucket
- ALL discovered files are accessed through S3
- Local file system is NOT used at all
- The `directory` field is ignored when `storageType` is specified

#### 2. **URL-based Detection** (Table-level)
When defining individual tables, the storage provider is determined by the URL scheme:

```json
{
  "tables": [
    {
      "name": "local_data",
      "url": "/local/path/data.csv"
    },
    {
      "name": "s3_data",
      "url": "s3://bucket/data.parquet"
    },
    {
      "name": "api_data", 
      "url": "https://api.example.com/data.json"
    }
  ]
}
```

**URL Scheme Mapping:**
- `/local/path/data.csv` → LocalFileStorageProvider
- `s3://bucket/data.parquet` → S3StorageProvider  
- `https://api.example.com/data.json` → HttpStorageProvider

### Storage Provider Selection Logic

```
Is storageType explicitly configured in schema?
├─> YES: Use that provider for ALL files in schema
│   └─> Provider lists files from remote storage (S3, SharePoint, etc.)
└─> NO: Default to local file system
    │
    ├─> Use 'directory' field to scan local filesystem
    └─> Individual tables can override with explicit URLs:
        ├─> s3:// → S3StorageProvider
        ├─> http(s):// → HttpStorageProvider
        ├─> ftp:// → FtpStorageProvider
        ├─> sftp:// → SftpStorageProvider
        └─> /path or file:// → LocalFileStorageProvider
```

### Key Concepts

**Schema-level Storage Provider:**
- Applies to ALL files discovered in that schema
- Provider lists files from remote storage
- No mixing of storage types within a schema

**Table-level Storage Provider:**
- Each table can use a different provider
- Determined by URL scheme
- Allows mixing storage types in one schema

## Common Usage Patterns

### Pattern 1: All Files in Cloud Storage

```json
{
  "schemas": [{
    "name": "CLOUD_DATA",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "storageType": "s3",
      "storageConfig": {
        "bucket": "data-lake",
        "region": "us-east-1",
        "prefix": "warehouse/"
      }
    }
  }]
}
```
**Result:** File Adapter lists all CSV, JSON, Parquet files in `s3://data-lake/warehouse/` and creates tables.

### Pattern 2: Mixed Storage in One Schema

```json
{
  "schemas": [{
    "name": "HYBRID_DATA",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/local/data",
      "tables": [
        {
          "name": "cloud_sales",
          "url": "s3://bucket/sales.parquet"
        },
        {
          "name": "api_customers",
          "url": "https://api.example.com/customers.json"
        }
      ]
    }
  }]
}
```
**Result:** 
- Discovers local files in `/local/data/`
- PLUS explicitly defined tables from S3 and HTTP

### Pattern 3: Multiple Schemas, Different Storage

```json
{
  "schemas": [
    {
      "name": "LOCAL",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/data/local"
      }
    },
    {
      "name": "S3",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "storageType": "s3",
        "storageConfig": {
          "bucket": "my-bucket",
          "region": "us-west-2"
        }
      }
    },
    {
      "name": "API",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "storageType": "http",
        "storageConfig": {
          "baseUrl": "https://data.api.com"
        }
      }
    }
  ]
}
```
**Result:** Three schemas, each using a different storage provider exclusively.

## Local File System

The default storage provider for accessing files on the local file system.

### Configuration

```json
{
  "storageProvider": {
    "type": "local",
    "basePath": "/data",
    "options": {
      "followSymlinks": true,
      "readOnly": false,
      "filePermissions": "644"
    }
  }
}
```

### Features

- **Direct file system access** - No additional dependencies
- **Symbolic link support** - Follow or ignore symbolic links
- **Permission checking** - Validate file permissions before access
- **Path validation** - Ensure paths are within allowed directories

### Security Considerations

```json
{
  "security": {
    "allowedPaths": ["/data/public", "/data/shared"],
    "deniedPaths": ["/etc", "/home", "/root"],
    "validatePermissions": true,
    "restrictToBasePath": true
  }
}
```

## Amazon S3

Cloud storage provider for AWS S3 buckets with full feature support.

### Basic Configuration

```json
{
  "storageProvider": {
    "type": "s3",
    "bucket": "my-data-bucket",
    "region": "us-east-1",
    "credentials": {
      "accessKey": "${AWS_ACCESS_KEY}",
      "secretKey": "${AWS_SECRET_KEY}"
    }
  }
}
```

### Advanced Configuration

```json
{
  "storageProvider": {
    "type": "s3",
    "bucket": "my-data-bucket",
    "region": "us-east-1",
    "prefix": "data/",
    "credentials": {
      "profileName": "production",
      "roleArn": "arn:aws:iam::123456789012:role/DataAccessRole"
    },
    "options": {
      "encryption": "AES256",
      "storageClass": "STANDARD_IA",
      "timeout": "30s",
      "maxConnections": 50,
      "usePathStyleAccess": false,
      "enableAcceleration": true
    }
  }
}
```

### Authentication Methods

**1. Environment Variables:**
```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1
```

**2. IAM Roles:**
```json
{
  "credentials": {
    "roleArn": "arn:aws:iam::123456789012:role/DataAccessRole",
    "sessionName": "CalciteFileAdapter"
  }
}
```

**3. AWS Profiles:**
```json
{
  "credentials": {
    "profileName": "data-access"
  }
}
```

### S3-Compatible Storage

For MinIO, Wasabi, or other S3-compatible services:

```json
{
  "storageProvider": {
    "type": "s3",
    "endpoint": "https://s3.wasabisys.com",
    "bucket": "my-bucket",
    "region": "us-east-1",
    "options": {
      "usePathStyleAccess": true,
      "disableSSL": false
    }
  }
}
```

## HTTP/HTTPS

Access files via HTTP/HTTPS including REST APIs and web services.

### Basic HTTP Configuration

```json
{
  "storageProvider": {
    "type": "http",
    "baseUrl": "https://api.example.com/data",
    "options": {
      "timeout": "60s",
      "followRedirects": true,
      "maxRedirects": 10
    }
  }
}
```

### Authentication

**Bearer Token:**
```json
{
  "authentication": {
    "type": "bearer",
    "token": "${API_TOKEN}"
  }
}
```

**Basic Authentication:**
```json
{
  "authentication": {
    "type": "basic",
    "username": "${API_USERNAME}",
    "password": "${API_PASSWORD}"
  }
}
```

**API Key:**
```json
{
  "authentication": {
    "type": "apikey",
    "keyName": "X-API-Key",
    "keyValue": "${API_KEY}",
    "location": "header"
  }
}
```

### Custom Headers

```json
{
  "headers": {
    "User-Agent": "Calcite-File-Adapter/1.0",
    "Accept": "application/json",
    "X-Custom-Header": "custom-value"
  }
}
```

### REST API Integration

```json
{
  "storageProvider": {
    "type": "http",
    "baseUrl": "https://api.example.com/v1",
    "endpoints": {
      "fileList": "/files",
      "fileContent": "/files/{filename}",
      "metadata": "/files/{filename}/metadata"
    },
    "authentication": {
      "type": "oauth2",
      "clientId": "${OAUTH_CLIENT_ID}",
      "clientSecret": "${OAUTH_CLIENT_SECRET}",
      "tokenUrl": "https://auth.example.com/oauth/token"
    }
  }
}
```

### GraphQL Support

```json
{
  "storageProvider": {
    "type": "http",
    "baseUrl": "https://api.example.com/graphql",
    "graphql": {
      "enabled": true,
      "query": "query GetFiles { files { name, url, size } }",
      "variables": {},
      "dataPath": "$.data.files"
    }
  }
}
```

## Microsoft SharePoint

Integration with SharePoint Online and on-premises SharePoint.

### SharePoint Online Configuration

```json
{
  "storageProvider": {
    "type": "sharepoint",
    "siteUrl": "https://company.sharepoint.com/sites/data",
    "authentication": {
      "type": "oauth",
      "clientId": "${SHAREPOINT_CLIENT_ID}",
      "clientSecret": "${SHAREPOINT_CLIENT_SECRET}",
      "tenantId": "${TENANT_ID}"
    },
    "options": {
      "apiVersion": "v1.0",
      "libraryName": "Documents",
      "timeout": "30s"
    }
  }
}
```

### SharePoint On-Premises

```json
{
  "storageProvider": {
    "type": "sharepoint",
    "siteUrl": "https://sharepoint.company.com/sites/data",
    "authentication": {
      "type": "ntlm",
      "username": "${DOMAIN}\\${USERNAME}",
      "password": "${PASSWORD}"
    },
    "options": {
      "apiVersion": "legacy",
      "useWebServices": true
    }
  }
}
```

### Document Library Access

```json
{
  "documentLibraries": [
    {
      "name": "Shared Documents",
      "path": "/Shared Documents",
      "recursive": true,
      "fileTypes": ["*.csv", "*.xlsx", "*.json"]
    },
    {
      "name": "Reports",
      "path": "/Reports/Monthly",
      "recursive": false
    }
  ]
}
```

## Microsoft Graph

Access files from Office 365, OneDrive, and Teams through Microsoft Graph API.

### Configuration

```json
{
  "storageProvider": {
    "type": "graph",
    "tenantId": "${TENANT_ID}",
    "authentication": {
      "clientId": "${CLIENT_ID}",
      "clientSecret": "${CLIENT_SECRET}",
      "scope": "Files.Read.All Sites.Read.All"
    },
    "sources": [
      {
        "type": "onedrive",
        "userId": "user@company.com",
        "path": "/data"
      },
      {
        "type": "sharepoint",
        "siteId": "company.sharepoint.com,site-guid",
        "driveId": "drive-guid"
      }
    ]
  }
}
```

### OneDrive Access

```json
{
  "onedrive": {
    "userPrincipalName": "user@company.com",
    "folderPath": "/Data Files",
    "recursive": true,
    "sharedWithMe": false
  }
}
```

### Teams Files Access

```json
{
  "teams": {
    "teamId": "team-guid",
    "channelId": "channel-guid",
    "folderPath": "/General/Data",
    "includePrivateChannels": false
  }
}
```

## FTP and SFTP

File Transfer Protocol support for legacy systems and secure file transfer.

### FTP Configuration

```json
{
  "storageProvider": {
    "type": "ftp",
    "host": "ftp.example.com",
    "port": 21,
    "credentials": {
      "username": "${FTP_USERNAME}",
      "password": "${FTP_PASSWORD}"
    },
    "options": {
      "passiveMode": true,
      "timeout": "30s",
      "encoding": "UTF-8"
    }
  }
}
```

### SFTP Configuration

```json
{
  "storageProvider": {
    "type": "sftp",
    "host": "sftp.example.com",
    "port": 22,
    "credentials": {
      "username": "${SFTP_USERNAME}",
      "password": "${SFTP_PASSWORD}"
    },
    "options": {
      "strictHostKeyChecking": false,
      "timeout": "30s",
      "keepAlive": true,
      "compression": true
    }
  }
}
```

### SSH Key Authentication

```json
{
  "credentials": {
    "username": "datauser",
    "privateKeyPath": "/path/to/private/key",
    "passphrase": "${KEY_PASSPHRASE}"
  }
}
```

### Known Hosts Configuration

```json
{
  "security": {
    "knownHostsFile": "/path/to/known_hosts",
    "strictHostKeyChecking": true,
    "hostKeyVerification": "strict"
  }
}
```

## Multi-Provider Configuration

Configure multiple storage providers for different data sources.

```json
{
  "storageProviders": [
    {
      "name": "local-data",
      "type": "local",
      "basePath": "/data/local",
      "tablePrefix": "local_"
    },
    {
      "name": "cloud-data",
      "type": "s3",
      "bucket": "cloud-data-bucket",
      "region": "us-east-1",
      "tablePrefix": "cloud_"
    },
    {
      "name": "api-data",
      "type": "http",
      "baseUrl": "https://api.partner.com/data",
      "tablePrefix": "api_"
    }
  ]
}
```

## Storage Provider Performance

### Performance Characteristics

| Provider | Latency | Throughput | Best Use Case |
|----------|---------|------------|---------------|
| Local | Lowest | Highest | Development, small datasets |
| S3 | Medium | High | Large datasets, analytics |
| HTTP | Variable | Medium | APIs, real-time data |
| SharePoint | High | Medium | Office documents, collaboration |
| FTP/SFTP | High | Low | Legacy systems, batch processing |

### Optimization Tips

**Local File System:**
- Use SSD storage for better I/O performance
- Consider RAID configurations for large datasets
- Enable file system caching

**Amazon S3:**
- Use appropriate storage class (Standard, IA, Glacier)
- Enable S3 Transfer Acceleration for global access
- Configure parallel downloads for large files
- Use CloudFront for frequently accessed data

**HTTP/HTTPS:**
- Implement connection pooling
- Enable HTTP/2 when supported
- Use CDN for static file content
- Configure appropriate timeout values

**SharePoint/Graph:**
- Use batch requests when possible
- Cache authentication tokens
- Limit concurrent requests to avoid throttling

## Error Handling

### Current Capabilities

The File Adapter provides basic error handling for storage providers:

| Feature | Description | Configuration |
|---------|-------------|--------------|
| HTTP Timeout | Connection timeout for HTTP/HTTPS requests | Set via `httpTimeout` property |
| HTTP Retries | Simple retry count for failed requests | Set via `httpRetries` property |

**Example Configuration:**
```json
{
  "storageProvider": {
    "type": "http",
    "baseUrl": "https://api.example.com/data",
    "options": {
      "timeout": "60s",         // Connection timeout
      "maxRetries": 3           // Retry failed requests 3 times
    }
  }
}
```

**Note:** The File Adapter does NOT currently support:
- Exponential backoff retry strategies
- Circuit breaker patterns  
- Health check monitoring
- Configurable retry policies
- Custom error recovery strategies

For production deployments requiring advanced error handling, consider implementing these features at the application level or using a reverse proxy/API gateway.

## Security Best Practices

### Credential Management

- Use environment variables for sensitive information
- Implement credential rotation
- Use IAM roles instead of access keys when possible
- Store credentials in secure vaults (HashiCorp Vault, AWS Secrets Manager)

### Network Security

- Use HTTPS/TLS for all external connections
- Implement certificate validation
- Configure appropriate firewall rules
- Use VPN for accessing private resources

### Access Control

- Implement least privilege principle
- Use resource-based policies where supported
- Regular audit of permissions and access logs
- Implement data classification and handling policies