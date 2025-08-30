# SharePoint List Adapter Configuration Guide

## Overview

The SharePoint List adapter for Apache Calcite provides comprehensive configuration options to support various SharePoint deployments and enterprise requirements.

## Configuration Schema

```json
{
  "schemas": [{
    "name": "SHAREPOINT",
    "factory": "org.apache.calcite.adapter.sharepoint.SharePointListSchemaFactory",
    "operand": {
      // Required
      "siteUrl": "string",

      // API Selection (optional)
      "useGraphApi": "boolean",
      "useRestApi": "boolean",
      "useLegacyAuth": "boolean",

      // Phase 1: Direct Authentication
      "clientId": "string",
      "clientSecret": "string",
      "tenantId": "string",
      "certificatePath": "string",
      "certificatePassword": "string",
      "accessToken": "string",

      // Phase 2: External Token Sources
      "tokenCommand": "string",
      "tokenEnv": "string",
      "tokenFile": "string",
      "tokenEndpoint": "string",

      // Phase 3: Auth Proxy
      "authProxy": {
        "endpoint": "string",
        "apiKey": "string",
        "resource": "string",
        "cacheEnabled": "boolean",
        "cacheTtl": "number"
      },

      // Custom Auth Provider
      "authProvider": "string",

      // Additional Headers
      "additionalHeaders": {
        "key": "value"
      }
    }
  }]
}
```

## Authentication Hierarchy

The adapter evaluates authentication methods in the following order:

1. **Auth Proxy** (`authProxy` configuration)
2. **Custom Provider** (`authProvider` class name)
3. **External Tokens**:
   - Command (`tokenCommand`)
   - Environment Variable (`tokenEnv`)
   - File (`tokenFile`)
   - Endpoint (`tokenEndpoint`)
4. **Static Token** (`accessToken`)
5. **Client Credentials** (`clientId` + `clientSecret`)
6. **Certificate** (`certificatePath` + `certificatePassword`)

## API Selection

### Automatic Selection

The adapter automatically selects the appropriate API based on the SharePoint URL:

| URL Pattern | Selected API | Reason |
|------------|--------------|--------|
| `*.sharepoint.com` | Microsoft Graph | SharePoint Online |
| `*.sharepoint.cn` | Microsoft Graph | SharePoint China |
| `*.sharepoint.de` | Microsoft Graph | SharePoint Germany |
| `*.sharepoint-mil.us` | Microsoft Graph | SharePoint US Gov |
| `*.local` | SharePoint REST | Private network |
| `*.corp` | SharePoint REST | Corporate network |
| Private IP | SharePoint REST | On-premises |
| Other | SharePoint REST | Assume on-premises |

### Manual Selection

Override automatic detection using configuration flags:

- `useGraphApi: true` - Force Microsoft Graph API
- `useRestApi: true` - Force SharePoint REST API
- `useLegacyAuth: true` - Force REST API with legacy authentication

## Configuration Examples

### SharePoint Online with Client Credentials

```json
{
  "schemas": [{
    "name": "SHAREPOINT",
    "factory": "org.apache.calcite.adapter.sharepoint.SharePointListSchemaFactory",
    "operand": {
      "siteUrl": "https://contoso.sharepoint.com/sites/data",
      "tenantId": "12345678-1234-1234-1234-123456789012",
      "clientId": "87654321-4321-4321-4321-210987654321",
      "clientSecret": "your-client-secret-here"
    }
  }]
}
```

### On-Premises SharePoint with Legacy Auth

```json
{
  "schemas": [{
    "name": "SHAREPOINT",
    "factory": "org.apache.calcite.adapter.sharepoint.SharePointListSchemaFactory",
    "operand": {
      "siteUrl": "https://sharepoint.company.local/sites/data",
      "clientId": "12345678-1234-1234-1234-123456789012",
      "clientSecret": "your-legacy-secret",
      "useLegacyAuth": true
    }
  }]
}
```

### Enterprise Auth Proxy

```json
{
  "schemas": [{
    "name": "SHAREPOINT",
    "factory": "org.apache.calcite.adapter.sharepoint.SharePointListSchemaFactory",
    "operand": {
      "siteUrl": "https://contoso.sharepoint.com/sites/data",
      "authProxy": {
        "endpoint": "https://auth-proxy.company.com",
        "apiKey": "${AUTH_PROXY_KEY}",
        "resource": "sharepoint-prod",
        "headers": {
          "X-Correlation-Id": "${CORRELATION_ID}",
          "X-Request-Source": "calcite-adapter"
        },
        "cacheEnabled": true,
        "cacheTtl": 300000
      }
    }
  }]
}
```

### Kubernetes with Mounted Secrets

```json
{
  "schemas": [{
    "name": "SHAREPOINT",
    "factory": "org.apache.calcite.adapter.sharepoint.SharePointListSchemaFactory",
    "operand": {
      "siteUrl": "https://contoso.sharepoint.com/sites/data",
      "tokenFile": "/var/run/secrets/sharepoint/token"
    }
  }]
}
```

### HashiCorp Vault Integration

```json
{
  "schemas": [{
    "name": "SHAREPOINT",
    "factory": "org.apache.calcite.adapter.sharepoint.SharePointListSchemaFactory",
    "operand": {
      "siteUrl": "https://contoso.sharepoint.com/sites/data",
      "tokenCommand": "vault kv get -field=token secret/sharepoint/prod"
    }
  }]
}
```

### AWS Secrets Manager

```json
{
  "schemas": [{
    "name": "SHAREPOINT",
    "factory": "org.apache.calcite.adapter.sharepoint.SharePointListSchemaFactory",
    "operand": {
      "siteUrl": "https://contoso.sharepoint.com/sites/data",
      "tokenCommand": "aws secretsmanager get-secret-value --secret-id sharepoint/token --query SecretString --output text"
    }
  }]
}
```

### Custom Authentication Provider

```json
{
  "schemas": [{
    "name": "SHAREPOINT",
    "factory": "org.apache.calcite.adapter.sharepoint.SharePointListSchemaFactory",
    "operand": {
      "siteUrl": "https://contoso.sharepoint.com/sites/data",
      "authProvider": "com.company.auth.SharePointVaultProvider",
      "vaultUrl": "https://vault.company.com",
      "vaultPath": "/v1/secret/sharepoint",
      "vaultRole": "sharepoint-reader"
    }
  }]
}
```

## Environment Variables

All configuration parameters can be set via environment variables with the `SHAREPOINT_` prefix:

```bash
# Basic configuration
export SHAREPOINT_SITE_URL="https://contoso.sharepoint.com/sites/data"
export SHAREPOINT_TENANT_ID="12345678-1234-1234-1234-123456789012"
export SHAREPOINT_CLIENT_ID="87654321-4321-4321-4321-210987654321"
export SHAREPOINT_CLIENT_SECRET="your-client-secret"

# API selection
export SHAREPOINT_USE_GRAPH_API="true"
export SHAREPOINT_USE_REST_API="false"
export SHAREPOINT_USE_LEGACY_AUTH="false"

# External token
export SHAREPOINT_TOKEN_COMMAND="/usr/local/bin/get-token"
export SHAREPOINT_TOKEN_FILE="/var/run/secrets/token"
export SHAREPOINT_TOKEN_ENDPOINT="https://auth.company.com/token"

# Auth proxy
export SHAREPOINT_AUTH_PROXY_ENDPOINT="https://auth-proxy.company.com"
export SHAREPOINT_AUTH_PROXY_KEY="secret-api-key"
export SHAREPOINT_AUTH_PROXY_RESOURCE="sharepoint-prod"
```

## Custom Authentication Provider Implementation

```java
package com.company.auth;

import org.apache.calcite.adapter.sharepoint.auth.SharePointAuthProvider;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

public class SharePointVaultProvider implements SharePointAuthProvider {
    private final VaultClient vaultClient;
    private final String vaultPath;
    private final String siteUrl;
    private String cachedToken;
    private long tokenExpiry;

    public SharePointVaultProvider(Map<String, Object> config) {
        this.siteUrl = (String) config.get("siteUrl");
        this.vaultPath = (String) config.get("vaultPath");
        this.vaultClient = new VaultClient(
            (String) config.get("vaultUrl"),
            (String) config.get("vaultRole")
        );
    }

    @Override
    public String getAccessToken() throws IOException {
        if (cachedToken != null && System.currentTimeMillis() < tokenExpiry) {
            return cachedToken;
        }

        Map<String, Object> secret = vaultClient.readSecret(vaultPath);
        cachedToken = (String) secret.get("token");
        tokenExpiry = System.currentTimeMillis() + 3600000; // 1 hour

        return cachedToken;
    }

    @Override
    public Map<String, String> getAdditionalHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Vault-Namespace", vaultClient.getNamespace());
        return headers;
    }

    @Override
    public void invalidateToken() {
        cachedToken = null;
        tokenExpiry = 0;
    }

    @Override
    public String getSiteUrl() {
        return siteUrl;
    }

    @Override
    public boolean supportsApiType(String apiType) {
        return true; // Support both APIs
    }

    @Override
    public String getTenantId() {
        // Fetch from vault if needed
        return null;
    }
}
```

## Troubleshooting

### Common Issues

1. **"Auth provider does not support Microsoft Graph API"**
   - Solution: Use `useRestApi: true` or configure appropriate authentication

2. **"On-premises SharePoint requires REST API support"**
   - Solution: The adapter auto-detected on-premises. Ensure REST API compatible auth is configured

3. **"Unsupported app only token"**
   - Issue: SharePoint REST API requires certificate auth for modern Azure AD apps
   - Solution: Use `useLegacyAuth: true` or switch to Graph API

4. **Token Expiry Issues**
   - Solution: Implement proper token refresh in custom providers
   - Auth proxy handles this automatically

### Debug Configuration

Enable debug logging to troubleshoot configuration issues:

```json
{
  "operand": {
    "debug": true,
    "logLevel": "DEBUG"
  }
}
```

## Security Best Practices

1. **Never hardcode credentials** in configuration files
2. **Use environment variables** or secret management systems
3. **Implement token rotation** for long-running applications
4. **Use auth proxy** in production environments
5. **Enable TLS/SSL** for all network communications
6. **Audit access** through centralized logging
7. **Apply principle of least privilege** for permissions
8. **Encrypt sensitive configuration** files at rest

## Performance Optimization

1. **Enable token caching** to reduce authentication overhead
2. **Use connection pooling** for multiple concurrent queries
3. **Configure appropriate timeout values** for your network
4. **Batch operations** when possible
5. **Use specific column selection** instead of SELECT *
6. **Implement pagination** for large result sets

## Migration Guide

### From Single API to Dual API

No changes required for existing configurations. The adapter maintains backward compatibility.

### From Direct Auth to Auth Proxy

1. Deploy auth proxy service
2. Update configuration:
   ```json
   // Before
   {
     "clientId": "...",
     "clientSecret": "..."
   }

   // After
   {
     "authProxy": {
       "endpoint": "https://auth-proxy.company.com",
       "apiKey": "${AUTH_PROXY_KEY}"
     }
   }
   ```

### From Cloud to On-Premises

1. Register app in SharePoint (not Azure AD)
2. Add `useLegacyAuth: true` or `useRestApi: true`
3. Update authentication credentials

## Support Matrix

| SharePoint Version | Graph API | REST API | Legacy Auth |
|-------------------|-----------|----------|-------------|
| SharePoint Online | ✅ | ✅ | ✅ |
| SharePoint 2019 | ❌ | ✅ | ✅ |
| SharePoint 2016 | ❌ | ✅ | ✅ |
| SharePoint 2013 | ❌ | ✅ | ✅ |

| Auth Method | Graph API | REST API |
|------------|-----------|----------|
| Client Credentials (Modern) | ✅ | ⚠️ (Requires Certificate) |
| Client Credentials (Legacy) | ❌ | ✅ |
| Certificate | ✅ | ✅ |
| External Token | ✅ | ✅ |
| Auth Proxy | ✅ | ✅ |
