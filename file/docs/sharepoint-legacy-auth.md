# SharePoint Legacy Authentication (Client Secret Support)

This document explains how to use SharePoint REST API with client secret authentication, which is not supported by the modern Azure AD authentication flow.

## Overview

SharePoint REST API requires certificate-based authentication when using modern Azure AD app registrations. However, there's a legacy authentication method that still works with client secrets.

## Authentication Methods Comparison

| Method | Endpoint | Client Secret | Certificate | Setup Complexity |
|--------|----------|--------------|-------------|------------------|
| Modern Azure AD | `login.microsoftonline.com` | ❌ (401 error) | ✅ | Medium |
| Legacy SharePoint | `accounts.accesscontrol.windows.net` | ✅ | ✅ | Low |
| Microsoft Graph | `graph.microsoft.com` | ✅ | ✅ | Low |

## Setting Up Legacy Authentication

### Step 1: Register SharePoint App

1. Navigate to: `https://[your-tenant].sharepoint.com/_layouts/15/appregnew.aspx`
2. Click "Generate" for Client ID and Client Secret
3. Fill in:
   - Title: Your app name (e.g., "Calcite File Adapter")
   - App Domain: `localhost` (or your domain)
   - Redirect URI: `https://localhost` (or your redirect URL)
4. Click "Create"
5. **Save the Client ID and Client Secret** - you won't see them again!

### Step 2: Grant Permissions

1. Navigate to: `https://[your-tenant].sharepoint.com/_layouts/15/appinv.aspx`
2. Enter your Client ID in the "App Id" field
3. Click "Lookup"
4. In the "Permission Request XML" field, enter:

```xml
<AppPermissionRequests AllowAppOnlyPolicy="true">
  <AppPermissionRequest Scope="http://sharepoint/content/tenant" Right="FullControl" />
</AppPermissionRequests>
```

For read-only access, use `Right="Read"` instead of `Right="FullControl"`.

5. Click "Create"
6. Trust the app when prompted

### Step 3: Configure Calcite

Add to your `local-test.properties` or environment variables:

```properties
# Legacy SharePoint App credentials
SHAREPOINT_LEGACY_CLIENT_ID=12345678-1234-1234-1234-123456789012
SHAREPOINT_LEGACY_CLIENT_SECRET=your-client-secret-here
SHAREPOINT_SITE_URL=https://your-tenant.sharepoint.com

# Optional: Realm (tenant ID) - will be auto-discovered if not provided
SHAREPOINT_REALM=12345678-1234-1234-1234-123456789012
```

### Step 4: Use in Code

#### Option A: Automatic Detection (in tests)
The integration tests will automatically use legacy auth if credentials are found:

```java
// The test will check for SHAREPOINT_LEGACY_CLIENT_ID
// and use legacy auth if present
```

#### Option B: Explicit Configuration

```java
Map<String, Object> operand = new HashMap<>();
operand.put("directory", "/Shared Documents");
operand.put("storageType", "sharepoint");

Map<String, Object> storageConfig = new HashMap<>();
storageConfig.put("siteUrl", "https://your-tenant.sharepoint.com");
storageConfig.put("clientId", "your-legacy-client-id");
storageConfig.put("clientSecret", "your-legacy-client-secret");
storageConfig.put("useLegacyAuth", true);  // Enable legacy authentication
operand.put("storageConfig", storageConfig);
```

## Comparison with Modern Methods

### Microsoft Graph API (Recommended)
```java
storageConfig.put("useGraphApi", true);  // Works with client secret
// Uses modern Azure AD app registration
```

### Modern SharePoint REST API (Requires Certificate)
```java
// Default behavior - requires certificate authentication
// Will fail with "Unsupported app only token" if using client secret
```

### Legacy SharePoint REST API (Works with Client Secret)
```java
storageConfig.put("useLegacyAuth", true);  // Works with client secret
// Uses legacy SharePoint app registration
```

## Troubleshooting

### "Unsupported app only token" Error
This means you're trying to use modern authentication with a client secret. Either:
1. Switch to legacy authentication (`useLegacyAuth: true`)
2. Use Microsoft Graph API (`useGraphApi: true`)
3. Configure certificate authentication for modern REST API

### Realm Discovery
If you don't know your realm (tenant ID), the system will auto-discover it by making an unauthenticated request to SharePoint.

### Token Expiration
Legacy tokens typically expire after 1 hour. The token manager automatically refreshes them.

## Security Considerations

1. **Legacy Method**: While functional, this is a legacy authentication method. Microsoft recommends using modern authentication with certificates for production scenarios.

2. **Permissions**: The app has tenant-wide permissions. Consider using Sites.Selected permissions with modern auth for more granular control.

3. **Client Secret**: Treat the client secret like a password. Never commit it to source control.

## Migration Path

For production use, consider migrating to:
1. **Microsoft Graph API** with modern Azure AD app registration
2. **Certificate-based authentication** for SharePoint REST API
3. **Sites.Selected permissions** for granular access control