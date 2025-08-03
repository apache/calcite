# Cloud Governance SQL Query Examples

This document provides practical SQL query examples for common cloud governance, security, and compliance use cases.

## Basic Resource Discovery

### List All Resources by Cloud Provider

```sql
-- Get count of resources by provider
SELECT 
  'kubernetes_clusters' as resource_type,
  cloud_provider,
  COUNT(*) as count
FROM kubernetes_clusters
GROUP BY cloud_provider

UNION ALL

SELECT 
  'storage_resources' as resource_type,
  cloud_provider,
  COUNT(*) as count  
FROM storage_resources
GROUP BY cloud_provider

UNION ALL

SELECT
  'compute_resources' as resource_type,
  cloud_provider,
  COUNT(*) as count
FROM compute_resources
GROUP BY cloud_provider

ORDER BY resource_type, cloud_provider;
```

### Find Resources by Application

```sql
-- Find all resources for a specific application
SELECT 
  cloud_provider,
  'kubernetes' as resource_type,
  cluster_name as resource_name,
  region,
  application
FROM kubernetes_clusters
WHERE application = 'MyApp'

UNION ALL

SELECT 
  cloud_provider,
  'storage' as resource_type,
  resource_name,
  region,
  application
FROM storage_resources  
WHERE application = 'MyApp'

ORDER BY cloud_provider, resource_type;
```

## Security Compliance Queries

### Kubernetes Security Posture

```sql
-- Kubernetes security compliance summary
SELECT 
  cloud_provider,
  COUNT(*) as total_clusters,
  
  -- RBAC Compliance
  SUM(CASE WHEN rbac_enabled = true THEN 1 ELSE 0 END) as rbac_enabled_count,
  ROUND(100.0 * SUM(CASE WHEN rbac_enabled = true THEN 1 ELSE 0 END) / COUNT(*), 1) as rbac_compliance_pct,
  
  -- Network Security
  SUM(CASE WHEN private_cluster = true THEN 1 ELSE 0 END) as private_cluster_count,
  ROUND(100.0 * SUM(CASE WHEN private_cluster = true THEN 1 ELSE 0 END) / COUNT(*), 1) as private_cluster_pct,
  
  -- Encryption
  SUM(CASE WHEN encryption_at_rest_enabled = true THEN 1 ELSE 0 END) as encryption_enabled_count,
  ROUND(100.0 * SUM(CASE WHEN encryption_at_rest_enabled = true THEN 1 ELSE 0 END) / COUNT(*), 1) as encryption_pct

FROM kubernetes_clusters
GROUP BY cloud_provider
ORDER BY cloud_provider;
```

### Storage Encryption Analysis

```sql
-- Storage encryption compliance
SELECT 
  cloud_provider,
  storage_type,
  COUNT(*) as total_resources,
  
  -- Encryption Status
  SUM(CASE WHEN encryption_enabled = true THEN 1 ELSE 0 END) as encrypted_count,
  SUM(CASE WHEN encryption_enabled = false OR encryption_enabled IS NULL THEN 1 ELSE 0 END) as unencrypted_count,
  
  -- Encryption Key Management
  SUM(CASE WHEN encryption_key_type = 'customer-managed' THEN 1 ELSE 0 END) as customer_managed_keys,
  SUM(CASE WHEN encryption_key_type = 'service-managed' THEN 1 ELSE 0 END) as service_managed_keys,
  
  -- Compliance Percentage
  ROUND(100.0 * SUM(CASE WHEN encryption_enabled = true THEN 1 ELSE 0 END) / COUNT(*), 1) as encryption_compliance_pct

FROM storage_resources
GROUP BY cloud_provider, storage_type
ORDER BY cloud_provider, storage_type;
```

### Public Access Analysis

```sql
-- Resources with public access exposure
SELECT 
  s.cloud_provider,
  s.application,
  s.resource_name,
  s.storage_type,
  s.public_access_enabled,
  s.public_access_level,
  s.https_only,
  
  -- Risk Assessment Factors
  CASE 
    WHEN s.public_access_enabled = true AND s.https_only = false THEN 'HIGH RISK'
    WHEN s.public_access_enabled = true AND s.https_only = true THEN 'MEDIUM RISK'
    ELSE 'LOW RISK'
  END as risk_level

FROM storage_resources s
WHERE s.public_access_enabled = true
ORDER BY 
  CASE 
    WHEN s.public_access_enabled = true AND s.https_only = false THEN 1
    WHEN s.public_access_enabled = true AND s.https_only = true THEN 2
    ELSE 3
  END,
  s.cloud_provider, s.application;
```

## Cost and Resource Optimization

### Orphaned Resources Detection

```sql
-- Find untagged/orphaned resources that may be unused
SELECT 
  cloud_provider,
  'storage' as resource_type,
  resource_name,
  region,
  storage_type,
  created_date,
  DATEDIFF('day', created_date, CURRENT_DATE) as days_old
FROM storage_resources
WHERE application IN ('Untagged/Orphaned', '', NULL)

UNION ALL

SELECT 
  cloud_provider,
  'compute' as resource_type,
  instance_name as resource_name,
  region,
  instance_type as storage_type,
  launch_time as created_date,
  DATEDIFF('day', launch_time, CURRENT_DATE) as days_old
FROM compute_resources  
WHERE application IN ('Untagged/Orphaned', '', NULL)

ORDER BY days_old DESC, cloud_provider;
```

### Resource Distribution by Region

```sql
-- Regional resource distribution for optimization
SELECT 
  cloud_provider,
  region,
  COUNT(DISTINCT k.cluster_name) as kubernetes_clusters,
  COUNT(DISTINCT s.resource_name) as storage_resources,  
  COUNT(DISTINCT c.instance_id) as compute_instances,
  COUNT(DISTINCT n.network_resource) as network_resources

FROM kubernetes_clusters k
FULL OUTER JOIN storage_resources s ON k.cloud_provider = s.cloud_provider AND k.region = s.region
FULL OUTER JOIN compute_resources c ON k.cloud_provider = c.cloud_provider AND k.region = c.region  
FULL OUTER JOIN network_resources n ON k.cloud_provider = n.cloud_provider AND n.region = n.region

GROUP BY cloud_provider, region
ORDER BY cloud_provider, 
  (COUNT(DISTINCT k.cluster_name) + COUNT(DISTINCT s.resource_name) + 
   COUNT(DISTINCT c.instance_id) + COUNT(DISTINCT n.network_resource)) DESC;
```

## Cross-Cloud Application Analysis

### Multi-Cloud Application Footprint

```sql
-- Applications deployed across multiple cloud providers
WITH app_clouds AS (
  SELECT application, cloud_provider, COUNT(*) as resource_count
  FROM (
    SELECT application, cloud_provider FROM kubernetes_clusters WHERE application != 'Untagged/Orphaned'
    UNION ALL
    SELECT application, cloud_provider FROM storage_resources WHERE application != 'Untagged/Orphaned'
    UNION ALL  
    SELECT application, cloud_provider FROM compute_resources WHERE application != 'Untagged/Orphaned'
  ) all_resources
  GROUP BY application, cloud_provider
),
app_summary AS (
  SELECT 
    application,
    COUNT(DISTINCT cloud_provider) as cloud_count,
    SUM(resource_count) as total_resources,
    STRING_AGG(cloud_provider, ', ') as cloud_providers
  FROM app_clouds
  GROUP BY application
)

SELECT *
FROM app_summary  
WHERE cloud_count > 1
ORDER BY cloud_count DESC, total_resources DESC;
```

### Application Resource Inventory

```sql
-- Detailed inventory for a specific application
WITH app_inventory AS (
  SELECT 
    'Kubernetes' as service_type,
    cloud_provider,
    region,
    cluster_name as resource_name,
    kubernetes_version as version_info,
    CASE WHEN rbac_enabled = true THEN 'Compliant' ELSE 'Non-Compliant' END as security_status
  FROM kubernetes_clusters 
  WHERE application = 'ProductionApp'
  
  UNION ALL
  
  SELECT 
    'Storage' as service_type,
    cloud_provider,
    region, 
    resource_name,
    storage_type as version_info,
    CASE WHEN encryption_enabled = true THEN 'Encrypted' ELSE 'Unencrypted' END as security_status
  FROM storage_resources
  WHERE application = 'ProductionApp'
  
  UNION ALL
  
  SELECT
    'Compute' as service_type,
    cloud_provider,
    region,
    instance_name as resource_name,
    instance_type as version_info,
    CASE WHEN disk_encryption_enabled = true THEN 'Encrypted' ELSE 'Unencrypted' END as security_status
  FROM compute_resources
  WHERE application = 'ProductionApp'
)

SELECT 
  service_type,
  cloud_provider,
  COUNT(*) as resource_count,
  COUNT(DISTINCT region) as region_count,
  SUM(CASE WHEN security_status LIKE '%Compliant' OR security_status = 'Encrypted' THEN 1 ELSE 0 END) as secure_resources
FROM app_inventory
GROUP BY service_type, cloud_provider
ORDER BY service_type, cloud_provider;
```

## Database and Storage Analytics

### Database Security Compliance

```sql
-- Database security posture analysis
SELECT 
  cloud_provider,
  database_type,
  COUNT(*) as total_databases,
  
  -- Encryption Analysis
  SUM(CASE WHEN encrypted = true THEN 1 ELSE 0 END) as encrypted_count,
  ROUND(100.0 * SUM(CASE WHEN encrypted = true THEN 1 ELSE 0 END) / COUNT(*), 1) as encryption_pct,
  
  -- Public Access Analysis  
  SUM(CASE WHEN publicly_accessible = true THEN 1 ELSE 0 END) as public_accessible_count,
  ROUND(100.0 * SUM(CASE WHEN publicly_accessible = true THEN 1 ELSE 0 END) / COUNT(*), 1) as public_access_pct,
  
  -- Backup Analysis
  SUM(CASE WHEN backup_retention_days > 0 THEN 1 ELSE 0 END) as backup_enabled_count,
  AVG(backup_retention_days) as avg_backup_retention_days

FROM database_resources
GROUP BY cloud_provider, database_type
ORDER BY cloud_provider, database_type;
```

### Storage Lifecycle Management

```sql
-- Storage lifecycle and data management analysis
SELECT 
  cloud_provider,
  storage_type,
  
  -- Lifecycle Management
  COUNT(*) as total_resources,
  SUM(CASE WHEN lifecycle_rules_count > 0 THEN 1 ELSE 0 END) as with_lifecycle_rules,
  AVG(lifecycle_rules_count) as avg_lifecycle_rules,
  
  -- Data Protection
  SUM(CASE WHEN versioning_enabled = true THEN 1 ELSE 0 END) as versioning_enabled_count,
  SUM(CASE WHEN soft_delete_enabled = true THEN 1 ELSE 0 END) as soft_delete_enabled_count,
  AVG(soft_delete_retention_days) as avg_soft_delete_retention,
  
  -- Access Patterns
  SUM(CASE WHEN access_tier = 'Hot' OR access_tier = 'Standard' THEN 1 ELSE 0 END) as hot_tier_count,
  SUM(CASE WHEN access_tier = 'Cool' OR access_tier = 'Infrequent' THEN 1 ELSE 0 END) as cool_tier_count,
  SUM(CASE WHEN access_tier = 'Archive' OR access_tier = 'Glacier' THEN 1 ELSE 0 END) as archive_tier_count

FROM storage_resources
GROUP BY cloud_provider, storage_type
ORDER BY cloud_provider, total_resources DESC;
```

## IAM and Access Management

### IAM Resource Analysis

```sql
-- IAM resource distribution and security analysis
SELECT 
  cloud_provider,
  iam_resource_type,
  COUNT(*) as total_resources,
  
  -- Activity Analysis
  SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as active_count,
  SUM(CASE WHEN is_active = false THEN 1 ELSE 0 END) as inactive_count,
  
  -- MFA Analysis (where applicable)
  SUM(CASE WHEN mfa_enabled = true THEN 1 ELSE 0 END) as mfa_enabled_count,
  SUM(CASE WHEN access_key_count > 0 THEN 1 ELSE 0 END) as with_access_keys,
  AVG(access_key_count) as avg_access_keys,
  
  -- Security Concerns
  SUM(CASE WHEN active_access_keys > 1 THEN 1 ELSE 0 END) as multiple_keys_count

FROM iam_resources
GROUP BY cloud_provider, iam_resource_type
ORDER BY cloud_provider, iam_resource_type;
```

### Privileged Access Review

```sql
-- Find IAM resources that may need review
SELECT 
  cloud_provider,
  iam_resource,
  iam_resource_type,
  application,
  
  -- Risk Factors
  is_active,
  mfa_enabled,
  access_key_count,
  active_access_keys,
  
  -- Age Analysis
  create_date,
  password_last_used,
  DATEDIFF('day', password_last_used, CURRENT_DATE) as days_since_last_use,
  
  -- Risk Score
  CASE 
    WHEN is_active = true AND mfa_enabled = false AND access_key_count > 1 THEN 'HIGH'
    WHEN is_active = true AND (mfa_enabled = false OR access_key_count > 1) THEN 'MEDIUM'  
    WHEN is_active = false THEN 'LOW'
    ELSE 'REVIEW'
  END as risk_level

FROM iam_resources
WHERE iam_resource_type IN ('IAM User', 'Service Account', 'Managed Identity')
ORDER BY 
  CASE 
    WHEN is_active = true AND mfa_enabled = false AND access_key_count > 1 THEN 1
    WHEN is_active = true AND (mfa_enabled = false OR access_key_count > 1) THEN 2
    ELSE 3
  END,
  days_since_last_use DESC NULLS LAST;
```

## Network Security Analysis

### Network Security Posture

```sql
-- Network security configuration analysis
SELECT 
  cloud_provider,
  network_resource_type,
  COUNT(*) as total_resources,
  
  -- Security Analysis
  SUM(CASE WHEN has_open_ingress = true THEN 1 ELSE 0 END) as open_ingress_count,
  SUM(CASE WHEN rule_count = 0 THEN 1 ELSE 0 END) as no_rules_count,
  SUM(CASE WHEN is_default = true THEN 1 ELSE 0 END) as default_resources,
  
  -- Configuration Distribution
  AVG(rule_count) as avg_rule_count,
  MAX(rule_count) as max_rule_count

FROM network_resources
GROUP BY cloud_provider, network_resource_type
ORDER BY cloud_provider, network_resource_type;
```

## Performance and Monitoring Queries

### Resource Age and Lifecycle

```sql
-- Resource age analysis for lifecycle management
SELECT 
  cloud_provider,
  'Kubernetes' as resource_type,
  cluster_name as resource_name,
  application,
  created_date,
  DATEDIFF('day', created_date, CURRENT_DATE) as age_days,
  CASE 
    WHEN DATEDIFF('day', created_date, CURRENT_DATE) > 365 THEN 'Very Old (>1 year)'
    WHEN DATEDIFF('day', created_date, CURRENT_DATE) > 180 THEN 'Old (6-12 months)'  
    WHEN DATEDIFF('day', created_date, CURRENT_DATE) > 90 THEN 'Mature (3-6 months)'
    WHEN DATEDIFF('day', created_date, CURRENT_DATE) > 30 THEN 'Recent (1-3 months)'
    ELSE 'New (<1 month)'
  END as age_category
FROM kubernetes_clusters
WHERE created_date IS NOT NULL

UNION ALL

SELECT 
  cloud_provider,
  'Storage' as resource_type, 
  resource_name,
  application,
  created_date,
  DATEDIFF('day', created_date, CURRENT_DATE) as age_days,
  CASE 
    WHEN DATEDIFF('day', created_date, CURRENT_DATE) > 365 THEN 'Very Old (>1 year)'
    WHEN DATEDIFF('day', created_date, CURRENT_DATE) > 180 THEN 'Old (6-12 months)'
    WHEN DATEDIFF('day', created_date, CURRENT_DATE) > 90 THEN 'Mature (3-6 months)' 
    WHEN DATEDIFF('day', created_date, CURRENT_DATE) > 30 THEN 'Recent (1-3 months)'
    ELSE 'New (<1 month)'
  END as age_category
FROM storage_resources
WHERE created_date IS NOT NULL

ORDER BY age_days DESC;
```

## Custom Reports

### Executive Dashboard Query

```sql
-- High-level executive summary
WITH resource_summary AS (
  SELECT 
    COUNT(DISTINCT CASE WHEN k.cluster_name IS NOT NULL THEN k.cloud_provider END) as k8s_providers,
    COUNT(k.cluster_name) as total_k8s_clusters,
    COUNT(DISTINCT s.cloud_provider) as storage_providers, 
    COUNT(s.resource_name) as total_storage_resources,
    COUNT(DISTINCT c.cloud_provider) as compute_providers,
    COUNT(c.instance_id) as total_compute_instances
  FROM kubernetes_clusters k
  FULL OUTER JOIN storage_resources s ON 1=1
  FULL OUTER JOIN compute_resources c ON 1=1
),
security_summary AS (
  SELECT
    SUM(CASE WHEN rbac_enabled = true THEN 1 ELSE 0 END) as k8s_rbac_compliant,
    COUNT(*) as total_k8s,
    (SELECT SUM(CASE WHEN encryption_enabled = true THEN 1 ELSE 0 END) FROM storage_resources) as storage_encrypted,
    (SELECT COUNT(*) FROM storage_resources) as total_storage
  FROM kubernetes_clusters
)

SELECT 
  'Multi-Cloud Infrastructure Summary' as report_section,
  r.k8s_providers as kubernetes_providers,
  r.total_k8s_clusters,
  r.storage_providers,
  r.total_storage_resources,
  r.compute_providers,
  r.total_compute_instances,
  
  -- Security Compliance
  ROUND(100.0 * s.k8s_rbac_compliant / NULLIF(s.total_k8s, 0), 1) as k8s_rbac_compliance_pct,
  ROUND(100.0 * s.storage_encrypted / NULLIF(s.total_storage, 0), 1) as storage_encryption_pct

FROM resource_summary r, security_summary s;
```

These examples demonstrate the power of SQL for cloud governance analysis. You can modify and combine these queries based on your specific compliance requirements and organizational needs.