# Cloud Governance Adapter for Apache Calcite

The Cloud Governance adapter provides a unified SQL interface for querying cloud infrastructure resources across Azure, AWS, and Google Cloud Platform. This adapter enables organizations to perform governance, compliance, and security analysis using familiar SQL syntax.

## Features

- **Multi-Cloud Support**: Query resources across Azure, AWS, and GCP from a single interface
- **Native SDKs**: Uses official cloud provider SDKs for reliable and secure access
- **Security-Focused**: Returns raw facts without subjective assessments for security analysis
- **SQL Interface**: Standard SQL queries for cloud resource discovery and analysis
- **Comprehensive Coverage**: Supports Kubernetes clusters, storage, compute, networking, IAM, databases, and container registries

## Supported Resources

| Resource Type | Azure | AWS | GCP | Description |
|---------------|-------|-----|-----|-------------|
| **Kubernetes Clusters** | AKS | EKS | GKE | Managed Kubernetes services |
| **Storage Resources** | Storage Accounts, Disks | S3 Buckets | Cloud Storage | Object and block storage |
| **Compute Instances** | Virtual Machines | EC2 Instances | Compute Engine | Virtual machines |
| **Network Resources** | VNets, NSGs | VPCs, Security Groups | VPC Networks | Network infrastructure |
| **IAM Resources** | Identities, Key Vault | Users, Roles, Policies | Service Accounts | Identity management |
| **Database Resources** | SQL, Cosmos DB | RDS, DynamoDB | Cloud SQL, Firestore | Database services |
| **Container Registries** | Container Registry | ECR | Artifact Registry | Container image repositories |

## Quick Start

### 1. Configuration

Create a model file (e.g., `cloud-governance-model.json`):

```json
{
  "version": "1.0",
  "defaultSchema": "cloud_governance",
  "schemas": [
    {
      "name": "cloud_governance",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.governance.CloudGovernanceSchemaFactory",
      "operand": {
        "azure.tenantId": "your-tenant-id",
        "azure.clientId": "your-client-id",
        "azure.clientSecret": "your-client-secret",
        "azure.subscriptionIds": "sub1,sub2,sub3",
        "gcp.credentialsPath": "/path/to/service-account.json",
        "gcp.projectIds": "project1,project2",
        "aws.accessKeyId": "your-access-key",
        "aws.secretAccessKey": "your-secret-key",
        "aws.region": "us-east-1",
        "aws.accountIds": "111111111111,222222222222"
      }
    }
  ]
}
```

### 2. Connect and Query

```java
Connection connection = DriverManager.getConnection(
    "jdbc:calcite:model=cloud-governance-model.json");

Statement statement = connection.createStatement();
ResultSet rs = statement.executeQuery(
    "SELECT cloud_provider, COUNT(*) FROM kubernetes_clusters GROUP BY cloud_provider");
```

### 3. Example Queries

**Find all Kubernetes clusters:**
```sql
SELECT cloud_provider, cluster_name, application, kubernetes_version, rbac_enabled
FROM kubernetes_clusters
ORDER BY cloud_provider, cluster_name;
```

**Storage security analysis:**
```sql
SELECT cloud_provider,
       COUNT(*) as total_resources,
       SUM(CASE WHEN encryption_enabled = true THEN 1 ELSE 0 END) as encrypted_count
FROM storage_resources
GROUP BY cloud_provider;
```

**Cross-cloud application inventory:**
```sql
SELECT application,
       COUNT(DISTINCT cloud_provider) as cloud_count,
       COUNT(*) as total_resources
FROM storage_resources
WHERE application != 'Untagged/Orphaned'
GROUP BY application
ORDER BY total_resources DESC;
```

**Security compliance check:**
```sql
SELECT k.cloud_provider,
       COUNT(*) as total_clusters,
       SUM(CASE WHEN rbac_enabled = true THEN 1 ELSE 0 END) as rbac_compliant,
       SUM(CASE WHEN private_cluster = true THEN 1 ELSE 0 END) as private_clusters
FROM kubernetes_clusters k
GROUP BY k.cloud_provider;
```

## Configuration Reference

### Azure Configuration

Requires an Azure AD App Registration with appropriate permissions:

```json
{
  "azure.tenantId": "your-azure-tenant-id",
  "azure.clientId": "your-azure-client-id",
  "azure.clientSecret": "your-azure-client-secret",
  "azure.subscriptionIds": "sub1,sub2,sub3"
}
```

**Required Azure Permissions:**
- `Microsoft.ResourceGraph/resources/read`
- Reader access to target subscriptions

### AWS Configuration

Supports IAM user credentials or cross-account role assumption:

```json
{
  "aws.accessKeyId": "AKIA...",
  "aws.secretAccessKey": "your-secret-key",
  "aws.region": "us-east-1",
  "aws.accountIds": "111111111111,222222222222",
  "aws.roleArn": "arn:aws:iam::{account-id}:role/CrossAccountRole"
}
```

**Required AWS Permissions:**
- `ec2:Describe*`
- `eks:Describe*`, `eks:List*`
- `s3:ListAllMyBuckets`, `s3:GetBucket*`
- `iam:List*`, `iam:Get*`
- `rds:Describe*`
- `dynamodb:List*`, `dynamodb:Describe*`
- `ecr:Describe*`

### GCP Configuration

Requires a Service Account with appropriate roles:

```json
{
  "gcp.credentialsPath": "/path/to/service-account.json",
  "gcp.projectIds": "project1,project2"
}
```

**Required GCP Roles:**
- `roles/cloudasset.viewer`
- `roles/storage.objectViewer`
- `roles/container.viewer`

## Schema Reference

### kubernetes_clusters

| Column | Type | Description |
|--------|------|-------------|
| `cloud_provider` | VARCHAR | Cloud provider (azure, aws, gcp) |
| `account_id` | VARCHAR | Subscription/Project/Account ID |
| `cluster_name` | VARCHAR | Kubernetes cluster name |
| `application` | VARCHAR | Application tag/label |
| `region` | VARCHAR | Geographic region |
| `resource_group` | VARCHAR | Azure resource group (Azure only) |
| `resource_id` | VARCHAR | Full cloud resource identifier |
| `kubernetes_version` | VARCHAR | Kubernetes version |
| `node_count` | INTEGER | Number of worker nodes |
| `node_pools` | INTEGER | Number of node pools |
| `rbac_enabled` | BOOLEAN | RBAC authorization enabled |
| `private_cluster` | BOOLEAN | Private cluster (no public endpoint) |
| `public_endpoint` | BOOLEAN | Public API endpoint available |
| `authorized_ip_ranges` | INTEGER | Number of authorized IP ranges |
| `network_policy_provider` | VARCHAR | Network policy implementation |
| `pod_security_policy_enabled` | BOOLEAN | Pod security policies enabled |
| `encryption_at_rest_enabled` | BOOLEAN | Encryption at rest enabled |
| `encryption_key_type` | VARCHAR | Encryption key type |
| `logging_enabled` | BOOLEAN | Control plane logging enabled |
| `monitoring_enabled` | BOOLEAN | Monitoring/metrics enabled |
| `created_date` | TIMESTAMP | Resource creation date |
| `modified_date` | TIMESTAMP | Last modification date |
| `tags` | VARCHAR | Resource tags as JSON |

### storage_resources

| Column | Type | Description |
|--------|------|-------------|
| `cloud_provider` | VARCHAR | Cloud provider |
| `account_id` | VARCHAR | Account identifier |
| `resource_name` | VARCHAR | Storage resource name |
| `storage_type` | VARCHAR | Type of storage service |
| `application` | VARCHAR | Application tag |
| `region` | VARCHAR | Geographic region |
| `resource_group` | VARCHAR | Resource group (Azure) |
| `resource_id` | VARCHAR | Full resource identifier |
| `size_bytes` | BIGINT | Storage size in bytes |
| `storage_class` | VARCHAR | Storage class/tier |
| `replication_type` | VARCHAR | Replication configuration |
| `encryption_enabled` | BOOLEAN | Encryption enabled |
| `encryption_type` | VARCHAR | Encryption implementation |
| `encryption_key_type` | VARCHAR | Key management type |
| `public_access_enabled` | BOOLEAN | Public access allowed |
| `public_access_level` | VARCHAR | Level of public access |
| `network_restrictions` | VARCHAR | Network access restrictions |
| `https_only` | BOOLEAN | HTTPS-only access enforced |
| `versioning_enabled` | BOOLEAN | Object versioning enabled |
| `soft_delete_enabled` | BOOLEAN | Soft delete protection |
| `soft_delete_retention_days` | INTEGER | Soft delete retention period |
| `backup_enabled` | BOOLEAN | Backup configured |
| `lifecycle_rules_count` | INTEGER | Number of lifecycle rules |
| `access_tier` | VARCHAR | Storage access tier |
| `last_access_time` | TIMESTAMP | Last access time |
| `created_date` | TIMESTAMP | Creation date |
| `modified_date` | TIMESTAMP | Last modification |
| `tags` | VARCHAR | Resource tags as JSON |

*Similar schemas exist for compute_resources, network_resources, iam_resources, database_resources, and container_registries.*

## Development

### Building

```bash
./gradlew :cloud-governance:build
```

### Testing

The Cloud Governance adapter uses JUnit categories to organize tests into different types:

#### Test Categories

- **Unit Tests** (`@Category(UnitTest.class)`): Fast, isolated tests without external dependencies
- **Integration Tests** (`@Category(IntegrationTest.class)`): Tests requiring real cloud credentials and API calls
- **Performance Tests** (`@Category(PerformanceTest.class)`): Tests measuring execution time, memory usage, and throughput

#### Running Tests

**Default Test Run:**
```bash
./gradlew :cloud-governance:test
```
- Automatically runs unit tests + integration tests if credentials are available
- Excludes integration tests if `local-test.properties` file is missing or incomplete
- Always excludes performance tests

**Unit Tests Only:**
```bash
./gradlew :cloud-governance:unitTest
```

**Integration Tests:**
1. Copy `src/test/resources/local-test.properties.sample` to `src/test/resources/local-test.properties`
2. Fill in your cloud credentials for at least one provider (Azure, AWS, or GCP)
3. Run integration tests:
```bash
./gradlew :cloud-governance:integrationTest
```
- Integration tests will be automatically included in default `test` task when credentials are present

**Performance Tests:**
```bash
./gradlew :cloud-governance:performanceTest
```

**All Tests (Including Integration and Performance):**
```bash
./gradlew :cloud-governance:allTests
```

#### Test Reports

Each test category generates separate reports:
- Unit Tests: `build/reports/tests/unit/index.html`
- Integration Tests: `build/reports/tests/integration/index.html`
- Performance Tests: `build/reports/tests/performance/index.html`
- All Tests: `build/reports/tests/all/index.html`

### Adding New Resource Types

1. Add query method to `CloudProvider` interface
2. Implement in each provider (`AzureProvider`, `AWSProvider`, `GCPProvider`)
3. Create new table class extending `AbstractCloudGovernanceTable`
4. Add table to `CloudGovernanceSchema.getTableMap()`
5. Add tests and documentation

## Security Considerations

- **Credentials**: Store credentials securely, never commit to version control
- **Permissions**: Follow principle of least privilege for cloud credentials
- **Network**: Consider using private endpoints where available
- **Audit**: Monitor cloud resource access through provider audit logs
- **Rate Limits**: Adapter respects cloud provider API rate limits

## Troubleshooting

### Authentication Issues

**Azure:**
```
Error: AADSTS70011: Invalid client credentials
```
- Verify tenantId, clientId, and clientSecret
- Ensure app registration has required permissions
- Check if admin consent is required

**AWS:**
```
Error: The security token included in the request is invalid
```
- Verify accessKeyId and secretAccessKey
- Check IAM permissions
- Verify cross-account role trust policy

**GCP:**
```
Error: Request had invalid authentication credentials
```
- Verify service account key file path
- Check service account has required roles
- Ensure API is enabled in the project

### Performance Tuning

- **Subscription/Project Filtering**: Limit to necessary subscriptions/projects
- **Result Limits**: Use LIMIT clauses for large result sets
- **Caching**: Results are not cached; consider caching at application level
- **Parallel Queries**: Provider queries run in parallel where possible

### Common Errors

- **Empty Results**: Check resource permissions and subscription/project access
- **Timeout**: Increase query timeout for large environments
- **API Limits**: Monitor cloud provider API quotas and throttling

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Update documentation
5. Submit a pull request

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.
