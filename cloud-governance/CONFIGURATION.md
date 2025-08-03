# Cloud Governance Adapter Configuration Guide

This guide covers detailed configuration options for the Cloud Governance adapter across all supported cloud providers.

## Configuration Methods

### 1. Model File Configuration

The most common approach using a JSON model file:

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
        "azure.tenantId": "${AZURE_TENANT_ID}",
        "azure.clientId": "${AZURE_CLIENT_ID}",
        "azure.clientSecret": "${AZURE_CLIENT_SECRET}",
        "azure.subscriptionIds": "${AZURE_SUBSCRIPTION_IDS}",
        "gcp.credentialsPath": "${GCP_CREDENTIALS_PATH}",
        "gcp.projectIds": "${GCP_PROJECT_IDS}",
        "aws.accessKeyId": "${AWS_ACCESS_KEY_ID}",
        "aws.secretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
        "aws.region": "${AWS_REGION}",
        "aws.accountIds": "${AWS_ACCOUNT_IDS}"
      }
    }
  ]
}
```

### 2. Programmatic Configuration

```java
CloudGovernanceConfig config = new CloudGovernanceConfig();

// Azure configuration
config.azure = new CloudGovernanceConfig.AzureConfig();
config.azure.tenantId = "your-tenant-id";
config.azure.clientId = "your-client-id";
config.azure.clientSecret = "your-client-secret";
config.azure.subscriptionIds = Arrays.asList("sub1", "sub2");

// Add to Calcite schema
SchemaPlus rootSchema = calciteConnection.getRootSchema();
CloudGovernanceSchema governanceSchema = new CloudGovernanceSchema(config);
rootSchema.add("cloud_governance", governanceSchema);
```

## Azure Configuration

### Prerequisites

1. **Azure AD App Registration**
   - Create an app registration in Azure Portal
   - Note the Application (client) ID and Directory (tenant) ID
   - Create a client secret

2. **Permissions**
   - Grant app registration "Reader" role on target subscriptions
   - Ensure "Microsoft.ResourceGraph/resources/read" permission

### Configuration Parameters

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `azure.tenantId` | Yes | Azure AD tenant ID | `12345678-1234-1234-1234-123456789012` |
| `azure.clientId` | Yes | App registration client ID | `87654321-4321-4321-4321-210987654321` |
| `azure.clientSecret` | Yes | App registration client secret | `your-client-secret` |
| `azure.subscriptionIds` | Yes | Comma-separated subscription IDs | `sub1,sub2,sub3` |

### Setup Steps

1. **Create App Registration:**
   ```bash
   az ad app create --display-name "Cloud Governance Adapter"
   ```

2. **Create Service Principal:**
   ```bash
   az ad sp create --id <app-id>
   ```

3. **Assign Reader Role:**
   ```bash
   az role assignment create \
     --assignee <app-id> \
     --role Reader \
     --scope /subscriptions/<subscription-id>
   ```

4. **Create Client Secret:**
   ```bash
   az ad app credential reset --id <app-id>
   ```

### Azure Resource Graph Queries

The adapter uses KQL queries against Azure Resource Graph:

```kql
Resources
| where type == 'microsoft.containerservice/managedclusters'
| extend Application = case(
    isnotempty(tags.Application), tags.Application,
    isnotempty(tags.app), tags.app,
    'Untagged/Orphaned'
)
| project SubscriptionId = subscriptionId,
          ClusterName = name,
          Application,
          Location = location
```

## AWS Configuration

### Prerequisites

1. **IAM User or Role**
   - Create IAM user with programmatic access
   - Or configure cross-account role assumption

2. **Permissions**
   - Attach required policies for resource read access

### Configuration Parameters

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `aws.accessKeyId` | Yes | AWS access key ID | `AKIA...` |
| `aws.secretAccessKey` | Yes | AWS secret access key | `your-secret-key` |
| `aws.region` | Yes | Default AWS region | `us-east-1` |
| `aws.accountIds` | Yes | Comma-separated account IDs | `111111111111,222222222222` |
| `aws.roleArn` | No | Cross-account role ARN template | `arn:aws:iam::{account-id}:role/Role` |

### IAM Policy Template

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:Describe*",
        "eks:Describe*",
        "eks:List*",
        "s3:ListAllMyBuckets",
        "s3:GetBucket*",
        "iam:List*",
        "iam:Get*",
        "rds:Describe*",
        "dynamodb:List*",
        "dynamodb:Describe*",
        "elasticache:Describe*",
        "ecr:Describe*",
        "ecr:List*"
      ],
      "Resource": "*"
    }
  ]
}
```

### Cross-Account Role Setup

1. **Create Role in Target Account:**
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "AWS": "arn:aws:iam::SOURCE-ACCOUNT:user/governance-user"
         },
         "Action": "sts:AssumeRole"
       }
     ]
   }
   ```

2. **Configure Role ARN:**
   ```json
   {
     "aws.roleArn": "arn:aws:iam::{account-id}:role/CloudGovernanceRole"
   }
   ```

The `{account-id}` placeholder will be replaced with each account ID from `aws.accountIds`.

## GCP Configuration

### Prerequisites

1. **Service Account**
   - Create service account in GCP Console
   - Download JSON key file

2. **IAM Roles**
   - Grant required roles to service account

### Configuration Parameters

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `gcp.credentialsPath` | Yes | Path to service account JSON key | `/path/to/key.json` |
| `gcp.projectIds` | Yes | Comma-separated project IDs | `project1,project2` |

### Setup Steps

1. **Create Service Account:**
   ```bash
   gcloud iam service-accounts create cloud-governance \
     --display-name="Cloud Governance Service Account"
   ```

2. **Grant Roles:**
   ```bash
   gcloud projects add-iam-policy-binding PROJECT_ID \
     --member="serviceAccount:cloud-governance@PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/cloudasset.viewer"
   
   gcloud projects add-iam-policy-binding PROJECT_ID \
     --member="serviceAccount:cloud-governance@PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/storage.objectViewer"
   ```

3. **Create Key:**
   ```bash
   gcloud iam service-accounts keys create key.json \
     --iam-account=cloud-governance@PROJECT_ID.iam.gserviceaccount.com
   ```

### Required GCP APIs

Enable these APIs in your projects:
- Cloud Asset API
- Cloud Resource Manager API  
- Kubernetes Engine API
- Cloud Storage API
- Compute Engine API

```bash
gcloud services enable cloudasset.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable container.googleapis.com
gcloud services enable storage-api.googleapis.com
gcloud services enable compute.googleapis.com
```

## Environment Variables

Use environment variables for sensitive configuration:

```bash
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
export AZURE_SUBSCRIPTION_IDS="sub1,sub2"

export GCP_CREDENTIALS_PATH="/path/to/key.json"
export GCP_PROJECT_IDS="project1,project2"

export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"
export AWS_ACCOUNT_IDS="111111111111,222222222222"
```

Reference in model file using `${VARIABLE_NAME}` syntax.

## Security Best Practices

### Credential Management

1. **Never commit credentials** to version control
2. **Use environment variables** or secure credential stores
3. **Rotate credentials** regularly
4. **Follow principle of least privilege**

### Network Security

1. **Use private endpoints** where available
2. **Restrict IP ranges** in cloud provider firewall rules
3. **Monitor access logs** for suspicious activity

### Monitoring

1. **Enable audit logging** in cloud providers
2. **Monitor API usage** and rate limits  
3. **Set up alerts** for unusual access patterns

## Troubleshooting

### Common Configuration Errors

**Azure: Invalid client credentials**
```
AADSTS70011: The provided value for the input parameter 'scope' is not valid
```
- Check tenantId, clientId, clientSecret
- Verify app registration permissions
- Ensure admin consent granted if required

**AWS: Access denied**
```
User: arn:aws:iam::123456789:user/governance is not authorized to perform: eks:DescribeCluster
```
- Check IAM policy permissions
- Verify account IDs are correct
- Test role assumption with AWS CLI

**GCP: Invalid credentials**
```
Error 401: Request had invalid authentication credentials
```
- Verify service account key file path
- Check service account has required roles
- Ensure APIs are enabled

### Testing Configuration

Use the integration tests to validate configuration:

```bash
# Copy sample properties
cp src/test/resources/local-test.properties.sample \
   src/test/resources/local-test.properties

# Edit with your credentials
vim src/test/resources/local-test.properties

# Run integration tests
./gradlew :cloud-governance:test --tests="*IntegrationTest"
```

### Debugging

Enable debug logging to troubleshoot issues:

```java
System.setProperty("org.apache.calcite.adapter.governance.debug", "true");
```

This will output:
- Authentication attempts
- API call details
- Resource query results
- Error details