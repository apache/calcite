# Cloud Ops Virtual Tables - Pushdown Capabilities Analysis

This document provides a comprehensive analysis of query pushdown opportunities for each virtual table across Azure, AWS, and GCP cloud providers.

## Critical Optimization: Cloud Provider Filtering

**THE MOST IMPORTANT PUSHDOWN**: Filtering by `cloud_provider` must happen BEFORE making any API calls to avoid unnecessary cloud API requests and costs.

### Provider Filter Strategy

When a query includes `WHERE cloud_provider = 'azure'` or `WHERE cloud_provider IN ('azure', 'gcp')`:

1. **Extract provider filter FIRST** in `AbstractCloudOpsTable.scan()`
2. **Only call APIs for specified providers**
3. **Skip API calls entirely for excluded providers**

Example optimization:
```sql
-- Query
SELECT cluster_name, region
FROM kubernetes_clusters
WHERE cloud_provider = 'azure' AND region = 'eastus'

-- Without provider pushdown: 3 API calls (Azure, AWS, GCP)
-- With provider pushdown: 1 API call (Azure only)
-- Cost reduction: 66% fewer API calls
```

### Implementation in AbstractCloudOpsTable

```java
public Enumerable<Object[]> scan(...) {
    // CRITICAL: Extract provider filter FIRST
    Set<String> requestedProviders = extractProviderFilter(filters);

    // If no provider filter, check all configured providers
    if (requestedProviders.isEmpty()) {
        requestedProviders = config.providers;
    }

    // Only make API calls to requested providers
    List<CompletableFuture<List<Object[]>>> futures = new ArrayList<>();

    if (requestedProviders.contains("azure") && config.azure != null) {
        futures.add(queryAzureAsync(...));
    }
    if (requestedProviders.contains("aws") && config.aws != null) {
        futures.add(queryAWSAsync(...));
    }
    if (requestedProviders.contains("gcp") && config.gcp != null) {
        futures.add(queryGCPAsync(...));
    }
}
```

## Summary Matrix

| Table | Azure | AWS | GCP |
|-------|-------|-----|-----|
| **KubernetesClusters** | ✅ Excellent (KQL) | ⚠️ Limited | ⚠️ Limited |
| **StorageResources** | ✅ Excellent (KQL) | ⚠️ Limited | ⚠️ Partial |
| **ComputeResources** | ✅ Excellent (KQL) | ⚠️ Partial | ✅ Good |
| **NetworkResources** | ✅ Excellent (KQL) | ⚠️ Partial | ✅ Good |
| **IAMResources** | ✅ Excellent (KQL) | ⚠️ Limited | ⚠️ Partial |
| **DatabaseResources** | ✅ Excellent (KQL) | ⚠️ Limited | ✅ Good |
| **ContainerRegistries** | ✅ Excellent (KQL) | ⚠️ Limited | ✅ Good |

Legend: ✅ Excellent/Good | ⚠️ Partial/Limited | ❌ None

## Detailed Analysis by Table

### 1. KubernetesClustersTable

Manages Kubernetes cluster information across AKS (Azure), EKS (AWS), and GKE (GCP).

#### Azure (AKS) - Resource Graph KQL
- **Filter Pushdown**: ✅ **EXCELLENT**
  ```sql
  WHERE cloud_provider = 'azure' AND region = 'eastus' AND rbac_enabled = true
  ```
  Translates to KQL:
  ```kql
  Resources
  | where type == "microsoft.containerservice/managedclusters"
  | where location == "eastus"
  | where properties.enableRBAC == true
  ```

- **Projection Pushdown**: ✅ **EXCELLENT**
  ```sql
  SELECT cluster_name, kubernetes_version, node_count
  ```
  Translates to:
  ```kql
  | project name, properties.kubernetesVersion, properties.agentPoolProfiles[0].count
  ```

- **Sort Pushdown**: ✅ **EXCELLENT**
  ```sql
  ORDER BY cluster_name ASC, created_date DESC
  ```
  Translates to:
  ```kql
  | order by name asc, properties.createdDate desc
  ```

- **Pagination Pushdown**: ✅ **EXCELLENT**
  ```sql
  LIMIT 50 OFFSET 100
  ```
  Translates to:
  ```kql
  | skip 100 | top 50
  ```

#### AWS (EKS) - SDK API
- **Filter Pushdown**: ⚠️ **LIMITED**
  - Can only filter by cluster names in ListClusters API
  - Tag filtering requires separate DescribeCluster calls
  - No native support for property-based filtering

- **Projection Pushdown**: ❌ **NONE**
  - SDK returns complete cluster objects
  - Must filter fields client-side

- **Sort Pushdown**: ❌ **NONE**
  - No server-side sorting support
  - Must sort results in memory

- **Pagination Pushdown**: ✅ **GOOD**
  ```java
  ListClustersRequest.builder()
    .maxResults(50)  // LIMIT
    .nextToken(token) // OFFSET equivalent
    .build()
  ```

#### GCP (GKE) - Container API
- **Filter Pushdown**: ⚠️ **LIMITED**
  - Can filter by parent (project/location)
  - No support for property filtering

- **Projection Pushdown**: ❌ **NONE**
  - API returns full cluster objects

- **Sort Pushdown**: ❌ **NONE**
  - Client-side sorting required

- **Pagination Pushdown**: ✅ **GOOD**
  ```java
  ListClustersRequest.newBuilder()
    .setPageSize(50)     // LIMIT
    .setPageToken(token)  // OFFSET equivalent
    .build()
  ```

### 2. StorageResourcesTable

Manages storage resources like Azure Storage Accounts, AWS S3 Buckets, and GCP Cloud Storage.

#### Azure (Storage Accounts) - Resource Graph
- **All Pushdowns**: ✅ **EXCELLENT**
  - Full KQL support for all operations
  - Example complex query:
  ```kql
  Resources
  | where type == "microsoft.storage/storageaccounts"
  | where properties.encryption.services.blob.enabled == true
  | project name, location, sku.name, properties.creationTime
  | order by properties.creationTime desc
  | top 100
  ```

#### AWS (S3 Buckets) - S3 API
- **Filter Pushdown**: ⚠️ **LIMITED**
  - Bucket name prefix filtering only
  - Region filtering by bucket location (requires additional API call)

- **Projection Pushdown**: ❌ **NONE**

- **Sort Pushdown**: ⚠️ **PARTIAL**
  ```java
  ListBucketsV2Request.builder()
    .sortBy(BucketSortBy.CREATION_DATE)
    .build()
  ```

- **Pagination Pushdown**: ✅ **GOOD**
  - ContinuationToken support for pagination

#### GCP (Cloud Storage) - Storage API
- **Filter Pushdown**: ⚠️ **LIMITED**
  - Prefix filtering for bucket names
  - Project-based filtering

- **Projection Pushdown**: ⚠️ **PARTIAL**
  ```java
  Storage.BucketListOption.fields("items(name,location,timeCreated)")
  ```

- **Sort Pushdown**: ❌ **NONE**

- **Pagination Pushdown**: ✅ **GOOD**
  - pageToken and maxResults support

### 3. ComputeResourcesTable

Manages virtual machines/instances across providers.

#### Azure (Virtual Machines) - Resource Graph
- **All Pushdowns**: ✅ **EXCELLENT**
  - Complete KQL support for complex queries

#### AWS (EC2 Instances) - EC2 API
- **Filter Pushdown**: ✅ **GOOD**
  ```java
  DescribeInstancesRequest.builder()
    .filters(
      Filter.builder()
        .name("instance-state-name")
        .values("running", "stopped")
        .build(),
      Filter.builder()
        .name("tag:Application")
        .values("web-app")
        .build()
    )
    .build()
  ```

- **Projection Pushdown**: ❌ **NONE**

- **Sort Pushdown**: ❌ **NONE**

- **Pagination Pushdown**: ✅ **GOOD**
  - MaxResults and NextToken support

#### GCP (Compute Engine) - Compute API
- **Filter Pushdown**: ✅ **GOOD**
  ```java
  ListInstancesRequest.newBuilder()
    .setFilter("status=RUNNING AND labels.app=web")
    .build()
  ```

- **Projection Pushdown**: ⚠️ **PARTIAL**
  ```java
  .setFields("items(name,status,machineType)")
  ```

- **Sort Pushdown**: ✅ **PARTIAL**
  ```java
  .setOrderBy("creationTimestamp desc")
  ```

- **Pagination Pushdown**: ✅ **GOOD**
  - pageToken and maxResults support

### 4. NetworkResourcesTable

Manages VPCs, Security Groups, and network infrastructure.

#### Azure - Resource Graph
- **All Pushdowns**: ✅ **EXCELLENT**
  - Full KQL support for VNets, NSGs, etc.

#### AWS (VPCs, Security Groups) - EC2 API
- **Filter Pushdown**: ✅ **GOOD**
  - Extensive filter support in DescribeVpcs, DescribeSecurityGroups
  - Tag filtering, state filtering, CIDR filtering

- **Projection Pushdown**: ❌ **NONE**

- **Sort Pushdown**: ❌ **NONE**

- **Pagination Pushdown**: ✅ **GOOD**

#### GCP (VPC Networks) - Compute API
- **Filter Pushdown**: ✅ **GOOD**
  - Filter expressions support

- **Projection Pushdown**: ⚠️ **PARTIAL**
  - Fields parameter support

- **Sort Pushdown**: ✅ **PARTIAL**
  - OrderBy parameter support

- **Pagination Pushdown**: ✅ **GOOD**

### 5. IAMResourcesTable

Manages identity and access management resources.

#### Azure (Identities, Key Vault) - Resource Graph
- **All Pushdowns**: ✅ **EXCELLENT**

#### AWS (IAM Users, Roles, Policies) - IAM API
- **Filter Pushdown**: ⚠️ **LIMITED**
  - PathPrefix filtering only
  ```java
  ListUsersRequest.builder()
    .pathPrefix("/engineering/")
    .build()
  ```

- **Projection Pushdown**: ❌ **NONE**

- **Sort Pushdown**: ❌ **NONE**

- **Pagination Pushdown**: ✅ **GOOD**
  - Marker-based pagination

#### GCP (Service Accounts) - IAM API
- **Filter Pushdown**: ⚠️ **PARTIAL**
  - Limited filtering options

- **Projection Pushdown**: ⚠️ **PARTIAL**

- **Sort Pushdown**: ❌ **NONE**

- **Pagination Pushdown**: ✅ **GOOD**

### 6. DatabaseResourcesTable

Manages database services across providers.

#### Azure (SQL Database, Cosmos DB) - Resource Graph
- **All Pushdowns**: ✅ **EXCELLENT**

#### AWS (RDS, DynamoDB)
- **Filter Pushdown**: ⚠️ **LIMITED**
  - Basic filters in DescribeDBInstances
  - Engine type filtering

- **Projection Pushdown**: ❌ **NONE**

- **Sort Pushdown**: ❌ **NONE**

- **Pagination Pushdown**: ✅ **GOOD**

#### GCP (Cloud SQL, Firestore)
- **Filter Pushdown**: ✅ **GOOD**

- **Projection Pushdown**: ⚠️ **PARTIAL**

- **Sort Pushdown**: ✅ **PARTIAL**

- **Pagination Pushdown**: ✅ **GOOD**

### 7. ContainerRegistriesTable

Manages container image registries.

#### Azure (Container Registry) - Resource Graph
- **All Pushdowns**: ✅ **EXCELLENT**

#### AWS (ECR) - ECR API
- **Filter Pushdown**: ⚠️ **LIMITED**
  - Repository name filtering
  - No tag-based filtering

- **Projection Pushdown**: ❌ **NONE**

- **Sort Pushdown**: ❌ **NONE**

- **Pagination Pushdown**: ✅ **GOOD**

#### GCP (Artifact Registry) - Artifact Registry API
- **Filter Pushdown**: ✅ **GOOD**

- **Projection Pushdown**: ⚠️ **PARTIAL**

- **Sort Pushdown**: ✅ **PARTIAL**

- **Pagination Pushdown**: ✅ **GOOD**

## Implementation Priority Matrix

### 🔴 High Priority (Maximum Impact)

#### 1. Azure Resource Graph KQL Builder
**Impact**: All 7 tables for Azure
**Implementation**:
```java
public class KQLQueryBuilder {
  public String buildQuery(
      String resourceType,
      List<FilterInfo> filters,
      int[] projections,
      SortInfo sortInfo,
      PaginationInfo pagination) {
    // Build complete KQL query
  }
}
```

#### 2. AWS EC2 Filter Builder
**Impact**: ComputeResources, NetworkResources
**Implementation**:
```java
public class EC2FilterBuilder {
  public List<Filter> buildFilters(List<FilterInfo> filters) {
    // Convert RexNode filters to EC2 Filters
  }
}
```

#### 3. GCP Compute Filter Translator
**Impact**: ComputeResources, NetworkResources
**Implementation**:
```java
public class GCPFilterTranslator {
  public String buildFilterExpression(List<FilterInfo> filters) {
    // Convert to GCP filter syntax
  }
}
```

### 🟡 Medium Priority

#### 1. Pagination Handlers
- AWS NextToken/Marker management
- GCP pageToken state management
- Result batching and streaming

#### 2. GCP Fields Parameter Builder
- Optimize response size
- Map projections to field paths

#### 3. AWS S3 Optimization
- Implement ListBucketsV2 with sorting
- Parallel bucket metadata fetching

### 🟢 Low Priority

#### 1. Client-Side Optimizations
- Efficient in-memory sorting for unsupported providers
- Projection filtering after fetch
- Result caching layer

#### 2. Hybrid Strategies
- Partial server-side filter + client-side refinement
- Progressive result loading

## Performance Impact Estimates

| Optimization | Azure | AWS | GCP |
|-------------|-------|-----|-----|
| **Filter Pushdown** | 90% reduction | 60% reduction | 70% reduction |
| **Projection Pushdown** | 70% reduction | N/A | 40% reduction |
| **Sort Pushdown** | 100% server-side | N/A | 80% server-side |
| **Pagination** | 95% reduction | 90% reduction | 90% reduction |

## Conclusion

Azure provides the most comprehensive pushdown support through Resource Graph's KQL, making it the ideal candidate for demonstrating the full potential of query optimization. AWS and GCP have varying levels of support, with GCP generally offering better flexibility than AWS for compute-related resources, while AWS provides good filtering capabilities but limited support for other optimizations.
