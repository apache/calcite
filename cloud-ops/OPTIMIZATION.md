# Cloud Ops Optimization Design

## Overview

The Cloud Ops adapter is designed with performance optimization as a core principle, implementing intelligent query pushdown capabilities to minimize data transfer and maximize query performance when working with cloud provider APIs.

## Current Implementation Status (as of 2024)

The foundational infrastructure for query optimization has been **completed**:
- ✅ Tables now receive all optimization hints (filters, projections, sorts, pagination)
- ✅ CloudOpsQueryOptimizer analyzes and extracts optimization information
- ✅ Comprehensive logging shows what optimizations are available for each query
- ✅ Unit tests verify optimization hint reception

**Next Steps**: Implement actual pushdown to cloud provider APIs using the received hints.

## Key Optimization Features

### 1. Sort Pushdown

#### Design Goals
- Push ORDER BY operations to cloud provider APIs where supported
- Minimize memory usage by receiving pre-sorted results
- Leverage cloud-native sorting capabilities for better performance

#### Implementation Strategy

##### Azure Resource Graph
Azure Resource Graph supports KQL `order by` clause natively:
```java
public class AzureProvider implements CloudProvider {
    public List<Map<String, Object>> queryWithSort(String resourceType, 
                                                   List<SortField> sortFields,
                                                   int limit) {
        StringBuilder kql = new StringBuilder();
        kql.append("Resources | where type == '").append(resourceType).append("'");
        
        // Push sort to Azure Resource Graph
        if (!sortFields.isEmpty()) {
            kql.append(" | order by ");
            for (SortField field : sortFields) {
                kql.append(field.name).append(" ")
                   .append(field.isAscending ? "asc" : "desc")
                   .append(", ");
            }
        }
        
        if (limit > 0) {
            kql.append(" | limit ").append(limit);
        }
        
        return executeKqlQuery(kql.toString());
    }
}
```

##### AWS SDK
AWS SDK supports sorting through request parameters:
```java
public class AWSProvider implements CloudProvider {
    public List<EC2Instance> getInstancesSorted(SortOrder sortOrder) {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
            .maxResults(1000)  // Pagination support
            .filters(Filter.builder()
                .name("instance-state-name")
                .values("running")
                .build())
            .build();
        
        // AWS doesn't support server-side sorting for EC2
        // But we can optimize by fetching only required fields
        List<Instance> instances = ec2Client.describeInstances(request)
            .reservations().stream()
            .flatMap(r -> r.instances().stream())
            .collect(Collectors.toList());
            
        // Client-side sort only when necessary
        return sortLocally(instances, sortOrder);
    }
    
    // For services that support sorting (like S3)
    public List<S3Bucket> getBucketsSorted() {
        ListBucketsV2Request request = ListBucketsV2Request.builder()
            .sortBy(BucketSortBy.CREATION_DATE)  // Native sort support
            .build();
        
        return s3Client.listBucketsV2(request).buckets();
    }
}
```

##### GCP Cloud Asset API
GCP supports ordering through API parameters:
```java
public class GCPProvider implements CloudProvider {
    public List<Asset> getAssetsSorted(String orderBy) {
        SearchAllResourcesRequest request = SearchAllResourcesRequest.newBuilder()
            .setScope("projects/" + projectId)
            .setOrderBy(orderBy)  // e.g., "name desc", "createTime"
            .setPageSize(500)
            .build();
        
        return assetClient.searchAllResources(request)
            .iterateAll()
            .stream()
            .collect(Collectors.toList());
    }
}
```

#### Sort Pushdown Rules

```java
public class CloudOpsSortRule extends RelOptRule {
    public static final CloudOpsSortRule INSTANCE = new CloudOpsSortRule();
    
    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final CloudOpsTableScan scan = call.rel(1);
        
        // Check if provider supports sort pushdown
        if (scan.getTable().supportsSort()) {
            CloudOpsTableScan newScan = scan.withSort(
                sort.getCollation(),
                sort.fetch
            );
            call.transformTo(newScan);
        }
    }
}
```

### 2. Pagination Pushdown

#### Design Goals
- Implement efficient pagination for large result sets
- Push LIMIT and OFFSET to cloud APIs
- Support cursor-based pagination where available
- Minimize memory footprint for large queries

#### Implementation Strategy

##### Limit Pushdown
```java
public abstract class AbstractCloudOpsTable extends AbstractTable 
    implements ProjectableFilterableTable {
    
    protected Integer limit;
    protected Integer offset;
    
    public Enumerable<Object[]> scan(DataContext root, 
                                     List<RexNode> filters,
                                     int[] projects,
                                     Integer fetchLimit,
                                     Integer fetchOffset) {
        // Push limit to cloud provider
        this.limit = fetchLimit;
        this.offset = fetchOffset;
        
        List<Map<String, Object>> results = queryCloudProvider(
            filters, 
            projects, 
            limit, 
            offset
        );
        
        return Linq4j.asEnumerable(results)
            .select(row -> projectRow(row, projects));
    }
}
```

##### Provider-Specific Pagination

**Azure Resource Graph** (supports top/skip):
```java
public List<Map<String, Object>> queryWithPagination(String kql, 
                                                     int limit, 
                                                     int offset) {
    String paginatedKql = kql;
    if (offset > 0) {
        paginatedKql += " | skip " + offset;
    }
    if (limit > 0) {
        paginatedKql += " | top " + limit;
    }
    
    QueryRequest request = new QueryRequest()
        .withQuery(paginatedKql)
        .withOptions(new QueryRequestOptions()
            .withTop(limit)
            .withSkip(offset));
    
    return resourceGraphClient.resources(request);
}
```

**AWS SDK** (cursor-based pagination):
```java
public class AWSPaginationHandler {
    public List<Instance> getAllInstances(int maxResults) {
        List<Instance> allInstances = new ArrayList<>();
        String nextToken = null;
        int fetched = 0;
        
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .nextToken(nextToken)
                .maxResults(Math.min(100, maxResults - fetched))
                .build();
            
            DescribeInstancesResponse response = ec2Client.describeInstances(request);
            
            response.reservations().forEach(reservation ->
                allInstances.addAll(reservation.instances())
            );
            
            nextToken = response.nextToken();
            fetched = allInstances.size();
            
        } while (nextToken != null && fetched < maxResults);
        
        return allInstances;
    }
}
```

**GCP** (page token pagination):
```java
public class GCPPaginationHandler {
    public List<Asset> getAssetsWithPagination(int pageSize, int maxPages) {
        List<Asset> allAssets = new ArrayList<>();
        String pageToken = null;
        int pagesRetrieved = 0;
        
        while (pagesRetrieved < maxPages) {
            SearchAllResourcesRequest.Builder requestBuilder = 
                SearchAllResourcesRequest.newBuilder()
                    .setScope("projects/" + projectId)
                    .setPageSize(pageSize);
            
            if (pageToken != null) {
                requestBuilder.setPageToken(pageToken);
            }
            
            SearchAllResourcesPagedResponse response = 
                assetClient.searchAllResources(requestBuilder.build());
            
            for (ResourceSearchResult result : response.getPage().getValues()) {
                allAssets.add(convertToAsset(result));
            }
            
            pageToken = response.getNextPageToken();
            pagesRetrieved++;
            
            if (pageToken == null) break;
        }
        
        return allAssets;
    }
}
```

### 3. Filter Pushdown Optimization

#### Enhanced Filter Translation
```java
public class CloudOpsFilterTranslator {
    public CloudProviderFilter translate(List<RexNode> filters) {
        CloudProviderFilter result = new CloudProviderFilter();
        
        for (RexNode filter : filters) {
            if (filter instanceof RexCall) {
                RexCall call = (RexCall) filter;
                SqlOperator op = call.getOperator();
                
                // Translate to provider-specific filter
                if (op == SqlStdOperatorTable.EQUALS) {
                    result.addEqualsFilter(
                        getFieldName(call.operands.get(0)),
                        getValue(call.operands.get(1))
                    );
                } else if (op == SqlStdOperatorTable.GREATER_THAN) {
                    result.addRangeFilter(
                        getFieldName(call.operands.get(0)),
                        getValue(call.operands.get(1)),
                        null
                    );
                } else if (op == SqlStdOperatorTable.LIKE) {
                    result.addPatternFilter(
                        getFieldName(call.operands.get(0)),
                        getValue(call.operands.get(1))
                    );
                }
            }
        }
        
        return result;
    }
}
```

### 4. Projection Pushdown

#### Minimize Data Transfer
```java
public class ProjectionOptimizer {
    public List<String> optimizeProjection(int[] projects, 
                                          RelDataType rowType) {
        List<String> requestedFields = new ArrayList<>();
        
        // Only request fields that are actually needed
        for (int project : projects) {
            String fieldName = rowType.getFieldList()
                .get(project).getName();
            requestedFields.add(fieldName);
        }
        
        // Add fields required for filters/sorts even if not projected
        requestedFields.addAll(getRequiredFields());
        
        return requestedFields;
    }
}
```

### 5. Parallel Query Execution

#### Multi-Account/Project Parallelization
```java
public class ParallelCloudQueryExecutor {
    private final ExecutorService executor = 
        Executors.newFixedThreadPool(10);
    
    public List<Map<String, Object>> queryAllAccounts(
            List<String> accounts,
            QuerySpec querySpec) {
        
        List<CompletableFuture<List<Map<String, Object>>>> futures = 
            accounts.stream()
                .map(account -> CompletableFuture.supplyAsync(() ->
                    queryAccount(account, querySpec), executor))
                .collect(Collectors.toList());
        
        // Combine results from all accounts
        return futures.stream()
            .map(CompletableFuture::join)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }
}
```

### 6. Caching Strategy

#### Result Caching
```java
public class CloudOpsCacheManager {
    private final Cache<QueryKey, List<Map<String, Object>>> cache;
    
    public CloudOpsCacheManager() {
        this.cache = Caffeine.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build();
    }
    
    public List<Map<String, Object>> getCachedOrQuery(
            QueryKey key,
            Supplier<List<Map<String, Object>>> querySupplier) {
        
        return cache.get(key, k -> querySupplier.get());
    }
}
```

## Performance Metrics

### Query Optimization Goals

| Optimization | Target Improvement | Measurement |
|-------------|-------------------|-------------|
| Sort Pushdown | 50-80% reduction in client-side processing | Memory usage, CPU time |
| Pagination | 90% reduction in memory for large queries | Peak heap usage |
| Filter Pushdown | 60-95% reduction in data transfer | Network bytes, row count |
| Projection Pushdown | 30-70% reduction in data transfer | Response size |
| Parallel Execution | 3-5x faster for multi-account queries | End-to-end query time |
| Caching | 95% faster for repeated queries | Query response time |

## Implementation Roadmap

### Phase 1: Core Optimizations
- [x] Basic filter pushdown - **COMPLETED**: Tables receive filter RexNodes
- [x] Query optimization hint reception - **COMPLETED**: Tables receive sort, pagination, and projection hints
- [x] ProjectableFilterableTable implementation - **COMPLETED**: AbstractCloudOpsTable now implements projection support
- [x] Enhanced scan method with optimization parameters - **COMPLETED**: scan() accepts RelCollation, offset, fetch
- [x] CloudOpsQueryOptimizer utility - **COMPLETED**: Analyzes and extracts optimization hints
- [ ] Sort pushdown to Azure Resource Graph KQL
- [ ] Limit pushdown to cloud provider APIs
- [ ] Projection pushdown to minimize data transfer

### Phase 2: Advanced Optimizations
- [ ] Cursor-based pagination for AWS
- [ ] Page token pagination for GCP
- [ ] Parallel query execution
- [ ] Smart caching with TTL

### Phase 3: Intelligent Optimization
- [ ] Cost-based optimization
- [ ] Adaptive query execution
- [ ] Statistics collection
- [ ] Query plan caching

## Testing Strategy

### Performance Tests
```java
@Test
@Category(PerformanceTest.class)
public void testSortPushdownPerformance() {
    long withoutPushdown = measureQuery(
        "SELECT * FROM kubernetes_clusters ORDER BY cluster_name",
        false
    );
    
    long withPushdown = measureQuery(
        "SELECT * FROM kubernetes_clusters ORDER BY cluster_name",
        true
    );
    
    double improvement = (1.0 - (withPushdown / (double) withoutPushdown)) * 100;
    assertTrue("Sort pushdown should improve performance by >50%", 
               improvement > 50);
}

@Test
@Category(PerformanceTest.class)
public void testPaginationMemoryUsage() {
    long memoryBefore = Runtime.getRuntime().totalMemory() - 
                       Runtime.getRuntime().freeMemory();
    
    // Query with pagination
    executeQuery("SELECT * FROM storage_resources LIMIT 100 OFFSET 1000");
    
    long memoryAfter = Runtime.getRuntime().totalMemory() - 
                       Runtime.getRuntime().freeMemory();
    
    long memoryUsed = memoryAfter - memoryBefore;
    assertTrue("Memory usage should be under 10MB for paginated query",
               memoryUsed < 10 * 1024 * 1024);
}
```

## Monitoring and Metrics

### Query Performance Metrics
```java
public class CloudOpsMetrics {
    private final MeterRegistry registry;
    
    public void recordQueryMetrics(QueryExecution execution) {
        registry.timer("cloud.ops.query.time")
            .record(execution.getDuration());
        
        registry.counter("cloud.ops.rows.fetched")
            .increment(execution.getRowCount());
        
        registry.gauge("cloud.ops.memory.used", 
            execution.getPeakMemoryUsage());
        
        registry.counter("cloud.ops.optimization.pushdown",
            "type", execution.getPushdownType())
            .increment();
    }
}
```

## Best Practices

1. **Always push filters first** - Reduce data at the source
2. **Combine sorts with limits** - Most effective together
3. **Use provider-native features** - Leverage cloud-specific optimizations
4. **Monitor API quotas** - Respect rate limits with intelligent batching
5. **Cache strategically** - Cache metadata, not volatile data
6. **Parallelize carefully** - Balance throughput with API limits