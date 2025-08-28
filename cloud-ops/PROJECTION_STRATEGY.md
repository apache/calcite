# Multi-Cloud Projection Strategy Design

## The Challenge

When querying across multiple cloud providers with different projection capabilities:
- **Azure**: Full projection support via KQL `project`
- **AWS**: No projection support (returns full objects)
- **GCP**: Partial projection support via `fields` parameter

How do we handle `SELECT cluster_name, region FROM kubernetes_clusters` when it queries all three providers?

## Design Approaches

### Approach 1: Early Projection with Provider-Aware Optimization

**Strategy**: Each provider projects what it can, then we standardize the results.

```java
public class AbstractCloudOpsTable {
    protected List<Object[]> queryAzure(List<String> subscriptions, int[] projections) {
        // Azure: Full projection via KQL
        String kql = buildKQLWithProjection(projections);
        List<Map<String, Object>> results = executeKQL(kql);
        // Returns only projected columns from Azure
        return convertToRows(results, projections);
    }

    protected List<Object[]> queryAWS(List<String> accounts, int[] projections) {
        // AWS: No projection support, must fetch all
        List<Cluster> clusters = eksClient.listClusters();
        // Manual projection after fetch
        return projectClientSide(clusters, projections);
    }

    protected List<Object[]> queryGCP(List<String> projects, int[] projections) {
        // GCP: Partial projection via fields
        String fields = buildFieldsParameter(projections);
        List<Cluster> clusters = gkeClient.listClusters(fields);
        // May need additional client-side projection
        return projectAsNeeded(clusters, projections);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
                                     int[] projections, ...) {
        // Each provider handles projection independently
        List<CompletableFuture<List<Object[]>>> futures = new ArrayList<>();

        if (queryAzure) {
            futures.add(CompletableFuture.supplyAsync(() ->
                queryAzure(subscriptions, projections)));
        }
        if (queryAWS) {
            futures.add(CompletableFuture.supplyAsync(() ->
                queryAWS(accounts, projections)));
        }
        if (queryGCP) {
            futures.add(CompletableFuture.supplyAsync(() ->
                queryGCP(projects, projections)));
        }

        // Combine pre-projected results
        List<Object[]> allResults = futures.stream()
            .map(CompletableFuture::join)
            .flatMap(List::stream)
            .collect(Collectors.toList());

        // All results already have the same projection
        return Linq4j.asEnumerable(allResults);
    }
}
```

**Pros:**
- ✅ Minimizes data transfer from cloud providers that support projection
- ✅ Reduces memory usage (Azure returns less data)
- ✅ Each provider optimized independently
- ✅ Consistent result format

**Cons:**
- ❌ More complex implementation
- ❌ Need projection logic for each provider
- ❌ Duplicate projection code for providers without native support

### Approach 2: Fetch Full Objects, Let Calcite Project

**Strategy**: Always fetch all columns from providers, return full rows to Calcite.

```java
public class AbstractCloudOpsTable {
    protected List<Object[]> queryAzure(List<String> subscriptions) {
        // Azure: Fetch all columns
        String kql = "Resources | where type == '...'";
        List<Map<String, Object>> results = executeKQL(kql);
        return convertToFullRows(results);
    }

    protected List<Object[]> queryAWS(List<String> accounts) {
        // AWS: Already fetches all
        List<Cluster> clusters = eksClient.listClusters();
        return convertToFullRows(clusters);
    }

    protected List<Object[]> queryGCP(List<String> projects) {
        // GCP: Don't use fields parameter
        List<Cluster> clusters = gkeClient.listClusters();
        return convertToFullRows(clusters);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
                                     int[] projections, ...) {
        // Ignore projections parameter
        List<Object[]> allResults = combineResults();

        // Let Calcite handle projection
        // Since we implement ProjectableFilterableTable,
        // Calcite won't add a Project operator
        return Linq4j.asEnumerable(allResults);
    }
}
```

**Pros:**
- ✅ Simpler implementation
- ✅ No duplicate projection logic
- ✅ Consistent handling across providers

**Cons:**
- ❌ Wastes Azure's projection capability
- ❌ More data transfer
- ❌ Higher memory usage
- ❌ Slower performance

### Approach 3: Hybrid Smart Projection (RECOMMENDED)

**Strategy**: Optimize where possible, standardize the output format.

```java
public class CloudOpsProjectionHandler {
    private final RelDataType rowType;
    private final int[] projections;

    public CloudOpsProjectionHandler(RelDataType rowType, int[] projections) {
        this.rowType = rowType;
        this.projections = projections;
    }

    /**
     * Build provider-specific query with best available projection
     */
    public String buildAzureKQL(String resourceType) {
        if (projections == null) {
            return buildFullKQL(resourceType);
        }

        // Azure: Full projection support
        StringBuilder kql = new StringBuilder();
        kql.append("Resources | where type == '").append(resourceType).append("'");
        kql.append(" | project ");

        for (int i = 0; i < projections.length; i++) {
            if (i > 0) kql.append(", ");
            String fieldName = rowType.getFieldList().get(projections[i]).getName();
            kql.append(mapToAzureField(fieldName));
        }
        return kql.toString();
    }

    public DescribeInstancesRequest buildAWSRequest() {
        // AWS: No projection, but we know what we need
        return DescribeInstancesRequest.builder()
            .filters(filters)
            .maxResults(100)
            .build();
    }

    public ListClustersRequest buildGCPRequest() {
        if (projections == null) {
            return ListClustersRequest.newBuilder().build();
        }

        // GCP: Partial projection via fields
        String fields = buildGCPFields(projections);
        return ListClustersRequest.newBuilder()
            .setFields(fields)
            .build();
    }

    /**
     * Convert provider results to standardized row format
     */
    public Object[] convertToRow(Map<String, Object> azureResult) {
        if (projections == null) {
            return convertFullRow(azureResult);
        }

        // Azure already projected, just map to array
        Object[] row = new Object[projections.length];
        for (int i = 0; i < projections.length; i++) {
            String field = rowType.getFieldList().get(projections[i]).getName();
            row[i] = azureResult.get(field);
        }
        return row;
    }

    public Object[] convertToRow(Instance awsInstance) {
        if (projections == null) {
            return convertFullRow(awsInstance);
        }

        // AWS: Manual projection from full object
        Object[] row = new Object[projections.length];
        for (int i = 0; i < projections.length; i++) {
            int fieldIndex = projections[i];
            row[i] = extractFieldValue(awsInstance, fieldIndex);
        }
        return row;
    }

    public Object[] convertToRow(Cluster gcpCluster, boolean partialProjection) {
        if (projections == null) {
            return convertFullRow(gcpCluster);
        }

        // GCP: May have partial data or full data
        Object[] row = new Object[projections.length];
        for (int i = 0; i < projections.length; i++) {
            int fieldIndex = projections[i];
            row[i] = extractFieldValue(gcpCluster, fieldIndex);
        }
        return row;
    }
}

public abstract class AbstractCloudOpsTable {
    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
                                     int[] projections, ...) {
        // Create projection handler
        CloudOpsProjectionHandler projectionHandler =
            new CloudOpsProjectionHandler(getRowType(root.getTypeFactory()), projections);

        List<CompletableFuture<List<Object[]>>> futures = new ArrayList<>();

        // Each provider optimizes as much as possible
        if (queryAzure) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                String kql = projectionHandler.buildAzureKQL(getResourceType());
                List<Map<String, Object>> results = executeKQL(kql);
                return results.stream()
                    .map(projectionHandler::convertToRow)
                    .collect(Collectors.toList());
            }));
        }

        if (queryAWS) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                // AWS can't project, but we convert to projected format
                List<Instance> instances = ec2Client.describeInstances();
                return instances.stream()
                    .map(projectionHandler::convertToRow)
                    .collect(Collectors.toList());
            }));
        }

        if (queryGCP) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                // GCP partial projection
                Request request = projectionHandler.buildGCPRequest();
                List<Resource> resources = gcpClient.list(request);
                return resources.stream()
                    .map(r -> projectionHandler.convertToRow(r, true))
                    .collect(Collectors.toList());
            }));
        }

        // All results now have consistent projection
        List<Object[]> projectedResults = futures.stream()
            .map(CompletableFuture::join)
            .flatMap(List::stream)
            .collect(Collectors.toList());

        return Linq4j.asEnumerable(projectedResults);
    }
}
```

## Recommendation: Hybrid Smart Projection

**Why this approach is best:**

1. **Optimizes where possible**: Azure gets full projection benefit, GCP gets partial benefit
2. **Consistent output format**: All providers return same column structure
3. **Memory efficient**: Only projected columns in final result
4. **Network efficient**: Reduces data transfer from providers that support it
5. **Clear separation of concerns**: ProjectionHandler manages complexity

## Implementation Guidelines

### Phase 1: Provider Detection
```java
// Detect projection capabilities
enum ProjectionSupport {
    FULL,    // Azure
    PARTIAL, // GCP
    NONE     // AWS
}
```

### Phase 2: Query Building
```java
// Build provider-specific queries with projection
if (provider.supportsProjection() == ProjectionSupport.FULL) {
    query = buildWithFullProjection(projections);
} else if (provider.supportsProjection() == ProjectionSupport.PARTIAL) {
    query = buildWithPartialProjection(projections);
} else {
    query = buildStandardQuery();
}
```

### Phase 3: Result Standardization
```java
// Convert all results to same projected format
Object[] standardRow = new Object[projections.length];
for (int i = 0; i < projections.length; i++) {
    standardRow[i] = extractFieldValue(providerResult, projections[i]);
}
```

## Performance Impact

| Scenario | Approach 1 (Early) | Approach 2 (Late) | Approach 3 (Hybrid) |
|----------|-------------------|-------------------|---------------------|
| Azure only | ⚡ Excellent | ❌ Poor | ⚡ Excellent |
| AWS only | ✅ Good | ✅ Good | ✅ Good |
| GCP only | ✅ Good | ❌ Poor | ✅ Good |
| All 3 providers | ✅ Good | ❌ Poor | ✅ Good |
| Memory usage | ⚡ Lowest | ❌ Highest | ⚡ Low |
| Network transfer | ⚡ Optimized | ❌ Maximum | ⚡ Optimized |
| Code complexity | ❌ High | ✅ Low | ⚠️ Medium |

## Conclusion

The **Hybrid Smart Projection** approach provides the best balance:
- Leverages each provider's capabilities
- Maintains consistent output format
- Optimizes performance where possible
- Reasonable implementation complexity

This ensures that when querying multiple providers, we:
1. Minimize data transfer from providers that support projection
2. Return consistent column structure to Calcite
3. Don't waste optimization opportunities
4. Keep the implementation maintainable
