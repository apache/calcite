# Apache Calcite File Adapter - Testing Guide

This document describes the test organization, categories, and execution strategies for the File Adapter.

## Test Categories

Tests are organized into three main categories using JUnit 5 tags:

### Unit Tests (`@Tag("unit")`)
- **Purpose**: Test individual components in isolation
- **Characteristics**: 
  - Run quickly (typically under 1 second)
  - No external dependencies or credentials required
  - Use mocks/stubs for external dependencies
  - Test business logic, data transformations, utilities
- **Examples**: `JsonEnumeratorTest`, `CsvParserTest`, `FileUtilsTest`

### Integration Tests (`@Tag("integration")`)
- **Purpose**: Test interactions between multiple components and external services
- **Characteristics**:
  - Test real integrations with external services
  - Require credentials or connection properties
  - Conditional execution based on availability of credentials
  - Take longer to run than unit tests
- **Examples**: `RedisIntegrationTest`, `SharePointIntegrationTest`, `SftpIntegrationTest`

### Performance Tests (`@Tag("performance")`)
- **Purpose**: Measure execution time, memory usage, and throughput
- **Characteristics**:
  - Generate performance reports and benchmarks
  - Take several seconds or minutes to complete
  - Disabled by default, enabled via system properties
  - Compare different execution engines
- **Examples**: `ComprehensiveEnginePerformanceTest`, `SeparatedPerformanceTest`

## Running Tests

### All Tests (Default)
```bash
./gradlew :file:test
```

### Unit Tests Only
```bash
./gradlew :file:test --tests "*" --exclude-tags "integration,performance"
```

### Integration Tests Only
```bash
./gradlew :file:test --tests "*" --include-tags "integration"
```

### Performance Tests Only
```bash
./gradlew :file:test --tests "*" --include-tags "performance" -DenablePerformanceTests=true
```

### Exclude Performance Tests
```bash
./gradlew :file:test --exclude-tags "performance"
```

## Integration Test Requirements

Integration tests require specific credentials or connection properties to run. They automatically skip themselves if credentials are not available.

### Redis Integration Tests
**Required Properties:**
```bash
-Dredis.host=localhost
-Dredis.port=6379
# Optional authentication:
-Dredis.password=your_password
```

**Environment Setup:**
```bash
# Local Redis instance
docker run -d -p 6379:6379 redis:alpine

# Run tests
./gradlew :file:test --tests "*RedisIntegrationTest*" -Dredis.host=localhost
```

### SharePoint Integration Tests
**Required Properties:**
```bash
-DSHAREPOINT_INTEGRATION_TESTS=true
-DSHAREPOINT_TENANT_ID=your_tenant_id
-DSHAREPOINT_CLIENT_ID=your_client_id
-DSHAREPOINT_CLIENT_SECRET=your_client_secret
-DSHAREPOINT_SITE_URL=https://yourtenant.sharepoint.com
```

**Example:**
```bash
./gradlew :file:test --tests "*SharePoint*" \
  -DSHAREPOINT_INTEGRATION_TESTS=true \
  -DSHAREPOINT_TENANT_ID=abc123... \
  -DSHAREPOINT_CLIENT_ID=def456... \
  -DSHAREPOINT_CLIENT_SECRET=ghi789... \
  -DSHAREPOINT_SITE_URL=https://contoso.sharepoint.com
```

### SFTP Integration Tests
**Required Properties:**
```bash
-DSFTP_INTEGRATION_TESTS=true
-DSFTP_HOST=your_sftp_host
-DSFTP_USERNAME=your_username
-DSFTP_PASSWORD=your_password
# Optional:
-DSFTP_PORT=22
-DSFTP_ROOT_PATH=/data
```

### FTP Integration Tests
**Required Properties:**
```bash
-DrunFtpTests=true
-DFTP_HOST=your_ftp_host
-DFTP_USERNAME=your_username
-DFTP_PASSWORD=your_password
# Optional:
-DFTP_PORT=21
```

## Test Configuration Files

### Local Properties File
Create `file/local-test.properties` (ignored by Git) for persistent test configuration:

```properties
# Redis Configuration
redis.host=localhost
redis.port=6379

# SharePoint Configuration (for CI/CD)
SHAREPOINT_INTEGRATION_TESTS=false
# SHAREPOINT_TENANT_ID=...
# SHAREPOINT_CLIENT_ID=...
# SHAREPOINT_CLIENT_SECRET=...
# SHAREPOINT_SITE_URL=...

# Performance Tests
enablePerformanceTests=false
```

### Environment Variables
Integration tests also read from environment variables:
```bash
export SHAREPOINT_TENANT_ID=your_tenant_id
export SHAREPOINT_CLIENT_ID=your_client_id
export SHAREPOINT_CLIENT_SECRET=your_client_secret
export SHAREPOINT_SITE_URL=https://yourtenant.sharepoint.com
export SHAREPOINT_INTEGRATION_TESTS=true

./gradlew :file:test --tests "*SharePoint*"
```

## CI/CD Considerations

### GitHub Actions / Jenkins
```yaml
# Example GitHub Actions configuration
- name: Run Unit Tests
  run: ./gradlew :file:test --exclude-tags "integration,performance"

- name: Run Integration Tests (if credentials available)
  run: ./gradlew :file:test --include-tags "integration"
  env:
    SHAREPOINT_TENANT_ID: ${{ secrets.SHAREPOINT_TENANT_ID }}
    SHAREPOINT_CLIENT_ID: ${{ secrets.SHAREPOINT_CLIENT_ID }}
    SHAREPOINT_CLIENT_SECRET: ${{ secrets.SHAREPOINT_CLIENT_SECRET }}
    SHAREPOINT_SITE_URL: ${{ secrets.SHAREPOINT_SITE_URL }}
    SHAREPOINT_INTEGRATION_TESTS: true
  if: env.SHAREPOINT_TENANT_ID != ''

- name: Run Performance Tests (on-demand)
  run: ./gradlew :file:test --include-tags "performance" -DenablePerformanceTests=true
  if: github.event_name == 'workflow_dispatch'
```

## Test Data and Fixtures

### Temporary Test Data
Tests use `@TempDir` for isolated test environments:
```java
@TempDir
Path tempDir;
```

### Test Resources
Static test data is located in:
- `src/test/resources/` - Test data files (CSV, JSON, Excel, etc.)
- `src/test/resources/bug/` - Files for regression tests
- `src/test/resources/data/` - Sample datasets

### Large Test Datasets
Performance tests generate synthetic data:
- **Small**: 1,000 rows (quick validation)
- **Medium**: 100,000 rows (realistic testing) 
- **Large**: 1,000,000+ rows (performance benchmarking)

## Test Execution Environments

### Local Development
```bash
# Fast feedback loop - unit tests only
./gradlew :file:test --exclude-tags "integration,performance"

# Full test suite with local services
./gradlew :file:test -Dredis.host=localhost
```

### Integration Environment
```bash
# With external service credentials
./gradlew :file:test --include-tags "integration" \
  -DSHAREPOINT_INTEGRATION_TESTS=true \
  -DSHAREPOINT_TENANT_ID=... \
  # ... other credentials
```

### Performance Benchmarking
```bash
# Dedicated performance runs
./gradlew :file:test --include-tags "performance" \
  -DenablePerformanceTests=true \
  --info
```

## Test Patterns and Best Practices

### Integration Test Pattern
```java
@Tag("integration")
public class MyIntegrationTest {
  @BeforeEach
  void checkCredentials() {
    assumeTrue(hasRequiredCredentials(), 
        "Skipping - credentials not available");
  }
  
  private boolean hasRequiredCredentials() {
    return System.getProperty("service.host") != null
        && System.getProperty("service.auth") != null;
  }
}
```

### Performance Test Pattern
```java
@Tag("performance")
public class MyPerformanceTest {
  @BeforeEach
  void checkEnabled() {
    assumeTrue(Boolean.getBoolean("enablePerformanceTests"), 
        "Performance tests disabled - use -DenablePerformanceTests=true");
  }
  
  @Test
  void measurePerformance() {
    // Warmup runs
    for (int i = 0; i < WARMUP_RUNS; i++) {
      runOperation();
    }
    
    // Measurement runs
    long totalTime = 0;
    for (int i = 0; i < TEST_RUNS; i++) {
      long start = System.currentTimeMillis();
      runOperation();
      totalTime += System.currentTimeMillis() - start;
    }
    
    System.out.printf("Average time: %d ms%n", totalTime / TEST_RUNS);
  }
}
```

## Troubleshooting

### Common Issues

**Integration tests always skip:**
- Check that required system properties are set
- Verify credentials are valid
- Check network connectivity to external services

**Performance tests don't run:**
- Add `-DenablePerformanceTests=true` system property
- Use `--include-tags "performance"` flag

**Tests fail with "Unknown module" warnings:**
- Arrow-related warnings are harmless and can be ignored
- These don't affect test execution

### Debug Mode
```bash
# Verbose test output
./gradlew :file:test --info --stacktrace

# Debug specific test
./gradlew :file:test --tests "MySpecificTest" --debug
```

## Performance Benchmarks

See [PERFORMANCE_BENCHMARKS.md](PERFORMANCE_BENCHMARKS.md) for detailed performance analysis and benchmarking results across different execution engines.