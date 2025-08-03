# Testing Guide for Cloud Governance Adapter

This guide explains how to run tests for the Cloud Governance adapter, including all test categories, configurations, and command-line options.

## Table of Contents

- [Test Categories](#test-categories)
- [Quick Start](#quick-start)
- [Test Commands Reference](#test-commands-reference)
- [Credential Configuration](#credential-configuration)
- [Command Line Options](#command-line-options)
- [Test Reports](#test-reports)
- [Troubleshooting](#troubleshooting)
- [CI/CD Integration](#cicd-integration)

## Test Categories

The Cloud Governance adapter uses JUnit categories to organize tests into three distinct types:

### Unit Tests (`@Category(UnitTest.class)`)
- **Purpose**: Fast, isolated tests without external dependencies
- **Characteristics**:
  - No network calls to cloud providers
  - Uses mock/fake credentials
  - Tests internal logic and data structures
  - Execution time: < 10 seconds total
- **Examples**: Configuration parsing, schema creation, table metadata

### Integration Tests (`@Category(IntegrationTest.class)`)
- **Purpose**: Tests requiring real cloud credentials and API calls
- **Characteristics**:
  - Makes actual API calls to Azure, AWS, and GCP
  - Requires valid credentials in `local-test.properties`
  - Tests end-to-end functionality
  - Execution time: 30 seconds to several minutes
- **Examples**: Querying real Kubernetes clusters, storage resources

### Performance Tests (`@Category(PerformanceTest.class)`)
- **Purpose**: Tests measuring execution time, memory usage, and throughput
- **Characteristics**:
  - Benchmarks component performance
  - Memory allocation testing
  - Large-scale object creation tests
  - Reports performance metrics
- **Examples**: Schema creation speed, memory usage patterns

## Quick Start

### 1. Run Unit Tests Only (Default)
```bash
./gradlew :cloud-governance:test
```
- Fastest option for development
- No credentials required
- Always runs regardless of environment

### 2. Run All Available Tests
```bash
# With credentials configured
./gradlew :cloud-governance:test

# Without credentials configured  
./gradlew :cloud-governance:allTests
```

### 3. Run Integration Tests
```bash
# First, configure credentials (see Credential Configuration section)
cp src/test/resources/local-test.properties.sample src/test/resources/local-test.properties
# Edit the file with your credentials

# Then run integration tests
./gradlew :cloud-governance:integrationTest
```

## Test Commands Reference

### Basic Test Tasks

| Command | Description | Duration | Prerequisites |
|---------|-------------|----------|---------------|
| `./gradlew :cloud-governance:test` | Smart default - unit tests + integration tests if credentials available | 10s - 2min | None |
| `./gradlew :cloud-governance:unitTest` | Unit tests only | ~10s | None |
| `./gradlew :cloud-governance:integrationTest` | Integration tests only | 1-5min | Credentials required |
| `./gradlew :cloud-governance:performanceTest` | Performance tests only | 10-30s | None |
| `./gradlew :cloud-governance:allTests` | All test categories | 1-5min | None (credentials optional) |

### Build Integration

| Command | Description | When to Use |
|---------|-------------|-------------|
| `./gradlew :cloud-governance:build` | Build + run default tests | Before committing |
| `./gradlew :cloud-governance:check` | Build + all checks including tests | Release preparation |
| `./gradlew :cloud-governance:clean test` | Clean build + tests | After major changes |

## Credential Configuration

### Setup Steps

1. **Copy the sample file**:
   ```bash
   cp src/test/resources/local-test.properties.sample src/test/resources/local-test.properties
   ```

2. **Configure at least one cloud provider** (you don't need all three):

### Azure Configuration
```properties
# Azure AD App Registration required
azure.tenantId=your-azure-tenant-id
azure.clientId=your-azure-client-id  
azure.clientSecret=your-azure-client-secret
azure.subscriptionIds=sub1,sub2,sub3
```

**Required Azure Permissions:**
- `Microsoft.ResourceGraph/resources/read`
- `Reader` role on target subscriptions

### AWS Configuration
```properties
# IAM User or Role credentials
aws.accessKeyId=AKIA...
aws.secretAccessKey=your-secret-key
aws.region=us-east-1
aws.accountIds=111111111111,222222222222
aws.roleArn=arn:aws:iam::{account-id}:role/CrossAccountRole  # Optional
```

**Required AWS Permissions:**
- `ec2:Describe*`, `eks:Describe*`, `eks:List*`
- `s3:ListAllMyBuckets`, `s3:GetBucket*`
- `iam:List*`, `iam:Get*`
- `rds:Describe*`, `dynamodb:List*`, `dynamodb:Describe*`
- `ecr:Describe*`

### GCP Configuration
```properties
# Service Account Key File
gcp.credentialsPath=/path/to/service-account.json
gcp.projectIds=project1,project2
```

**Required GCP Roles:**
- `roles/cloudasset.viewer`
- `roles/storage.objectViewer`
- `roles/container.viewer`

### Credential Detection Logic

The build system automatically detects credentials:

```
IF local-test.properties exists AND
   (Azure credentials complete OR AWS credentials complete OR GCP credentials complete)
THEN include integration tests in default 'test' task
ELSE exclude integration tests from default 'test' task
```

## Command Line Options

### Gradle Test Options

```bash
# Verbose output
./gradlew :cloud-governance:test --info

# Debug output  
./gradlew :cloud-governance:test --debug

# Continue on failure
./gradlew :cloud-governance:test --continue

# Run specific test class
./gradlew :cloud-governance:test --tests "CloudGovernanceConfigTest"

# Run specific test method
./gradlew :cloud-governance:test --tests "CloudGovernanceConfigTest.testAzureConfig"

# Run tests matching pattern
./gradlew :cloud-governance:test --tests "*Config*"

# Parallel execution
./gradlew :cloud-governance:test --parallel

# Dry run (show what would execute)
./gradlew :cloud-governance:test --dry-run

# Force re-run even if up-to-date
./gradlew :cloud-governance:test --rerun-tasks
```

### Memory and Performance Options

```bash
# Increase heap size for performance tests
./gradlew :cloud-governance:performanceTest -Dorg.gradle.jvmargs="-Xmx4g"

# Enable detailed GC logging
./gradlew :cloud-governance:test -Dorg.gradle.jvmargs="-Xmx2g -verbose:gc"

# Set test timeout
./gradlew :cloud-governance:test -Dtest.timeout=600
```

### System Properties

```bash
# Force headless mode
./gradlew :cloud-governance:test -Djava.awt.headless=true

# Set specific region for AWS tests
./gradlew :cloud-governance:integrationTest -Daws.region=eu-west-1

# Enable debug logging
./gradlew :cloud-governance:test -Dorg.slf4j.simpleLogger.defaultLogLevel=debug
```

## Test Reports

### Report Locations

Each test category generates separate HTML reports:

| Test Type | Report Location |
|-----------|-----------------|
| Default/Unit | `build/reports/tests/test/index.html` |
| Unit Tests | `build/reports/tests/unit/index.html` |
| Integration Tests | `build/reports/tests/integration/index.html` |
| Performance Tests | `build/reports/tests/performance/index.html` |
| All Tests | `build/reports/tests/all/index.html` |

### JUnit XML Reports

JUnit XML reports for CI/CD integration:

| Test Type | XML Location |
|-----------|--------------|
| Default/Unit | `build/test-results/test/` |
| Unit Tests | `build/test-results/unit/` |
| Integration Tests | `build/test-results/integration/` |
| Performance Tests | `build/test-results/performance/` |
| All Tests | `build/test-results/all/` |

### Opening Reports

```bash
# Open unit test report
open build/reports/tests/unit/index.html

# Open integration test report  
open build/reports/tests/integration/index.html

# Open performance test report
open build/reports/tests/performance/index.html
```

## Troubleshooting

### Common Issues

#### 1. Integration Tests Skipped
```
Symptom: Integration tests show "0 completed" or WARNING status
Cause: Missing or incomplete credentials
Solution: Verify local-test.properties file exists and has valid credentials
```

#### 2. Authentication Failures
```
Symptom: AADSTS70011, "Invalid client credentials", "Security token invalid"
Azure: Check tenantId, clientId, clientSecret, and app permissions
AWS: Verify accessKeyId, secretAccessKey, and IAM permissions  
GCP: Confirm service account key file path and roles
```

#### 3. Permission Denied Errors
```
Symptom: 403 Forbidden, "Access denied"
Solution: Review required permissions in Credential Configuration section
Ensure service accounts/users have appropriate roles assigned
```

#### 4. Network/Timeout Issues
```
Symptom: Connection timeouts, network errors
Solution: Check network connectivity to cloud providers
Consider running tests behind corporate proxy or firewall
```

#### 5. Memory Issues in Performance Tests
```
Symptom: OutOfMemoryError during performance tests
Solution: Increase heap size: -Dorg.gradle.jvmargs="-Xmx4g"
```

### Debug Commands

```bash
# Check credential detection logic
./gradlew :cloud-governance:test --dry-run --info | grep -i credential

# Verbose test output
./gradlew :cloud-governance:test --info --debug

# Test specific authentication
./gradlew :cloud-governance:test --tests "*Azure*" --info

# Check classpath issues
./gradlew :cloud-governance:dependencies --configuration testRuntimeClasspath
```

### Log Analysis

Enable detailed logging:
```bash
# Create log4j2-test.xml in src/test/resources
echo '<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">
  <Appenders>
    <Console name="Console" target="SYSTEM_OUT">
      <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
    </Console>
  </Appenders>
  <Loggers>
    <Logger name="org.apache.calcite.adapter.governance" level="DEBUG"/>
    <Root level="INFO">
      <AppenderRef ref="Console"/>
    </Root>
  </Loggers>
</Configuration>' > src/test/resources/log4j2-test.xml
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Test Cloud Governance Adapter

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          java-version: '17'
          distribution: 'temurin'
      
      # Unit tests only (no credentials)
      - name: Run Unit Tests
        run: ./gradlew :cloud-governance:unitTest
      
      - name: Upload Unit Test Reports
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: unit-test-reports
          path: cloud-governance/build/reports/tests/unit/

  integration-tests:
    runs-on: ubuntu-latest
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          java-version: '17'
          distribution: 'temurin'
      
      # Integration tests with secrets
      - name: Create Test Credentials
        run: |
          cat > cloud-governance/src/test/resources/local-test.properties << EOF
          azure.tenantId=${{ secrets.AZURE_TENANT_ID }}
          azure.clientId=${{ secrets.AZURE_CLIENT_ID }}
          azure.clientSecret=${{ secrets.AZURE_CLIENT_SECRET }}
          azure.subscriptionIds=${{ secrets.AZURE_SUBSCRIPTION_IDS }}
          EOF
      
      - name: Run Integration Tests
        run: ./gradlew :cloud-governance:integrationTest
      
      - name: Upload Integration Test Reports
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: integration-test-reports
          path: cloud-governance/build/reports/tests/integration/
```

### Jenkins Pipeline Example

```groovy
pipeline {
    agent any
    
    stages {
        stage('Unit Tests') {
            steps {
                sh './gradlew :cloud-governance:unitTest'
            }
            post {
                always {
                    publishHTML([
                        allowMissing: false,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: 'cloud-governance/build/reports/tests/unit',
                        reportFiles: 'index.html',
                        reportName: 'Unit Test Report'
                    ])
                }
            }
        }
        
        stage('Integration Tests') {
            when {
                branch 'main'
            }
            steps {
                withCredentials([
                    string(credentialsId: 'azure-tenant-id', variable: 'AZURE_TENANT_ID'),
                    string(credentialsId: 'azure-client-id', variable: 'AZURE_CLIENT_ID'),
                    string(credentialsId: 'azure-client-secret', variable: 'AZURE_CLIENT_SECRET')
                ]) {
                    sh '''
                        cat > cloud-governance/src/test/resources/local-test.properties << EOF
                        azure.tenantId=${AZURE_TENANT_ID}
                        azure.clientId=${AZURE_CLIENT_ID}
                        azure.clientSecret=${AZURE_CLIENT_SECRET}
                        azure.subscriptionIds=${AZURE_SUBSCRIPTION_IDS}
                        EOF
                        
                        ./gradlew :cloud-governance:integrationTest
                    '''
                }
            }
        }
    }
}
```

### Docker Testing

```dockerfile
# Dockerfile.test
FROM eclipse-temurin:17-jdk

WORKDIR /app
COPY . .

# Unit tests only by default
RUN ./gradlew :cloud-governance:unitTest

# Optional: Run with mounted credentials
# docker run -v $(pwd)/credentials:/app/credentials test-image ./gradlew :cloud-governance:integrationTest
```

## Performance Benchmarking

### Running Performance Tests

```bash
# Basic performance tests
./gradlew :cloud-governance:performanceTest

# Performance tests with detailed output
./gradlew :cloud-governance:performanceTest --info

# Performance tests with memory profiling
./gradlew :cloud-governance:performanceTest -Dorg.gradle.jvmargs="-Xmx4g -XX:+PrintGCDetails"
```

### Expected Performance Metrics

| Test | Expected Time | Memory Usage |
|------|---------------|--------------|
| Schema Creation (1000x) | < 1 second | < 100 MB |
| Row Type Creation (10000x) | < 2 seconds | < 200 MB |
| Configuration Creation (100000x) | < 3 seconds | < 50 MB |

### Performance Regression Detection

Add performance assertions to catch regressions:

```java
@Test
public void testPerformanceRegression() {
    long startTime = System.currentTimeMillis();
    
    // Test operation
    for (int i = 0; i < 1000; i++) {
        createSchema();
    }
    
    long duration = System.currentTimeMillis() - startTime;
    
    // Fail if performance degrades significantly
    assertThat("Performance regression detected", duration < 1000, is(true));
}
```

This comprehensive testing guide should help developers and CI/CD systems run tests effectively across all scenarios and environments.