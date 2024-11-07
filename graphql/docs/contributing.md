# Contributing Guide

## Table of Contents
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Documentation](#documentation)
- [Release Process](#release-process)
- [Community Guidelines](#community-guidelines)

## Getting Started

### Prerequisites
```bash
# Required software
Java 11 or later
Maven 3.6+
Git 2.20+
Docker (for integration tests)
Redis (optional, for cache testing)
```

### Initial Setup
```bash
# Clone the repository
git clone https://github.com/apache/calcite
cd calcite/adapter/graphql

# Build the project
mvn clean install

# Run tests
mvn test
```

## Development Setup

### IDE Configuration

#### IntelliJ IDEA
```xml
<!-- Recommended plugins -->
<plugins>
    <plugin>Checkstyle-IDEA</plugin>
    <plugin>SonarLint</plugin>
    <plugin>SpotBugs</plugin>
    <plugin>Save Actions</plugin>
</plugins>

<!-- Code style settings -->
<code-style>
    <java>
        <indent>4 spaces</indent>
        <continuation-indent>8 spaces</continuation-indent>
        <max-line-length>100</max-line-length>
    </java>
</code-style>
```

#### Eclipse
```properties
# Recommended configuration
org.eclipse.jdt.core.formatter.lineSplit=100
org.eclipse.jdt.core.formatter.tabulation.char=space
org.eclipse.jdt.core.formatter.tabulation.size=4
```

### Build Configuration
```xml
<profiles>
    <!-- Development profile -->
    <profile>
        <id>dev</id>
        <properties>
            <skipTests>false</skipTests>
            <checkstyle.skip>false</checkstyle.skip>
            <spotbugs.skip>false</spotbugs.skip>
        </properties>
    </profile>
    
    <!-- Quick build profile -->
    <profile>
        <id>quick</id>
        <properties>
            <skipTests>true</skipTests>
            <checkstyle.skip>true</checkstyle.skip>
            <spotbugs.skip>true</spotbugs.skip>
        </properties>
    </profile>
</profiles>
```

## Coding Standards

### Java Code Style

#### General Rules
```java
// Class structure
public class MyClass {
    // Constants first
    private static final Logger LOGGER = LogManager.getLogger(MyClass.class);
    
    // Fields next
    private final String field;
    
    // Constructor(s)
    public MyClass(String field) {
        this.field = Objects.requireNonNull(field, "field must not be null");
    }
    
    // Public methods
    public void doSomething() {
        // Implementation
    }
    
    // Private methods last
    private void helperMethod() {
        // Implementation
    }
}
```

#### Naming Conventions
```java
// Classes are PascalCase
public class DatabaseConnection {}

// Methods and variables are camelCase
public void connectToDatabase() {}
private String userName;

// Constants are UPPER_CASE
public static final int MAX_CONNECTIONS = 100;

// Interfaces should be adjectives when possible
public interface Closeable {}
public interface Runnable {}
```

### Documentation Standards

#### JavaDoc
```java
/**
 * Executes a GraphQL query with caching support.
 *
 * @param query The GraphQL query to execute. Must not be null.
 * @param variables Optional variables for the query. May be null.
 * @return The query result, never null.
 * @throws GraphQLException if the query execution fails
 * @throws IllegalArgumentException if query is null
 *
 * @since 1.0
 */
public ExecutionResult executeQuery(String query, Map<String, Object> variables) {
    // Implementation
}
```

#### Method Comments
```java
// Single-line comment for simple methods
private void clearCache() { /* ... */ }

/*
 * Multi-line comments for complex logic:
 * 1. First check the local cache
 * 2. If not found, check Redis
 * 3. If still not found, execute query
 */
private Object fetchWithCaching() { /* ... */ }
```

## Testing Guidelines

### Test Structure
```java
public class MyClassTest {
    // Test setup
    @BeforeEach
    void setUp() {
        // Setup code
    }
    
    // Group related tests
    @Nested
    class WhenDoingSomething {
        @Test
        void shouldSucceedWithValidInput() {
            // Test implementation
        }
        
        @Test
        void shouldFailWithInvalidInput() {
            // Test implementation
        }
    }
}
```

### Test Categories
```java
// Unit tests
@Tag("unit")
class MyUnitTest {}

// Integration tests
@Tag("integration")
class MyIntegrationTest {}

// Performance tests
@Tag("performance")
class MyPerformanceTest {}
```

### Test Coverage Requirements
```xml
<plugin>
    <groupId>org.jacoco</groupId>
    <artifactId>jacoco-maven-plugin</artifactId>
    <configuration>
        <rules>
            <rule>
                <element>CLASS</element>
                <limits>
                    <limit>
                        <counter>LINE</counter>
                        <value>COVEREDRATIO</value>
                        <minimum>0.80</minimum>
                    </limit>
                </limits>
            </rule>
        </rules>
    </configuration>
</plugin>
```

## Pull Request Process

### PR Guidelines
1. Create feature branch
```bash
git checkout -b feature/my-feature
```

2. Make changes and commit
```bash
git add .
git commit -m "feat: add new feature"
```

3. Update documentation
```bash
# Update relevant docs
docs/features.md
docs/implementation.md
CHANGELOG.md
```

4. Run tests
```bash
mvn clean verify
```

5. Submit PR
```bash
git push origin feature/my-feature
# Create PR on GitHub
```

### PR Template
```markdown
## Description
Brief description of changes

## Type of change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Checklist
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] Change log updated
- [ ] Code style followed
```

## Documentation

### Documentation Structure
```
docs/
├── features.md
├── configuration.md
├── query-examples.md
├── caching.md
├── sql-processing.md
├── type-system.md
├── implementation.md
├── limitations.md
├── requirements.md
└── contributing.md
```

### Documentation Guidelines
- Use clear, concise language
- Include code examples
- Keep formatting consistent
- Update all relevant docs
- Include diagrams when helpful
- Cross-reference related sections

## Release Process

### Version Numbering
```
MAJOR.MINOR.PATCH
Example: 1.2.3

MAJOR - Breaking changes
MINOR - New features
PATCH - Bug fixes
```

### Release Steps
1. Prepare release
```bash
mvn release:prepare -DautoVersionSubmodules=true
```

2. Perform release
```bash
mvn release:perform
```

3. Update documentation
```bash
# Update version numbers
pom.xml
README.md
CHANGELOG.md
```

4. Create release notes
```markdown
# Release 1.2.3

## New Features
- Feature 1
- Feature 2

## Bug Fixes
- Fix 1
- Fix 2

## Breaking Changes
- Change 1
- Change 2
```

## Community Guidelines

### Communication Channels
- GitHub Issues
- Dev mailing list
- Slack channel
- Monthly video calls

### Issue Reporting
```markdown
## Issue Template

### Description
[Clear description of the issue]

### Steps to Reproduce
1. Step 1
2. Step 2
3. Step 3

### Expected Behavior
[What you expected to happen]

### Actual Behavior
[What actually happened]

### Environment
- Java version:
- OS:
- Version:
```

### Code Review Process
1. Submit PR
2. Wait for CI checks
3. Address review comments
4. Get approvals (2 required)
5. Merge when ready

### Community Standards
- Be respectful and professional
- Follow code of conduct
- Help others learn
- Document decisions
- Share knowledge

See also:
- [Requirements](requirements.md)
- [Implementation Details](implementation.md)
- [Apache Calcite Website](https://calcite.apache.org)
