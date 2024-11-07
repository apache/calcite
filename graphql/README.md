# Apache Calcite Hasura GraphQL Adapter

This adapter enables Apache Calcite to query Hasura GraphQL endpoints using SQL. It provides a bridge between SQL and Hasura's GraphQL API by translating SQL queries into Hasura's GraphQL query format.

## Quick Links
- [Features and SQL Support](docs/features.md)
- [Configuration Guide](docs/configuration.md)
- [Query Examples](docs/query-examples.md)
- [Caching System](docs/caching.md)
- [SQL Processing](docs/sql-processing.md)
- [Type System](docs/type-system.md)
- [Implementation Details](docs/implementation.md)
- [Limitations](docs/limitations.md)
- [Requirements](docs/requirements.md)
- [Contributing](docs/contributing.md)

## Overview

The Apache Calcite Hasura GraphQL Adapter is a SQL:2003 compliant system that enables SQL queries against Hasura GraphQL endpoints. It supports a wide range of SQL features including window functions, common table expressions, and set operations, while providing efficient caching mechanisms and comprehensive query optimization.

### Key Features Highlights
- SQL:2003 compliance with extensive feature support
- Comprehensive query capabilities including window functions
- Flexible caching system (in-memory and Redis)
- Advanced query optimization
- Robust type system
- Security-focused design

See [Features and SQL Support](docs/features.md) for detailed information.

### Quick Start

1. Add the adapter to your project:
```xml
<dependency>
    <groupId>org.apache.calcite</groupId>
    <artifactId>calcite-hasura</artifactId>
    <version>${calcite.version}</version>
</dependency>
```

2. Configure your model:
```json
{
  "version": "1.0",
  "defaultSchema": "hasura",
  "schemas": [
    {
      "name": "hasura",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.graphql.GraphQLSchemaFactory",
      "operand": {
        "endpoint": "https://your-hasura-instance.hasura.app/v1/graphql"
      }
    }
  ]
}
```

3. See [Configuration Guide](docs/configuration.md) for detailed setup instructions.

## License

Licensed under the Apache License 2.0 - see the LICENSE file for details.

## Support and Contact

- [File an issue](https://github.com/apache/calcite/issues)
- [Contributing Guide](docs/contributing.md)
- [Join the community](https://calcite.apache.org/community/)
