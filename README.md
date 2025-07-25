# Hasura Fork of Apache Calcite

This is a fork of Apache Calcite customized for integration with Hasura DDN (Data Delivery Network). This fork includes significant enhancements and new adapters to support a wider range of data sources and improved functionality.

## Key Differences from Upstream Apache Calcite

### New Adapters Added
- **Arrow Adapter**: Support for Apache Arrow format files and Parquet files
  - Direct Arrow file reading with memory-efficient streaming
  - Parquet file support with schema inference
  - Integration with Arrow's columnar format for better performance

- **OpenAPI Adapter**: Query REST APIs through SQL
  - Configure multiple OpenAPI endpoints as SQL tables
  - Support for parameterized API calls
  - Response transformation and type mapping

- **GraphQL Adapter**: Native Hasura GraphQL endpoint support
  - SQL:2003 compliant query translation to GraphQL
  - Support for window functions, CTEs, and set operations
  - Built-in caching system (in-memory and Redis)
  - Comprehensive type system mapping

### Enhanced Existing Adapters

#### File Adapter Enhancements
- **Entity Caching**: Redis-based caching for improved performance
- **New File Formats**:
  - XLSX (Excel) files with automatic sheet detection
  - YAML files as data sources
  - HML (Hasura Metadata Language) files
- **S3 Protocol Support**: Direct querying of files in S3 buckets
- **Recursive File Search**: Automatic discovery of files in nested directories
- **Improved JSON/CSV handling**: Better type inference and streaming

#### Splunk Adapter Improvements
- HTTP timeout configuration and handling
- Token expiration retry logic
- Support for Common Information Models (CIM)
- Enhanced field comparison operations
- Nullable field handling improvements

#### Additional Database Support
- **SQLite**: Native SQLite database connectivity
- **Redshift**: Amazon Redshift specific optimizations
- **Cassandra**: Enhanced Cassandra adapter features

### Technical Enhancements
- **Type System Enhancements**: Stronger type mapping between SQL and native data sources
- **Model-Driven Configuration**: JSON-based adapter configuration system
- **Multi-level Caching**: Query result and schema metadata caching
- **Performance Improvements**:
  - Connection pooling enhancements
  - Better error handling and retry logic
  - Streaming support for large datasets
  - Memory-efficient processing for file adapters

## Upstream Merge Status
This fork was last merged with upstream Apache Calcite on commit b5fb7233, maintaining compatibility while preserving all custom enhancements.
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.calcite/calcite-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.calcite/calcite-core)
[![CI Status](https://github.com/apache/calcite/workflows/CI/badge.svg?branch=main)](https://github.com/apache/calcite/actions?query=branch%3Amain)

# Apache Calcite

Apache Calcite is a dynamic data management framework.

It contains many of the pieces that comprise a typical
database management system but omits the storage primitives.
It provides an industry standard SQL parser and validator,
a customisable optimizer with pluggable rules and cost functions,
logical and physical algebraic operators, various transformation
algorithms from SQL to algebra (and the opposite), and many
adapters for executing SQL queries over Cassandra, Druid,
Elasticsearch, MongoDB, Kafka, and others, with minimal
configuration.

For more details, see the [home page](http://calcite.apache.org).

The project uses [JIRA](https://issues.apache.org/jira/browse/CALCITE)
for issue tracking. For further information, please see the [JIRA accounts guide](https://calcite.apache.org/develop/#jira-accounts).
