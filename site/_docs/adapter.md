---
layout: docs
title: Adapters
permalink: /docs/adapter.html
---
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

* Cassandra adapter (<a href="{{ site.apiRoot }}/org/apache/calcite/adapter/cassandra/package-summary.html">calcite-cassandra</a>)
* CSV adapter (<a href="{{ site.apiRoot }}/org/apache/calcite/adapter/csv/package-summary.html">example/csv</a>)
* JDBC adapter (part of <a href="{{ site.apiRoot }}/org/apache/calcite/adapter/jdbc/package-summary.html">calcite-core</a>)
* MongoDB adapter (<a href="{{ site.apiRoot }}/org/apache/calcite/adapter/mongodb/package-summary.html">calcite-mongodb</a>)
* Spark adapter (<a href="{{ site.apiRoot }}/org/apache/calcite/adapter/spark/package-summary.html">calcite-spark</a>)
* Splunk adapter (<a href="{{ site.apiRoot }}/org/apache/calcite/adapter/splunk/package-summary.html">calcite-splunk</a>)
* Eclipse Memory Analyzer (MAT) adapter (<a href="https://github.com/vlsi/mat-calcite-plugin">mat-calcite-plugin</a>)

## Engines

The following standalone engines are powered by Apache Calcite.

* <a href="https://drill.apache.org">Apache Drill</a>
  uses Calcite for SQL parsing and query optimization
* <a href="https://flink.apache.org">Apache Flink</a>
  uses Calcite for parsing both regular and streaming SQL,
  and for query optimization
* <a href="https://hive.apache.org">Apache Hive</a>
  uses Calcite for query optimization
* <a href="https://kylin.apache.org">Apache Kylin</a>
  uses Calcite for SQL parsing and query optimization
* <a href="https://phoenix.apache.org">Apache Phoenix</a>
  uses Calcite for SQL parsing and query optimization (under development),
  and also uses Avatica for its remote JDBC driver
* <a href="https://github.com/milinda/samza-sql">SamzaSQL</a>,
  an extension to
  <a href="https://samza.apache.org">Apache Samza</a>,
  uses Calcite for parsing streaming SQL and query optimization
* <a href="https://storm.apache.org">Apache Storm</a>
  uses Calcite for parsing streaming SQL and query optimization
* Cascading adapter (<a href="https://github.com/Cascading/lingual">Lingual</a>)
* <a href="https://github.com/twilmes/sql-gremlin">SQL-Gremlin</a>,
  a SQL interface to a
  <a href="http://tinkerpop.incubator.apache.org/">Apache TinkerPop</a>-enabled
  graph database

## Drivers

* <a href="{{ site.apiRoot }}/org/apache/calcite/jdbc/package-summary.html">JDBC driver</a>

The basic form of the JDBC connect string is

  jdbc:calcite:property=value;property2=value2

where `property`, `property2` are properties as described below.
(Connect strings are compliant with OLE DB Connect String syntax,
as implemented by Avatica's
<a href="{{ site.apiRoot }}/org/apache/calcite/avatica/ConnectStringParser.html">ConnectStringParser</a>.)

JDBC connect string parameters

| Property | Description |
|:-------- |:------------|
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#CASE_SENSITIVE">caseSensitive</a> | Whether identifiers are matched case-sensitively. If not specified, value from `lex` is used.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#CREATE_MATERIALIZATIONS">createMaterializations</a> | Whether Calcite should create materializations. Default false.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#DEFAULT_NULL_COLLATION">materializationsEnabled</a> | How NULL values should be sorted if neither NULLS FIRST nor NULLS LAST are specified in a query. The default, HIGH, sorts NULL values the same as Oracle.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#FORCE_DECORRELATE">forceDecorrelate</a> | Whether the planner should try de-correlating as much as possible. Default true.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#FUN">fun</a> | Collection of built-in functions and operators. Valid values: "standard" (the default), "oracle".
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#LEX">lex</a> | Lexical policy. Values are ORACLE (default), MYSQL, MYSQL_ANSI, SQL_SERVER, JAVA.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#MATERIALIZATIONS_ENABLED">materializationsEnabled</a> | Whether Calcite should use materializations. Default false.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#MODEL">model</a> | URI of the JSON model file.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#QUOTING">quoting</a> | How identifiers are quoted. Values are DOUBLE_QUOTE, BACK_QUOTE, BRACKET. If not specified, value from `lex` is used.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#SCHEMA">schema</a> | Name of initial schema.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#SPARK">spark</a> | Specifies whether Spark should be used as the engine for processing that cannot be pushed to the source system. If false (the default), Calcite generates code that implements the Enumerable interface.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#TIME_ZONE">timeZone</a> | Time zone, for example "gmt-3". Default is the JVM's time zone.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#TYPE_SYSTEM">typeSystem</a> | Type system. The name of a class that implements <a href="{{ site.apiRoot }}/org/apache/calcite/rel/type/RelDataTypeSystem.html">RelDataTypeSystem</a> and has a public default constructor or an `INSTANCE` constant.
| <a href="{{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#UNQUOTED_CASING">unquotedCasing</a> | How identifiers are stored if they are not quoted. Values are UNCHANGED, TO_UPPER, TO_LOWER. If not specified, value from `lex` is used.
