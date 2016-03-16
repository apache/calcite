---
layout: news_item
date: "2016-03-22 13:00:00 +0000"
author: jhyde
categories: ["new features"]
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

A new Apache Calcite adapter allows you to access
[Apache Cassandra](http://cassandra.apache.org/) via industry-standard SQL.

You can map a Cassandra keyspace into Calcite as a schema, Cassandra
CQL tables as tables, and execute SQL queries on them, which Calcite
converts into [CQL](https://cassandra.apache.org/doc/cql/CQL.html).
Cassandra can define and maintain materialized views but the adapter
goes further: it can transparently rewrite a query to use a
materialized view even if the view is not mentioned in the query.

Read more about the adapter [here]({{ site.baseurl }}/docs/cassandra.html).

The Cassandra adapter is available as part of
[Apache Calcite version 1.7.0]({{ site.baseurl }}/docs/history.html#v1-7-0),
which has just been released. Calcite also has
[adapters]({{ site.baseurl }}/docs/adapter.html)
for CSV and JSON files, and JDBC data source, MongoDB, Spark and Splunk.
