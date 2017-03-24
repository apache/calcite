---
layout: docs
title: Materialized Views
permalink: /docs/materialized_views.html
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

There are several different ways to exploit materialized views in Calcite.

* TOC
{:toc}

## Materialized views maintained by Calcite

For details, see the [lattices documentation]({{ site.baseurl }}/docs/lattice.html).

## Expose materialized views from adapters

Some adapters have their own notion of materialized views.
For example, Apache Cassandra allows the user to define materialized views based on existing tables which are automatically maintained.
The Cassandra adapter automatically exposes these materialized views to Calcite.
By understanding some tables as materialized views, Calcite has the opportunity to automatically rewrite queries to use these views.

## View-based query rewriting

View-based query rewriting aims to take an input query which can be answered using a preexisting view and rewrite the query to make use of the view.
Calcite employs two forms of view-based query rewriting.
The first is based on view substitution before the planning phase based on an extension of a {SubstitutionVisitor}.
{MaterializedViewSubstitutionVisitor} aims to substitute part of the relational algebra tree with an equivalent expression which makes use of a materialized view.

The following example is taken from the documentation of {SubstitutionVisitor}:

 * Query: `SELECT a, c FROM t WHERE x = 5 AND b = 4`
 * Target (materialized view definition): `SELECT a, b, c FROM t WHERE x = 5`
 * Replacement: `SELECT * FROM mv`
 * Result: `SELECT a, c FROM mv WHERE b = 4`

Note that {result} uses the materialized view table {mv} and a simplified condition {b = 4}.
This can accomplish a large number of rewritings, but only those based on star schemas.
This type of rewriting cannot be used for more complex views.
{MaterializedViewJoinRule} attempts to match queries to views defined using arbitrary queries.
The logic of the rule is based on [this paper](http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.95.113).

There are several limitations to the current implementation:

* The query defining the view must use only inner joins
* Only equality predicates are supported
* Predicates on tables used in the view must exactly match predicates in the query
* Rewriting is unoptimized and will attempt to match all views against each query

These limitations are not fundamental the approach however and will hopefully be removed in the future.
Note that the rule is currently disabled by default.
To make use of the rule, {MaterializedViewJoinRule.INSTANCE_PROJECT} and {MaterializedViewJoinRule.INSTANCE_TABLE_SCAN} need to be added to the planner.
