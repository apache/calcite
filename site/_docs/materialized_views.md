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

Some adapters and projects that rely on Calcite have their own notion of materialized views.
For example, Apache Cassandra allows the user to define materialized views based on existing tables which are automatically maintained.
The Cassandra adapter automatically exposes these materialized views to Calcite.
Another example is Apache Hive, whose integration with Calcite materialized views is ongoing.
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
This can accomplish a large number of rewritings.
However, this approach is not scalable in the presence of complex views, e.g., views containing many join operators, 
since it relies on the planner rules to create the equivalence between expressions.

In turn, an alternative rule that attempts to match queries to views defined using arbitrary queries 
has been proposed.

{AbstractMaterializedViewRule} builds on the ideas presented [here](http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.95.113).
The rule can rewrite expressions containing arbitrary chains of Join, Filter, and Project operators.
Additionally, the rule can rewrite expressions rooted at an Aggregate operator, rolling aggregations up if necessary.

However, this rule still presents some limitations. In particular, the rule presents the following
shortcomings that we plan to address with follow-up extensions:

* Rewriting is unoptimized and will attempt to match all views against each query.
* The query defining the view must use only inner joins.
* It does not produce rewritings using Union operators, e.g., a given query could be partially answered from the
{mv} (year = 2014) and from the query (not(year=2014)). This can be useful if {mv} is stored in a system such as
Druid.

This rule is currently enabled by default.
