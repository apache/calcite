---
layout: docs
title: Algebra
permalink: /docs/algebra.html
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

Relational algebra is at the heart of Calcite. Every query is
represented as a tree of relational operators. You can translate from
SQL to relational algebra, or you can build the tree directly.

Planner rules transform expression trees using mathematical identities
that preserve semantics. For example, it is valid to push a filter
into an input of an inner join if the filter does not reference
columns from the other input.

Calcite optimizes queries by repeatedly applying planner rules to a
relational expression. A cost model guides the process, and the
planner engine generates an alternative expression that has the same
semantics as the original but a lower cost.

The planning process is extensible. You can add your own relational
operators, planner rules, cost model, and statistics.
