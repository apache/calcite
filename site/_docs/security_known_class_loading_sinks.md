---
layout: docs
title: Known class-loading sinks
permalink: /docs/security_known_class_loading_sinks.html
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

This is a **non-normative, living inventory**. The normative rule lives in the
[threat model]({{ site.baseurl }}/docs/security_threat_model.html): a reachable
sink that loads an attacker-named class is a vulnerability on its own, and the
fix loads with `initialize=false`, gates on the expected interface (or an
allowlist), and only then instantiates.

This page tracks the concrete sinks that rule currently applies to. It changes
as sinks are found and fixed; a sink dropping off this list never narrows the
rule. Add a `Last reviewed` date whenever you touch an entry.

**Last reviewed:** June 2026

* TOC
{:toc}

## Open sinks

* `AvaticaUtils.instantiatePlugin`: `Class.forName(name)` initializes
  the class before the `isAssignableFrom` check (standard form) or the
  `cast` (`#FIELD` form). The type check is too late to stop the static
  initializer. It lives in the
  [calcite-avatica](https://github.com/apache/calcite-avatica) repository,
  so the root fix needs cross-repo coordination.
* `RelJson.typeNameToClass`: raw `Class.forName(type)` with no
  interface gate; returns the `Class`.
* `JdbcSchema.create`: the `dataSource` and `sqlDialectFactory`
  operands both reach `instantiatePlugin`.

## Related controls

* `ClassNameFilter`
  ([CALCITE-7532](https://issues.apache.org/jira/browse/CALCITE-7532))
  is a denylist. Necessary defense-in-depth, not sufficient: it catches
  known gadget names but leaves the primitive open. The control that closes
  the primitive is load-without-init plus an interface gate or allowlist,
  applied at the single sink so that DDL, RelJson, and operand paths all
  benefit.

## Paths to audit

* Object-reconstruction paths that read attacker-controlled input
  (`RelJson` today) fall under the same surprising-class-loading
  rule. Audit for other such paths and gate them the same way.
