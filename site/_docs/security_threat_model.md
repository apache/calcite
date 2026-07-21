---
layout: docs
title: Security threat model
permalink: /docs/security_threat_model.html
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

Calcite is an embedded library: it runs inside a host application's JVM and
exposes no network port of its own. This threat model covers what an attacker
who reaches that embedded engine over a JDBC connection can and cannot do.

Calcite treats the behaviors below as security vulnerabilities, so that
reporters and committers triage them the same way. A report that
contradicts this model is a feature request or a documentation gap, not a
vulnerability.

**Status:** draft for PMC discussion; not yet ratified.

* TOC
{:toc}

## Attacker and trust boundary

One attacker profile: a *query author* who reaches Calcite over a JDBC
connection.

The attacker can:

* set any connection property to any value: `model`, `parserFactory`,
  `schemaFactory`, `fun`, `typeSystem`, `dataSource`, `jdbcUrl`, and the
  rest;
* execute any SQL, including DDL.

The attacker cannot:

* change JVM system properties;
* change the classpath (add or replace classes or JARs).

Out of scope by definition: the configuration and behavior of a
third-party driver or service that a [model]({{ site.baseurl }}/docs/model.html)
points at. If a model references h2, h2's own settings and behavior are
h2's concern, not Calcite's.

Because Calcite listens on no socket of its own, network transport, TLS, and
authentication belong to the host application, not to this model. A host that
lets an untrusted principal set connection properties has handed that principal
the attacker's full capability above.

## Assets

* the host running Calcite: no code execution, and no file access beyond
  what an adapter is configured to perform;
* the internal network reachable from that host: no attacker-directed
  outbound requests.

## Security properties

* **P1: no code execution.** Neither connecting nor running SQL may
  execute code outside Calcite's query-processing semantics. This covers
  `Runtime.exec` and `ProcessBuilder`, and the weaker primitive of loading
  an attacker-named class so that its static initializer, constructor, or
  an accessed static field runs. Exception: the os-adapter.
* **P2: no incidental file access.** Neither connecting nor running
  SQL may read or create a file, except where a file-oriented adapter or
  table function reads the local path it was explicitly configured with. The
  carve-out covers local filesystem paths only; a file adapter that fetches a
  URL (`http://`, `https://`) is making a network request and falls under P3.
* **P3: no server-side request forgery.** Neither connecting nor
  running SQL may open a network connection to an attacker-chosen host.
* **P4: no escape from the configured schemas.** Neither connecting nor
  running SQL may read data outside the schemas the connection exposes. A query
  that reaches another schema, a file, or a catalog that the connection's root
  schema does not make visible is a vulnerability.

## Always a vulnerability

1. **Code execution** that results from connecting or running SQL, except
   through the os-adapter. The bar is the primitive, not a full chain: a
   reachable sink that loads an attacker-named class qualifies, because
   class loading runs the static initializer before any type check.
2. **Arbitrary file read or write** that no explicitly-configured file
   adapter was asked to perform.
3. **Server-side request forgery**, forcing Calcite to connect to a
   host the attacker chooses (internal services, cloud metadata endpoints,
   port scans).
4. **Reading beyond the configured schemas**, reaching another schema,
   a file, or a catalog that the connection's root schema does not make
   visible.

## Not a vulnerability

* The os-adapter running OS commands. It exists to do that, and an operator
  must add it on purpose.
* A file, CSV, or JSON adapter reading the local path it was configured
  with. Opt-in, by the same reasoning as the os-adapter.
* Anything that needs a changed system property or classpath. Both are
  outside the attacker's reach by assumption.
* The behavior of a third-party driver once Calcite has connected to the
  endpoint it was configured with.
* Cross-tenant reads that follow from the embedder exposing more than one
  principal's schemas on a single connection. Calcite has no authentication or
  authorization; scoping each connection's root schema to what its principal may
  see is the embedder's job. P4 applies where the user submits only SQL; a user
  who also sets connection properties configures their own schema visibility.

## Surprising vs unsurprising class loading

Calcite loads a class named in a connection property or in SQL only to use it
through a specific SPI: a schema factory, table factory, function, operator,
data source, or driver. The security boundary follows that contract, not a
blanket trust of the classpath or of SQL.

* **Unsurprising.** The class implements the SPI interface for the position it
  was named in, and Calcite invokes it through that interface. This is working
  as designed, even when the class is otherwise dangerous. The operator who
  placed the class on the classpath, and the author of the class, own that
  contract.
* **Surprising.** Naming a class runs the class's own code (a static
  initializer, constructor, method, or static-field read) even though it
  does not implement the SPI for that position. `java.lang.Runtime`,
  `org.springframework.boot.SpringApplication`, and `javax.naming.InitialContext`
  are surprising in a `SchemaFactory` slot. Surprising class loading is always a
  vulnerability.

This boundary lets three goals hold at once:

* untrusted classes may sit on the classpath; a dangerous class that does
  not implement a Calcite SPI is never instantiated by name, so the operator
  need not audit every class;
* SQL may be arbitrary; it can name SPI classes, but only SPI
  implementations run, and only through their SPI;
* a reasonable-looking query cannot trigger an unexpected process launch, file
  read, or network call, because the interface gate rejects the classes that
  would cause one.

The same rule governs any path that reconstructs objects or loads classes from
attacker-controlled input, including JSON plan deserialization.

**Mechanism.** Load with `Class.forName(name, false, loader)`, check
`pluginClass.isAssignableFrom(clazz)`, and only then initialize and instantiate.
A class that fails the check never runs its static initializer.

**Standard-Java SPIs.** The gate is tightest when the SPI is a Calcite interface
(`SchemaFactory`, `TableFactory`, `Function`, `SqlOperator`): only a class
written to be a Calcite plugin passes. Two positions name a standard Java
interface instead, `dataSource` (`javax.sql.DataSource`) and `jdbcDriver`
(`java.sql.Driver`), which many unrelated libraries implement. The
interface gate still blocks the surprising case, since `java.lang.Runtime`
implements neither, so naming a `DataSource` or `Driver` implementation is the
documented feature, not a vulnerability. An allowlist of permitted
implementations is optional hardening for these positions, not a security
boundary. The host a driver then connects to is governed by P3, independently of
which class is named.

### Triage rule for class-loading sinks

A reachable sink that loads an attacker-named class is a vulnerability on
its own. A reporter need not demonstrate end-to-end remote code execution
on a specific classpath: the demonstrated primitive (a static
initializer, a constructor, or a static-field read on an attacker-named
class) is enough to require a fix. The fix loads with
`initialize=false`, gates on the expected interface (or an allowlist), and
then instantiates.

The concrete sinks known at a given point in time, and their fix status, live
in a separate living document:
[Known class-loading sinks]({{ site.baseurl }}/docs/security_known_class_loading_sinks.html).
That inventory changes as sinks are found and fixed; the rule above does not.

## Denial of service

A single query should not be able to exhaust the host. This is in scope as a
hardening goal. The controls are not all in place yet, so treat the gaps below
as known limitations rather than per-report vulnerabilities until the controls
land.

* **Planning.** A crafted query can drive the planner into a combinatorial
  blow-up. The fix is a set of bounds: a planning deadline, a cap on rule
  firings, and a cap on the number of explored alternatives. Calcite already
  carries a `CancelFlag` in the planner context, so a deadline can build on it;
  the firing and size caps are new.
* **Execution.** Catastrophic regex backtracking in `LIKE`, `SIMILAR TO`, or
  `RLIKE`, or an unbounded join, exhausts resources at run time. Planning bounds
  do not help here; the mitigation is a match-time limit or a backtracking-free
  regex engine.
* **Parsing.** Deeply nested expressions can overflow the parser stack. The
  mitigation is a nesting-depth limit.

Once the planning bounds exist, a single reasonably-sized query that exceeds
them is a configuration choice, not a vulnerability.
