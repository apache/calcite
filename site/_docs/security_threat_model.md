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

# Apache Calcite Threat Model

Calcite treats the behaviors below as security vulnerabilities, so that
reporters and committers triage them the same way. A report that
contradicts this model is a feature request or a documentation gap, not a
vulnerability.

**Status:** draft for PMC discussion; not yet ratified.

* TOC
{:toc}
## 1. Introduction & Scope
Apache Calcite is a dynamic data management framework used to parse SQL, optimize queries, and execute federated data processing across heterogeneous backends.

Because Calcite is designed exclusively as an **embedded library** rather than a standalone database server daemon, it runs directly inside the Java Virtual Machine (JVM) process space of a host application (e.g., Apache Hive, Apache Flink, Apache Storm). The primary goal of this threat model is to identify architecture lines where untrusted external input crosses into Calcite’s compiler, optimization planner, and bytecode generation subsystems.

### System Components in Scope
1. **SQL Parser & Validator:** Converts raw text strings into abstract syntax trees (`SqlNode`) and validates them against metadata.
2. **Relational Expression Engine (`RelNode`):** The relational algebra engine that transforms and optimizes query plans.
3. **Model & Schema Managers (JDBC/Linq4j Connectors):** Dynamically loads schemas and routes queries to backends via custom adapters.
4. **Code Generation Engine (Enumerable / Janino compiler):** Compiles relational expressions into executable Java bytecode at runtime to process data.

---

## 2. Data Flow & Trust Boundaries


```

[ Untrusted Client ] ──( Raw SQL / Custom Model JSON )──► [ Host Application ]

┌─────────────────── TRUST BOUNDARY ───────────────────────────┼─────────────────────┐
│ JVM Process Space                                            ▼                     │
│                                                    [ Calcite Core Engine ]         │
│                                                              │                     │
│                                                              ▼                     │
│                                                    [ Parser & Validator ]          │
│                                                              │                     │
│                                                              ▼                     │
│                                                   [ Optimizer & Planner ]          │
│                                                              │                     │
│                                                              ▼                     │
│                                                   [ Bytecode Generator ]           │
│                                                    (Janino Compilation)            │
│                                                              │                     │
│ ┌───────────────── TRUST BOUNDARY ───────────────────────────┼───────────────────┐ │
│ │ Remote Data Tier (OUT OF SCOPE FOR CALCITE)                ▼                   │ │
│ │                                                  [ Storage Adapters ]          │ │
│ │                                             (JDBC, Elasticsearch, Druid)       │ │
│ └────────────────────────────────────────────────────────────┬───────────────────┘ │
└──────────────────────────────────────────────────────────────┼─────────────────────┘
                                                               ▼
                                                   [ Third-Party Databases ]

```

### Critical Trust Boundaries
* **Boundary 1: Host Application to Calcite Core (Input Surface):** Untrusted string blocks (SQL queries, custom data models, or connection parameters) enter Calcite's parsing space.
* **Boundary 2: Code Generation to JVM Runtime (Execution Surface):** Calcite translates algebra plans into raw Java bytecode and compiles them into memory. If plan generation is manipulated, the JVM executes malicious logic.
* **Boundary 3: Storage Adapters to External Backends (Federation Surface):** Calcite pushes modified projections down to underlying databases.

### Attacker and trust boundary

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

### Assets

* the host running Calcite: no code execution, and no file access beyond
  what an adapter is configured to perform;
* the internal network reachable from that host: no attacker-directed
  outbound requests.

---

## 3. Out of Scope Items

To establish a strict perimeter for safety remediation, the following domains and behavior vectors are explicitly **out of scope** for Apache Calcite:

* **Vulnerabilities in Connected JDBC Drivers:** Security flaws, memory leaks, or execution bugs residing inside target backend drivers (e.g., PostgreSQL JDBC driver, MySQL JDBC driver) are entirely the responsibility of those standalone projects. Calcite assumes the driver boundary is secure.
* **Network-Layer Transportation and Encryption:** Because Calcite does not expose a network port or listen on a socket interface, configuring TLS/SSL, managing firewall perimeters, and thwarting MitM (Man-in-the-Middle) attacks are out of scope.
* **Authentication and Authorization Policies:** Calcite executes queries based on metadata flags passed to it by the host system. Defining user roles, managing column-level access lists (RBAC), and verifying passwords must be handled entirely by the host application layer before invoking Calcite.
* **Storage-Engine Sanitization (Underlying Database Exploits):** If an underlying database cluster is vulnerable to specialized execution syntax via raw terminal pass-throughs, patching the database engine itself is out of scope.

---

## 4. Security properties

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

### Always a vulnerability

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

### Not a vulnerability

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

---

## 5. Surprising vs unsurprising class loading

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

---

## 6. STRIDE Threat Analysis

| Threat Category | Threat Description & Attack Vector                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Impact | Existing/Target Mitigations                                                                                                                                                                                                                                                                                                                                                   |
| :--- |:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| :--- |:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Spoofing** | An attacker spoofs adapter configurations or authentication credentials routed through a custom Model JSON schema file.                                                                                                                                                                                                                                                                                                                                                                                                                              | Unauthorized access to backend data sources. | Ensure Calcite models are only loaded from trusted local paths, not from arbitrary client uploads. Use secure Credential Providers.                                                                                                                                                                                                                                           |
| **Tampering** | **SQL Injection via Pushdown Mechanics:** An attacker inputs carefully crafted strings that slip past Calcite's parser but evaluate as valid malicious syntax when passed down to a target backend database driver.                                                                                                                                                                                                                                                                                                                                  | Data modification or exfiltration on the underlying cluster. | Storage adapters must use parameterized `PreparedStatement` boundaries rather than text concatenation when pushing filter clauses down to remote data layers.                                                                                                                                                                                                                 |
| **Repudiation** | Malicious queries cause runtime crashes or execute side effects on storage tiers, but auditing logs fail to bind the logical plan to the original client connection context.                                                                                                                                                                                                                                                                                                                                                                         | Inability to perform post-incident forensics. | Host applications must intercept and map Calcite diagnostic logging tracks (`org.apache.calcite.runtime`) directly to the active session user ID.                                                                                                                                                                                                                             |
| **Information Disclosure** | **Exploitation of Unsafe Reflection (CWE-470):** External identifiers passed via models or SQL parameters coerce Calcite into reflectively instantiating or exposing unintended system classes or files.                                                                                                                                                                                                                                                                                                                                             | Disclosure of local file pathways, JVM environment variables, or metadata. | **Resolution Framework:** Implement strict allow-listing of class references during runtime compilation. Sandbox file paths accessible to CSV or file-based query adapters.                                                                                                                                                                                                   |
| **Denial of Service (DoS)** | **Planning:** An attacker submits an immensely complex nested query (e.g., thousands of joins or unions) designed to trigger geometric state expansions inside the Volcano Planner framework. **Execution:** Catastrophic regex backtracking in `LIKE`, `SIMILAR TO`, or `RLIKE`, or an unbounded join, exhausts resources at run time. Planning bounds do not help here. **Parsing:** Deeply nested expressions can overflow the parser stack.                                         | Exhaustion of heap memory or CPU loops, crashing the host application JVM. | Enforce rigid time constraints on the query optimization cycle via `RelOptPlanner.setCancelFlag`. Define a match-time limit or a backtracking-free. Define absolute query length and nesting depth maximum thresholds in the parser config. Once the planning bounds exist, a single reasonably-sized query that exceeds them is a configuration choice, not a vulnerability. |
| **Elevation of Privilege** | **Arbitrary Code Execution via Janino Compilation:** An attacker injects malicious snippets through user-defined functions (UDFs) or expression logic that gets evaluated during the Linq4j/Janino dynamic bytecode generation pass.                                                                                                                                                                                                                                                                                                                 | Persistent Remote Code Execution (RCE) inside the hosting application’s execution environment. | Sanitize, escape, and validate all custom expression blocks before feeding them into internal generation builders. Restrict the application classpath matrix to the absolute minimum privilege set required.                                                                                                                                                                  |

---

## 7. Review & Assessment Framework

This threat model must be updated whenever major architectural changes occur in Calcite, specifically regarding:

1. Upgrades or replacements to the code generation engine (Janino / Linq4j).
2. Changes to the reflective logic used by storage adapters.
3. The addition of new standard functions or UDF registration mechanics.
