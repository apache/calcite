---
layout: docs
title: Avatica
permalink: /docs/avatica.html
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

Avatica is a framework for building JDBC and ODBC drivers for databases,
and an RPC wire protocol.

![Avatica Architecture](https://raw.githubusercontent.com/julianhyde/share/master/slides/avatica-architecture.png)

Avatica's Java binding has very few dependencies.
Even though it is part of Apache Calcite it does not depend on other parts of
Calcite. It depends only on JDK 1.7+ and Jackson.

Avatica's wire protocol is JSON over HTTP.
The Java implementation uses Jackson to convert request/response command
objects to/from JSON.

Avatica-Server is a Java implementation of Avatica RPC.
It embeds the Jetty HTTP server.

Core concepts:

* Meta is a local API sufficient to implement any Avatica provider
* Factory creates implementations of the JDBC classes (Driver, Connection,
  Statement, ResultSet) on top of a Meta
* Service is an interface that implements the functions of Meta in terms
  of request and response command objects

## JDBC

Avatica implements JDBC by means of Factory.
Factory creates implementations of the JDBC classes (Driver, Connection,
Statement, PreparedStatement, ResultSet) on top of a Meta.

## ODBC

Work has not started on Avatica ODBC.

Avatica ODBC would use the same wire protocol and could use the same server
implementation in Java. The ODBC client would be written in C or C++.

Since the Avatica protocol abstracts many of the differences between providers,
the same ODBC client could be used for different databases.

## Project structure

We know that it is important that client libraries have minimal dependencies.

Avatica is currently part of Apache Calcite.
It does not depend upon any other part of Calcite.
At some point Avatica could become a separate project.

Packages:

* [org.apache.calcite.avatica](/apidocs/org/apache/calcite/avatica/package-summary.html) Core framework
* [org.apache.calcite.avatica.remote](/apidocs/org/apache/calcite/avatica/remote/package-summary.html) JDBC driver that uses remote procedure calls
* [org.apache.calcite.avatica.server](/apidocs/org/apache/calcite/avatica/server/package-summary.html) HTTP server
* [org.apache.calcite.avatica.util](/apidocs/org/apache/calcite/avatica/util/package-summary.html) Utilities

## Status

### Implemented

* Create connection, create statement, metadata, prepare, bind, execute, fetch
* RPC using JSON over HTTP
* Local implementation
* Implementation over an existing JDBC driver
* Composite RPCs (combining several requests into one round trip)
  * Execute-Fetch
  * Metadata-Fetch (metadata calls such as getTables return all rows)

### Not implemented

* ODBC
* RPCs
  * CloseStatement
  * CloseConnection
* Composite RPCs
  * CreateStatement-Prepare
  * CloseStatement-CloseConnection
  * Prepare-Execute-Fetch (Statement.executeQuery should fetch first N rows)
* Remove statements from statement table
* DML (INSERT, UPDATE, DELETE)
* Statement.execute applied to SELECT statement
