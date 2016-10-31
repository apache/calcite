---
layout: docs
title: Background
permalink: /docs/index.html
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

Avatica's wire protocols are JSON or Protocol Buffers over HTTP. The
Java implementation of the JSON protocol uses
[Jackson](https://github.com/FasterXML/jackson) to convert
request/response command objects to/from JSON.

Avatica-Server is a Java implementation of Avatica RPC.

Core concepts:

* [Meta]({{ site.apiRoot }}/org/apache/calcite/avatica/Meta.html)
  is a local API sufficient to implement any Avatica provider
* [AvaticaFactory]({{ site.apiRoot }}/org/apache/calcite/avatica/AvaticaFactory.html)
  creates implementations of the JDBC classes on top of a `Meta`
* [Service]({{ site.apiRoot }}/org/apache/calcite/avatica/remote/Service.html)
  is an interface that implements the functions of `Meta` in terms
  of request and response command objects

## JDBC

Avatica implements JDBC by means of
[AvaticaFactory]({{ site.apiRoot }}/org/apache/calcite/avatica/AvaticaFactory.html).
An implementation of `AvaticaFactory` creates implementations of the
JDBC classes ([Driver]({{ site.jdkApiRoot }}/java/sql/Driver.html),
[Connection]({{ site.jdkApiRoot }}/java/sql/Connection.html),
[Statement]({{ site.jdkApiRoot }}/java/sql/Statement.html),
[ResultSet]({{ site.jdkApiRoot }}/java/sql/ResultSet.html))
on top of a `Meta`.

## ODBC

Work has not started on Avatica ODBC.

Avatica ODBC would use the same wire protocol and could use the same server
implementation in Java. The ODBC client would be written in C or C++.

Since the Avatica protocol abstracts many of the differences between providers,
the same ODBC client could be used for different databases.

Although the Avatica project does not include an ODBC driver, there
are ODBC drivers written on top of the Avatica protocol, for example
[an ODBC driver for Apache Phoenix](http://hortonworks.com/hadoop-tutorial/bi-apache-phoenix-odbc/).

## HTTP Server

Avatica-server embeds the Jetty HTTP server, providing a class
[HttpServer]({{ site.apiRoot }}/org/apache/calcite/avatica/server/HttpServer.html)
that implements the Avatica RPC protocol
and can be run as a standalone Java application.

Connectors in HTTP server can be configured if needed by extending
`HttpServer` class and overriding its `configureConnector()` method.
For example, user can set `requestHeaderSize` to 64K bytes as follows:

{% highlight java %}
HttpServer server = new HttpServer(handler) {
  @Override
  protected ServerConnector configureConnector(
      ServerConnector connector, int port) {
    HttpConnectionFactory factory = (HttpConnectionFactory)
        connector.getDefaultConnectionFactory();
    factory.getHttpConfiguration().setRequestHeaderSize(64 << 10);
    return super.configureConnector(connector, port);
  }
};
server.start();
{% endhighlight %}

## Project structure

We know that it is important that client libraries have minimal dependencies.

Avatica is currently part of Apache Calcite.
It does not depend upon any other part of Calcite.
At some point Avatica could become a separate project.

Packages:

* [org.apache.calcite.avatica]({{ site.apiRoot }}/org/apache/calcite/avatica/package-summary.html) Core framework
* [org.apache.calcite.avatica.remote]({{ site.apiRoot }}/org/apache/calcite/avatica/remote/package-summary.html) JDBC driver that uses remote procedure calls
* [org.apache.calcite.avatica.server]({{ site.apiRoot }}/org/apache/calcite/avatica/server/package-summary.html) HTTP server
* [org.apache.calcite.avatica.util]({{ site.apiRoot }}/org/apache/calcite/avatica/util/package-summary.html) Utilities

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

## Clients

The following is a list of available Avatica clients. Several describe
themselves as adapters for
[Apache Phoenix](http://phoenix.apache.org) but also work with other
Avatica back-ends. Contributions for clients in other languages are
highly welcomed!

### Microsoft .NET driver for Apache Phoenix Query Server
* [Home page](https://github.com/Azure/hdinsight-phoenix-sharp)
* Language: C#
* *License*: [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)
* Avatica version 1.2.0 onwards
* *Maintainer*: Microsoft Azure

### Boostport
* [Home page](https://github.com/Boostport/avatica)
* *Language*: Go
* *License*: [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)
* Avatica version 1.8.0 onwards
* *Maintainer*: Boostport

### Avatica thin client
* [Home page](https://calcite.apache.org/avatica)
* *Language*: Java
* *License*: [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)
* Any Avatica version
* *Maintainer*: Apache Calcite community

### Apache Phoenix database adapter for Python
* [Home page](https://bitbucket.org/lalinsky/python-phoenixdb)
* Language: Python
* *License*: [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)
* Avatica version 1.2.0 to 1.6.0
* *Maintainer*: Lukáš Lalinský
