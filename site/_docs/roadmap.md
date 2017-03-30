---
layout: docs
title: Roadmap
sidebar_title: Roadmap
permalink: /docs/roadmap.html
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
