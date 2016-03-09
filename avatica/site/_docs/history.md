---
layout: docs
title: History
permalink: "/docs/history.html"
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

For a full list of releases, see
<a href="https://github.com/apache/calcite/releases">github</a>.
Downloads are available on the
[downloads page]({{ site.baseurl }}/downloads/).

## 1.7.0 / (Under Development)
{: #v1-7-0}

This is the first release of Avatica as an independent project. (It
is still governed by Apache Calcite's PMC, and stored in the same git
repository as Calcite, but releases are no longer synchronized, and
Avatica does not depend on any Calcite modules.)

One notable technical change is that we have replaced JUL (`java.util.logging`)
with [SLF4J](http://slf4j.org/). SLF4J provides an API that Avatica can use
independent of the logging implementation. This is more
flexible for users: they can configure Avatica's logging within their
own chosen logging framework. This work was done in
[CALCITE-669](https://issues.apache.org/jira/browse/CALCITE-669).

If you have configured JUL in Calcite/Avatica previously, you'll
notice some differences, because JUL's `FINE`, `FINER` and `FINEST`
logging levels do not exist in SLF4J. To deal with this, we mapped
`FINE` to SLF4J's `DEBUG` level, and mapped `FINER` and `FINEST` to
SLF4J's `TRACE`.

The performance of Avatica was an important focus for this release as well.
Numerous improvements aimed at reducing the overall latency of Avatica RPCs
was reduced. Some general testing showed an overall reduction of latency
by approximately 15% over the previous release.

Compatibility: This release is tested on Linux, Mac OS X, Microsoft
Windows; using Oracle JDK 1.7, 1.8; Guava versions 12.0.1 to 19.0;
other software versions as specified in `pom.xml`.

Features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1132">CALCITE-1132</a>]
  Update `artifactId`, `groupId` and `name` for Avatica
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1064">CALCITE-1064</a>]
  Address problematic `maven-remote-resources-plugin`
* In `TimeUnit` add `WEEK`, `QUARTER`, `MICROSECOND` values, and change type of
  `multiplier`
* Update `groupId` when Calcite POMs reference Avatica modules
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1078">CALCITE-1078</a>]
  Detach avatica from the core calcite Maven project
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1117">CALCITE-1117</a>]
  Default to a `commons-httpclient` implementation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1118">CALCITE-1118</a>]
  Add a noop-JDBC driver for testing Avatica server
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1119">CALCITE-1119</a>]
  Additional metrics instrumentation for request processing
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1094">CALCITE-1094</a>]
  Replace `ByteArrayOutputStream` to avoid synchronized writes
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1092">CALCITE-1092</a>]
  Use singleton descriptor instances for protobuf field presence checks
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1093">CALCITE-1093</a>]
  Reduce impact of `ArrayList` performance
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1086">CALCITE-1086</a>]
  Avoid sending `Signature` on `Execute` for updates
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1031">CALCITE-1031</a>]
  In prepared statement, `CsvScannableTable.scan` is called twice
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1085">CALCITE-1085</a>]
  Use a `NoopContext` singleton in `NoopTimer`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-642">CALCITE-642</a>]
  Add an avatica-metrics API
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1071">CALCITE-1071</a>]
  Improve hash functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-669">CALCITE-669</a>]
  Mass removal of Java Logging for SLF4J
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1067">CALCITE-1067</a>]
  Test failures due to clashing temporary table names
* [<a href="https://issues.apache.org/jira/browse/CALCITE-999">CALCITE-999</a>]
  Clean up maven POM files

Web site and documentation

* Instructions for Avatica site
* New logo and color scheme for Avatica site
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1079">CALCITE-1079</a>]
  Split out an Avatica website, made to slot into the Calcite site at `/avatica`

## Past releases

Prior to release 1.7.0, Avatica was released as part of Calcite. Maven
modules had groupId 'org.apache.calcite' and module names
'calcite-avatica', 'calcite-avatica-server' etc.

Please refer to the [Calcite release page](https://calcite.apache.org/docs/history.html)
for information about previous Avatica releases.
