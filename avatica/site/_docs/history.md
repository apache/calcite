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

## <a href="https://github.com/apache/calcite/releases/tag/calcite-avatica-1.8.0">1.8.0</a> / 2016-05-20
{: #v1-8-0}

Apache Calcite Avatica 1.8.0 continues the focus on compatibility with previous
versions while also adding support for authentication between Avatica client and server.
Performance, notably on the write-path, is also major area of improvement
in this release, increasing as much as two to three times over previous versions
with the addition of new API support. The documentation for both users and developers
continues to receive improvements.

Authentication is a major theme of this release, providing multiple layers of
additional authentication mechanisms over previous versions. In these earlier
versions, the only authentication provided by Avatica was achieved via the JDBC URL's
standard user and password options. These have always been passed directly into
the backend database's authentication system, but not all databases provide username
and password based authentication systems. [CALCITE-1173](https://issues.apache.org/jira/browse/CALCITE-1173)
adds Avatica-level authentication over [HTTP Basic](https://en.wikipedia.org/wiki/Basic_access_authentication)
and [HTTP Digest](https://en.wikipedia.org/wiki/Digest_access_authentication)
authentication mechanisms. These are provided specifically for the case when
Avatica is used with a database that _does not already_ provide its own authentication
implementation.

Some systems rely on [Kerberos](http://web.mit.edu/kerberos/) for strong, centrally-
managed authentication. [CALCITE-1159](https://issues.apache.org/jira/browse/CALCITE-1159)
introduces Kerberos-based authentication for clients via [SPNEGO](https://en.wikipedia.org/wiki/SPNEGO).
The Avatica server can be configured to only allow clients with a valid Kerberos ticket,
optionally, also passing this information to the backend database to implement
basic "impersonation" (where the Avatica server issues requests on behalf of the end user).

Building on top of the work done in Avatica-1.7.0 in [CALCITE-1091](https://issues.apache.org/jira/browse/CALCITE-1091),
this release also contains [CALCITE-1128](https://issues.apache.org/jira/browse/CALCITE-1128) which
implements the batch-oriented JDBC APIs on `Statement`. Through careful inspection, it
was observed that the overall performance of Avatica clients in 100% write workloads was
dominated by the cost of the HTTP calls. By leveraging the `Statement#addBatch()`
and `Statement#executeBatch()` API calls, clients can efficiently batch multiple updates
in a single HTTP call. In testing this over the previous single HTTP call per update with
[Apache Phoenix](https://phoenix.apache.org), it was observed that performance increased by
two to three times, bringing Avatica's performance on par with the Phoenix "native" driver.

Returning back to compatibility, a new component appears in this release which is designed to
test versions of Avatica against each other. [CALCITE-1190](https://issues.apache.org/jira/browse/CALCITE-1190)
introduces a "Technology Compatibility Kit" (TCK) which automates the testing of one version
of Avatica against other versions. To further ease this testing, a runnable JAR to launch
an HSQLDB instance and an Avatica server also makes it debut with these changes. This TCK
makes it much easier to run tests of newer clients against older servers and vice versa.
Validating the backwards compatibility that is being built is extremely important to be
confident in the guarantees being provided to users.

Finally, a number of bugs are also fixed in the Protocol Buffer wire API. Some of these
include [CALCITE-1113](https://issues.apache.org/jira/browse/CALCITE-1113) and
[CALCITE-1103](https://issues.apache.org/jira/browse/CALCITE-1103) which fix how certain
numbers are serialized, [CALITE-1243](https://issues.apache.org/jira/browse/CALCITE-1243)
which corrects some fields in Protocol Buffer messages which were incorrectly marked
as unsigned integers instead of signed integers, and [CALCITE-1209](https://issues.apache.org/jira/browse/CALCITE-1209)
which removes incorrect parsing of binary fields as Base64-encoded strings. All of
these issues are fixed in a backwards-compatible manner and should have no additional negative
impact on older clients (older clients will not break, but they may continue to return
incorrect data for certain numbers).

For users of the Avatica driver, a new [client reference page]({{ base_url }}/avatica/docs/client_reference.html)
is added which details the options that are available in the Avatica JDBC Driver's URL.
The wire API documentation for Protocol Buffers continues to receive updates as the API continues to evolve.

## <a href="https://github.com/apache/calcite/releases/tag/calcite-avatica-1.7.1">1.7.1</a> / 2016-03-18
{: #v1-7-1}

This is the first release of Avatica as an independent project. (It
is still governed by Apache Calcite's PMC, and stored in the same git
repository as Calcite, but releases are no longer synchronized, and
Avatica does not depend on any Calcite modules.)

One notable technical change is that we have replaced JUL (`java.util.logging`)
with [SLF4J](http://slf4j.org/). SLF4J provides an API that Avatica can use
independent of the logging implementation. This is more
flexible for users: they can configure Avatica's logging within their
own chosen logging framework. This work was done in
[[CALCITE-669](https://issues.apache.org/jira/browse/CALCITE-669)].

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

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1156">CALCITE-1156</a>]
  Upgrade Jetty from 9.2.7.v20150116 to 9.2.15.v20160210
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1141">CALCITE-1141</a>]
  Bump `version.minor` for Avatica
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

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1142">CALCITE-1142</a>]
  Create a `README` for Avatica
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1144">CALCITE-1144</a>]
  Fix `LICENSE` for Avatica
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1143">CALCITE-1143</a>]
  Remove unnecessary `NOTICE` for Avatica
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1139">CALCITE-1139</a>]
  Update Calcite's `KEYS` and add a copy for Avatica
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1140">CALCITE-1140</a>]
  Release notes and website updates for Avatica 1.7
* Instructions for Avatica site
* New logo and color scheme for Avatica site
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1079">CALCITE-1079</a>]
  Split out an Avatica website, made to slot into the Calcite site at `/avatica`

## Past releases

Prior to release 1.7.1, Avatica was released as part of Calcite. Maven
modules had groupId 'org.apache.calcite' and module names
'calcite-avatica', 'calcite-avatica-server' etc.

Please refer to the
[Calcite release page](https://calcite.apache.org/docs/history.html)
for information about previous Avatica releases.
