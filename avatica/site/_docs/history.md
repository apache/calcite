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

## <a href="https://github.com/apache/calcite/releases/tag/calcite-avatica-1.9.0">1.9.0</a> / 2016-11-01
{: #v1-9-0}

Apache Calcite Avatica 1.9.0 includes various improvements to make it
more robust and secure, while maintaining API and protocol
compatibility with previous versions. We now include non-shaded and
shaded artifacts, to make it easier to embed Avatica in your
application. There is improved support for the JDBC API, including
better type conversions and support for canceling statements. The
transport is upgraded to use protobuf-3.1.0 (previously 3.0 beta).

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 1.7, 1.8;
Guava versions 14.0 to 19.0;
other software versions as specified in `pom.xml`.

Features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1471">CALCITE-1471</a>]
  `HttpServerSpnegoWithJaasTest.testAuthenticatedClientsAllowed` fails on Windows
  (Aaron Mihalik)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1464">CALCITE-1464</a>]
  Upgrade Jetty version to 9.2.19v20160908
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1463">CALCITE-1463</a>]
  In `standalone-server` jar, relocate dependencies rather than excluding them
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1355">CALCITE-1355</a>]
  Upgrade to protobuf-java 3.1.0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1462">CALCITE-1462</a>]
  Remove Avatica pom cruft
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1458">CALCITE-1458</a>]
  Add column values to the deprecated protobuf attribute
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1433">CALCITE-1433</a>]
  Add missing dependencies to `avatica-server`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1433">CALCITE-1433</a>]
  Fix missing avatica `test-jar` dependency
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1423">CALCITE-1423</a>]
  Add method `ByteString.indexOf(ByteString, int)`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1408">CALCITE-1408</a>]
  `ResultSet.getXxx` methods should throw `SQLDataException` if cannot convert to
  the requested type (Laurent Goujon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1410">CALCITE-1410</a>]
  Fix JDBC metadata classes (Laurent Goujon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1224">CALCITE-1224</a>]
  Publish non-shaded and shaded versions of Avatica client artifacts
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1407">CALCITE-1407</a>]
  `MetaImpl.fieldMetaData` wrongly uses 1-based column ordinals
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1361">CALCITE-1361</a>]
  Remove entry from `AvaticaConnection.flagMap` when session closed
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1399">CALCITE-1399</a>]
  Make the jcommander `SerializationConverter` public
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1394">CALCITE-1394</a>]
  Javadoc warnings due to `CoreMatchers.containsString` and `mockito-all`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1390">CALCITE-1390</a>]
  Avatica JDBC driver wrongly modifies `Properties` object
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1371">CALCITE-1371</a>]
  `PreparedStatement` does not process Date type correctly (Billy (Yiming) Liu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1301">CALCITE-1301</a>]
  Add `cancel` flag to `AvaticaStatement`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1315">CALCITE-1315</a>]
  Retry the request on `NoHttpResponseException`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1300">CALCITE-1300</a>]
  Retry on HTTP-503 in hc-based `AvaticaHttpClient`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1263">CALCITE-1263</a>]
  Case-insensitive match and null default value for `enum` properties
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1282">CALCITE-1282</a>]
  Adds an API method to set extra allowed Kerberos realms

Tests

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1226">CALCITE-1226</a>]
  Disable `AvaticaSpnegoTest` due to intermittent failures

Web site and documentation

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1369">CALCITE-1369</a>]
  Add list of Avatica clients to the web site
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1229">CALCITE-1229</a>]
  Restore API and Test API links to site
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1287">CALCITE-1287</a>]
  TCK test for binary columns
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1285">CALCITE-1285</a>]
  Fix client URL template in example config file

## <a href="https://github.com/apache/calcite/releases/tag/calcite-avatica-1.8.0">1.8.0</a> / 2016-06-04
{: #v1-8-0}

Apache Calcite Avatica 1.8.0 continues the focus on compatibility with previous
versions while also adding support for authentication between Avatica client and server.
Performance, notably on the write-path, is also major area of improvement
in this release, increasing as much as two to three times over previous versions
with the addition of new API support. The documentation for both users and developers
continues to receive improvements.

A number of protocol issues are resolved relating to the proper serialization of
decimals, the wire-API semantics of signed integers that were marked as unsigned
integers, and the unintentional Base64-encoding of binary data using the Protocol
Buffers serialization in Avatica. These issues were fixed in such a way to be
backwards compatible, but older clients/servers may still compute incorrect data.

Users of Avatica 1.7.x should not notice any issues in upgrading existing applications
and are encouraged to upgrade as soon as possible.

Features and bug fixes

* [<a href='https://issues.apache.org/jira/browse/CALCITE-1159'>CALCITE-1159</a>]
  Support Kerberos-authenticated clients using SPNEGO
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1173'>CALCITE-1173</a>]
  Basic and Digest authentication
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1249'>CALCITE-1249</a>]
  L&N incorrect for source and non-shaded jars for avatica-standalone-server module
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1103'>CALCITE-1103</a>]
  Decimal data serialized as Double in Protocol Buffer API
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1205'>CALCITE-1205</a>]
  Inconsistency in protobuf TypedValue field names
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1207'>CALCITE-1207</a>]
  Allow numeric connection properties, and K, M, G suffixes
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1209'>CALCITE-1209</a>]
  Byte strings not being correctly decoded when sent to avatica using protocol buffers
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1213'>CALCITE-1213</a>]
  Changing AvaticaDatabaseMetaData from class to interface breaks compatibility
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1218'>CALCITE-1218</a>]
  Mishandling of uncaught exceptions results in no ErrorResponse sent to client
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1230'>CALCITE-1230</a>]
  Add SQLSTATE reference data as enum SqlState
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1243'>CALCITE-1243</a>]
  max_row_count in protobuf Requests should be signed int
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1247'>CALCITE-1247</a>]
  JdbcMeta#prepare doesn't set maxRowCount on the Statement
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1254'>CALCITE-1254</a>]
  Support PreparedStatement.executeLargeBatch
* [<a href='https://issues.apache.org/jira/browse/CALCITE-643'>CALCITE-643</a>]
  User authentication for avatica clients
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1128'>CALCITE-1128</a>]
  Support addBatch()/executeBatch() in remote driver
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1179'>CALCITE-1179</a>]
  Extend list of time units and time unit ranges
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1180'>CALCITE-1180</a>]
  Support clearBatch() in remote driver
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1185'>CALCITE-1185</a>]
  Send back ErrorResponse on failure to parse requests
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1192'>CALCITE-1192</a>]
  Document protobuf and json REP types with examples
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1214'>CALCITE-1214</a>]
  Support url-based kerberos login
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1236'>CALCITE-1236</a>]
  Log exceptions sent back to client in server log
* [<a href='https://issues.apache.org/jira/browse/CALCITE-836'>CALCITE-836</a>]
  Provide a way for the Avatica client to query the server versions
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1156'>CALCITE-1156</a>]
  Bump jetty version
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1184'>CALCITE-1184</a>]
  Update Kerby dependency to 1.0.0-RC2

Tests

* [<a href='https://issues.apache.org/jira/browse/CALCITE-1190'>CALCITE-1190</a>]
  Cross-Version Compatibility Test Harness
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1113'>CALCITE-1113</a>]
  Parameter precision and scale are not returned from Avatica REST API
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1186'>CALCITE-1186</a>]
  Parameter 'signed' metadata is always returned as false
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1189'>CALCITE-1189</a>]
  Unit test failure when JVM default charset is not UTF-8
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1061'>CALCITE-1061</a>]
  RemoteMetaTest#testRemoteStatementInsert's use of hsqldb isn't guarded
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1194'>CALCITE-1194</a>]
  Avatica metrics has non-test dependency on JUnit
* [<a href='https://issues.apache.org/jira/browse/CALCITE-835'>CALCITE-835</a>]
  Unicode character seems to be handled incorrectly in Avatica

Web site and documentation

* [<a href='https://issues.apache.org/jira/browse/CALCITE-1251'>CALCITE-1251</a>]
  Write release notes
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1201'>CALCITE-1201</a>]
  Bad character in JSON docs
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1267'>CALCITE-1267</a>]
  Point to release notes on website in README
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1163'>CALCITE-1163</a>]
  Avatica sub-site logo leads to Calcite site instead of Avatica's
* [<a href='https://issues.apache.org/jira/browse/CALCITE-1202'>CALCITE-1202</a>]
  Lock version of Jekyll used by website

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
