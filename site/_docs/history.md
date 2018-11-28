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
<a href="https://github.com/apache/calcite-avatica/releases">github</a>.
Downloads are available on the
[downloads page]({{ site.baseurl }}/downloads/avatica.html).

## <a href="https://github.com/apache/calcite-avatica/releases/tag/rel/avatica-1.13.0">1.13.0</a> / 2018-11-XX
{: #v1-13-0}

Apache Calcite Avatica 1.13.0 includes around 30 bugs fixes and enhancements. This release adds the ability to
prepare and make a release, run tests and execute `mvn clean` in a docker container.

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 8, 9, 10, 11, 12;
using IBM Java 8;
Guava versions 14.0 to 23.0;
other software versions as specified in `pom.xml`.

Features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2386">CALCITE-2386</a>]
  Naively wire up struct support
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2390">CALCITE-2390</a>]
  Remove uses of `X509CertificateObject` which is deprecated in current version of bouncycastle
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2467">CALCITE-2467</a>]
  Update owasp-dependency-check maven plugin to 3.3.1, protobuf-java to 3.5.1, jackson to 2.9.6 and jetty to 9.4.11.v20180605
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2503">CALCITE-2503</a>]
  AvaticaCommonsHttpClientImpl client needs to set user-token on HttpClientContext before sending the request
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2570">CALCITE-2570</a>]
  Upgrade forbiddenapis to 2.6 for JDK 11 support
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1183">CALCITE-1183</a>]
  Upgrade kerby to 1.1.1 and re-enable AvaticaSpnegoTest
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2486">CALCITE-2486</a>]
  Upgrade Apache parent POM to version 21 and update other dependencies
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2583">CALCITE-2583</a>]
  Upgrade dropwizard metrics to 4.0.3
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1006">CALCITE-1006</a>]
  Enable spotbugs-maven-plugin
* Move spotbugs-filter.xml to src/main/config/spotbugs/
* Update usage of JCommander after upgrading to 1.72
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2587">CALCITE-2587</a>]
  Regenerate protobuf files for protobuf 3.6.1
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2594">CALCITE-2594</a>]
  Ensure forbiddenapis and maven-compiler use the correct JDK version
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2595">CALCITE-2595</a>]
  Add maven wrapper
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2385">CALCITE-2385</a>]
  Add flag to disable dockerfile checks when executing a dry-run build
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2676">CALCITE-2676</a>]
  Add release script and docker-compose.yml to support building releases using docker
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2680">CALCITE-2680</a>]
  Downgrade maven-scm-provider to 1.10.0 due to API incompatibility that prevents releases from building
* Update release script to use GPG agent
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2681">CALCITE-2681</a>]
  Add maven-scm-api as a dependency, so that Avatica can build
* Include -Dgpg.keyname when executing release:perform in the release script
* Prompt user for git username when using release script
* Fix release script to ensure git usernames are not truncated
* Remove requirement to set maven master password when using the release script
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2682">CALCITE-2682</a>]
  Add ability to run tests in docker
* Update travis-ci status badge to the correct one in README.md
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2385">CALCITE-2385</a>]
  Update travis configuration to disable dockerfile checks during testing
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2698">CALCITE-2698</a>]
  Use Docker Hub hooks to select Avatica version during image build and publish HSQLDB image

Tests

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2568">CALCITE-2568</a>]
  Ensure that IBM JDK TLS cipher list matches Oracle/OpenJDK for Travis CI
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2655">CALCITE-2655</a>]
  Enable Travis to test against JDK12
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2412">CALCITE-2412</a>]
  Add appveyor.yml to run Windows tests

Website and Documentation

* Fix broken links to Github release on the history page
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2381">CALCITE-2381</a>]
  Document how to authenticate against the Apache maven repository, select GPG keys and version numbers when building
  a release
* Fix Go client download links
* Fix download link to release history in news item template
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2550">CALCITE-2550</a>]
  Update download links for avatica-go to link to `apache-calcite-avatica-go-x.x.x-src.tar.gz` for release 3.2.0 and onwards
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2574">CALCITE-2574</a>]
  Update download pages to include instructions for verifying downloaded artifacts
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2577">CALCITE-2577</a>]
  Update URLs on download page to HTTPS
* Update links on Go client download page to reference `go_history.html` and not `history.html`

## <a href="https://github.com/apache/calcite-avatica/releases/tag/rel/avatica-1.12.0">1.12.0</a> / 2018-06-24
{: #v1-12-0}

Apache Calcite Avatica 1.12.0 includes more than 20 bugs fixes and new features. ZIP archives will no longer be
produced from this release onwards.

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 8, 9, 10, 11;
using IBM Java 8;
Guava versions 14.0 to 23.0;
other software versions as specified in `pom.xml`.

Features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1520">CALCITE-1520</a>]
  Implement method `AvaticaConnection.isValid()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2212">CALCITE-2212</a>]
  Enforce minimum JDK 8 via `maven-enforcer-plugin`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2268">CALCITE-2268</a>]
  Bump HSQLDB to 2.4.0 in Avatica Docker image
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2272">CALCITE-2272</a>]
  Bump dependencies: Apache Parent POM 19, JDK 10 Surefire and JDK 10 Javadoc
  Fix Javadoc generation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2218">CALCITE-2218</a>]
  Fix `AvaticaResultSet.getRow()`
* Add Docker Hub image for HSQLDB
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2289">CALCITE-2289</a>]
  Enable html5 for Javadoc on JDK 9+
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2284">CALCITE-2284</a>]
  Allow Jetty Server to be customized before startup (Alex Araujo)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2333">CALCITE-2333</a>]
  Stop generating ZIP archives for release
* Bump HSQLDB dependency to 2.4.1
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2294">CALCITE-2294</a>]
  Allow customization for `AvaticaServerConfiguration` for plugging new
  authentication mechanisms (Karan Mehta)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1884">CALCITE-1884</a>]
  `DateTimeUtils` produces incorrect results for days before the Gregorian
  cutover (Haohui Mai and Sergey Nuyanzin)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2303">CALCITE-2303</a>]
  Support `MICROSECONDS`, `MILLISECONDS`, `EPOCH`, `ISODOW`, `ISOYEAR` and
  `DECADE` time units in `EXTRACT` function (Sergey Nuyanzin)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2350">CALCITE-2350</a>]
  Fix cannot shade Avatica with Guava 21.0 or higher
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2341">CALCITE-2341</a>]
  Fix Javadoc plugin incompatibility
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2352">CALCITE-2352</a>]
  Update checkstyle to 6.18 and update `suppressions.xml`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2219">CALCITE-2219</a>]
  Update `Connection`, `Statement`, `PreparedStatement` and `ResultSet` to throw
  an exception if resource is closed
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2361">CALCITE-2361</a>]
  Upgrade Bouncycastle to 1.59
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2299">CALCITE-2299</a>]
  Add support for `NANOSECOND` in `TimeUnit` and `TimeUnitRange`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2285">CALCITE-2285</a>]
  Support client cert keystore for Avatica client (Karan Mehta)

Tests

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2210">CALCITE-2210</a>]
  Remove `oraclejdk7`, add `oraclejdk9`, add `oraclejdk10`, and add `ibmjava` to
  `.travis.yml`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2255">CALCITE-2255</a>]
  Add JDK 11 `.travis.yml`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2022">CALCITE-2022</a>]
  Add dynamic drive calculation to correctly determine trust store location when
  testing on Windows (Sergey Nuyanzin)

Website and Documentation

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1160">CALCITE-1160</a>]
  Redirect from Avatica community to Calcite community
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1937">CALCITE-1937</a>]
  Update Avatica website to support the inclusion of Avatica-Go's content and add
  option for using docker to develop and build the website
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1914">CALCITE-1914</a>]
  Add DOAP (Description of a Project) file for Avatica
* Fix broken link in `HOWTO`
* Add missing license header to avatica-go docs generation script

## <a href="https://github.com/apache/calcite-avatica/releases/tag/rel/avatica-1.11.0">1.11.0</a> / 2018-03-09
{: #v1-11-0}

Apache Calcite Avatica 1.11.0 adds support for JDK 10 and drops
support for JDK 7. There are more than 20 bug fixes and new features.

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 8, 9, 10;
Guava versions 14.0 to 23.0;
other software versions as specified in `pom.xml`.

Features and bug fixes

* Generate sha256 checksums for releases (previous releases used md5 and sha1)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2199">CALCITE-2199</a>]
  Allow methods overriding `AvaticaResultSet.getStatement()` to throw a
  `SQLException` (Benjamin Cogrel)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-508">CALCITE-508</a>]
  Ensure that `RuntimeException` is wrapped in `SQLException`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2013">CALCITE-2013</a>]
  Upgrade HSQLDB to 2.4
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2154">CALCITE-2154</a>]
  Upgrade jackson to 2.9.4
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2140">CALCITE-2140</a>]
  Basic implementation of `Statement.getMoreResults()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2086">CALCITE-2086</a>]
  Increased max allowed HTTP header size to 64KB
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2073">CALCITE-2073</a>]
  Allow disabling of the `maven-protobuf-plugin`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2017">CALCITE-2017</a>]
  Support JAAS-based Kerberos login on IBM Java
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2003">CALCITE-2003</a>]
  Remove global synchronization on `openConnection`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1922">CALCITE-1922</a>]
  Allow kerberos v5 OID in SPNEGO authentication
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1915">CALCITE-1915</a>]
  Work around a Jetty bug where the SPNEGO challenge is not sent
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1902">CALCITE-1902</a>]
  In `AvaticaResultSet` methods, throw `SQLFeatureNotSupportedException` rather
  than `UnsupportedOperationException` (Sergio Sainz)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1487">CALCITE-1487</a>]
  Set the request as handled with authentication failures
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1904">CALCITE-1904</a>]
  Allow SSL hostname verification to be turned off
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1879">CALCITE-1879</a>]
  Log incoming protobuf requests at `TRACE`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1880">CALCITE-1880</a>]
  Regenerate protobuf files for 3.3.0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1836">CALCITE-1836</a>]
  Upgrade to protobuf-java-3.3.0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1195">CALCITE-1195</a>]
  Add a curl+jq example for interacting with Avatica
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1813">CALCITE-1813</a>]
  Use correct noop-driver artifactId

Tests

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2145">CALCITE-2145</a>]
  `RemoteDriverTest.testBatchInsertWithDates` fails in certain time zones
  (Alessandro Solimando)
* Fix tests on Windows; disable SPNEGO test on Windows

Web site and documentation

* Update description of the `signature` field in `ResultSetResponse`
* Correct field name in `PrepareAndExecuteRequest` documentation (Lukáš Lalinský)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2083">CALCITE-2083</a>]
  Update documentation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2036">CALCITE-2036</a>]
  Fix "next" link in `history.html`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1878">CALCITE-1878</a>]
  Update the website for protobuf changes in 1.10.0

## <a href="https://github.com/apache/calcite-avatica/releases/tag/rel/avatica-1.10.0">1.10.0</a> / 2017-05-30
{: #v1-10-0}

Apache Calcite Avatica 1.10.0 is the first release since
[Avatica's git repository](https://git-wip-us.apache.org/repos/asf/calcite-avatica.git)
separated from
[Calcite's repository](https://git-wip-us.apache.org/repos/asf/calcite.git) in
[[CALCITE-1717](https://issues.apache.org/jira/browse/CALCITE-1717)].
Avatica now runs on JDK 9 (and continues to run on JDK 7 and 8),
and there is now a Docker image for an Avatica server.
You may now send and receive Array data via the JDBC API.
Several improvements to date/time support in DateTimeUtils.

Compatibility: This release is tested
on Linux, macOS, Microsoft Windows;
using Oracle JDK 7, 8, 9;
Guava versions 14.0 to 19.0;
other software versions as specified in `pom.xml`.

Features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1690">CALCITE-1690</a>]
  Timestamp literals cannot express precision above millisecond
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1539">CALCITE-1539</a>]
  Enable proxy access to Avatica server for third party on behalf of end users
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1756">CALCITE-1756</a>]
  Differentiate between implicitly null and explicitly null `TypedValue`s
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1050">CALCITE-1050</a>]
  Array support for Avatica
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1746">CALCITE-1746</a>]
  Remove `KEYS` file from git and from release tarball
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1353">CALCITE-1353</a>]
  Convert `first_frame_max_size` to an `int32`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1744">CALCITE-1744</a>]
  Clean up the Avatica poms
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1741">CALCITE-1741</a>]
  Upgrade `maven-assembly-plugin` to version 3.0.0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1364">CALCITE-1364</a>]
  Docker images for an Avatica server
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1717">CALCITE-1717</a>]
  Remove Calcite code and lift Avatica
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1700">CALCITE-1700</a>]
  De-couple the `HsqldbServer` into a generic JDBC server
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1699">CALCITE-1699</a>]
  Statement may be null
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1667">CALCITE-1667</a>]
  Forbid calls to JDK APIs that use the default locale, time zone or character
  set
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1664">CALCITE-1664</a>]
  `CAST('<string>' as TIMESTAMP)` wrongly adds part of sub-second fraction to the
  value
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1654">CALCITE-1654</a>]
  Avoid generating a string from the Request/Response when it will not be logged
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1609">CALCITE-1609</a>]
  In `DateTimeUtils`, implement `unixDateExtract` and `unixTimeExtract` for more
  time units
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1608">CALCITE-1608</a>]
  Move `addMonths` and `subtractMonths` methods from Calcite class `SqlFunctions`
  to Avatica class `DateTimeUtils`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1600">CALCITE-1600</a>]
  In `Meta.Frame.create()`, change type of `offset` parameter from `int` to `long`
  (Gian Merlino)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1602">CALCITE-1602</a>]
  Remove uses of deprecated APIs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1599">CALCITE-1599</a>]
  Remove unused `createIterable` call in `LocalService` (Gian Merlino)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1576">CALCITE-1576</a>]
  Use the `protobuf-maven-plugin`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1567">CALCITE-1567</a>]
  JDK9 support
* Remove non-ASCII characters from Java source files
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1538">CALCITE-1538</a>]
  Support `truststore` and `truststore_password` JDBC options
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1485">CALCITE-1485</a>]
  Upgrade Avatica's Apache parent POM to version 18

Tests

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1752">CALCITE-1752</a>]
  Use `URLDecoder` instead of manually replacing "%20" in URLs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1736">CALCITE-1736</a>]
  Address test failures when the path contains spaces
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1568">CALCITE-1568</a>]
  Upgrade `mockito` to 2.5.5

Web site and documentation

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1743">CALCITE-1743</a>]
  Add instructions to release docs to move git tag from `rcN` to `rel/`

## <a href="https://github.com/apache/calcite-avatica/releases/tag/calcite-avatica-1.9.0">1.9.0</a> / 2016-11-01
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

## <a href="https://github.com/apache/calcite-avatica/releases/tag/calcite-avatica-1.8.0">1.8.0</a> / 2016-06-04
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

## <a href="https://github.com/apache/calcite-avatica/releases/tag/calcite-avatica-1.7.1">1.7.1</a> / 2016-03-18
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
