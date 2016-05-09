---
layout: docs
title: Compatibility
sidebar_title: Compatibility
permalink: /docs/compatibility.html
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

Given Avatica's client-server model, it is no surprise that compatibility is
important to the users and developers of Apache Calcite Avatica. This document
defines what guarantees are made by Avatica with respect to compatibility
between Avatica client and server across versions. This document is still a work
in progress with many areas still requiring definition. Contributions are always
welcome.

## Avatica Technology Compatibility Kit (TCK)

The [Avatica TCK project][github-tck] is a framework designed to automatically
test an Avatica client against an Avatica server. A collection of JUnit tests,
a YAML configuration file, and a Ruby script defines the TCK.
The JUnit tests invoke specific portions of both the client and
server components of Avatica to verify that they work as expected. The Ruby
script uses the YAML configuration file to define the set of
client and server version pairs to run the JUnit tests against.

In the YAML configuration file, a name (e.g. `1.6.0`) and the following three
items define an Avatica version:

1. A filesystem path to an Avatica client jar (e.g. groupId=org.apache.calcite.avatica, artifactId=avatica)
2. A URL to a running instance of an Avatica server
3. A JDBC URL template to the Avatica server for the Avatica client JDBC driver.

Users of the TCK define the collection of versions (defined by the above
information) and the location of the [avatica-tck][github-tck] jar by a YAML
configuration file. An [example YAML configuration file][github-tck-yml-file] is
bundled with the project.

Traditionally, Avatica does not provide any implementation of the Avatica server
as the value in Avatica is recognized in the integrating project (e.g. Apache
Drill or Apache Phoenix). However, for the purposes of compatibility testing, it
makes sense that Avatica provides a standalone server instance.  A new artifact
is introduced to Avatica with the original TCK codebase called
[avatica-standalone-server][github-standalone-server].  This artifact is a
runnable jar (e.g. `java -jar`) which starts an instance of the Avatica server
on a random port using the in-memory [HSQLDB database](http://hsqldb.org/). This
artifacts makes it extremely simple to start a version of the Avatica server for
the specific version of Avatica to be tested.

As mentioned, the Ruby script is the entry point for the TCK. Invoking the Ruby
script which prints a summary of testing each specified version against itself and
all other versions in the YAML configuration. An example summary is presented
below which is the result of testing versions 1.6.0, 1.7.1 and 1.8.0-SNAPSHOT:

```
Summary:

Identity test scenarios (ran 3)

Testing identity for version v1.6.0: Passed
Testing identity for version v1.7.1: Passed
Testing identity for version v1.8.0-SNAPSHOT: Failed

All test scenarios (ran 6)

Testing client v1.6.0 against server v1.7.1: Passed
Testing client v1.6.0 against server v1.8.0-SNAPSHOT: Failed
Testing client v1.7.1 against server v1.6.0: Passed
Testing client v1.7.1 against server v1.8.0-SNAPSHOT: Failed
Testing client v1.8.0-SNAPSHOT against server v1.6.0: Failed
Testing client v1.8.0-SNAPSHOT against server v1.7.1: Failed
```

It is not always expected that all tested version-pairs will pass unless the
test is written with specific knowledge about past bugs in Avatica itself. While
Avatica tries to handle all of these edge cases implicitly, it is not always
feasible or desirable to do so. Adding new test cases is as easy as writing a
JUnit test case in the [TCK module][github-tck-tests], but there is presently no
automation around verifying the test cases as a part of the Maven build.

For more information on running this TCK, including specific instructions for
running the TCK, reference the provided [README][github-tck-readme] file.

[github-tck]: https://github.com/apache/calcite/tree/master/avatica/tck
[github-tck-tests]: https://github.com/apache/calcite/tree/master/avatica/tck/src/main/java/org/apache/calcite/avatica/tck/tests
[github-standalone-server]: https://github.com/apache/calcite/tree/master/avatica/standalone-server
[github-tck-readme]: https://github.com/apache/calcite/tree/master/avatica/tck/README.md
[github-tck-yml-file]: https://github.com/apache/calcite/tree/master/avatica/tck/src/main/resources/example_config.yml
