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
important to the users and developers. While the scope of that compatibility is
still being defined, we always welcome any changes which can improve our understanding
or definition of compatibility.

## Technology Compatibility Kit (TCK)

The [Avatica TCK project][github-tck] is a collection of JUnit tests, a Java runner, and
a Ruby script. The JUnit tests are designed to invoke specific portions of both
the client and server components of Avatica and verify that they work as expected
across versions.

A verison of Avatica is defined by three things:

1. Filesystem path to an Avatica client jar (e.g. groupId=org.apache.calcite.avatica, artifactId=avatica)
2. URL to a running instance of an Avatica server
3. A JDBC URL template to the Avatica server for the Avatica client JDBC driver.

Users of the TCK define a collection of versions (defined by the above information)
and the [avatica-tck][github-tck] jar by a YAML configuration file.

Traditionally, Avatica does not provide any implementation of the Avatica server as
the worth is typically derived from downstream projects. However, for the purposes of
compatibility testing, it makes sense for Avatica to provide a standalone server instance.
A new artifact was also introduced with the original TCK codebase called [hsqldb-server][github-hsqldb-server].
This artifact is a runnable jar (e.g. `java -jar`) which will start an instance of the
Avatica server on a random port using the in-memory [HSQLDB database](http://hsqldb.org/).

Invoking the Ruby script will print a summary of testing each specified version
against itself and all other versions in the YAML configuration. An example summary
is presented below.

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

It is not entirely expected that all tested version-pairs will pass unless the
test is written with specific knowledge about past bugs in Avatica itself. While
Avatica tries to handle all of these implicitly, it is not always feasible or desirable
to do so. New test cases can be easily written as any other JUnit test case is written,
but there is presently no automation around verifying the test case works as
a part of the Maven build process.

For more information on running this TCK, including specific instructions, reference
the provided [README][github-tck-readme] file.

[github-tck]: https://github.com/apache/calcite/tree/master/avatica/tck
[github-hsqldb-server]: https://github.com/apache/calcite/tree/master/avatica/hsqldb-server
[github-tck-readme]: https://github.com/apache/calcite/tree/master/avatica/tck/README.md
