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
# Micro-benchmarks

This directory, `ubenchmark`, contains micro-benchmarks written using
the [jmh](https://openjdk.java.net/projects/code-tools/jmh/) framework.

The benchmarks are tools for development and are not distributed as
Calcite artifacts. (Besides, jmh's license does not allow that.)

## Running all benchmark from the command line

To run all benchmarks:

{noformat}bash
$ cd calcite
$ ./gradlew :ubenchmark:jmh
{noformat}

## Running one benchmark from the command line

To run just one benchmark, modify `ubenchmark/build.gradle.kts` and add the
following task:

{noformat}kotlin
jmh {
    include = listOf("removeAllVertices.*Benchmark")
}
{noformat}

and run

{noformat}bash
$ ./gradlew :ubenchmark:jmh
{noformat}

as before. In this case, `removeAllVertices.*Benchmark` is a
regular expression that matches a few methods -- benchmarks -- in
`class DefaultDirectedGraphBenchmark`.

The `jmd-gradle-plugin` has
[many other options](https://github.com/melix/jmh-gradle-plugin#configuration-options)
but you will need to translate them from Groovy syntax to our Kotlin syntax.

## Recording results

When you have run the benchmarks, please record them in the relevant JIRA
case and link them here:

* ParserBenchmark:
  [459](https://issues.apache.org/jira/browse/CALCITE-459),
  [1012](https://issues.apache.org/jira/browse/CALCITE-1012)
* ArrayListTest:
  [3878](https://issues.apache.org/jira/browse/CALCITE-3878)
* DefaultDirectedGraphBenchmark:
  [3827](https://issues.apache.org/jira/browse/CALCITE-3827)
* RelNodeBenchmark:
  [3836](https://issues.apache.org/jira/browse/CALCITE-3836)
* ReflectVisitorDispatcherTest:
  [3873](https://issues.apache.org/jira/browse/CALCITE-3873)
