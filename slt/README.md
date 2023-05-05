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

# SQL logic tests

This project uses SqlLogicTests
<https://github.com/hydromatic/sql-logic-test>.  for testing the
Calcite compiler infrastructure.  

## Running the tests

This project provides a standalone Main class with a main function,
and it can be compiled into a standalone executable.  The executable
recognizes the following command-line arguments:

```
slt [options] files_or_directories_with_tests
Executes the SQL Logic Tests using a SQL execution engine
See https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
Options:
-h            Show this help message and exit
-x            Stop at the first encountered query error
-n            Do not execute, just parse the test files
-e executor   Executor to use
-b filename   Load a list of buggy commands to skip from this file
-v            Increase verbosity
-u username   Postgres user name
-p password   Postgres password
Registered executors:
       calcite
       hsql
       psql
       none
```

Here is an example invocation:

`slt -e calcite select1.test`

This invocation will run the tests through Calcite, and will execute
the test script `select1.test` and all the test scripts in the `index`
directory.  

### The 'Calcite' executor

This executor uses the JDBC executor to execute the statements storing
the data, and the default Calcite compiler settings to compile and
execute the queries.

### Adding new executors

Calcite is not just a compiler, it is framework for building
compilers.  This program only tests the default Calcite compiler
configuration.  To test other Calcite-based compilers one should
subclass the `SqlSLTTestExecutor` class (or the `CalciteExecutor`
class).
