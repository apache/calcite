<!--
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
-->

# Apache Calcite Avatica Technology Compatibility Kit (TCK)

The Avatica TCK is a framework to test the compatibilty between
various versions of Avatica clients and servers.

The TCK is configured by a YAML file which specifies a TCK framework
jar and a list of Avatica client and server versions. Each version
must specify the path to a client jar, the URL of the server for
that version, and a template for the JDBC url.

An example YAML configuration file is provided ([example_config.yml][example-config-yml])
which can be used as a starting point for users to fill out. Most elements are
straightforward to understand; however, it does require that you provide a URL
to a running Avatica server for each version to be tested.

## Running a standalone Avatica server

To make it easy to run a standalone Avatica server, an instance of the Avatica
server running against an in-memory SQL database (HSQLDB) is provided starting
in version 1.8.0. The `avatica-hsqldb-server-1.8.0-SNAPSHOT-shaded.jar` artifact
can be used to start an Avatica server on a random port (printed to the console).

  `java -jar avatica-hsqldb-server-1.8.0-SNAPSHOT-shaded.jar`

For convenience in testing against earlier versions of Avatica, the following
repository can be used to simplify building the same instance of a standalone
HSQLDB-backed Avatica server https://github.com/joshelser/legacy-avatica-hsqldb-server

Follow the instructions in the [README][legacy-readme] to build a standalone jar
against a specific version of Calcite/Avatica which can be used as specified above.

## Running the TCK

A ruby script, [test_runner.rb][test-runner-script] is provided which consumes the modified YAML configuration
file. This script will first run each provided version against itself as a sanity
check for the tests itself (as the scope of what is implemented in Avatica does
change over time), and then enumerate all possible combinations of client and server
version.

  `./test_runner.rb my_tck_config.yml`

For example, if versions 1.6.0, 1.7.1 and 1.8.0-SNAPSHOT are defined in the YAML configuration
file, the following identity tests will be run from client to server:

* 1.6.0 to 1.6.0
* 1.7.1 to 1.7.1
* 1.8.0-SNAPSHOT to 1.8.0-SNAPSHOT

while the following tests will be run for cross-version compatibility:

* 1.6.0 to 1.7.1
* 1.6.0 to 1.8.0-SNAPSHOT
* 1.7.1 to 1.6.0
* 1.7.1 to 1.8.0-SNAPSHOT
* 1.8.0-SNAPSHOT to 1.6.0
* 1.8.0-SNAPSHOT to 1.7.1

Any errors encountered will be printed to the terminal. The final output of the script
will be a summary of both the identity tests and the cross-version tests for easy consumption.

[example-config-yml]: https://github.com/apache/calcite/tree/master/avatica/tck/src/main/resources/example_config.yml
[legacy-readme]: https://github.com/joshelser/legacy-avatica-hsqldb-server/blob/master/README.md
[test-runner-script]: https://github.com/apache/calcite/tree/master/avatica/tck/src/main/ruby/test_runner.rb
