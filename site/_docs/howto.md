---
layout: docs
title: HOWTO
permalink: /docs/howto.html
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

Here's some miscellaneous documentation about using Calcite and its various
adapters.

* TOC
{:toc}

## Building from a source distribution

Prerequisite is Java (JDK 8, 9, 10, 11, 12, 13, or 14) on your path.

Unpack the source distribution `.tar.gz` file,
`cd` to the root directory of the unpacked source,
then build using the included maven wrapper:

{% highlight bash %}
$ tar xvfz apache-calcite-1.23.0-src.tar.gz
$ cd apache-calcite-1.23.0-src
$ ./gradlew build
{% endhighlight %}

[Running tests](#running-tests) describes how to run more or fewer
tests.

## Building from Git

Prerequisites are git
and Java (JDK 8, 9, 10, 11, 12, 13, or 14) on your path.

Create a local copy of the github repository,
`cd` to its root directory,
then build using the included maven wrapper:

{% highlight bash %}
$ git clone git://github.com/apache/calcite.git
$ cd calcite
$ ./gradlew build
{% endhighlight %}

Calcite includes a number of machine-generated codes. By default, these are
regenerated on every build, but this has the negative side-effect of causing
a re-compilation of the entire project when the non-machine-generated code
has not changed.

Typically re-generation is called automatically when the relevant templates
are changed, and it should work transparently.
However if your IDE does not generate sources (e.g. `core/build/javacc/javaCCMain/org/apache/calcite/sql/parser/impl/SqlParserImpl.java`),
then you can call `./gradlew generateSources` tasks manually.

[Running tests](#running-tests) describes how to run more or fewer
tests.

## Gradle vs Gradle wrapper

Calcite uses Gradle wrapper to make a consistent build environment.
In the typical case you don't need to install Gradle manually, and
`./gradlew` would download the proper version for you and verify the expected checksum.

You can install Gradle manually, however please note that there might
be impedance mismatch between different versions.

For more information about Gradle, check the following links:
[Gradle five things](https://docs.gradle.org/current/userguide/what_is_gradle.html#five_things);
[Gradle multi-project builds](https://docs.gradle.org/current/userguide/intro_multi_project_builds.html).

## Running tests

The test suite will run by default when you build, unless you specify
`-x test`

{% highlight bash %}
$ ./gradlew assemble # build the artifacts
$ ./gradlew build -x test # build the artifacts, verify code style, skip tests
$ ./gradlew check # verify code style, execute tests
$ ./gradlew test # execute tests
$ ./gradlew style # update code formatting (for auto-correctable cases) and verify style
$ ./gradlew autostyleCheck checkstyleAll # report code style violations
{% endhighlight %}

You can use `./gradlew assemble` to build the artifacts and skip all tests and verifications.

There are other options that control which tests are run, and in what
environment, as follows.

* `-Dcalcite.test.db=DB` (where db is `h2`, `hsqldb`, `mysql`, or `postgresql`) allows you
  to change the JDBC data source for the test suite. Calcite's test
  suite requires a JDBC data source populated with the foodmart data
  set.
   * `hsqldb`, the default, uses an in-memory hsqldb database.
   * All others access a test virtual machine
     (see [integration tests](#running-integration-tests) below).
     `mysql` and `postgresql` might be somewhat faster than hsqldb, but you need
     to populate it (i.e. provision a VM).
* `-Dcalcite.debug` prints extra debugging information to stdout.
* `-Dcalcite.test.splunk` enables tests that run against Splunk.
  Splunk must be installed and running.
* `./gradlew testSlow` runs tests that take longer to execute. For
  example, there are tests that create virtual TPC-H and TPC-DS schemas
  in-memory and run tests from those benchmarks.

Note: tests are executed in a forked JVM, so system properties are not passed automatically
when running tests with Gradle.
By default, the build script passes the following `-D...` properties
(see `passProperty` in `build.gradle.kts`):

* `java.awt.headless`
* `junit.jupiter.execution.parallel.enabled`, default: `true`
* `junit.jupiter.execution.timeout.default`, default: `5 m`
* `user.language`, default: `TR`
* `user.country`, default: `tr`
* `calcite.**` (to enable `calcite.test.db` and others above)

## Running integration tests

For testing Calcite's external adapters, a test virtual machine should be used.
The VM includes Cassandra, Druid, H2, HSQLDB, MySQL, MongoDB, and PostgreSQL.

Test VM requires 5GiB of disk space and it takes 30 minutes to build.

Note: you can use [calcite-test-dataset](https://github.com/vlsi/calcite-test-dataset)
 to populate your own database, however it is recommended to use test VM so the test environment can be reproduced.

### VM preparation

0) Install dependencies: [Vagrant](https://www.vagrantup.com/) and [VirtualBox](https://www.virtualbox.org/)

1) Clone https://github.com/vlsi/calcite-test-dataset.git at the same level as calcite repository.
For instance:

{% highlight bash %}
code
  +-- calcite
  +-- calcite-test-dataset
{% endhighlight %}

Note: integration tests search for ../calcite-test-dataset or ../../calcite-test-dataset.
 You can specify full path via calcite.test.dataset system property.

2) Build and start the VM:

{% highlight bash %}
cd calcite-test-dataset && mvn install
{% endhighlight %}

### VM management

Test VM is provisioned by Vagrant, so regular Vagrant `vagrant up` and `vagrant halt` should be used to start and stop the VM.
The connection strings for different databases are listed in [calcite-test-dataset](https://github.com/vlsi/calcite-test-dataset) readme.

### Suggested test flow

Note: test VM should be started before you launch integration tests. Calcite itself does not start/stop the VM.

Command line:

* Executing regular unit tests (does not require external data): no change. `./gradlew test` or `./gradlew build`.
* Executing all tests, for all the DBs: `./gradlew test integTestAll`.
* Executing just tests for external DBs, excluding unit tests: `./gradlew integTestAll`
* Executing PostgreSQL JDBC tests: `./gradlew integTestPostgresql`
* Executing just MongoDB tests: `./gradlew :mongo:build`

From within IDE:

* Executing regular unit tests: no change.
* Executing MongoDB tests: run `MongoAdapterTest.java` with `calcite.integrationTest=true` system property
* Executing MySQL tests: run `JdbcTest` and `JdbcAdapterTest` with setting `-Dcalcite.test.db=mysql`
* Executing PostgreSQL tests: run `JdbcTest` and `JdbcAdapterTest` with setting `-Dcalcite.test.db=postgresql`

### Integration tests technical details

Tests with external data are executed at maven's integration-test phase.
We do not currently use pre-integration-test/post-integration-test, however we could use that in future.
The verification of build pass/failure is performed at verify phase.
Integration tests should be named `...IT.java`, so they are not picked up on unit test execution.

## Contributing

See the [developers guide]({{ site.baseurl }}/develop/#contributing).

## Getting started

See the [developers guide]({{ site.baseurl }}/develop/#getting-started).

## Setting up an IDE for contributing

### Setting up IntelliJ IDEA

Download a version of [IntelliJ IDEA](https://www.jetbrains.com/idea/) greater than (2018.X). Versions 2019.2, and
2019.3 have been tested by members of the community and appear to be stable. Older versions of IDEA may still work
without problems for Calcite sources that do not use the Gradle build (release 1.21.0 and before).

Follow the standard steps for the installation of IDEA and set up one of the JDK versions currently supported by Calcite.

Start with [building Calcite from the command line](#building-from-a-source-distribution).

Go to *File > Open...* and open up Calcite's root `build.gradle.kts` file.
When IntelliJ asks if you want to open it as a project or a file, select project.
Also, say yes when it asks if you want a new window.
IntelliJ's Gradle project importer should handle the rest.

There is a partially implemented IntelliJ code style configuration that you can import located [on GitHub](https://gist.github.com/gianm/27a4e3cad99d7b9b6513b6885d3cfcc9).
It does not do everything needed to make Calcite's style checker happy, but
it does a decent amount of it.
To import, go to *Preferences > Editor > Code Style*, click the gear next to "scheme",
then *Import Scheme > IntelliJ IDEA Code Style XML*.

Once the importer is finished, test the project setup.
For example, navigate to the method `JdbcTest.testWinAgg` with
*Navigate > Symbol* and enter `testWinAgg`. Run `testWinAgg` by right-clicking and selecting *Run* (or the equivalent keyboard shortcut).

### Setting up NetBeans

From the main menu, select *File > Open Project* and navigate to a name of the project (Calcite) with a small Gradle icon, and choose to open.
Wait for NetBeans to finish importing all dependencies.

To ensure that the project is configured successfully, navigate to the method `testWinAgg` in `org.apache.calcite.test.JdbcTest`.
Right-click on the method and select to *Run Focused Test Method*.
NetBeans will run a Maven process, and you should see in the command output window a line with
 `Running org.apache.calcite.test.JdbcTest` followed by `"BUILD SUCCESS"`.

Note: it is not clear if NetBeans automatically generates relevant sources on project import,
so you might need to run `./gradlew generateSources` before importing the project (and when you
update template parser sources, and project version)

## Tracing

To enable tracing, add the following flags to the java command line:

`-Dcalcite.debug=true`

The first flag causes Calcite to print the Java code it generates
(to execute queries) to stdout. It is especially useful if you are debugging
mysterious problems like this:

`Exception in thread "main" java.lang.ClassCastException: Integer cannot be cast to Long
  at Baz$1$1.current(Unknown Source)`

By default, Calcite uses the Log4j bindings for SLF4J. There is a provided configuration
file which outputs logging at the INFO level to the console in `core/src/test/resources/log4j.properties`.
You can modify the level for the rootLogger to increase verbosity or change the level
for a specific class if you so choose.

{% highlight properties %}
# Change rootLogger level to WARN
log4j.rootLogger=WARN, A1
# Increase level to DEBUG for RelOptPlanner
log4j.logger.org.apache.calcite.plan.RelOptPlanner=DEBUG
# Increase level to TRACE for HepPlanner
log4j.logger.org.apache.calcite.plan.hep.HepPlanner=TRACE
{% endhighlight %}

## Debugging generated classes in Intellij

Calcite uses [Janino](https://janino-compiler.github.io/janino/) to generate Java
code. The generated classes can be debugged interactively
(see [the Janino tutorial](https://janino-compiler.github.io/janino/)).

To debug generated classes, set two system properties when starting the JVM:

* `-Dorg.codehaus.janino.source_debugging.enable=true`
* `-Dorg.codehaus.janino.source_debugging.dir=C:\tmp` (This property is optional;
  if not set, Janino will create temporary files in the system's default location
  for temporary files, such as `/tmp` on Unix-based systems.)

After code is generated, either go into Intellij and mark the folder that
contains generated temporary files as a generated sources root or sources root,
or directly set the value of `org.codehaus.janino.source_debugging.dir` to an
existing source root when starting the JVM.

## CSV adapter

See the [tutorial]({{ site.baseurl }}/docs/tutorial.html).

## MongoDB adapter

First, download and install Calcite,
and <a href="https://www.mongodb.org/downloads">install MongoDB</a>.

Note: you can use MongoDB from integration test virtual machine above.

Import MongoDB's zipcode data set into MongoDB:

{% highlight bash %}
$ curl -o /tmp/zips.json https://media.mongodb.org/zips.json
$ mongoimport --db test --collection zips --file /tmp/zips.json
Tue Jun  4 16:24:14.190 check 9 29470
Tue Jun  4 16:24:14.469 imported 29470 objects
{% endhighlight %}

Log into MongoDB to check it's there:

{% highlight bash %}
$ mongo
MongoDB shell version: 2.4.3
connecting to: test
> db.zips.find().limit(3)
{ "city" : "ACMAR", "loc" : [ -86.51557, 33.584132 ], "pop" : 6055, "state" : "AL", "_id" : "35004" }
{ "city" : "ADAMSVILLE", "loc" : [ -86.959727, 33.588437 ], "pop" : 10616, "state" : "AL", "_id" : "35005" }
{ "city" : "ADGER", "loc" : [ -87.167455, 33.434277 ], "pop" : 3205, "state" : "AL", "_id" : "35006" }
> exit
bye
{% endhighlight %}

Connect using the
[mongo-model.json]({{ site.sourceRoot }}/mongodb/src/test/resources/mongo-model.json)
Calcite model:

{% highlight bash %}
$ ./sqlline
sqlline> !connect jdbc:calcite:model=mongodb/src/test/resources/mongo-model.json admin admin
Connecting to jdbc:calcite:model=mongodb/src/test/resources/mongo-model.json
Connected to: Calcite (version 1.x.x)
Driver: Calcite JDBC Driver (version 1.x.x)
Autocommit status: true
Transaction isolation: TRANSACTION_REPEATABLE_READ
sqlline> !tables
+------------+--------------+-----------------+---------------+
| TABLE_CAT  | TABLE_SCHEM  |   TABLE_NAME    |  TABLE_TYPE   |
+------------+--------------+-----------------+---------------+
| null       | mongo_raw    | zips            | TABLE         |
| null       | mongo_raw    | system.indexes  | TABLE         |
| null       | mongo        | ZIPS            | VIEW          |
| null       | metadata     | COLUMNS         | SYSTEM_TABLE  |
| null       | metadata     | TABLES          | SYSTEM_TABLE  |
+------------+--------------+-----------------+---------------+
sqlline> select count(*) from zips;
+---------+
| EXPR$0  |
+---------+
| 29467   |
+---------+
1 row selected (0.746 seconds)
sqlline> !quit
Closing: org.apache.calcite.jdbc.FactoryJdbc41$CalciteConnectionJdbc41
$
{% endhighlight %}

## Splunk adapter

To run the test suite and sample queries against Splunk,
load Splunk's `tutorialdata.zip` data set as described in
<a href="https://docs.splunk.com/Documentation/Splunk/6.0.2/PivotTutorial/GetthetutorialdataintoSplunk">the Splunk tutorial</a>.

(This step is optional, but it provides some interesting data for the sample
queries. It is also necessary if you intend to run the test suite, using
`-Dcalcite.test.splunk=true`.)

## Implementing an adapter

New adapters can be created by implementing `CalcitePrepare.Context`:

{% highlight java %}
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;

public class AdapterContext implements CalcitePrepare.Context {
  @Override
  public JavaTypeFactory getTypeFactory() {
    // adapter implementation
    return typeFactory;
  }

  @Override
  public CalciteSchema getRootSchema() {
    // adapter implementation
    return rootSchema;
  }
}
{% endhighlight %}

### Testing adapter in Java

The example below shows how SQL query can be submitted to
`CalcitePrepare` with a custom context (`AdapterContext` in this
case). Calcite prepares and implements the query execution, using the
resources provided by the `Context`. `CalcitePrepare.PrepareResult`
provides access to the underlying enumerable and methods for
enumeration. The enumerable itself can naturally be some adapter
specific implementation.

{% highlight java %}
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.junit.Test;

public class AdapterContextTest {
  @Test
  public void testSelectAllFromTable() {
    AdapterContext ctx = new AdapterContext();
    String sql = "SELECT * FROM TABLENAME";
    Class elementType = Object[].class;
    CalcitePrepare.PrepareResult<Object> prepared =
        new CalcitePrepareImpl().prepareSql(ctx, sql, null, elementType, -1);
    Object enumerable = prepared.getExecutable();
    // etc.
  }
}
{% endhighlight %}

# Advanced topics for developers

The following sections might be of interest if you are adding features
to particular parts of the code base. You don't need to understand
these topics if you are just building from source and running tests.

## JavaTypeFactory

When Calcite compares types (instances of `RelDataType`), it requires them to be the same
object. If there are two distinct type instances that refer to the
same Java type, Calcite may fail to recognize that they match.  It is
recommended to:

* Use a single instance of `JavaTypeFactory` within the calcite context;
* Store the types so that the same object is always returned for the same type.

## Rebuilding generated Protocol Buffer code

Calcite's Avatica Server component supports RPC serialization
using [Protocol Buffers](https://developers.google.com/protocol-buffers/).
In the context of Avatica, Protocol Buffers can
generate a collection of messages defined by a schema. The library
itself can parse old serialized messages using a
new schema. This is highly desirable in an environment where the
client and server are not guaranteed to have the same version of
objects.

Typically, the code generated by the Protocol Buffers library doesn't
need to be re-generated only every build, only when the schema changes.

First, install Protobuf 3.0:

{% highlight bash %}
$ wget https://github.com/google/protobuf/releases/download/v3.0.0-beta-1/protobuf-java-3.0.0-beta-1.tar.gz
$ tar xf protobuf-java-3.0.0-beta-1.tar.gz && cd protobuf-3.0.0-beta-1
$ ./configure
$ make
$ sudo make install
{% endhighlight %}

Then, re-generate the compiled code:

{% highlight bash %}
$ cd avatica/core
$ ./src/main/scripts/generate-protobuf.sh
{% endhighlight %}

# Advanced topics for committers

The following sections are of interest to Calcite committers and in
particular release managers.

## Merging pull requests (for Calcite committers)

These are instructions for a Calcite committer who has reviewed a pull request
from a contributor, found it satisfactory, and is about to merge it to master.
Usually the contributor is not a committer (otherwise they would be committing
it themselves, after you gave approval in a review).

There are certain kinds of continuous integration tests that are not run
automatically against the PR. These tests can be triggered explicitly by adding
an appropriate label to the PR. For instance, you can run slow tests by adding
the `slow-tests-needed` label. It is up to you to decide if these additional
tests need to run before merging.

If the PR has multiple commits, squash them into a single commit. The
commit message should follow the conventions outined in
[contribution guidelines]({{ site.baseurl }}/develop/#contributing).
If there are conflicts it is better to ask the contributor to take this step,
otherwise it is preferred to do this manually since it saves time and also
avoids unnecessary notification messages to many people on GitHub.

If the contributor is not a committer, add their name in parentheses at the end
of the first line of the commit message.

If the merge is performed via command line (not through the GitHub web
interface), make sure the message contains a line "Close apache/calcite#YYY",
where YYY is the GitHub pull request identifier.

When the PR has been merged and pushed, be sure to update the JIRA case. You
must:
 * resolve the issue (do not close it as this will be done by the release
manager);
 * select "Fixed" as resolution cause;
 * mark the appropriate version (e.g., 1.23.0) in the "Fix version" field;
 * add a comment (e.g., "Fixed in ...") with a hyperlink pointing to the commit
which resolves the issue (in GitHub or GitBox), and also thank the contributor
for their contribution.

## Set up PGP signing keys (for Calcite committers)

Follow instructions [here](https://www.apache.org/dev/release-signing) to
create a key pair. (On macOS, I did `brew install gpg` and
`gpg --gen-key`.)

Add your public key to the
[`KEYS`](https://dist.apache.org/repos/dist/release/calcite/KEYS)
file by following instructions in the `KEYS` file.
(The `KEYS` file is not present in the git repo or in a release tar
ball because that would be
[redundant](https://issues.apache.org/jira/browse/CALCITE-1746).)

## Set up Nexus repository credentials (for Calcite committers)

Gradle provides multiple ways to [configure project properties](https://docs.gradle.org/current/userguide/build_environment.html#sec:gradle_configuration_properties).
For instance, you could update `$HOME/.gradle/gradle.properties`.

Note: the build script would print the missing properties, so you can try running it and let it complain on the missing ones.

The following options are used:

{% highlight properties %}
asfCommitterId=
asfNexusUsername=
asfNexusPassword=
asfSvnUsername=
asfSvnPassword=
{% endhighlight %}

Note: when https://github.com/vlsi/asflike-release-environment is used, the credentials are takend from
`asfTest...` (e.g. `asfTestNexusUsername=test`)

Note: if you want to uses `gpg-agent`, you need to pass `useGpgCmd` property, and specify the key id
via `signing.gnupg.keyName`.

## Making a snapshot (for Calcite committers)

Before you start:

* Make sure you are using JDK 8.
* Make sure build and tests succeed with `-Dcalcite.test.db=hsqldb` (the default)

{% highlight bash %}
# Make sure that there are no junk files in the sandbox
git clean -xn
# Publish snapshot artifacts
./gradlew clean publish -Pasf
{% endhighlight %}

## Making a release candidate (for Calcite committers)

Note: release artifacts (dist.apache.org and repository.apache.org) are managed with
[stage-vote-release-plugin](https://github.com/vlsi/vlsi-release-plugins/tree/master/plugins/stage-vote-release-plugin)

Before you start:

* Set up signing keys as described above.
* Make sure you are using JDK 8 (not 9 or 10).
* Check that `README` and `site/_docs/howto.md` have the correct version number.
* Check that `NOTICE` has the current copyright year.
* Check that `calcite.version` has the proper value in `/gradle.properties`.
* Make sure build and tests succeed
* Make sure that `./gradlew javadoc` succeeds
  (i.e. gives no errors; warnings are OK)
* Generate a report of vulnerabilities that occur among dependencies,
  using `./gradlew dependencyCheckUpdate dependencyCheckAggregate`.
  Report to [private@calcite.apache.org](mailto:private@calcite.apache.org)
  if new critical vulnerabilities are found among dependencies.
* Decide the supported configurations of JDK, operating system and
  Guava.  These will probably be the same as those described in the
  release notes of the previous release.  Document them in the release
  notes.  To test Guava version _x.y_, specify `-Pguava.version=x.y`
* Optional tests using properties:
  * `-Dcalcite.test.db=mysql`
  * `-Dcalcite.test.db=hsqldb`
  * `-Dcalcite.test.mongodb`
  * `-Dcalcite.test.splunk`
* Optional tests using tasks:
  * `./gradlew testSlow`
* Trigger a
  <a href="https://scan.coverity.com/projects/julianhyde-calcite">Coverity scan</a>
  by merging the latest code into the `julianhyde/coverity_scan` branch,
  and when it completes, make sure that there are no important issues.
* Add release notes to `site/_docs/history.md`. Include the commit history,
  and say which versions of Java, Guava and operating systems the release is
  tested against.
* Make sure that
  <a href="https://issues.apache.org/jira/issues/?jql=project%20%3D%20CALCITE%20AND%20status%20%3D%20Resolved%20and%20fixVersion%20is%20null">
  every "resolved" JIRA case</a> (including duplicates) has
  a fix version assigned (most likely the version we are
  just about to release)

Smoke-test `sqlline` with Spatial and Oracle function tables:

{% highlight sql %}
$ ./sqlline
> !connect jdbc:calcite:fun=spatial,oracle "sa" ""
SELECT NVL(ST_Is3D(ST_PointFromText('POINT(-71.064544 42.28787)')), TRUE);
+--------+
| EXPR$0 |
+--------+
| false  |
+--------+
1 row selected (0.039 seconds)
> !quit
{% endhighlight %}

The release candidate process does not add commits,
so there's no harm if it fails. It might leave `-rc` tag behind
which can be removed if required.

You can perform a dry-run release with a help of https://github.com/vlsi/asflike-release-environment
That would perform the same steps, however it would push changes to the mock Nexus, Git, and SVN servers.

If any of the steps fail, fix the problem, and
start again from the top.

### To prepare a release candidate directly in your environment:

Pick a release candidate index and ensure it does not interfere with previous candidates for the version.

{% highlight bash %}
# Tell GPG how to read a password from your terminal
export GPG_TTY=$(tty)

# Make sure that there are no junk files in the sandbox
git clean -xn

# Dry run the release candidate (push to asf-like-environment)
./gradlew prepareVote -Prc=1

# Push release candidate to ASF servers
./gradlew prepareVote -Prc=1 -Pasf
{% endhighlight %}

#### Checking the artifacts

* In the `release/build/distributions` directory should be these 3 files, among others:
  * `apache-calcite-X.Y.Z-src.tar.gz`
  * `apache-calcite-X.Y.Z-src.tar.gz.asc`
  * `apache-calcite-X.Y.Z-src.tar.gz.sha256`
* Note that the file names start `apache-calcite-`.
* In the source distro `.tar.gz` (currently there is
  no binary distro), check that all files belong to a directory called
  `apache-calcite-X.Y.Z-src`.
* That directory must contain files `NOTICE`, `LICENSE`,
  `README`, `README.md`
  * Check that the version in `README` is correct
  * Check that the copyright year in `NOTICE` is correct
* Make sure that there is no `KEYS` file in the source distros
* In each .jar (for example
  `core/build/libs/calcite-core-X.Y.Z.jar` and
  `mongodb/build/libs/calcite-mongodb-X.Y.Z-sources.jar`), check
  that the `META-INF` directory contains `LICENSE`,
  `NOTICE`
* Check PGP, per [this](https://httpd.apache.org/dev/verification.html)

Verify the staged artifacts in the Nexus repository:

* Go to [https://repository.apache.org/](https://repository.apache.org/) and login
* Under `Build Promotion`, click `Staging Repositories`
* In the `Staging Repositories` tab there should be a line with profile `org.apache.calcite`
* Navigate through the artifact tree and make sure the .jar, .pom, .asc files are present
* Check the box on in the first column of the row,
  and press the 'Close' button to publish the repository at
  https://repository.apache.org/content/repositories/orgapachecalcite-1000
  (or a similar URL)

## Cleaning up after a failed release attempt (for Calcite committers)

If something is not correct, you can fix it, commit it, and prepare the next candidate.
The release candidate tags might be kept for a while.

## Validate a release

{% highlight bash %}
# Check that the signing key (e.g. DDB6E9812AD3FAE3) is pushed
gpg --recv-keys key

# Check keys
curl -O https://dist.apache.org/repos/dist/release/calcite/KEYS

# Sign/check sha256 hashes
# (Assumes your O/S has a 'shasum' command.)
function checkHash() {
  cd "$1"
  for i in *.{pom,gz}; do
    if [ ! -f $i ]; then
      continue
    fi
    if [ -f $i.sha256 ]; then
      if [ "$(cat $i.sha256)" = "$(shasum -a 256 $i)" ]; then
        echo $i.sha256 present and correct
      else
        echo $i.sha256 does not match
      fi
    else
      shasum -a 256 $i > $i.sha256
      echo $i.sha256 created
    fi
  done
}
checkHash apache-calcite-X.Y.Z-rcN
{% endhighlight %}

## Get approval for a release via Apache voting process (for Calcite committers)

Release vote on dev list
Note: the draft mail is printed as the final step of `prepareVote` task,
and you can find the draft in `/build/prepareVote/mail.txt`

{% highlight text %}
To: dev@calcite.apache.org
Subject: [VOTE] Release apache-calcite-X.Y.Z (release candidate N)

Hi all,

I have created a build for Apache Calcite X.Y.Z, release candidate N.

Thanks to everyone who has contributed to this release.
<Further details about release.> You can read the release notes here:
https://github.com/apache/calcite/blob/XXXX/site/_docs/history.md

The commit to be voted upon:
https://gitbox.apache.org/repos/asf?p=calcite.git;a=commit;h=NNNNNN

Its hash is XXXX.

The artifacts to be voted on are located here:
https://dist.apache.org/repos/dist/dev/calcite/apache-calcite-X.Y.Z-rcN/

The hashes of the artifacts are as follows:
src.tar.gz.sha256 XXXX

A staged Maven repository is available for review at:
https://repository.apache.org/content/repositories/orgapachecalcite-NNNN

Release artifacts are signed with the following key:
https://people.apache.org/keys/committer/jhyde.asc

Please vote on releasing this package as Apache Calcite X.Y.Z.

The vote is open for the next 72 hours and passes if a majority of
at least three +1 PMC votes are cast.

[ ] +1 Release this package as Apache Calcite X.Y.Z
[ ]  0 I don't feel strongly about it, but I'm okay with the release
[ ] -1 Do not release this package because...


Here is my vote:

+1 (binding)

Julian
{% endhighlight %}

After vote finishes, send out the result:

{% highlight text %}
Subject: [RESULT] [VOTE] Release apache-calcite-X.Y.Z (release candidate N)
To: dev@calcite.apache.org

Thanks to everyone who has tested the release candidate and given
their comments and votes.

The tally is as follows.

N binding +1s:
<names>

N non-binding +1s:
<names>

No 0s or -1s.

Therefore I am delighted to announce that the proposal to release
Apache Calcite X.Y.Z has passed.

Thanks everyone. We’ll now roll the release out to the mirrors.

There was some feedback during voting. I shall open a separate
thread to discuss.


Julian
{% endhighlight %}

Use the [Apache URL shortener](https://s.apache.org) to generate
shortened URLs for the vote proposal and result emails. Examples:
[s.apache.org/calcite-1.2-vote](https://s.apache.org/calcite-1.2-vote) and
[s.apache.org/calcite-1.2-result](https://s.apache.org/calcite-1.2-result).


## Publishing a release (for Calcite committers)

After a successful release vote, we need to push the release
out to mirrors, and other tasks.

Choose a release date.
This is based on the time when you expect to announce the release.
This is usually a day after the vote closes.
Remember that UTC date changes at 4pm Pacific time.


### Publishing directly in your environment:

{% highlight bash %}
# Dry run publishing the release (push to asf-like-environment)
./gradlew publishDist -Prc=1

# Publish the release to ASF servers
./gradlew publishDist -Prc=1 -Pasf
{% endhighlight %}

Svnpubsub will publish to the
[release repo](https://dist.apache.org/repos/dist/release/calcite) and propagate to the
[mirrors](https://www.apache.org/dyn/closer.cgi/calcite) within 24 hours.

If there are now more than 2 releases, clear out the oldest ones:

{% highlight bash %}
cd ~/dist/release/calcite
svn rm apache-calcite-X.Y.Z
svn ci
{% endhighlight %}

The old releases will remain available in the
[release archive](https://archive.apache.org/dist/calcite/).

You should receive an email from the [Apache Reporter Service](https://reporter.apache.org/).
Make sure to add the version number and date of the latest release at the site linked to in the email.

Update the site with the release note, the release announcement, and the javadoc of the new version.
The javadoc can be generated only from a final version (not a SNAPSHOT) so checkout the most recent
tag and start working there (`git checkout calcite-X.Y.Z`). Add a release announcement by copying
[site/_posts/2016-10-12-release-1.10.0.md]({{ site.sourceRoot }}/site/_posts/2016-10-12-release-1.10.0.md).
Generate the javadoc, and [preview](http://localhost:4000/news/) the site by following the
instructions in [site/README.md]({{ site.sourceRoot }}/site/README.md). Check that the announcement,
javadoc, and release note appear correctly and then publish the site following the instructions
in the same file. Now checkout again the release branch (`git checkout branch-X.Y`) and commit
the release announcement.

Merge the release branch back into `master` (e.g., `git merge --ff-only branch-X.Y`) and align
the `master` with the `site` branch (e.g., `git merge --ff-only site`).

In JIRA, search for
[all issues resolved in this release](https://issues.apache.org/jira/issues/?jql=project%20%3D%20CALCITE%20and%20fixVersion%20%3D%201.5.0%20and%20status%20%3D%20Resolved%20and%20resolution%20%3D%20Fixed),
and do a bulk update changing their status to "Closed",
with a change comment
"Resolved in release X.Y.Z (YYYY-MM-DD)"
(fill in release number and date appropriately).
Uncheck "Send mail for this update". Under the [releases tab](https://issues.apache.org/jira/projects/CALCITE?selectedItem=com.atlassian.jira.jira-projects-plugin%3Arelease-page&status=released-unreleased)
of the Calcite project mark the release X.Y.Z as released. If it does not already exist create also
a new version (e.g., X.Y+1.Z) for the next release.

After 24 hours, announce the release by sending an email to
[announce@apache.org](https://mail-archives.apache.org/mod_mbox/www-announce/).
You can use
[the 1.20.0 announcement](https://mail-archives.apache.org/mod_mbox/www-announce/201906.mbox/%3CCA%2BEpF8tcJcZ41rVuwJODJmyRy-qAxZUQm9OxKsoDi07c2SKs_A%40mail.gmail.com%3E)
as a template. Be sure to include a brief description of the project.

## Publishing the web site (for Calcite committers)
{: #publish-the-web-site}

See instructions in
[site/README.md]({{ site.sourceRoot }}/site/README.md).
