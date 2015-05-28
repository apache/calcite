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
# Calcite HOWTO

Here's some miscellaneous documentation about using Calcite and its various
adapters.

## Building from a source distribution

Prerequisites are maven (3.2.1 or later)
and Java (JDK 1.7 or later, 1.8 preferred) on your path.

Unpack the source distribution `.tar.gz` or `.zip` file,
`cd` to the root directory of the unpacked source,
then build using maven:

```bash
$ tar xvfz calcite-1.3.0-incubating-source.tar.gz
$ cd calcite-1.3.0-incubating
$ mvn install
```

[Running tests](howto.md#running-tests) describes how to run more or fewer
tests.

## Building from git

Prerequisites are git, maven (3.2.1 or later)
and Java (JDK 1.7 or later, 1.8 preferred) on your path.

Create a local copy of the github repository,
`cd` to its root directory,
then build using maven:

```bash
$ git clone git://github.com/apache/incubator-calcite.git
$ cd incubator-calcite
$ mvn install
```

[Running tests](howto.md#running-tests) describes how to run more or fewer
tests.

## Running tests

The test suite will run by default when you build, unless you specify
`-DskipTests`:

```bash
$ mvn clean # Note: mvn clean install does not work, use mvn clean && mvn install
$ mvn -DskipTests install
```

There are other options that control which tests are run, and in what
environment, as follows.

* `-Dcalcite.test.db=DB` (where db is `h2`, `hsqldb`, `mysql`, or `postgresql`) allows you
  to change the JDBC data source for the test suite. Calcite's test
  suite requires a JDBC data source populated with the foodmart data
  set.
   * `hsqldb`, the default, uses an in-memory hsqldb database.
   * all others access test virtual machine (see [integration tests](howto.md#running-integration-tests) below)
     `mysql` and `postgresql` might be somewhat faster than hsqldb, but you need to populate it (i.e. provision a VM).
* `-Dcalcite.debug` prints extra debugging information to stdout.
* `-Dcalcite.test.slow` enables tests that take longer to execute. For
  example, there are tests that create virtual TPC-H and TPC-DS schemas
  in-memory and run tests from those benchmarks.
* `-Dcalcite.test.splunk=true` enables tests that run against Splunk.
  Splunk must be installed and running.

## Running integration tests

For testing Calcite's external adapters, a test virtual machine should be used.
The VM includes H2, HSQLDB, MySQL, MongoDB, and PostgreSQL.

Test VM requires 5GiB of disk space and it takes 30 minutes to build.

Note: you can use [calcite-test-dataset](https://github.com/vlsi/calcite-test-dataset)
 to populate your own database, however it is recommended to use test VM so the test environment can be reproduced.

### VM preparation

0) Install dependencies: [Vagrant](https://www.vagrantup.com/) and [VirtualBox](https://www.virtualbox.org/)

1) Clone https://github.com/vlsi/calcite-test-dataset.git at the same level as calcite repository.
For instance:
```bash
code
  +-- calcite
  +-- calcite-test-dataset
```

Note: integration tests search for ../calcite-test-dataset or ../../calcite-test-dataset.
 You can specify full path via calcite.test.dataset system property.

2) Build and start the VM:
```bash
cd calcite-test-dataset && mvn install
```

### VM management

Test VM is provisioned by Vagrant, so regular Vagrant `vagrant up` and `vagrant halt` should be used to start and stop the VM.
The connection strings for different databases are listed in [calcite-test-dataset](https://github.com/vlsi/calcite-test-dataset) readme.

### Suggested test flow

Note: test VM should be started before you launch integration tests. Calcite itself does not start/stop the VM.

Command line:
* Executing regular unit tests (does not require external data): no change. `mvn test` or `mvn install`.
* Executing all tests, for all the DBs: `mvn verify -Pit`. `it` stands for "integration-test". `mvn install -Pit` works as well.
* Executing just tests for external DBs, excluding unit tests: `mvn -Dtest=foo -DfailIfNoTests=false -Pit verify`
* Executing just MongoDB tests: `cd mongo; mvn verify -Pit`

From within IDE:
* Executing regular unit tests: no change.
* Executing MongoDB tests: run `MongoAdapterIT.java` as usual (no additional properties are required)
* Executing MySQL tests: run `JdbcTest` and `JdbcAdapterTest` with setting `-Dcalcite.test.db=mysql`
* Executing PostgreSQL tests: run `JdbcTest` and `JdbcAdapterTest` with setting `-Dcalcite.test.db=postgresql`

### Integration tests technical details

Tests with external data are executed at maven's integration-test phase.
We do not currently use pre-integration-test/post-integration-test, however we could use that in future.
The verification of build pass/failure is performed at verify phase.
Integration tests should be named `...IT.java`, so they are not picked up on unit test execution.

## Contributing

We welcome contributions.

If you are planning to make a large contribution, talk to us first! It
helps to agree on the general approach. Log a
[JIRA case](https://issues.apache.org/jira/browse/CALCITE) for your
proposed feature or start a discussion on the dev list.

Fork the github repository, and create a branch for your feature.

Develop your feature and test cases, and make sure that `mvn
install` succeeds. (Run extra tests if your change warrants it.)

Commit your change to your branch, and use a comment that starts with
the JIRA case number, like this:

```
[CALCITE-345] AssertionError in RexToLixTranslator comparing to date literal
```

If your change had multiple commits, use `git rebase -i master` to
combine them into a single commit, and to bring your code up to date
with the latest on the main line.

Then push your commit(s) to github, and create a pull request from
your branch to the incubator-calcite master branch. Update the JIRA case
to reference your pull request, and a committer will review your
changes.

## Getting started

Calcite is a community, so the first step to joining the project is to introduce yourself.
Join the [developers list](http://mail-archives.apache.org/mod_mbox/incubator-calcite-dev/)
and send an email.

If you have the chance to attend a [meetup](http://www.meetup.com/Apache-Calcite/),
or meet [members of the community](http://calcite.incubator.apache.org/team-list.html)
at a conference, that's also great.

Choose an initial task to work on. It should be something really simple,
such as a bug fix or a [Jira task that we have labeled
"newbie"](https://issues.apache.org/jira/issues/?jql=labels%20%3D%20newbie%20%26%20project%20%3D%20Calcite%20%26%20status%20%3D%20Open).
Follow the [contributing guidelines](#contributing) to get your change committed.

After you have made several useful contributions we may
[invite you to become a committer](https://community.apache.org/contributors/).
We value all contributions that help to build a vibrant community, not just code.
You can contribute by testing the code, helping verify a release,
writing documentation or the web site,
or just by answering questions on the list.

## Tracing

To enable tracing, add the following flags to the java command line:

```
-Dcalcite.debug=true -Djava.util.logging.config.file=core/src/test/resources/logging.properties
```

The first flag causes Calcite to print the Java code it generates
(to execute queries) to stdout. It is especially useful if you are debugging
mysterious problems like this:

```
Exception in thread "main" java.lang.ClassCastException: Integer cannot be cast to Long
  at Baz$1$1.current(Unknown Source)
```

The second flag specifies a config file for
the <a href="http://docs.oracle.com/javase/7/docs/api/java/util/logging/package-summary.html">java.util.logging</a>
framework. Put the following into core/src/test/resources/logging.properties:

```properties
handlers= java.util.logging.ConsoleHandler
.level= INFO
org.apache.calcite.plan.RelOptPlanner.level=FINER
java.util.logging.ConsoleHandler.level=ALL
```

The line `org.apache.calcite.plan.RelOptPlanner.level=FINER` tells the planner to produce
fairly verbose output. You can modify the file to enable other loggers, or to change levels.
For instance, if you change `FINER` to `FINEST` the planner will give you an account of the
planning process so detailed that it might fill up your hard drive.

## CSV adapter

See the <a href="tutorial.md">tutorial</a>.

## MongoDB adapter

First, download and install Calcite,
and <a href="http://www.mongodb.org/downloads">install MongoDB</a>.

Note: you can use MongoDB from integration test virtual machine above.

Import MongoDB's zipcode data set into MongoDB:

```bash
$ curl -o /tmp/zips.json http://media.mongodb.org/zips.json
$ mongoimport --db test --collection zips --file /tmp/zips.json
Tue Jun  4 16:24:14.190 check 9 29470
Tue Jun  4 16:24:14.469 imported 29470 objects
```

Log into MongoDB to check it's there:

```bash
$ mongo
MongoDB shell version: 2.4.3
connecting to: test
> db.zips.find().limit(3)
{ "city" : "ACMAR", "loc" : [ -86.51557, 33.584132 ], "pop" : 6055, "state" : "AL", "_id" : "35004" }
{ "city" : "ADAMSVILLE", "loc" : [ -86.959727, 33.588437 ], "pop" : 10616, "state" : "AL", "_id" : "35005" }
{ "city" : "ADGER", "loc" : [ -87.167455, 33.434277 ], "pop" : 3205, "state" : "AL", "_id" : "35006" }
> exit
bye
```

Connect using the
<a href="https://github.com/apache/incubator-calcite/blob/master/mongodb/src/test/resources/mongo-zips-model.json">mongo-zips-model.json</a>
Calcite model:
```bash
$ ./sqlline
sqlline> !connect jdbc:calcite:model=mongodb/target/test-classes/mongo-zips-model.json admin admin
Connecting to jdbc:calcite:model=mongodb/target/test-classes/mongo-zips-model.json
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
```

## Splunk adapter

To run the test suite and sample queries against Splunk,
load Splunk's `tutorialdata.zip` data set as described in
<a href="http://docs.splunk.com/Documentation/Splunk/6.0.2/PivotTutorial/GetthetutorialdataintoSplunk">the Splunk tutorial</a>.

(This step is optional, but it provides some interesting data for the sample
queries. It is also necessary if you intend to run the test suite, using
`-Dcalcite.test.splunk=true`.)

## Implementing an adapter

New adapters can be created by implementing `CalcitePrepare.Context`:

```java
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteRootSchema;

public class AdapterContext implements CalcitePrepare.Context {
  @Override
  public JavaTypeFactory getTypeFactory() {
    // adapter implementation
    return typeFactory;
  }

  @Override
  public CalciteRootSchema getRootSchema() {
    // adapter implementation
    return rootSchema;
  }
}
```

### Testing adapter in Java

The example below shows how SQL query can be submitted to
`CalcitePrepare` with a custom context (`AdapterContext` in this
case). Calcite prepares and implements the query execution, using the
resources provided by the `Context`. `CalcitePrepare.PrepareResult`
provides access to the underlying enumerable and methods for
enumeration. The enumerable itself can naturally be some adapter
specific implementation.

```java
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
```

### JavaTypeFactory

When Calcite compares types (instances of `RelDataType`), it requires them to be the same
object. If there are two distinct type instances that refer to the
same Java type, Calcite may fail to recognize that they match.  It is
recommended to:
* Use a single instance of `JavaTypeFactory` within the calcite context;
* Store the types so that the same object is always returned for the same type.

## Set up PGP signing keys (for Calcite committers)

Follow instructions at http://www.apache.org/dev/release-signing to
create a key pair. (On Mac OS X, I did `brew install gpg` and `gpg
--gen-key`.)

Add your public key to the `KEYS` file by following instructions in
the `KEYS` file.

## Making a snapshot (for Calcite committers)

Before you start:
* Set up signing keys as described above.
* Make sure you are using JDK 1.7 (not 1.8).
* Make sure build and tests succeed with `-Dcalcite.test.db=hsqldb` (the default)

```bash
# Set passphrase variable without putting it into shell history
read -s GPG_PASSPHRASE

# Make sure that there are no junk files in the sandbox
git clean -xn
mvn clean

mvn -Papache-release -Dgpg.passphrase=${GPG_PASSPHRASE} install
```

When the dry-run has succeeded, change `install` to `deploy`.

## Making a release (for Calcite committers)

Before you start:
* Set up signing keys as described above.
* Make sure you are using JDK 1.7 (not 1.8).
* Check that `README`, `README.md` and `doc/howto.md` have the correct version number.
* Set `version.major` and `version.minor` in `pom.xml`.
* Make sure build and tests succeed, including with
  -Dcalcite.test.db={mysql,hsqldb}, -Dcalcite.test.slow=true,
  -Dcalcite.test.mongodb=true, -Dcalcite.test.splunk=true.
* Trigger a
  <a href="https://scan.coverity.com/projects/2966">Coverity scan</a>
  by merging the latest code into the `julianhyde/coverity_scan` branch,
  and when it completes, make sure that there are no important issues.
* Make sure that
  <a href="https://issues.apache.org/jira/issues/?jql=project%20%3D%20CALCITE%20AND%20status%20%3D%20Resolved%20and%20fixVersion%20is%20null">
  every "resolved" JIRA case</a> (including duplicates) has
  a fix version assigned (most likely the version we are
  just about to release)

Create a release branch named after the release, e.g. `branch-1.1`, and push it to Apache.

```bash
$ git checkout -b branch-X.Y
$ git push -u origin branch-X.Y
```

We will use the branch for the entire the release process. Meanwhile,
we do not allow commits to the master branch. After the release is
final, we can use `git merge --ff-only` to append the changes on the
release branch onto the master branch. (Apache does not allow reverts
to the master branch, which makes it difficult to clean up the kind of
messy commits that inevitably happen while you are trying to finalize
a release.)

Now, set up your environment and do a dry run. The dry run will not
commit any changes back to git and gives you the opportunity to verify
that the release process will complete as expected.

If any of the steps fail, clean up (see below), fix the problem, and
start again from the top.

```bash
# Set passphrase variable without putting it into shell history
read -s GPG_PASSPHRASE

# Make sure that there are no junk files in the sandbox
git clean -xn
mvn clean

# Do a dry run of the release:prepare step, which sets version numbers.
mvn -DdryRun=true -DskipTests -DreleaseVersion=X.Y.Z-incubating -DdevelopmentVersion=X.Y.Z+1-incubating-SNAPSHOT -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE}" release:prepare 2>&1 | tee /tmp/prepare-dry.log
```

Check the artifacts:
* In the `target` directory should be these 8 files, among others:
  * apache-calcite-X.Y.Z-incubating-src.tar.gz
  * apache-calcite-X.Y.Z-incubating-src.tar.gz.asc
  * apache-calcite-X.Y.Z-incubating-src.tar.gz.md5
  * apache-calcite-X.Y.Z-incubating-src.tar.gz.sha1
  * apache-calcite-X.Y.Z-incubating-src.zip
  * apache-calcite-X.Y.Z-incubating-src.zip.asc
  * apache-calcite-X.Y.Z-incubating-src.zip.md5
  * apache-calcite-X.Y.Z-incubating-src.zip.sha1
* Note that the file names start `apache-calcite-` and include
  `incubating` in the version.
* In the two source distros `.tar.gz` and `.zip` (currently there is
  no binary distro), check that all files belong to a directory called
  `apache-calcite-X.Y.Z-incubating-src`.
* That directory must contain files `DISCLAIMER`, `NOTICE`, `LICENSE`,
  `README`, `README.md`
  * Check that the version in `README` is correct
* In each .jar (for example
  `core/target/calcite-core-X.Y.Z-incubating.jar` and
  `mongodb/target/calcite-mongodb-X.Y.Z-incubating-sources.jar`), check
  that the `META-INF` directory contains `DEPENDENCIES`, `LICENSE`,
  `NOTICE` and `git.properties`
* In each .jar, check that `org-apache-calcite-jdbc.properties` is
  present and does not contain un-substituted `${...}` variables
* Check PGP, per https://httpd.apache.org/dev/verification.html

Now, remove the `-DdryRun` flag and run the release for real.

```bash
# Prepare sets the version numbers, creates a tag, and pushes it to git.
mvn -DdryRun=false -DskipTests -DreleaseVersion=X.Y.Z-incubating -DdevelopmentVersion=X.Y.Z+1-incubating-SNAPSHOT -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE}" release:prepare 2>&1 | tee /tmp/prepare.log

# Perform checks out the tagged version, builds, and deploys to the staging repository
mvn -DskipTests -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE}" release:perform 2>&1 | tee /tmp/perform.log
```

Verify the staged artifacts in the Nexus repository:
* Go to https://repository.apache.org/
* Under `Build Promotion`, click `Staging Repositories`
* In the `Staging Repositories` tab there should be a line with profile `org.apache.calcite`
* Navigate through the artifact tree and make sure the .jar, .pom, .asc files are present
* Check the box on in the first column of the row,
  and press the 'Close' button to publish the repository at
  https://repository.apache.org/content/repositories/orgapachecalcite-1000
  (or a similar URL)

Upload the artifacts via subversion to a staging area,
https://dist.apache.org/repos/dist/dev/incubator/calcite/apache-calcite-X.Y.Z-incubating-rcN:

```bash
# Create a subversion workspace, if you haven't already
mkdir -p ~/dist/dev
pushd ~/dist/dev
svn co https://dist.apache.org/repos/dist/dev/incubator/calcite
popd

# Move the files into a directory
cd target
mkdir ~/dist/dev/calcite/apache-calcite-X.Y.Z-incubating-rcN
mv apache-calcite-* ~/dist/dev/calcite/apache-calcite-X.Y.Z-incubating-rcN

# Check in
cd ~/dist/dev/calcite
svn add apache-calcite-X.Y.Z-incubating-rcN
svn ci
```

## Cleaning up after a failed release attempt (for Calcite committers)

```
# Make sure that the tag you are about to generate does not already
# exist (due to a failed release attempt)
git tag

# If the tag exists, delete it locally and remotely
git tag -d apache-calcite-X.Y.Z-incubating
git push origin :refs/tags/apache-calcite-X.Y.Z-incubating

# Remove modified files
mvn release:clean

# Check whether there are modified files and if so, go back to the
# original git commit
git status
git reset --hard HEAD
```

## Validate a release

```bash
# Check that the signing key (e.g. 2AD3FAE3) is pushed
gpg --recv-keys key

# Check keys
curl -O https://dist.apache.org/repos/dist/release/incubator/calcite/KEYS

# Sign/check md5 and sha1 hashes
# (Assumes your O/S has 'md5' and 'sha1' commands.)
function checkHash() {
  cd "$1"
  for i in *.{zip,pom,gz}; do
    if [ ! -f $i ]; then
      continue
    fi
    if [ -f $i.md5 ]; then
      if [ "$(cat $i.md5)" = "$(md5 -q $i)" ]; then
        echo $i.md5 present and correct
      else
        echo $i.md5 does not match
      fi
    else
      md5 -q $i > $i.md5
      echo $i.md5 created
    fi
    if [ -f $i.sha1 ]; then
      if [ "$(cat $i.sha1)" = "$(sha1 -q $i)" ]; then
        echo $i.sha1 present and correct
      else
        echo $i.sha1 does not match
      fi
    else
      sha1 -q $i > $i.sha1
      echo $i.sha1 created
    fi
  done
}
checkHash apache-calcite-X.Y.Z-incubating-rcN
```

## Get approval for a release via Apache voting process (for Calcite committers)

Release vote on dev list

```
To: dev@calcite.incubator.apache.org
Subject: [VOTE] Release apache-calcite-X.Y.Z-incubating (release candidate N)

Hi all,

I have created a build for Apache Calcite X.Y.Z-incubating, release candidate N.

Thanks to everyone who has contributed to this release.
<Further details about release.> You can read the release notes here:
https://github.com/apache/incubator-calcite/blob/XXXX/doc/history.md

The commit to be voted upon:
http://git-wip-us.apache.org/repos/asf/incubator-calcite/commit/NNNNNN

Its hash is XXXX.

The artifacts to be voted on are located here:
https://dist.apache.org/repos/dist/dev/incubator/calcite/apache-calcite-X.Y.Z-incubating-rcN/

The hashes of the artifacts are as follows:
src.tar.gz.md5 XXXX
src.tar.gz.sha1 XXXX
src.zip.md5 XXXX
src.zip.sha1 XXXX

A staged Maven repository is available for review at:
https://repository.apache.org/content/repositories/orgapachecalcite-NNNN

Release artifacts are signed with the following key:
https://people.apache.org/keys/committer/jhyde.asc

Please vote on releasing this package as Apache Calcite X.Y.Z-incubating.

The vote is open for the next 72 hours and passes if a majority of
at least three +1 PPMC votes are cast.

[ ] +1 Release this package as Apache Calcite X.Y.Z-incubating
[ ]  0 I don't feel strongly about it, but I'm okay with the release
[ ] -1 Do not release this package because...


Here is my vote:

+1 (binding)

Julian
```

After vote finishes, send out the result:

```
Subject: [RESULT] [VOTE] Release apache-calcite-X.Y.Z-incubating (release candidate N)
To: dev@calcite.incubator.apache.org

Thanks to everyone who has tested the release candidate and given
their comments and votes.

The tally is as follows.

N binding +1s:
<names>

N non-binding +1s:
<names>

No 0s or -1s.

Therefore I am delighted to announce that the proposal to release
Apache Calcite X.Y.Z-incubating has passed.

I'll now start a vote on the general list. Those of you in the IPMC,
please recast your vote on the new thread.

Julian
```

Use the [Apache URL shortener](http://s.apache.org) to generate
shortened URLs for the vote proposal and result emails. Examples:
[s.apache.org/calcite-1.2-vote](http://s.apache.org/calcite-1.2-vote) and
[s.apache.org/calcite-1.2-result](http://s.apache.org/calcite-1.2-result).

Propose a vote on the incubator list.

```
To: general@incubator.apache.org
Subject: [VOTE] Release Apache Calcite X.Y.Z (incubating)

Hi all,

The Calcite community has voted on and approved a proposal to release
Apache Calcite X.Y.Z (incubating).

Proposal:
http://s.apache.org/calcite-X.Y.Z-vote

Vote result:
N binding +1 votes
N non-binding +1 votes
No -1 votes
http://s.apache.org/calcite-X.Y.Z-result

The commit to be voted upon:
http://git-wip-us.apache.org/repos/asf/incubator-calcite/commit/NNNNNN

Its hash is XXXX.

The artifacts to be voted on are located here:
https://dist.apache.org/repos/dist/dev/incubator/calcite/apache-calcite-X.Y.Z-incubating-rcN/

The hashes of the artifacts are as follows:
src.tar.gz.md5 XXXX
src.tar.gz.sha1 XXXX
src.zip.md5 XXXX
src.zip.sha1 XXXX

A staged Maven repository is available for review at:
https://repository.apache.org/content/repositories/orgapachecalcite-NNNN

Release artifacts are signed with the following key:
https://people.apache.org/keys/committer/jhyde.asc

Pursuant to the Releases section of the Incubation Policy and with
the endorsement of NNN of our mentors we would now like to request
the permission of the Incubator PMC to publish the release. The vote
is open for 72 hours, or until the necessary number of votes (3 +1)
is reached.

[ ] +1 Release this package as Apache Calcite X.Y.Z incubating
[ ] -1 Do not release this package because...

Julian Hyde, on behalf of Apache Calcite PPMC
```

After vote finishes, send out the result:

```
To: general@incubator.apache.org
Subject: [RESULT] [VOTE] Release Apache Calcite X.Y.Z (incubating)

This vote passes with N +1s and no 0 or -1 votes:
+1 <name> (mentor)

There was some feedback during voting. I shall open a separate
thread to discuss.

Thanks everyone. We’ll now roll the release out to the mirrors.

Julian
```

## Publishing a release (for Calcite committers)

After a successful release vote, we need to push the release
out to mirrors, and other tasks.

In JIRA, search for all issues resolved in this release,
and do a bulk update changing their status to "Closed",
with a change comment
"Resolved in release X.Y.Z-incubating (YYYY-MM-DD)"
(fill in release number and date appropriately).

Promote the staged nexus artifacts.
* Go to https://repository.apache.org/
* Under "Build Promotion" click "Staging Repositories"
* In the line with "orgapachecalcite-xxxx", check the box
* Press "Release" button

Check the artifacts into svn.

```bash
# Get the release candidate.
mkdir -p ~/dist/dev
cd ~/dist/dev
svn co https://dist.apache.org/repos/dist/dev/incubator/calcite

# Copy the artifacts. Note that the copy does not have '-rcN' suffix.
mkdir -p ~/dist/release
cd ~/dist/release
svn co https://dist.apache.org/repos/dist/release/incubator/calcite
cd calcite
cp -rp ../../dev/calcite/apache-calcite-X.Y.Z-incubating-rcN apache-calcite-X.Y.Z-incubating
svn add apache-calcite-X.Y.Z-incubating

# Check in.
svn ci
```

Svnpubsub will publish to
https://dist.apache.org/repos/dist/release/incubator/calcite and propagate to
http://www.apache.org/dyn/closer.cgi/incubator/calcite within 24 hours.

## Publishing the web site (for Calcite committers)

Get the code:

```bash
$ svn co https://svn.apache.org/repos/asf/incubator/calcite/site calcite-site
```

(Note: `https:`, not `http:`.)

Build the site:

```bash
$ cd calcite-site
$ ./build.sh
```

It will prompt you to install jekyll, redcarpet and pygments, if you
do not have them installed. It will also check out the git source code
repo, so that it can generate javadoc.

Check in:

```bash
svn ci -m"Commit message" file...
```

The site will automatically be deployed as http://calcite.incubator.apache.org.
