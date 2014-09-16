# Optiq HOWTO

Here's some miscellaneous documentation about using Optiq and its various
adapters.

## Building from a source distribution

Prerequisites are maven (3.2.1 or later)
and Java (JDK 1.6 or later, 1.8 preferred) on your path.

Unpack the source distribution `.tar.gz` or `.zip` file,
`cd` to the root directory of the unpacked source,
then build using maven:

```bash
$ tar xvfz optiq-0.9.0-incubating-source.tar.gz
$ cd optiq-0.9.0-incubating
$ mvn install
```

[Running tests](HOWTO.md#running-tests) describes how to run more or fewer
tests.

## Building from git

Prerequisites are git, maven (3.2.1 or later)
and Java (JDK 1.6 or later, 1.8 preferred) on your path.

Create a local copy of the github repository,
`cd` to its root directory,
then build using maven:

```bash
$ git clone git://github.com/apache/incubator-optiq.git
$ cd incubator-optiq
$ mvn install
```

[Running tests](HOWTO.md#running-tests) describes how to run more or fewer
tests.

## Running tests

The test suite will run by default when you build, unless you specify
`-DskipTests`:

```bash
$ mvn -DskipTests clean install
```

There are other options that control which tests are run, and in what
environment, as follows.

* `-Doptiq.test.db=DB` (where db is `hsqldb` or `mysql`) allows you
  to change the JDBC data source for the test suite. Optiq's test
  suite requires a JDBC data source populated with the foodmart data
  set.
   * `hsqldb`, the default, uses an in-memory hsqldb database.
   * `mysql` uses a MySQL database in `jdbc:mysql://localhost/foodmart`.
     It is somewhat faster than hsqldb, but you need to populate it
     manually.
* `-Doptiq.debug` prints extra debugging information to stdout.
* `-Doptiq.test.slow` enables tests that take longer to execute. For
  example, there are tests that create virtual TPC-H and TPC-DS schemas
  in-memory and run tests from those benchmarks.
* `-Doptiq.test.mongodb=true` enables tests that run against
  MongoDB. MongoDB must be installed, running, and
  [populated with the zips.json data set](HOWTO.md#mongodb-adapter).
* `-Doptiq.test.splunk=true` enables tests that run against Splunk.
  Splunk must be installed and running.

## Contributing

We welcome contributions.

If you are planning to make a large contribution, talk to us first! It
helps to agree on the general approach. Log a
[JIRA case](https://issues.apache.org/jira/browse/OPTIQ) for your
proposed feature or start a discussion on the dev list.

Fork the github repository, and create a branch for your feature.

Develop your feature and test cases, and make sure that `mvn clean
install` succeeds. (Run extra tests if your change warrants it.)

Commit your change to your branch, and use a comment that starts with
the JIRA case number, like this:

```
[OPTIQ-345] AssertionError in RexToLixTranslator comparing to date literal
```

If your change had multiple commits, use `git rebase -i master` to
combine them into a single commit, and to bring your code up to date
with the latest on the main line.

Then push your commit(s) to github, and create a pull request from
your branch to the incubator-optiq master branch. Update the JIRA case
to reference your pull request, and a committer will review your
changes.

## Tracing

To enable tracing, add the following flags to the java command line:

```
-Doptiq.debug=true -Djava.util.logging.config.file=core/src/test/resources/logging.properties
```

The first flag causes Optiq to print the Java code it generates
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
org.eigenbase.relopt.RelOptPlanner.level=FINER
java.util.logging.ConsoleHandler.level=ALL
```

The line org.eigenbase.relopt.RelOptPlanner.level=FINER tells the planner to produce
fairly verbose outout. You can modify the file to enable other loggers, or to change levels.
For instance, if you change FINER to FINEST the planner will give you an account of the
planning process so detailed that it might fill up your hard drive.

## CSV adapter

See <a href="https://github.com/julianhyde/optiq-csv/blob/master/TUTORIAL.md">optiq-csv
tutorial</a>.

## MongoDB adapter

First, download and install Optiq,
and <a href="http://www.mongodb.org/downloads">install MongoDB</a>.

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

Connect using the <a href="https://github.com/julianhyde/optiq/blob/master/mongodb/src/test/resources/mongo-zips-model.json">mongo-zips-model.json</a> Optiq model:
```bash
$ ./sqlline
sqlline> !connect jdbc:optiq:model=mongodb/target/test-classes/mongo-zips-model.json admin admin
Connecting to jdbc:optiq:model=mongodb/target/test-classes/mongo-zips-model.json
Connected to: Optiq (version 0.4.x)
Driver: Optiq JDBC Driver (version 0.4.x)
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
Closing: net.hydromatic.optiq.jdbc.FactoryJdbc41$OptiqConnectionJdbc41
$
```

## Splunk adapter

To run the test suite and sample queries against Splunk,
load Splunk's `tutorialdata.zip` data set as described in
<a href="http://docs.splunk.com/Documentation/Splunk/6.0.2/PivotTutorial/GetthetutorialdataintoSplunk">the Splunk tutorial</a>.

(This step is optional, but it provides some interesting data for the sample
queries. It is also necessary if you intend to run the test suite, using
`-Doptiq.test.splunk=true`.)

## Implementing an adapter

New adapters can be created by implementing `OptiqPrepare.Context`:

```java
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
public class AdapterContext implements OptiqPrepare.Context {

    @Override
    public JavaTypeFactory getTypeFactory() {
        // adapter implementation
        return typeFactory;
    }

    @Override
    public Schema getRootSchema() {
        // adapter implementation
        return rootSchema;
    }

}
```

### Testing adapter in Java

The example below shows how SQL query can be submitted to
`OptiqPrepare` with a custom context (`AdapterContext` in this
case). Optiq prepares and implements the query execution, using the
resources provided by the `Context`. `OptiqPrepare.PrepareResult`
provides access to the underlying enumerable and methods for
enumeration. The enumerable itself can naturally be some adapter
specific implementation.

```java
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import org.junit.Test;

public class AdapterContextTest {

    @Test
    public void testSelectAllFromTable() {
        AdapterContext ctx = new AdapterContext();
        String sql = "SELECT * FROM TABLENAME";
        Type elementType = Object[].class;
        OptiqPrepare.PrepareResult<Object> prepared = new OptiqPrepareImpl()
                .prepareSql(ctx, sql, null, elementType, -1);
        Object enumerable = prepared.getExecutable();
        // etc.
    }

}
```

## JavaTypeFactory

When Optiq compares `Type` instances, it requires them to be the same
object. If there are two distinct `Type` instances that refer to the
same Java type, Optiq may fail to recognize that they match.  It is
recommended to:
-   Use a single instance of `JavaTypeFactory` within the optiq context
-   Store the `Type` instances so that the same object is always returned for the same `Type`.

## Set up PGP signing keys (for Optiq committers)

Follow instructions at http://www.apache.org/dev/release-signing to
create a key pair. (On Mac OS X, I did `brew install gpg` and `gpg
--gen-key`.)

Add your public key to the `KEYS` file by following instructions in
the `KEYS` file.

## Making a snapshot (for Optiq committers)

Before you start:
* Set up signing keys as described above.
* Make sure you are using JDK 1.7 (not 1.6 or 1.8).
* Make sure build and tests succeed with `-Doptiq.test.db=hsqldb` (the default)

```bash
# set passphrase variable without putting it into shell history
read GPG_PASSPHRASE

# make sure that there are no junk files in the sandbox
git clean -x

mvn -Papache-release -Dgpg.passphrase=${GPG_PASSPHRASE} clean install
```

When the dry-run has succeeded, change `install` to `deploy`.

## Making a release (for Optiq committers)

Before you start:
* Set up signing keys as described above.
* Make sure you are using JDK 1.7 (not 1.6 or 1.8).
* Make sure build and tests succeed, including with
  -Doptiq.test.db={mysql,hsqldb}, -Doptiq.test.slow=true,
  -Doptiq.test.mongodb=true, -Doptiq.test.splunk=true.
* Trigger a
  <a href="https://scan.coverity.com/projects/2966">Coverity scan</a>
  by merging the latest code into the `julianhyde/coverity_scan` branch,
  and when it completes, make sure that there are no important issues.
* Make sure that
  <a href="https://issues.apache.org/jira/issues/?jql=project%20%3D%20OPTIQ%20AND%20status%20%3D%20Resolved">
  every "resolved" JIRA case</a> (including duplicates) has
  a fix version assigned (most likely the version we are
  just about to release)

```bash
# Set passphrase variable without putting it into shell history
read GPG_PASSPHRASE

# Make sure that there are no junk files in the sandbox
git clean -xn

# Set the version numbers
mvn -DskipTests -DreleaseVersion=X.Y.Z-incubating -DdevelopmentVersion=X.Y.Z+1-incubating-SNAPSHOT -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE}" clean release:prepare 2>&1 | tee /tmp/prepare.log

# Perform the release
mvn -DskipTests -Papache-release -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE}" clean release:prepare 2>&1 | tee /tmp/perform.log

```

If any of the above steps fail, clean up (see below), fix the problem,
and start again from the top.

Check the artifacts:
* In the `target` directory should be these 8 files, among others:
  * apache-optiq-X.Y.Z-incubating-src.tar.gz
  * apache-optiq-X.Y.Z-incubating-src.tar.gz.asc
  * apache-optiq-X.Y.Z-incubating-src.tar.gz.md5
  * apache-optiq-X.Y.Z-incubating-src.tar.gz.sha1
  * apache-optiq-X.Y.Z-incubating-src.zip
  * apache-optiq-X.Y.Z-incubating-src.zip.asc
  * apache-optiq-X.Y.Z-incubating-src.zip.md5
  * apache-optiq-X.Y.Z-incubating-src.zip.sha1
* Note that the file names start `apache-optiq-` and include
  `incubating` in the version.
* In the two source distros `.tar.gz` and `.zip` (currently there is
  no binary distro), check that all files belong to a directory called
  `apache-optiq-X.Y.Z-incubating-src`.
* That directory must contain files `NOTICE`, `LICENSE`, `README`,
  `README.md`, `git.properties`
  * Check that  the version in `README` is correct
  * Check that `git.properties` is current
* In each .jar (for example
  `core/target/optiq-core-X.Y.Z-incubating.jar` and
  `mongodb/target/optiq-mongodb-X.Y.Z-incubating-sources.jar`), check
  that the `META-INF` directory contains `DEPENDENCIES`, `LICENSE`,
  `NOTICE` and `git.properties`
* Check PGP, per https://httpd.apache.org/dev/verification.html

Upload the artifacts to a staging area (in this case, your
people.apache.org home directory):

```bash
# Move the files into a directory
cd target
mkdir apache-optiq-X.Y.Z-incubating-rcN
mv apache-optiq-* optiq-X.Y.Z-incubating-rcN

# Upload to staging area (your people.apache.org home directory)
scp -rp apache-optiq-X.Y.Z-incubating-rcN people.apache.org:public_html
```

## Cleaning up after a failed release attempt (for Optiq committers)

```
# Make sure that the tag you are about to generate does not already
# exist (due to a failed release attempt)
git tag

# If the tag exists, delete it locally and remotely
git tag -d optiq-X.Y.Z-incubating
git push origin :refs/tags/optiq-X.Y.Z-incubating

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
curl http://people.apache.org/keys/group/optiq.asc > KEYS

# Check keys
curl -O https://dist.apache.org/repos/dist/release/incubator/optiq/KEYS

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
checkHash optiq-X.Y.Z-incubating
```

## Get approval for a release via Apache voting process (for Optiq committers)

Release vote on dev list

```
To: dev@optiq.incubator.apache.org
Subject: Release Optiq-X.Y.Z-incubating (release candidate N)

Hi all,

I have created a build for Apache Optiq X.Y.Z-incubating, release candidate N.

Thanks to everyone who has contributed to this release.

The commit to be voted upon:
http://git-wip-us.apache.org/repos/asf/incubator-optiq/commit/NNNNNN

The artifacts to be voted on are located here:
http://people.apache.org/~jhyde/optiq-X.Y.Z-incubating-rcN/

A staged Maven repository is available for review at:
https://repository.apache.org/content/repositories/orgapacheoptiq-NNNN

Release artifacts are signed with the following key:
https://people.apache.org/keys/committer/jhyde.asc

Please vote on releasing this package as Apache Optiq X.Y.Z-incubating.

The vote is open for the next 72 hours and passes if a majority of
at least three +1 PPMC votes are cast.

[ ] +1 Release this package as Apache Optiq X.Y.Z-incubating
[ ]  0 I don't feel strongly about it, but I'm okay with the release
[ ] -1 Do not release this package because...


Here is my vote:

+1 (binding)

Julian
```

After vote finishes, send out the result:

```
Subject: [RESULT] [VOTE] Release Optiq-X.Y.Z-incubating (release candidate N)
To: dev@optiq.incubator.apache.org

Thanks to everyone who has tested the release candidate and given
their comments and votes.

The tally is as follows.

N binding +1s:
<names>

N non-binding +1s:
<names>

No 0s or -1s.

Therefore I am delighted to announce that the proposal to release
Apache Optiq X.Y.Z-incubating has passed.

I'll now start a vote on the general list. Those of you in the IPMC,
please recast your vote on the new thread.

Julian
```

Propose a vote on the incubator list.

```
To: general@incubator.apache.org
Subject: [VOTE] Release Apache Optiq X.Y.Z (incubating)

Hi all,

The Optiq community has voted on and approved a proposal to release
Apache Optiq X.Y.Z (incubating).

Pursuant to the Releases section of the Incubation Policy and with
the endorsement of NNN of our mentors we would now like to request
the permission of the Incubator PMC to publish the release. The vote
is open for 72 hours, or until the necessary number of votes (3 +1)
is reached.

[ ] +1 Release this package as Apache Optiq X.Y.Z incubating
[ ] -1 Do not release this package because ...

Apache Optiq PPMC


Proposal:
http://mail-archives.apache.org/mod_mbox/incubator-optiq-dev/201408.mbox/MESSAGE-URI

Vote result:
N binding +1 votes
N non-binding +1 votes
No -1 votes
http://mail-archives.apache.org/mod_mbox/incubator-optiq-dev/201408.mbox/MESSAGE-URI

Artifacts:
http://people.apache.org/~jhyde/optiq-X.Y.Z-incubating-rcN/

```

After vote finishes, send out the result:

```
To: general@incubator.apache.org
Subject: [RESULT] [VOTE] Release Apache Optiq X.Y.Z (incubating)

This vote passes with N +1s and no 0 or -1 votes:
+1 <name> (mentor)

There was some feedback during voting. I shall open a separate
thread to discuss.

Thanks everyone. We’ll now roll the release out to the mirrors.

Julian
```

## Publishing a release (for Optiq committers)

After a successful release vote, we need to push the release
out to mirrors, and other tasks.

* In JIRA, search for all issues resolved in this release,
  and do a bulk update changing their status to "Closed",
  with a change comment
  "Resolved in release X.Y.Z-incubating (YYYY-MM-DD)"
  (fill in release number and date appropriately).

## Publishing the web site (for Optiq committers)

Get the code:

```bash
$ svn co https://svn.apache.org/repos/asf/incubator/optiq/site optiq-site
```

(Note: `https:`, not `http:`.)

Build the site:

```bash
$ cd optiq-site
$ ./build.sh
```

It will prompt you to install jekyll, redcarpet and pygments, if you
do not have them installed. It will also check out the git source code
repo, so that it can generate javadoc.

Check in:

```bash
svn ci -m"Commit message" file...
```

The site will automatically be deployed as http://optiq.incubator.apache.org.
