# Optiq HOWTO

Here's some miscellaneous documentation about using Optiq and its various
adapters.

# Tracing

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

# CSV adapter

See <a href="https://github.com/julianhyde/optiq-csv/blob/master/TUTORIAL.md">optiq-csv
tutorial</a>.

# MongoDB adapter

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

# Splunk adapter

To run the test suite and sample queries against Splunk,
load Splunk's `tutorialdata.zip` data set as described in
<a href="http://docs.splunk.com/Documentation/Splunk/6.0.2/PivotTutorial/GetthetutorialdataintoSplunk">the Splunk tutorial</a>.

(This step is optional, but it provides some interesting data for the sample
queries. It is also necessary if you intend to run the test suite, using
`-Doptiq.test.splunk=true`.)

# Implementing adapters

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

## Testing adapter in Java

The example below shows how SQL query can be submitted to `OptiqPrepare` with a custom context (`AdapterContext` in this case). Optiq prepares and implements the query execution, using the resources provided by the `Context`. `OptiqPrepare.PrepareResult` provides access to the underlying enumerable and methods for enumeration. The enumerable itself can naturally be some adapter specific implementation.

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

When Optiq compares `Type` instances, it requires them to be the same object. If there are two distinct `Type` instances that refer to the same Java type, Optiq may fail to recognize that they match.
It is recommended to
-   Use a single instance of `JavaTypeFactory` within the optiq context
-   Store the `Type` instances so that the same object is always returned for the same `Type`.
