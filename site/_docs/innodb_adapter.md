---
layout: docs
title: InnoDB adapter
permalink: /docs/innodb_adapter.html

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

[MySQL](https://www.mysql.com/) is the most popular Open Source SQL
database management system, is developed, distributed, and supported
by Oracle Corporation. InnoDB is a general-purpose storage engine that
balances high reliability and high performance in MySQL, since 5.6
InnoDB has become the default MySQL storage engine.

Calcite's InnoDB adapter allows you to query the data based on InnoDB
data files directly as illustrated below, data files are also known as
`.ibd` files. It leverages the
[innodb-java-reader](https://github.com/alibaba/innodb-java-reader). This
adapter is different from JDBC adapter which maps a schema in a JDBC
data source and requires a MySQL server to serve response.

With `.ibd` files and the corresponding DDLs, the InnoDB adapter acts
as a simple "MySQL server": it accepts SQL queries and attempts to
compile each query based on InnoDB file access APIs provided by
[innodb-java-reader](https://github.com/alibaba/innodb-java-reader).
It projects, filters and sorts directly in the InnoDB data files where
possible.

{% highlight json %}
                      SQL query
                       |     |
                      /       \
             ---------         ---------
            |                           |
            v                           v
+-------------------------+  +------------------------+
|      MySQL Server       |  | Calcite InnoDB Adapter |
|                         |  +------------------------+
| +---------------------+ |    +--------------------+
| |InnoDB Storage Engine| |    | innodb-java-reader |
| +---------------------+ |    +--------------------+
+-------------------------+

-------------------- File System --------------------

        +------------+      +-----+
        | .ibd files | ...  |     |    InnoDB Data files
        +------------+      +-----+

{% endhighlight %}

What's more, with DDL statements, the adapter is "index aware". It
leverages rules to choose the appropriate index to scan, for example,
using primary key or secondary keys to look up data, then it tries to
push down some conditions into storage engine. The adapter also
supports hints, so that users can tell the optimizer to use a
particular index.

A basic example of a model file is given below, this schema reads from
a MySQL "scott" database:

{% highlight json %}
{
  "version": "1.0",
  "defaultSchema": "scott",
  "schemas": [
    {
      "name": "scott",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.innodb.InnodbSchemaFactory",
      "operand": {
        "sqlFilePath": [ "/path/scott.sql" ],
        "ibdDataFileBasePath": "/usr/local/mysql/data/scott"
      }
    }
  ]
}
{% endhighlight %}

`sqlFilePath` is a list of DDL files, you can generate table
definitions by executing `mysqldump` in command-line:

{% highlight bash %}
mysqldump -d -u<username> -p<password> -h <hostname> <dbname>
{% endhighlight %}

The file content of `/path/scott.sql` is as follows:

{% highlight bash %}
CREATE TABLE `DEPT`(
    `DEPTNO` TINYINT NOT NULL,
    `DNAME` VARCHAR(50) NOT NULL,
    `LOC` VARCHAR(20),
    UNIQUE KEY `DEPT_PK` (`DEPTNO`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `EMP`(
    `EMPNO` INT(11) NOT NULL,
    `ENAME` VARCHAR(100) NOT NULL,
    `JOB` VARCHAR(15) NOT NULL,
    `AGE` SMALLINT,
    `MGR` BIGINT,
    `HIREDATE` DATE,
    `SAL` DECIMAL(8,2) NOT NULL,
    `COMM` DECIMAL(6,2),
    `DEPTNO` TINYINT,
    `EMAIL` VARCHAR(100) DEFAULT NULL,
    `CREATE_DATETIME` DATETIME,
    `CREATE_TIME` TIME,
    `UPSERT_TIME` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`EMPNO`),
    KEY `ENAME_KEY` (`ENAME`),
    KEY `HIREDATE_KEY` (`HIREDATE`),
    KEY `CREATE_DATETIME_JOB_KEY` (`CREATE_DATETIME`, `JOB`),
    KEY `CREATE_TIME_KEY` (`CREATE_TIME`),
    KEY `UPSERT_TIME_KEY` (`UPSERT_TIME`),
    KEY `DEPTNO_JOB_KEY` (`DEPTNO`, `JOB`),
    KEY `DEPTNO_SAL_COMM_KEY` (`DEPTNO`, `SAL`, `COMM`),
    KEY `DEPTNO_MGR_KEY` (`DEPTNO`, `MGR`),
    KEY `AGE_KEY` (`AGE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
{% endhighlight %}

ibdDataFileBasePath is the parent file path of `.ibd` files.

Assuming the model file is stored as `model.json`, you can connect to
InnoDB data file to perform query via
[sqlline](https://github.com/julianhyde/sqlline) as follows:

{% highlight bash %}
sqlline> !connect jdbc:calcite:model=model.json admin admin
{% endhighlight %}

We can query all employees by writing standard SQL:

{% highlight bash %}
sqlline> select empno, ename, job, age, mgr from "EMP";
+-------+--------+-----------+-----+------+
| EMPNO | ENAME  |    JOB    | AGE | MGR  |
+-------+--------+-----------+-----+------+
| 7369  | SMITH  | CLERK     | 30  | 7902 |
| 7499  | ALLEN  | SALESMAN  | 24  | 7698 |
| 7521  | WARD   | SALESMAN  | 41  | 7698 |
| 7566  | JONES  | MANAGER   | 28  | 7839 |
| 7654  | MARTIN | SALESMAN  | 27  | 7698 |
| 7698  | BLAKE  | MANAGER   | 38  | 7839 |
| 7782  | CLARK  | MANAGER   | 32  | 7839 |
| 7788  | SCOTT  | ANALYST   | 45  | 7566 |
| 7839  | KING   | PRESIDENT | 22  | null |
| 7844  | TURNER | SALESMAN  | 54  | 7698 |
| 7876  | ADAMS  | CLERK     | 35  | 7788 |
| 7900  | JAMES  | CLERK     | 40  | 7698 |
| 7902  | FORD   | ANALYST   | 28  | 7566 |
| 7934  | MILLER | CLERK     | 32  | 7782 |
+-------+--------+-----------+-----+------+
{% endhighlight %}

While executing this query, the InnoDB adapter scans the InnoDB data
file `EMP.ibd` using primary key, also known as clustering B+ tree
index in MySQL, and is able to push down projection to underlying
storage engine. Projection can reduce the size of data fetched from
the storage engine.

We can look up one employee by filtering. The InnoDB adapter retrieves
all indexes through DDL file provided in `model.json`.

{% highlight bash %}
sqlline> select empno, ename, job, age, mgr from "EMP" where empno = 7782;
+-------+-------+---------+-----+------+
| EMPNO | ENAME |   JOB   | AGE | MGR  |
+-------+-------+---------+-----+------+
| 7782  | CLARK | MANAGER | 32  | 7839 |
+-------+-------+---------+-----+------+
{% endhighlight %}

The InnoDB adapter recognizes that `empno` is the primary key and
performs a point-lookup by using the clustering index instead of a
full table scan.

We can also do range queries on the primary key:

{% highlight bash %}
sqlline> select empno, ename, job, age, mgr from "EMP" where empno > 7782 and empno < 7900;
{% endhighlight %}

Note that such query with acceptable range is usually efficient in
MySQL with InnoDB storage engine, because for clustering B+ tree
index, records close in index are close in data file, which is good
for scanning.

We can look up employee by secondary key. For example, in the
following query, the filtering condition is a field `ename` of type
`VARCHAR`.

{% highlight bash %}
sqlline> select empno, ename, job, age, mgr from "EMP" where ename = 'smith';
+-------+-------+-------+-----+------+
| EMPNO | ENAME |  JOB  | AGE | MGR  |
+-------+-------+-------+-----+------+
| 7369  | SMITH | CLERK | 30  | 7902 |
+-------+-------+-------+-----+------+
{% endhighlight %}

The InnoDB adapter works well on almost all the commonly used data
types in MySQL, for more information on supported data types, please
refer to
[innodb-java-reader](https://github.com/alibaba/innodb-java-reader#3-features).

We can query by composite key. For example, given secondary index of
`DEPTNO_MGR_KEY`.

{% highlight bash %}
sqlline> select empno, ename, job, age, mgr from "EMP" where deptno = 20 and mgr = 7566;
+-------+-------+---------+-----+------+
| EMPNO | ENAME |   JOB   | AGE | MGR  |
+-------+-------+---------+-----+------+
| 7788  | SCOTT | ANALYST | 45  | 7566 |
| 7902  | FORD  | ANALYST | 28  | 7566 |
+-------+-------+---------+-----+------+
{% endhighlight %}

The InnoDB adapter leverages the matched key `DEPTNO_MGR_KEY` to push
down filtering condition of `deptno = 20 and mgr = 7566`.

In some cases, only part of the conditions can be pushed down since
there is a limitation in the underlying storage engine API; other
conditions remain in the rest of the plan. Given the following SQL,
only `deptno = 20` is pushed down.

{% highlight bash %}
select empno, ename, job, age, mgr from "EMP" where deptno = 20 and upsert_time > '2018-01-01 00:00:00';
{% endhighlight %}

`innodb-java-reader` only supports range queries with lower and upper
bound using an index, not fully `Index Condition Pushdown (ICP)`. The
storage engine returns a range of rows and Calcite evaluates the rest
of `WHERE` condition from the rows fetched.

For the following SQL, there are multiple indexes satisfying the
left-prefix index rule: the possible indexes are `DEPTNO_JOB_KEY`,
`DEPTNO_SAL_COMM_KEY` and `DEPTNO_MGR_KEY`. The InnoDB adapter chooses
one of them according to the ordinal defined in DDL; only the `deptno
= 20` condition is pushed down, leaving the rest of `WHERE` condition
handled by Calcite's built-in execution engine.

{% highlight bash %}
sqlline> select empno, deptno, sal from "EMP" where deptno = 20 and sal > 2000;
+-------+--------+---------+
| EMPNO | DEPTNO |   SAL   |
+-------+--------+---------+
| 7788  | 20     | 3000.00 |
| 7902  | 20     | 3000.00 |
| 7566  | 20     | 2975.00 |
+-------+--------+---------+
{% endhighlight %}

Accessing rows through secondary key requires scanning by secondary
index and retrieving records back to clustering index in InnoDB, for a
"big" scan, that would introduce many random I/O operations, so
performance is usually not good enough. Note that the query above can
be more performant by using `EPTNO_SAL_COMM_KEY` index, because
covering index does not need to retrieve back to clustering index. We
can force using `DEPTNO_SAL_COMM_KEY` index by hint as follows.

{% highlight bash %}
sqlline> select empno, deptno, sal from "EMP"/*+ index(DEPTNO_SAL_COMM_KEY) */ where deptno = 20 and sal > 2000;
{% endhighlight %}

Hint can be configured in `SqlToRelConverter`, to enable hint, you
should register `index` HintStrategy on `TableScan` in
`SqlToRelConverter.ConfigBuilder`. Index hint takes effect on the base
`TableScan` relational node, if there are conditions matching the
index, index condition can be pushed down as well. For the following SQL,
although none of the indexes can be used, but by leveraging covering
index, the performance is better than full table scan, we can force to
use `DEPTNO_MGR_KEY` to scan in secondary index.

{% highlight bash %}
sqlline> select empno,mgr from "EMP"/*+ index(DEPTNO_MGR_KEY) */ where mgr = 7839;
{% endhighlight %}

Ordering can be pushed down if it matches the natural collation of the index used.

{% highlight bash %}
sqlline> select deptno,ename,hiredate from "EMP" where hiredate < '2020-01-01' order by hiredate desc;
+--------+--------+------------+
| DEPTNO | ENAME  |  HIREDATE  |
+--------+--------+------------+
| 20     | ADAMS  | 1987-05-23 |
| 20     | SCOTT  | 1987-04-19 |
| 10     | MILLER | 1982-01-23 |
| 20     | FORD   | 1981-12-03 |
| 30     | JAMES  | 1981-12-03 |
| 10     | KING   | 1981-11-17 |
| 30     | MARTIN | 1981-09-28 |
| 30     | TURNER | 1981-09-08 |
| 10     | CLARK  | 1981-06-09 |
| 30     | WARD   | 1981-02-22 |
| 30     | ALLEN  | 1981-02-20 |
| 20     | JONES  | 1981-02-04 |
| 30     | BLAKE  | 1981-01-05 |
| 20     | SMITH  | 1980-12-17 |
+--------+--------+------------+
{% endhighlight %}

## About time zone

MySQL converts `TIMESTAMP` values from the current time zone to UTC
for storage, and back from UTC to the current time zone for
retrieval. So in this adapter, MySQL's `TIMESTAMP` is mapped to
Calcite's `TIMESTAMP WITH LOCAL TIME ZONE`. The per-session time zone
setting can be configured in Calcite connection config `timeZone`,
which tells the MySQL server which time zone the `TIMESTAMP` value was
in. Currently the InnoDB adapter cannot pass the property to the
underlying storage engine, but you can specify `timeZone` in
`model.json` like below. Note that you only need to specify the
property if `timeZone` is set in connection config and it is different
from system default time zone where the InnoDB adapter runs.

{% highlight bash %}
{
  "version": "1.0",
  "defaultSchema": "test",
  "schemas": [
    {
      "name": "test",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.innodb.InnodbSchemaFactory",
      "operand": {
        "sqlFilePath": ["src/test/resources/data_types.sql"],
        "ibdDataFileBasePath": "src/test/resources/data",
        "timeZone": "America/Los_Angeles"
      }
    }
  ]
}
{% endhighlight %}

## Limitations

`innodb-java-reader` has some prerequisites for `.ibd` files.

* The `COMPACT` and `DYNAMIC` row formats are supported. `COMPRESSED`,
  `REDUNDANT` and `FIXED` are not supported.
* `innodb_file_per_table` should set to `ON`, `innodb_file_per_table`
  is enabled by default in MySQL 5.6 and higher.
* Page size should set to `16K` which is also the default value.

For more information, please refer to
[prerequisites](https://github.com/alibaba/innodb-java-reader#2-prerequisites).

In terms of data consistency, you can think of the adapter as a simple
MySQL server, with the ability to query directly through InnoDB data
file, dump data by offloading from MySQL. If pages are not flushed
from InnoDB Buffer Pool to disk, then the result may be inconsistent
(the LSN in `.ibd` file might smaller than in-memory pages). InnoDB
leverages write ahead log in terms of performance, so there is no
command available to flush all dirty pages. Only internal mechanism
manages when and where to persist pages to disk, like Page Cleaner
thread, adaptive flushing, etc.

Currently the InnoDB adapter is not aware of row count and cardinality
of a `.ibd` data file, so it relies on simple rules to perform
optimization. If, in future, the underlying storage engine can provide
such metrics and metadata, this could be integrated into Calcite by
leveraging cost based optimization.
