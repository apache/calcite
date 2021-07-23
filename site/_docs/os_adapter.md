---
layout: docs
title: OS adapter and sqlsh
permalink: /docs/os_adapter.html
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

# Overview

The OS (operating system) adapter allows you to access data in your operating
system and environment using SQL queries.

It aims to solve similar problems that have traditionally been solved using UNIX
command pipelines, but with the power and type-safety of SQL.

The adapter also includes a wrapper called `sqlsh` that allows you to execute
commands from your favorite shell.

# Security warning

The OS adapter launches processes, and is potentially a security loop-hole.
It is included in Calcite's "plus" module, which is not enabled by default.
You must think carefully before enabling it in a security-sensitive situation.

# Compatibility

We try to support all tables on every operating system, and to make sure that
the tables have the same columns. But we rely heavily on operating system
commands, and these differ widely. So:

* These commands only work on Linux and macOS (not Windows, even with Cygwin);
* `vmstat` has very different columns between Linux and macOS;
* `files` and `ps` have the same column names but semantics differ;
* Other commands work largely the same.

# A simple example

Every bash hacker knows that to find the 3 largest files you type

{% highlight bash %}
$ find . -type f -print0 | xargs -0 ls -l  | sort -nr -k 5 | head -3
-rw-r--r-- 1 jhyde jhyde 194877 Jul 16 16:10 ./validate/SqlValidatorImpl.java
-rw-r--r-- 1 jhyde jhyde  73826 Jul  4 21:51 ./fun/SqlStdOperatorTable.java
-rw-r--r-- 1 jhyde jhyde  39214 Jul  4 21:51 ./type/SqlTypeUtil.java
{% endhighlight %}

This actually a pipeline of relational operations, each tuple represented
by line of space-separated fields. What if we were able to access the list of
files as a relation and use it in a SQL query? And what if we could easily
execute that SQL query from the shell? This is what `sqlsh` does:

{% highlight bash %}
$ sqlsh select size, path from files where type = \'f\' order by size desc limit 3
194877 validate/SqlValidatorImpl.java
73826 fun/SqlStdOperatorTable.java
39214 type/SqlTypeUtil.java
{% endhighlight %}

# sqlsh

`sqlsh` launches a connection to Calcite whose default schema is the OS adapter.

It uses the JAVA lexical mode, which means that unquoted table and column names
remain in the case that they were written. This is consistent with how shells like
bash behave.

Shell meta-characters such as `*`, `>`, `<`, `(`, and `)` have to be treated with
care. Often adding a back-slash will suffice.

# Tables and commands

The OS adapter contains the following tables:

* `du` - Disk usage (based on `du` command)
* `ps` - Processes (based on `ps` command)
* `stdin` - Standard input
* `files` - Files (based on the `find` command)
* `git_commits` - Git commits (based on `git log`)
* `vmstat` - Virtual memory (based on `vmstat` command)

Most tables are implemented as views on top of table functions.

New data sources are straightforward to add; please contribute yours!

## Example: du

How many class files, and what is their total size? In `bash`:

{% highlight bash %}
$ du -ka . | grep '\.class$' | awk '{size+=$1} END {print FNR, size}'
4416 27960
{% endhighlight %}

In `sqlsh`:

{% highlight bash %}
$ sqlsh select count\(\*\), sum\(size_k\) from du where path like \'%.class\'
4416 27960
{% endhighlight %}

The back-slashes are necessary because `(`, `*`, `)`, and `'` are shell meta-characters.

## Example: files

How many files and directories? In `bash`, you would use `find`:

{% highlight bash %}
$ find . -printf "%Y %p\n" | grep '/test/' | cut -d' ' -f1 | sort | uniq -c
    143 d
   1336 f
{% endhighlight %}

In `sqlsh`, use the `files` table:

{% highlight bash %}
$ sqlsh select type, count\(\*\) from files where path like \'%/test/%\' group by type
d 143
f 1336
{% endhighlight %}

## Example: ps

Which users have processes running? In `sqlsh`:

{% highlight bash %}
$ sqlsh select distinct ps.\`user\` from ps
avahi
root
jhyde
syslog
nobody
daemon
{% endhighlight %}

The `ps.` qualifier and back-quotes are necessary because USER is a SQL reserved word.

Now a 'top N' problem: Which three users have the most processes? In `bash`:

{% highlight bash %}
$ ps aux | awk '{print $1}' | sort | uniq -c | sort -nr | head -3
{% endhighlight %}

In `sqlsh`:

{% highlight bash %}
$ ./sqlsh select count\(\*\), ps.\`user\` from ps group by ps.\`user\` order by 1 desc limit 3
185 root
69 jhyde
2 avahi
{% endhighlight %}

## Example: vmstat

How's my memory?

{% highlight bash %}
$ ./sqlsh -o mysql select \* from vmstat
+--------+--------+----------+----------+----------+-----------+---------+---------+-------+-------+-----------+-----------+--------+--------+--------+--------+--------+
| proc_r | proc_b | mem_swpd | mem_free | mem_buff | mem_cache | swap_si | swap_so | io_bi | io_bo | system_in | system_cs | cpu_us | cpu_sy | cpu_id | cpu_wa | cpu_st |
+--------+--------+----------+----------+----------+-----------+---------+---------+-------+-------+-----------+-----------+--------+--------+--------+--------+--------+
|     12 |      0 |    54220 |  5174424 |   402180 |   4402196 |       0 |       0 |    15 |    35 |         3 |         2 |      7 |      1 |     92 |      0 |      0 |
+--------+--------+----------+----------+----------+-----------+---------+---------+-------+-------+-----------+-----------+--------+--------+--------+--------+--------+
(1 row)
{% endhighlight %}

## Example: explain

To find out what columns a table has, use `explain`:

{% highlight bash %}
$ sqlsh explain plan with type for select \* from du
size_k BIGINT NOT NULL,
path VARCHAR NOT NULL,
size_b BIGINT NOT NULL
{% endhighlight %}

## Example: git

How many commits and distinct authors per year?
The `git_commits` table is based upon the `git log` command.

{% highlight bash %}
./sqlsh select floor\(commit_timestamp to year\) as y, count\(\*\), count\(distinct author\) from git_commits group by y order by 1
2012-01-01 00:00:00 180 6
2013-01-01 00:00:00 502 13
2014-01-01 00:00:00 679 36
2015-01-01 00:00:00 470 45
2016-01-01 00:00:00 465 67
2017-01-01 00:00:00 279 53
{% endhighlight %}

Note that `group by y` is possible because `sqlsh` uses Calcite's
[lenient mode]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#isGroupByAlias--).

## Example: stdin

Print the stdin, adding a number to each line.

{% highlight bash %}
$ (echo cats; echo and dogs) | cat -n -
     1  cats
     2  and dogs
{% endhighlight %}

In `sqlsh`:

{% highlight bash %}
$ (echo cats; echo and dogs) | ./sqlsh select \* from stdin
1 cats
2 and dogs
{% endhighlight %}

## Example: output format

The `-o` option controls output format.

{% highlight bash %}
$ ./sqlsh -o mysql select min\(size_k\), max\(size_k\) from du
+--------+--------+
| EXPR$0 | EXPR$1 |
+--------+--------+
|      0 |  94312 |
+--------+--------+
(1 row)

{% endhighlight %}

## Example: jps

provides a display of all current java process pids In `sqlsh`:

{% highlight bash %}
$ ./sqlsh select distinct jps.\`pid\`, jps.\`info\` from jps
+--------+---------------------+
| pid    |  info               |
+--------+---------------------+
|  49457 | RemoteMavenServer   |
|  48326 | KotlinCompileDaemon |
+--------+---------------------+
(1 row)

{% endhighlight %}

Format options:

* spaced - spaces between fields (the default)
* headers - as spaced, but with headers
* csv - comma-separated values
* json - JSON, one object per row
* mysql - an aligned table, in the same format used by MySQL

# Further work

The OS adapter was created in
[[CALCITE-1896](https://issues.apache.org/jira/browse/CALCITE-1896)]
but is not complete.

Some ideas for further work:

* Allow '-'and '.' in unquoted table names (to match typical file names)
* Allow ordinal field references, for example '$3'. This would help for files
  that do not have named fields, for instance `stdin`, but you could use them
  even if fields have names. Also '$0' to mean the whole input line.
* Use the file adapter, e.g. `select * from file.scott.emp` would use the
  [file adapter](file_adapter.html) to open the file `scott/emp.csv`
* More tables based on git, e.g. branches, tags, files changed in each commit
* `wc` function, e.g. `select path, lineCount from git_ls_files cross apply wc(path)`
* Move `sqlsh` command, or at least the java code underneath it,
  into [sqlline](https://github.com/julianhyde/sqlline)
