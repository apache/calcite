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
* `du` - Disk usage
* `ps` - Processes
* `stdin` - Standard input
* `files` - Files (based on the `find` command)
* `git_commits` - Git commits (based on `git log`)

Most tables are implemented as views on top of table functions.

New data sources are straightforward to add; please contribute yours!

## Example: du

{% highlight bash %}
$ sqlsh select count\(\*\), sum\(size_k\) from du where path like \'%.class\'
4416 27960
{% endhighlight %}

## Example: files

{% highlight bash %}
$ sqlsh select type, count\(\*\) from files where path like \'%/test/%\' group by type
4416 27960
{% endhighlight %}

## Example: ps

{% highlight bash %}
$ sqlsh select distinct ps.\`user\` from ps
avahi
root
jhyde
syslog
nobody
daemon
{% endhighlight %}

The `ps.` qualifier is necessary because USER is a SQL reserved word.

## Example: explain

To find out what columns a table has, use {{explain}}:

{% highlight bash %}
$ sqlsh explain plan with type for select \* from du
size_k BIGINT NOT NULL,
path VARCHAR CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary" NOT NULL,
size_b BIGINT NOT NULL
{% endhighlight %}

## Aggregation and top-N

Which user has the most processes? In bash:

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
