---
layout: news_item
title: "Algebra builder"
date: "2015-06-05 19:29:07 -0800"
author: jhyde
categories: ["new features"]
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

Calcite's foundation is a comprehensive implementation of relational
algebra (together with transformation rules, cost model, and metadata)
but to create algebra expressions you had to master a complex API.

We're solving this problem by introducing an
[algebra builder]({{ site.apiRoot }}/org/apache/calcite/tools/RelBuilder.html),
a single class with all the methods you need to build any relational
expression.

For example,

{% highlight java %}
final FrameworkConfig config;
final RelBuilder builder = RelBuilder.create(config);
final RelNode node = builder
  .scan("EMP")
  .aggregate(builder.groupKey("DEPTNO"),
      builder.count(false, "C"),
      builder.sum(false, "S", builder.field("SAL")))
  .filter(
      builder.call(SqlStdOperatorTable.GREATER_THAN,
          builder.field("C"),
          builder.literal(10)))
  .build();
System.out.println(RelOptUtil.toString(node));
{% endhighlight %}

creates the algebra

{% highlight text %}
LogicalFilter(condition=[>($1, 10)])
  LogicalAggregate(group=[{7}], C=[COUNT()], S=[SUM($5)])
    LogicalTableScan(table=[[scott, EMP]])
{% endhighlight %}

which is equivalent to the SQL

{% highlight sql %}
SELECT deptno, count(*) AS c, sum(sal) AS s
FROM emp
GROUP BY deptno
HAVING count(*) > 10
{% endhighlight %}

The [algebra builder documentation]({{ site.baseurl }}/docs/algebra.html) describes the
full API and has lots of examples.

We're still working on the algebra builder, but plan to release it
with Calcite 1.4 (see
[[CALCITE-748](https://issues.apache.org/jira/browse/CALCITE-748)]).

The algebra builder will make some existing tasks easier (such as
writing planner rules), but will also enable new things, such as
writing applications directly on top of Calcite, or implementing
non-SQL query languages. These applications and languages will be able
to take advantage of Calcite's existing back-ends (including
Hive-on-Tez, Drill, MongoDB, Splunk, Spark, JDBC data sources) and
extensive set of query-optimization rules.

If you have questions or comments, please post to the
[mailing list]({{ site.baseurl }}/develop).
