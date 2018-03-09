---
layout: docs
title: Pig adapter
permalink: /docs/pig_adapter.html
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

The Pig adapter allows you to write queries in SQL and execute them using
<a href="https://pig.apache.org">Apache Pig</a>.

# A simple example

Let's start with a simple example. First, we need a
[model definition]({{ site.baseurl }}/docs/model.html),
as follows.

{% highlight json %}
{
  "version": "1.0",
  "defaultSchema": "SALES",
  "schemas": [ {
    "name": "PIG",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.pig.PigSchemaFactory",
    "tables": [ {
      "name": "t",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.pig.PigTableFactory",
      "operand": {
        "file": "data.txt",
        "columns": ["tc0", "tc1"]
      }
    }, {
      "name": "s",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.pig.PigTableFactory",
      "operand": {
        "file": "data2.txt",
        "columns": ["sc0", "sc1"]
      }
    } ]
  } ]
}
{% endhighlight %}

Now, if you write the SQL query

{% highlight sql %}
select *
from "t"
join "s" on "tc1" = "sc0"
{% endhighlight %}

the Pig adapter will generate the Pig Latin script

{% highlight sql %}
t = LOAD 'data.txt' USING PigStorage() AS (tc0:chararray, tc1:chararray);
s = LOAD 'data2.txt' USING PigStorage() AS (sc0:chararray, sc1:chararray);
t = JOIN t BY tc1, s BY sc0;
{% endhighlight %}

which is then executed using Pig's runtime, typically MapReduce on
<a href="https://hadoop.apache.org/">Apache Hadoop</a>.

# Relationship to Piglet

Calcite has another component called
<a href="{{ site.apiRoot }}/org/apache/calcite/piglet/package-summary.html">Piglet</a>.
It allows you to write queries in a subset of Pig Latin,
and execute them using any applicable Calcite adapter.
So, Piglet is basically the opposite of the Pig adapter.
