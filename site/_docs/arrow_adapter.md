---
layout: docs
title: Arrow adapter
permalink: /docs/arrow_adapter.html
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

**Note**: Arrow Adapter is an experimental feature;
changes in public API and usage are expected.

## Overview

Calcite's adapter for Apache Arrow is able to read and process data in Arrow
format using SQL.

It can read files in Arrow's
[Feather format](https://arrow.apache.org/docs/python/feather.html)
(which generally have a `.arrow` suffix) in the same way that the
[File Adapter](file_adapter.html) can read `.csv` files.

## A simple example

Let's start with a simple example. First, we need a
[model definition]({{ site.baseurl }}/docs/model.html),
as follows.

{% highlight json %}
{
  "version": "1.0",
  "defaultSchema": "ARROW",
  "schemas": [
    {
      "name": "ARROW",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.arrow.ArrowSchemaFactory",
      "operand": {
        "directory": "arrow"
      }
    }
  ]
}
{% endhighlight %}

The model file is stored as `arrow/src/test/resources/arrow-model.json`,
so you can connect via [`sqlline`](https://github.com/julianhyde/sqlline)
as follows:

{% highlight bash %}
$ ./sqlline
sqlline> !connect jdbc:calcite:model=arrow/src/test/resources/arrow-model.json admin admin
sqlline> select * from arrow.test;
+----------+----------+------------+
| fieldOne | fieldTwo | fieldThree |
+----------+----------+------------+
|        1 | abc      |        1.2 |
|        2 | def      |        3.4 |
|        3 | xyz      |        5.6 |
|        4 | abcd     |       1.22 |
|        5 | defg     |       3.45 |
|        6 | xyza     |       5.67 |
+----------+----------+------------+
6 rows selected
{% endhighlight %}

The `arrow` directory contains a file called `test.arrow`, and so it shows up as
a table called `test`.
