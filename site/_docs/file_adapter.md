---
layout: docs
title: File adapter
permalink: /docs/file_adapter.html
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

## Overview

The file adapter is able to read files in a variety of formats,
and can also read files over various protocols, such as HTTP.

For example if you define:

* States - https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States
* Cities - https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population

You can then write a query like:

{% highlight sql %}
select
    count(*) "City Count",
    sum(100 * c."Population" / s."Population") "Pct State Population"
from "Cities" c, "States" s
where c."State" = s."State" and s."State" = 'California';
{% endhighlight %}

And learn that California has 69 cities of 100k or more
comprising almost 1/2 of the state's population:

```
+---------------------+----------------------+
|     City Count      | Pct State Population |
+---------------------+----------------------+
| 69                  | 48.574217177106576   |
+---------------------+----------------------+
```

For simple file formats such as CSV, the file is self-describing and
you don't even need a model.
See [CSV files and model-free browsing](#csv_files_and_model_free_browsing).

## A simple example

Let's start with a simple example. First, we need a
[model definition]({{ site.baseurl }}/docs/model.html),
as follows.

{% highlight json %}
{
  "version": "1.0",
  "defaultSchema": "SALES",
  "schemas": [ {
    "name": "SALES",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "tables": [ {
        "name": "EMPS",
        "url": "file:file/src/test/resources/sales/EMPS.html"
      }, {
        "name": "DEPTS"
        "url": "file:file/src/test/resources/sales/DEPTS.html"
      } ]
    }
  ]
}
{% endhighlight %}

Schemas are defined as a list of tables, each containing minimally a
table name and a url.  If a page has more than one table, you can
include in a table definition `selector` and `index` fields to specify the
desired table.  If there is no table specification, the file adapter
chooses the largest table on the page.

`EMPS.html` contains a single HTML table:

{% highlight xml %}
<html>
  <body>
    <table>
      <thead>
        <tr>
          <th>EMPNO</th>
          <th>NAME</th>
          <th>DEPTNO</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>100</td>
          <td>Fred</td>
          <td>30</td>
        </tr>
        <tr>
          <td>110</td>
          <td>Eric</td>
          <td>20</td>
        </tr>
        <tr>
          <td>110</td>
          <td>John</td>
          <td>40</td>
        </tr>
        <tr>
          <td>120</td>
          <td>Wilma</td>
          <td>20</td>
        </tr>
        <tr>
          <td>130</td>
          <td>Alice</td>
          <td>40</td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
{% endhighlight %}

The model file is stored as `file/src/test/resources/sales.json`,
so you can connect via [`sqlline`](https://github.com/julianhyde/sqlline)
as follows:

{% highlight bash %}
$ ./sqlline
sqlline> !connect jdbc:calcite:model=file/src/test/resources/sales.json admin admin
sqlline> select * from sales.emps;
+-------+--------+------+
| EMPNO | DEPTNO | NAME |
+-------+--------+------+
| 100   | 30     | Fred |
| 110   | 20     | Eric |
| 110   | 40     | John |
| 120   | 20     | Wilma |
| 130   | 40     | Alice |
+-------+--------+------+
5 rows selected
{% endhighlight %}

## Mapping tables

Now for a more complex example. This time we connect to Wikipedia via
HTTP, read pages for US states and cities, and extract data from HTML
tables on those pages. The tables have more complex formats, and the
file adapter helps us locate and parse data in those tables.

Tables can be simply defined for immediate gratification:

{% highlight json %}
{
  tableName: "RawCities",
  url: "https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population"
}
{% endhighlight %}

And subsequently refined for better usability / querying:

{% highlight json %}
{
  tableName: "Cities",
  url: "https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population",
  path: "#mw-content-text > table.wikitable.sortable",
  index: 0,
  fieldDefs: [
    {th: "2012 rank", name: "Rank", type: "int", pattern: "(\\d+)", matchGroup: 0},
    {th: "City", selector: "a", selectedElement: 0},
    {th: "State[5]", name: "State", selector: "a:eq(0)"},
    {th: "2012 estimate", name: "Population", type: "double"},
    {th: "2010 Census", skip: "true"},
    {th: "Change", skip: "true"},
    {th: "2012 land area", name: "Land Area (sq mi)", type: "double", selector: ":not(span)"},
    {th: "2012 population density", skip: "true"},
    {th: "ANSI", skip: "true"}
  ]
}
{% endhighlight %}

Connect and execute queries, as follows.

{% highlight bash %}
$ ./sqlline
sqlline> !connect jdbc:calcite:model=file/src/test/resources/wiki.json admin admin
sqlline> select * from wiki."RawCities";
sqlline> select * from wiki."Cities";
{% endhighlight %}

Note that `Cities` is easier to consume than `RawCities`,
because its table definition has a field list.

The file adapter uses [Jsoup](https://jsoup.org/) for HTML DOM
navigation; selectors for both tables and fields follow the
[Jsoup selector specification](https://jsoup.org/cookbook/extracting-data/selector-syntax).

Field definitions may be used to rename or skip source fields, to
select and condition the cell contents and to set a data type.

### Parsing cell contents

The file adapter can select DOM nodes within a cell, replace text
within the selected element, match within the selected text, and
choose a data type for the resulting database column.  Processing
steps are applied in the order described and replace and match
patterns are based on
[Java regular expressions](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html).

### Further examples

There are more examples in the form of a script:

{% highlight bash %}
$ ./sqlline -f file/src/test/resources/webjoin.sql
{% endhighlight %}

(When running `webjoin.sql` you will see a number of warning messages for
each query containing a join.  These are expected and do not affect
query results.  These messages will be suppressed in the next release.)

## CSV files and model-free browsing

Some files are describe their own schema, and for these files, we do not need a model. For example, `DEPTS.csv` has an
integer `DEPTNO` column and a string `NAME` column:

{% highlight json %}
DEPTNO:int,NAME:string
10,"Sales"
20,"Marketing"
30,"Accounts"
{% endhighlight %}

You can launch `sqlline`, and pointing the file adapter that directory,
and every CSV file becomes a table:

{% highlight bash %}
$ ls file/src/test/resources/sales-csv
 -rw-r--r-- 1 jhyde jhyde  62 Mar 15 10:16 DEPTS.csv
 -rw-r--r-- 1 jhyde jhyde 262 Mar 15 10:16 EMPS.csv.gz

$ ./sqlline -u "jdbc:calcite:schemaFactory=org.apache.calcite.adapter.file.FileSchemaFactory;schema.directory=file/src/test/resources/sales-csv"
sqlline> !tables
+-----------+-------------+------------+------------+
| TABLE_CAT | TABLE_SCHEM | TABLE_NAME | TABLE_TYPE |
+-----------+-------------+------------+------------+
|           | adhoc       | DEPTS      | TABLE      |
|           | adhoc       | EMPS       | TABLE      |
+-----------+-------------+------------+------------+

sqlline> select distinct deptno from depts;
+--------+
| DEPTNO |
+--------+
| 20     |
| 10     |
| 30     |
+--------+
3 rows selected (0.985 seconds)
{% endhighlight %}

## JSON files and model-free browsing

Some files describe their own schema, and for these files, we do not need a model. For example, `DEPTS.json` has an integer `DEPTNO` column and a string `NAME` column:

{% highlight json %}
[
  {
    "DEPTNO": 10,
    "NAME": "Sales"
  },
  {
    "DEPTNO": 20,
    "NAME": "Marketing"
  },
  {
    "DEPTNO": 30,
    "NAME": "Accounts"
  }
]
{% endhighlight %}

You can launch `sqlline`, and pointing the file adapter that directory,
and every JSON file becomes a table:

{% highlight bash %}
$ ls file/src/test/resources/sales-json
 -rw-r--r-- 1 jhyde jhyde  62 Mar 15 10:16 DEPTS.json

$ ./sqlline -u "jdbc:calcite:schemaFactory=org.apache.calcite.adapter.file.FileSchemaFactory;schema.directory=file/src/test/resources/sales-json"
sqlline> !tables
+-----------+-------------+------------+------------+
| TABLE_CAT | TABLE_SCHEM | TABLE_NAME | TABLE_TYPE |
+-----------+-------------+------------+------------+
|           | adhoc       | DATE       | TABLE      |
|           | adhoc       | DEPTS      | TABLE      |
|           | adhoc       | EMPS       | TABLE      |
|           | adhoc       | EMPTY      | TABLE      |
|           | adhoc       | SDEPTS     | TABLE      |
+-----------+-------------+------------+------------+

sqlline> select distinct deptno from depts;
+--------+
| DEPTNO |
+--------+
| 20     |
| 10     |
| 30     |
+--------+
3 rows selected (0.985 seconds)
{% endhighlight %}

## Future improvements

We are continuing to enhance the adapter, and would welcome
contributions of new parsing capabilities (for example parsing JSON
files) and being able to form URLs dynamically to push down filters.
