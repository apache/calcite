---
layout: docs
title: Druid adapter
permalink: /docs/druid_adapter.html
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

[Druid](https://druid.io/) is a fast column-oriented distributed data
store. It allows you to execute queries via a
[JSON-based query language](https://druid.io/docs/0.9.2/querying/querying.html),
in particular OLAP-style queries.
Druid can be loaded in batch mode or continuously; one of Druid's key
differentiators is its ability to
[load from a streaming source such as Kafka](https://druid.io/docs/0.9.2/ingestion/stream-ingestion.html)
and have the data available for query within milliseconds.

Calcite's Druid adapter allows you to query the data using SQL,
combining it with data in other Calcite schemas.

First, we need a
[model definition]({{ site.baseurl }}/docs/model.html).
The model gives Calcite the necessary parameters to create an instance
of the Druid adapter.

A basic example of a model file is given below:

{% highlight json %}
{
  "version": "1.0",
  "defaultSchema": "wiki",
  "schemas": [
    {
      "type": "custom",
      "name": "wiki",
      "factory": "org.apache.calcite.adapter.druid.DruidSchemaFactory",
      "operand": {
        "url": "http://localhost:8082",
        "coordinatorUrl": "http://localhost:8081"
      },
      "tables": [
        {
          "name": "wiki",
          "factory": "org.apache.calcite.adapter.druid.DruidTableFactory",
          "operand": {
            "dataSource": "wikiticker",
            "interval": "1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z",
            "timestampColumn": {
              "name": "time",
              "type": "timestamp"
            },
            "dimensions": [
              "channel",
              "cityName",
              "comment",
              "countryIsoCode",
              "countryName",
              "isAnonymous",
              "isMinor",
              "isNew",
              "isRobot",
              "isUnpatrolled",
              "metroCode",
              "namespace",
              "page",
              "regionIsoCode",
              "regionName",
            ],
            "metrics": [
              {
                "name" : "count",
                "type" : "count"
              },
              {
                "name" : "added",
                "type" : "longSum",
                "fieldName" : "added"
              },
              {
                "name" : "deleted",
                "type" : "longSum",
                "fieldName" : "deleted"
              },
              {
                "name" : "delta",
                "type" : "longSum",
                "fieldName" : "delta"
              },
              {
                "name" : "user_unique",
                "type" : "hyperUnique",
                "fieldName" : "user_id"
              }
            ],
            "complexMetrics" : [
              "user_id"
            ]
          }
        }
      ]
    }
  ]
}
{% endhighlight %}

This file is stored as `druid/src/test/resources/druid-wiki-model.json`,
so you can connect to Druid via
[`sqlline`](https://github.com/julianhyde/sqlline)
as follows:

{% highlight bash %}
$ ./sqlline
sqlline> !connect jdbc:calcite:model=druid/src/test/resources/druid-wiki-model.json admin admin
sqlline> select "countryName", cast(count(*) as integer) as c
         from "wiki"
         group by "countryName"
         order by c desc limit 5;
+----------------+------------+
| countryName    |     C      |
+----------------+------------+
|                | 35445      |
| United States  | 528        |
| Italy          | 256        |
| United Kingdom | 234        |
| France         | 205        |
+----------------+------------+
5 rows selected (0.279 seconds)
sqlline>
{% endhighlight %}

That query shows the top 5 countries of origin of wiki page edits
on 2015-09-12 (the date covered by the `wikiticker` data set).

Now let's see how the query was evaluated:

{% highlight bash %}
sqlline> !set outputformat csv
sqlline> explain plan for
         select "countryName", cast(count(*) as integer) as c
         from "wiki"
         group by "countryName"
         order by c desc limit 5;
'PLAN'
'EnumerableInterpreter
  BindableProject(countryName=[$0], C=[CAST($1):INTEGER NOT NULL])
    BindableSort(sort0=[$1], dir0=[DESC], fetch=[5])
      DruidQuery(table=[[wiki, wiki]], groups=[{4}], aggs=[[COUNT()]])
'
1 row selected (0.024 seconds)
{% endhighlight %}

That plan shows that Calcite was able to push down the `GROUP BY`
part of the query to Druid, including the `COUNT(*)` function,
but not the `ORDER BY ... LIMIT`. (We plan to lift this restriction;
see [[CALCITE-1206](https://issues.apache.org/jira/browse/CALCITE-1206)].)

# Complex Metrics
Druid has special metrics that produce quick but approximate results.
Currently there are two types:

* `hyperUnique` - HyperLogLog data sketch used to estimate the cardinality of a dimension
* `thetaSketch` - Theta sketch used to also estimate the cardinality of a dimension,
  but can be used to perform set operations as well.

In the model definition, there is an array of Strings called `complexMetrics` that declares
the alias for each complex metric defined. The alias is used in SQL, but its real column name
is used when Calcite generates the JSON query for druid.

# Foodmart data set

The test VM also includes a data set that denormalizes
the sales, product and customer tables of the Foodmart schema
into a single Druid data set called "foodmart".

You can access it via the
`druid/src/test/resources/druid-foodmart-model.json` model.

# Simplifying the model

If less metadata is provided in the model, the Druid adapter can discover
it automatically from Druid. Here is a schema equivalent to the previous one
but with `dimensions`, `metrics` and `timestampColumn` removed:

{% highlight json %}
{
  "version": "1.0",
  "defaultSchema": "wiki",
  "schemas": [
    {
      "type": "custom",
      "name": "wiki",
      "factory": "org.apache.calcite.adapter.druid.DruidSchemaFactory",
      "operand": {
        "url": "http://localhost:8082",
        "coordinatorUrl": "http://localhost:8081"
      },
      "tables": [
        {
          "name": "wiki",
          "factory": "org.apache.calcite.adapter.druid.DruidTableFactory",
          "operand": {
            "dataSource": "wikiticker",
            "interval": "1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z"
          }
        }
      ]
    }
  ]
}
{% endhighlight %}

Calcite dispatches a
[segmentMetadataQuery](https://druid.io/docs/latest/querying/segmentmetadataquery.html)
to Druid to discover the columns of the table.
Now, let's take out the `tables` element:

{% highlight json %}
{
  "version": "1.0",
  "defaultSchema": "wiki",
  "schemas": [
    {
      "type": "custom",
      "name": "wiki",
      "factory": "org.apache.calcite.adapter.druid.DruidSchemaFactory",
      "operand": {
        "url": "http://localhost:8082",
        "coordinatorUrl": "http://localhost:8081"
      }
    }
  ]
}
{% endhighlight %}

Calcite discovers the "wikiticker" data source via the
[/druid/coordinator/v1/metadata/datasources](https://druid.io/docs/latest/design/coordinator.html#metadata-store-information)
REST call. Now that the "wiki" table element is removed, the table is called
"wikiticker". Any other data sources present in Druid will also appear as
tables.

Our model is now a single schema based on a custom schema factory with only two
operands, so we can
[dispense with the model](https://issues.apache.org/jira/browse/CALCITE-1206)
and supply the operands as part of the connect string:

{% highlight bash %}
  jdbc:calcite:schemaFactory=org.apache.calcite.adapter.druid.DruidSchemaFactory; schema.url=http://localhost:8082; schema.coordinatorUrl=http://localhost:8081
{% endhighlight %}

In fact, those are the
[default values of the operands]({{ site.apiRoot }}/org/apache/calcite/adapter/druid/DruidSchemaFactory.html),
so we can omit them:

{% highlight bash %}
  jdbc:calcite:schemaFactory=org.apache.calcite.adapter.druid.DruidSchemaFactory
{% endhighlight %}

Now, we can connect to `sqlline` using a very simple connect string, and list
the available tables:

{% highlight bash %}
$ ./sqlline
sqlline> !connect jdbc:calcite:schemaFactory=org.apache.calcite.adapter.druid.DruidSchemaFactory admin admin
sqlline> !tables
+-----------+-------------+------------+--------------+
| TABLE_CAT | TABLE_SCHEM | TABLE_NAME | TABLE_TYPE   |
+-----------+-------------+------------+--------------+
|           | adhoc       | foodmart   | TABLE        |
|           | adhoc       | wikiticker | TABLE        |
|           | metadata    | COLUMNS    | SYSTEM_TABLE |
|           | metadata    | TABLES     | SYSTEM_TABLE |
+-----------+-------------+------------+--------------+
{% endhighlight %}

We see the two system tables (`TABLES` and `COLUMNS`),
and the two tables in Druid (`foodmart` and `wikiticker`).
