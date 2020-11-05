---
layout: docs
title: Apache Geode adapter
permalink: /docs/geode_adapter.html
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

For instructions on downloading and building Calcite, start with the
[tutorial]({{ site.baseurl }}/docs/tutorial.html).

> Optionally: add `-Puberjdbc` to your maven build to create a single self-contained Geode JDBC adapter jar.


Once you've managed to compile the project, you can return here to
start querying Apache Geode with Calcite. First, we need a
[model definition]({{ site.baseurl }}/docs/model.html).
The model gives Calcite the necessary parameters to create an instance
of the Geode adapter. The models can contain definitions of
[materializations]({{ site.baseurl }}/docs/model.html#materialization).
The name of the tables defined in the model definition corresponds to
[Regions](https://geode.apache.org/docs/guide/12/developing/region_options/chapter_overview.html)
in Geode.

A basic example of a model file is given below:

{% highlight json %}
{
  "version": "1.0",
  "defaultSchema": "geode",
  "schemas": [
    {
      "name": "geode_raw",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.geode.rel.GeodeSchemaFactory",
      "operand": {
        "locatorHost": "localhost",
        "locatorPort": "10334",
        "regions": "Zips",
        "pdxSerializablePackagePath": ".*"
      }
    }
  ]
}
{% endhighlight %}

This adapter is targeted for Geode 1.3.x. The `regions` field allows to list (comma separated)
all Geode regions to appear as relational tables.

Assuming this file is stored as `model.json`, you can connect to
Geode via [`sqlline`](https://github.com/julianhyde/sqlline) as
follows:

{% highlight bash %}
$ ./sqlline
sqlline> !connect jdbc:calcite:model=model.json admin admin
{% endhighlight %}

`sqlline` will now accept SQL queries which access your Regions using OQL.
However, you're not restricted to issuing queries supported by
[OQL](https://geode.apache.org/docs/guide/latest/developing/querying_basics/chapter_overview.html).
Calcite allows you to perform complex operations such as aggregations
or joins. The adapter will attempt to compile the query into the most
efficient OQL possible by exploiting filtering, sorting and aggregation directly
in Geode where possible.

For example, in the example Bookshop dataset there is a Regions `BookMaster`.

We can issue a SQL query to fetch the annual retail cost ordered by the cost:

{% highlight sql %}
sqlline> SELECT
           "yearPublished",
           SUM("retailCost") AS "totalCost"
         FROM "TEST"."BookMaster"
         GROUP BY "yearPublished"
         ORDER BY "totalCost";
+---------------+--------------------+
| yearPublished | totalCost          |
+---------------+--------------------+
| 1971          | 11.989999771118164 |
| 2011          | 94.9800033569336   |
+---------------+--------------------+
{% endhighlight %}

While executing this query, the Geode adapter is able to recognize
that the projection, grouping and ordering can be performed natively by Geode.

The final OQL query given to Geode is below:

{% highlight sql %}
SELECT  yearPublished AS yearPublished,  SUM(retailCost) AS totalCost
FROM /BookMaster
GROUP BY yearPublished
ORDER BY totalCost ASC
{% endhighlight %}

Operations that are not supported in Geode are handled by Calcite itself.
For example the following JOIN query on the same Bookshop dataset

{% highlight sql %}
sqlline> SELECT
           "i"."itemNumber",
           "m"."author",
           "m"."retailCost"
         FROM "TEST"."BookInventory" "i"
           JOIN "TEST"."BookMaster" "m" ON "i"."itemNumber" = "m"."itemNumber"
         WHERE "m"."retailCost" > 20;
+------------+----------------+------------+
| itemNumber | author         | retailCost |
+------------+----------------+------------+
| 123        | Daisy Mae West | 34.99      |
+------------+----------------+------------+
{% endhighlight %}

Will result in two separate OQL queries:

{% highlight sql %}
SELECT  itemNumber AS itemNumber, retailCost AS retailCost, author AS author
FROM /BookMaster
WHERE retailCost > 20
{% endhighlight %}

{% highlight sql %}
SELECT  itemNumber AS itemNumber
FROM /BookInventory
{% endhighlight %}

And the result will be joined in Calcite.

To select a particular item in Geode array field use the `fieldName[index]`
syntax:
{% highlight sql %}
sqlline> SELECT
           "loc" [0] AS "lon",
           "loc" [1] AS "lat"
         FROM "geode".ZIPS
{% endhighlight %}

To select a nested fields use the map `fieldName[nestedFiledName]`
syntax:
{% highlight sql %}
sqlline> SELECT "primaryAddress" ['postalCode'] AS "postalCode"
         FROM "TEST"."BookCustomer"
         WHERE "primaryAddress" ['postalCode'] > '0';
{% endhighlight %}
This will project `BookCustomer.primaryAddress.postalCode` value field.

The following presentations and video tutorials provide further dails
about Geode adapter:

* [Enable SQL/JDBC Access to Apache Geode/GemFire Using Apache Calcite](https://www.slideshare.net/slideshow/embed_code/key/2Mil7I0ZPMLuJU)
  (GeodeSummit/SpringOne 2017)
* [Access Apache Geode/GemFire over SQL/JDBC](https://www.linkedin.com/pulse/access-apache-geode-gemfire-over-sqljdbc-christian-tzolov)
* [Explore Geode & GemFire Data with IntelliJ SQL/Database tool](https://www.linkedin.com/pulse/explore-your-geode-gemfire-data-from-within-intellij-tool-tzolov)
* [Advanced Apache Geode Data Analytics with Apache Zeppelin over SQL/JDBC](https://www.linkedin.com/pulse/advanced-apache-geode-data-analytics-zeppelin-over-sqljdbc-tzolov)
* [Unified Access to Geode/Greenplum/...](https://www.linkedin.com/pulse/unified-access-geodegreenplum-christian-tzolov)
* [Apache Calcite for Enabling SQL Access to NoSQL Data Systems such as Apache Geode](https://schd.ws/hosted_files/apachebigdataeu2016/b6/ApacheCon2016ChristianTzolov.v3.pdf)
  (ApacheCon Big Data, 2016)

There is still significant work to do in improving the flexibility and
performance of the adapter, but if you're looking for a quick way to
gain additional insights into data stored in Geode, Calcite should
prove useful.
