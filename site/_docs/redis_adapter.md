---
layout: docs
title: Redis adapter
permalink: /docs/redis_adapter.html

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

[Redis](https://redis.io/) is an open source (BSD licensed), in-memory data structure store, used as a database, cache and message broker. It supports data structures such as strings, hashes, lists, sets, sorted sets with range queries, bitmaps, HyperLogLogs, geospatial indexes with radius queries, and streams. Redis has built-in replication, Lua scripting, LRU eviction, transactions and different levels of on-disk persistence, and provides high availability via Redis Sentinel and automatic partitioning with Redis Cluster.

Calcite's Redis adapter allows you to query data in Redis using SQL, combining it with data in other Calcite schemas.

The Redis adapter allows querying of live data stored in Redis. Each Redis key-value pair is presented as a single row. Rows can be broken down into cells by using table definition files.
Redis `string` ,`hash`, `sets`, `zsets`, `list` value types are supported;

First, we need a [model definition]({{ site.baseurl }}/docs/model.html).
The model gives Calcite the necessary parameters to create an instance of the Redis adapter.

A basic example of a model file is given below:

{% highlight json %}
{
  "version": "1.0",
  "defaultSchema": "foodmart",
  "schemas": [
    {
      "type": "custom",
      "name": "foodmart",
      "factory": "org.apache.calcite.adapter.redis.RedisSchemaFactory",
      "operand": {
        "host": "localhost",
        "port": 6379,
        "database": 0,
        "password": ""
      },
      "tables": [
        {
          "name": "json_01",
          "factory": "org.apache.calcite.adapter.redis.RedisTableFactory",
          "operand": {
            "dataFormat": "json",
            "fields": [
              {
                "name": "DEPTNO",
                "type": "varchar",
                "mapping": "DEPTNO"
              },
              {
                "name": "NAME",
                "type": "varchar",
                "mapping": "NAME"
              }
            ]
          }
        },
        {
          "name": "raw_01",
          "factory": "org.apache.calcite.adapter.redis.RedisTableFactory",
          "operand": {
            "dataFormat": "raw",
            "fields": [
              {
                "name": "id",
                "type": "varchar",
                "mapping": "id"
              },
              {
                "name": "city",
                "type": "varchar",
                "mapping": "city"
              },
              {
                "name": "pop",
                "type": "int",
                "mapping": "pop"
              }
            ]
          }
        },
        {
          "name": "csv_01",
          "factory": "org.apache.calcite.adapter.redis.RedisTableFactory",
          "operand": {
            "dataFormat": "csv",
            "keyDelimiter": ":",
            "fields": [
              {
                "name": "EMPNO",
                "type": "varchar",
                "mapping": 0
              },
              {
                "name": "NAME",
                "type": "varchar",
                "mapping": 1
              }
            ]
          }
        }
      ]
    }
  ]
}
{% endhighlight %}

This file is stored as [`redis/src/test/resources/redis-mix-model.json`](https://github.com/apache/calcite/blob/main/redis/src/test/resources/redis-mix-model.json),
so you can connect to Redis via
[`sqlline`](https://github.com/julianhyde/sqlline)
as follows:

{% highlight bash %}
From the model above, you can see the schema information of the table. You need to start a Redis service before the query, When the source code build is executed, a redis service is started to load the following test data.`redis/src/test/resources/start.sh` is used to start this service.`redis/src/test/resources/stop.sh` is used to stop this service.
$ ./sqlline
sqlline> !connect jdbc:calcite:model=redis/src/test/resources/redis-mix-model.json admin admin
sqlline> !tables
+-----------+-------------+------------+--------------+---------+----------+------------+-----------+---------------------------+----------------+
| TABLE_CAT | TABLE_SCHEM | TABLE_NAME |  TABLE_TYPE  | REMARKS | TYPE_CAT | TYPE_SCHEM | TYPE_NAME | SELF_REFERENCING_COL_NAME | REF_GENERATION |
+-----------+-------------+------------+--------------+---------+----------+------------+-----------+---------------------------+----------------+
|           | foodmart    | csv_01     | TABLE        |         |          |            |           |                           |                |
|           | foodmart    | json_01    | TABLE        |         |          |            |           |                           |                |
|           | foodmart    | raw_01     | TABLE        |         |          |            |           |                           |                |
+-----------+-------------+------------+--------------+---------+----------+------------+-----------+---------------------------+----------------+
sqlline> Select a.DEPTNO, b.NAME from "csv_01" a left join "json_02" b on a.DEPTNO=b.DEPTNO;
+--------+----------+
| DEPTNO |   NAME   |
+--------+----------+
| 10     | "Sales1" |
+--------+----------+
1 row selected (3.304 seconds)
{% endhighlight %}

This query shows the result of the join query in a CSV format table `csv_01` and a JSON format table `json_02`.

Here are a few details about the fields:

The `keyDelimiter` is used to split the value, the default is a colon, and the split value is used to map the field column. This only works for the CSV format.

The `format` key is used to specify the format of the data in Redis. Currently, it supports: `"csv"`, `"json"`, and `"raw"`. The `"raw"` format keeps the original Redis key and value intact and only one field key is used for the query. The details are not described below.

The function of `mapping` is to map the columns of Redis to the underlying data. Since there is no concept of columns in Redis, the specific mapping method varies according to the format. For example, with `"csv"`, we know that the CSV data will be formed after being parsed. The corresponding column mapping uses the index (subscript) of the underlying array. In the example above, `EMPNO` is mapped to index 0, `NAME` is mapped to index 1 and so on.

Currently the Redis adapter supports three formats: raw, JSON, and CSV.

## Example: raw

The raw format maintains the original Redis key-value format with only one column `key`:

{% highlight bash %}
127.0.0.1:6379> LPUSH raw_02 "book1"
sqlline> select * from "raw_02";
+-------+
|  key  |
+-------+
| book2 |
| book1 |
+-------+
{% endhighlight %}

## Example: JSON

The JSON format parses a Redis string value and uses the mapping to convert fields into multiple columns.

{% highlight bash %}
127.0.0.1:6379> LPUSH json_02 {"DEPTNO":10,"NAME":"Sales1"}
{% endhighlight %}

The schema contains mapping:

{% highlight bash %}
{
   "name": "json_02",
   "factory": "org.apache.calcite.adapter.redis.RedisTableFactory",
   "operand": {
     "dataFormat": "json",
     "fields": [
       {
         "name": "DEPTNO",
         "type": "varchar",
         "mapping": "DEPTNO"
       },
       {
         "name": "NAME",
         "type": "varchar",
         "mapping": "NAME"
       }
     ]
   }
 }
{% endhighlight %}

{% highlight bash %}
sqlline> select * from "json_02";
+--------+----------+
| DEPTNO |   NAME   |
+--------+----------+
| 20     | "Sales2" |
| 10     | "Sales1" |
+--------+----------+
2 rows selected (0.014 seconds)
{% endhighlight %}

## Example: CSV

The CSV format parses a Redis string value and combines the mapping in fields into multiple columns. The default separator is `:`.

{% highlight bash %}
127.0.0.1:6379> LPUSH csv_02 "10:Sales"
{% endhighlight %}

The schema contains mapping:

{% highlight bash %}
{
  "name": "csv_02",
  "factory": "org.apache.calcite.adapter.redis.RedisTableFactory",
  "operand": {
    "dataFormat": "csv",
    "keyDelimiter": ":",
    "fields": [
      {
        "name": "DEPTNO",
        "type": "varchar",
        "mapping": 0
      },
      {
        "name": "NAME",
        "type": "varchar",
        "mapping": 1
      }
    ]
  }
}
{% endhighlight %}

{% highlight bash %}
sqlline> select * from "csv_02";
+--------+-------+
| DEPTNO | NAME  |
+--------+-------+
| 20     | Sales |
| 10     | Sales |
+--------+-------+
{% endhighlight %}

Future plan:
More Redis features need to be further refined: for example HyperLogLog and Pub/Sub.
