---
layout: docs
title: Cassandra adapter
permalink: /docs/cassandra_adapter.html
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

Once you've managed to compile the project, you can return here to
start querying Cassandra with Calcite.  First, we need a
[model definition]({{ site.baseurl }}/docs/model.html).
The model gives Calcite the necessary parameters to create an instance
of the Cassandra adapter. Note that while models can contain
definitions of
[materializations]({{ site.baseurl }}/docs/model.html#materialization),
the adapter will attempt to automatically populate any materialized views
[defined in Cassandra](https://www.datastax.com/dev/blog/new-in-cassandra-3-0-materialized-views).

A basic example of a model file is given below:

{% highlight json %}
{
  version: '1.0',
  defaultSchema: 'twissandra',
  schemas: [
    {
      name: 'twissandra',
      type: 'custom',
      factory: 'org.apache.calcite.adapter.cassandra.CassandraSchemaFactory',
      operand: {
        host: 'localhost',
        keyspace: 'twissandra'
      }
    }
  ]
}
{% endhighlight %}

Note that you can also specify `username` and `password` keys along with
the `host` and `keyspace` if your server requires authentication.
Assuming this file is stored as `model.json`, you can connect to
Cassandra via [`sqlline`](https://github.com/julianhyde/sqlline) as
follows:

{% highlight bash %}
$ ./sqlline
sqlline> !connect jdbc:calcite:model=model.json admin admin
{% endhighlight %}

`sqlline` will now accept SQL queries which access your CQL tables.
However, you're not restricted to issuing queries supported by
[CQL](https://cassandra.apache.org/doc/cql3/CQL-2.2.html).
Calcite allows you to perform complex operations such as aggregations
or joins. The adapter will attempt to compile the query into the most
efficient CQL possible by exploiting filtering and sorting directly in
Cassandra where possible.

For example, in the example dataset there is a CQL table named `timeline`
with `username` as the partition key and `time` as the clustering key.

We can issue a simple query to fetch the most recent tweet ID of the
user by writing standard SQL:

{% highlight sql %}
sqlline> SELECT "tweet_id"
         FROM "timeline"
         WHERE "username" = 'JmuhsAaMdw'
         ORDER BY "time" DESC LIMIT 1;
+--------------------------------------+
| tweet_id                             |
+--------------------------------------+
| f3d3d4dc-d05b-11e5-b58b-90e2ba530b12 |
+--------------------------------------+
{% endhighlight %}

While executing this query, the Cassandra adapter is able to recognize
that `username` is the partition key and can be filtered by Cassandra.
It also recognizes the clustering key `time` and pushes the ordering to
Cassandra as well.

The final CQL query given to Cassandra is below:

{% highlight sql %}
SELECT username, time, tweet_id
FROM "timeline"
WHERE username = 'JmuhsAaMdw'
ORDER BY time DESC ALLOW FILTERING;
{% endhighlight %}

There is still significant work to do in improving the flexibility and
performance of the adapter, but if you're looking for a quick way to
gain additional insights into data stored in Cassandra, Calcite should
prove useful.
