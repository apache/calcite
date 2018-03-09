---
layout: docs
title: Elasticsearch adapter
permalink: /docs/elasticsearch_adapter.html
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
start querying Elasticsearch with Calcite. First, we need a
[model definition]({{ site.baseurl }}/docs/model.html).
The model gives Calcite the necessary parameters to create an instance
of the Elasticsearch adapter. The models can contain
definitions of
[materializations]({{ site.baseurl }}/docs/model.html#materialization).
The name of the tables defined in the model definition corresponds to
[types](https://www.elastic.co/blog/what-is-an-elasticsearch-index) in
Elasticsearch. The schema/database is represented by the `index` parameter
in the model definition.

A basic example of a model file is given below:

{% highlight json %}
{
  "version": "1.0",
  "defaultSchema": "elasticsearch",
  "schemas": [
    {
      "type": "custom",
      "name": "elasticsearch",
      "factory": "org.apache.calcite.adapter.elasticsearch2.Elasticsearch2SchemaFactory",
      "operand": {
        "coordinates": "{'127.0.0.1': 9300}",
        "userConfig": "{'bulk.flush.max.actions': 10, 'bulk.flush.max.size.mb': 1}",
        "index": "usa"
      }
    }
  ]
}
{% endhighlight %}

This adapter is targeted for Elasticsearch 2.x. To use Calcite with Elasticsearch 5.x+ you can use the factory
of the adapter targeted for Elasticsearch 5.x: `org.apache.calcite.adapter.elasticsearch5.Elasticsearch5SchemaFactory`

Assuming this file is stored as `model.json`, you can connect to
Elasticsearch via [`sqlline`](https://github.com/julianhyde/sqlline) as
follows:

{% highlight bash %}
$ ./sqlline
sqlline> !connect jdbc:calcite:model=model.json admin admin
{% endhighlight %}

`sqlline` will now accept SQL queries which access your Elasticsearch types.
The purpose of this adapter is to compile the query into the most efficient
Elasticsearch SEARCH JSON possible by exploiting filtering and sorting directly
in Elasticsearch where possible.

For example, in the example dataset there is an Elasticsearch type
named `zips` under index named `usa`.

We can issue a simple query to fetch the names of all the states
stored in the type `zips`. By default, Elasticsearch returns only 10 rows:

{% highlight sql %}
sqlline> SELECT * from "zips";
{% endhighlight %}

{% highlight json %}
_MAP={pop=13367, loc=[-72.505565, 42.067203], city=EAST LONGMEADOW, id=01028, state=MA}
_MAP={pop=1652, loc=[-72.908793, 42.070234], city=TOLLAND, id=01034, state=MA}
_MAP={pop=3184, loc=[-72.616735, 42.38439], city=HATFIELD, id=01038, state=MA}
_MAP={pop=43704, loc=[-72.626193, 42.202007], city=HOLYOKE, id=01040, state=MA}
_MAP={pop=2084, loc=[-72.873341, 42.265301], city=HUNTINGTON, id=01050, state=MA}
_MAP={pop=1350, loc=[-72.703403, 42.354292], city=LEEDS, id=01053, state=MA}
_MAP={pop=8194, loc=[-72.319634, 42.101017], city=MONSON, id=01057, state=MA}
_MAP={pop=1732, loc=[-72.204592, 42.062734], city=WALES, id=01081, state=MA}
_MAP={pop=9808, loc=[-72.258285, 42.261831], city=WARE, id=01082, state=MA}
_MAP={pop=4441, loc=[-72.203639, 42.20734], city=WEST WARREN, id=01092, state=MA}
{% endhighlight %}

While executing this query, the Elasticsearch adapter is able to recognize
that `city` can be filtered by Elasticsearch and `state` can be sorted by
Elasticsearch in ascending order.

The final source json given to Elasticsearch is below:

{% highlight json %}
{
  "query": {
    "constant_score": {
      "filter": {
        "bool": {
          "must": [
            {
              "term": {
                "city": "springfield"
              }
            }
          ]
        }
      }
    }
  },
  "fields": [
    "city",
    "state"
  ],
  "script_fields": {},
  "sort": [
    {
      "state": "asc"
    }
  ]
}
{% endhighlight %}

This is the initial version of the Calcite Elasticsearch adapter.
Work is in progress to introduce new features like aggregations into
it.
