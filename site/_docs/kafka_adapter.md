---
layout: docs
title: Kafka adapter
permalink: /docs/kafka_adapter.html
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

**Note**:

KafkaAdapter is an experimental feature, changes in public API and usage are expected.

For instructions on downloading and building Calcite, start with the[tutorial]({{ site.baseurl }}/docs/tutorial.html).

The Kafka adapter exposes an Apache Kafka topic as a STREAM table, so it can be queried using
[Calcite Stream SQL]({{ site.baseurl }}/docs/stream.html). Note that the adapter will not attempt to scan all topics,
instead users need to configure tables manually, one Kafka stream table is mapping to one Kafka topic.

A basic example of a model file is given below:

{% highlight json %}
{
  "version": "1.0",
  "defaultSchema": "KAFKA",
  "schemas": [
    {
      "name": "KAFKA",
      "tables": [
        {
          "name": "TABLE_NAME",
          "type": "custom",
          "factory": "org.apache.calcite.adapter.kafka.KafkaTableFactory",
          "row.converter": "com.example.CustKafkaRowConverter",
          "operand": {
            "bootstrap.servers": "host1:port,host2:port",
            "topic.name": "kafka.topic.name",
            "consumer.params": {
              "key.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
              "value.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer"
            }
          }
        }
      ]
    }
  ]
}
{% endhighlight %}

Note that:

1. As Kafka message is schemaless, a [KafkaRowConverter]({{ site.apiRoot }}/org/apache/calcite/adapter/kafka/KafkaRowConverter.html)
 is required to specify row schema explicitly(with parameter `row.converter`), and
 how to decode Kafka message to Calcite row. [KafkaRowConverterImpl]({{ site.apiRoot }}/org/apache/calcite/adapter/kafka/KafkaRowConverterImpl.html)
 is used if not provided;

2. More consumer settings can be added in parameter `consumer.params`;

Assuming this file is stored as `kafka.model.json`, you can connect to Kafka via
[`sqlline`](https://github.com/julianhyde/sqlline) as follows:

{% highlight bash %}
$ ./sqlline
sqlline> !connect jdbc:calcite:model=kafka.model.json admin admin
{% endhighlight %}

`sqlline` will now accept SQL queries which access your Kafka topics.

With the Kafka table configured in above model. We can run a simple query to fetch messages:

{% highlight sql %}
sqlline> SELECT STREAM *
         FROM KAFKA.TABLE_NAME;
+---------------+---------------------+---------------------+---------------+-----------------+
| MSG_PARTITION |    MSG_TIMESTAMP    |     MSG_OFFSET      | MSG_KEY_BYTES | MSG_VALUE_BYTES |
+---------------+---------------------+---------------------+---------------+-----------------+
| 0             | -1                  | 0                   | mykey0        | myvalue0        |
| 0             | -1                  | 1                   | mykey1        | myvalue1        |
+---------------+---------------------+---------------------+---------------+-----------------+
{% endhighlight %}

Kafka table is a streaming table, which runs continuously.

If you want the query to end quickly, add `LIMIT` as follows:

{% highlight sql %}
sqlline> SELECT STREAM *
         FROM KAFKA.TABLE_NAME
         LIMIT 5;
{% endhighlight %}
