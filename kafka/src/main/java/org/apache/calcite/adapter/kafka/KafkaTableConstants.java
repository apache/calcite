/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.kafka;

/**
 * Parameter constants used to define a Kafka table.
 */
interface KafkaTableConstants {
  String SCHEMA_TOPIC_NAME = "topic.name";
  String SCHEMA_BOOTSTRAP_SERVERS = "bootstrap.servers";
  String SCHEMA_ROW_CONVERTER = "row.converter";
  String SCHEMA_CUST_CONSUMER = "consumer.cust";
  String SCHEMA_CONSUMER_PARAMS = "consumer.params";
}

// End KafkaTableConstants.java
