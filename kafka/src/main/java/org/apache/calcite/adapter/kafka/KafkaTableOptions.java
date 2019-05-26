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

import org.apache.kafka.clients.consumer.Consumer;

import java.util.Map;

/**
 * Available options for {@link KafkaStreamTable}.
 */
public final class KafkaTableOptions {
  private String bootstrapServers;
  private String topicName;
  private KafkaRowConverter rowConverter;
  private Map<String, String> consumerParams;
  //added to inject MockConsumer for testing.
  private Consumer consumer;

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public KafkaTableOptions setBootstrapServers(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }

  public String getTopicName() {
    return topicName;
  }

  public KafkaTableOptions setTopicName(final String topicName) {
    this.topicName = topicName;
    return this;
  }

  public KafkaRowConverter getRowConverter() {
    return rowConverter;
  }

  public KafkaTableOptions setRowConverter(
      final KafkaRowConverter rowConverter) {
    this.rowConverter = rowConverter;
    return this;
  }

  public Map<String, String> getConsumerParams() {
    return consumerParams;
  }

  public KafkaTableOptions setConsumerParams(final Map<String, String> consumerParams) {
    this.consumerParams = consumerParams;
    return this;
  }

  public Consumer getConsumer() {
    return consumer;
  }

  public KafkaTableOptions setConsumer(final Consumer consumer) {
    this.consumer = consumer;
    return this;
  }
}

// End KafkaTableOptions.java
