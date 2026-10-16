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

import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Available options for {@link KafkaStreamTable}.
 */
public final class KafkaTableOptions {
  private @Nullable String bootstrapServers;
  private @Nullable String topicName;
  /** Set by {@link KafkaTableFactory} right after construction, which is why
   * it is not initialized here. */
  @SuppressWarnings("NullAway.Init")
  private KafkaRowConverter rowConverter;
  private @Nullable Map<String, String> consumerParams;
  // added to inject MockConsumer for testing.
  private @Nullable Consumer consumer;

  public @Nullable String getBootstrapServers() {
    return bootstrapServers;
  }

  public KafkaTableOptions setBootstrapServers(final @Nullable String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }

  public @Nullable String getTopicName() {
    return topicName;
  }

  public KafkaTableOptions setTopicName(final @Nullable String topicName) {
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

  public @Nullable Map<String, String> getConsumerParams() {
    return consumerParams;
  }

  public KafkaTableOptions setConsumerParams(
      final @Nullable Map<String, String> consumerParams) {
    this.consumerParams = consumerParams;
    return this;
  }

  public @Nullable Consumer getConsumer() {
    return consumer;
  }

  public KafkaTableOptions setConsumer(final @Nullable Consumer consumer) {
    this.consumer = consumer;
    return this;
  }
}
