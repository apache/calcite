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

import org.apache.calcite.rel.type.RelDataType;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Interface to handle formatting between Kafka message and Calcite row.
 *
 * @param <K> type for Kafka message key,
 *           refer to {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG};
 * @param <V> type for Kafka message value,
 *           refer to {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG};
 *
 */
public interface KafkaRowConverter<K, V> {

  /**
   * Generates row type for a given Kafka topic.
   *
   * @param topicName, Kafka topic name;
   * @return row type
   */
  RelDataType rowDataType(String topicName);

  /**
   * Parses and reformats Kafka message from consumer,
   * to align with row type defined as {@link #rowDataType(String)}.
   *
   * @param message, the raw Kafka message record;
   * @return fields in the row
   */
  Object[] toRow(ConsumerRecord<K, V> message);
}

// End KafkaRowConverter.java
