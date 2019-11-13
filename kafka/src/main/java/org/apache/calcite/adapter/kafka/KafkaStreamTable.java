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

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A table that maps to an Apache Kafka topic.
 *
 * <p>Currently only {@link KafkaStreamTable} is
 * implemented as a STREAM table.
 */
public class KafkaStreamTable implements ScannableTable, StreamableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamTable.class);

  final KafkaTableOptions tableOptions;
  // Identify whether the method "seekToTimestamp" has been already called.
  private boolean hasSeekToTimestamp = false;

  KafkaStreamTable(final KafkaTableOptions tableOptions) {
    this.tableOptions = tableOptions;
  }

  @Override public Enumerable<Object[]> scan(final DataContext root) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            tableOptions.getBootstrapServers());
        //by default it's <byte[], byte[]>
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        if (tableOptions.getConsumerParams() != null) {
          consumerConfig.putAll(tableOptions.getConsumerParams());
        }
        Consumer consumer;
        if (tableOptions.getConsumer() != null) {
          consumer = tableOptions.getConsumer();
        } else {
          consumer = new KafkaConsumer<>(consumerConfig);
        }
        if (tableOptions.getTimestamp() != -1) {
          // seek to special timestamp
          consumer.subscribe(Collections.singletonList(tableOptions.getTopicName()),
              new ConsumerRebalanceListener() {
                @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                  // consumer can seek to special offset after the consumer join the consumer group
                  seekToTimestamp(consumer, tableOptions.getTopicName(),
                      tableOptions.getTimestamp());
                }
              });
        } else {
          consumer.subscribe(Collections.singletonList(tableOptions.getTopicName()));
        }
        return new KafkaMessageEnumerator(consumer, tableOptions.getRowConverter(), cancelFlag);
      }
    };
  }

  /**
   * Seek to special timestamp. The method can only be called only once, otherwise the consumer
   * will seek to the specified offset again, which leads to consumption of old data.
   *
   * @param consumer  KafkaConsumer;
   * @param topicName topic to subscribe;
   * @param timestamp the timestamp need to seek;
   */
  private void seekToTimestamp(Consumer consumer, String topicName, long timestamp) {
    if (hasSeekToTimestamp) {
      // just execute seekToTimestamp once
      return;
    }
    List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topicName);
    Map<TopicPartition, Long> topicPartitionTimestampMap = new HashMap<>();
    for (PartitionInfo partitionInfo : partitionInfoList) {
      topicPartitionTimestampMap.put(
          new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), timestamp);
    }
    Map<TopicPartition, OffsetAndTimestamp> partitionOffsetAndTimestampMap =
        consumer.offsetsForTimes(topicPartitionTimestampMap);
    for (TopicPartition topicPartition : partitionOffsetAndTimestampMap.keySet()) {
      long offset = partitionOffsetAndTimestampMap.get(topicPartition).offset();
      LOGGER.info("seek offset to {} for partition({})", offset, topicPartition.toString());
      consumer.seek(topicPartition, offset);
    }
    hasSeekToTimestamp = true;
  }

  @Override public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
    return tableOptions.getRowConverter().rowDataType(tableOptions.getTopicName());
  }

  @Override public Statistic getStatistic() {
    return Statistics.of(100d, ImmutableList.of(),
        RelCollations.createSingleton(0));
  }

  @Override public boolean isRolledUp(final String column) {
    return false;
  }

  @Override public boolean rolledUpColumnValidInsideAgg(final String column, final SqlCall call,
      final SqlNode parent,
      final CalciteConnectionConfig config) {
    return false;
  }

  @Override public Table stream() {
    return this;
  }

  @Override public Schema.TableType getJdbcTableType() {
    return Schema.TableType.STREAM;
  }
}

// End KafkaStreamTable.java
