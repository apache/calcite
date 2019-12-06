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
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A table that maps to an Apache Kafka topic.
 *
 * <p>Currently only {@link KafkaStreamTable} is
 * implemented as a STREAM table.
 */
public class KafkaStreamTable implements ScannableTable, StreamableTable {
  final KafkaTableOptions tableOptions;

  KafkaStreamTable(final KafkaTableOptions tableOptions) {
    this.tableOptions = tableOptions;
  }

  @Override public Enumerable<Object[]> scan(final DataContext root) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        if (tableOptions.getConsumer() != null) {
          return new KafkaMessageEnumerator(tableOptions.getConsumer(),
              tableOptions.getRowConverter(), cancelFlag);
        }

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
        Consumer consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singletonList(tableOptions.getTopicName()));

        return new KafkaMessageEnumerator(consumer, tableOptions.getRowConverter(), cancelFlag);
      }
    };
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
