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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Implementation of {@link KafkaRowConverter} for test, both key and value are saved as byte[].
 */
public class KafkaRowConverterTest implements KafkaRowConverter<String, String> {
  /**
   * Generate row schema for a given Kafka topic.
   *
   * @param topicName, Kafka topic name;
   * @return row type
   */
  @Override public RelDataType rowDataType(final String topicName) {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    fieldInfo.add("TOPIC_NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)).nullable(false);
    fieldInfo.add("PARTITION_ID", typeFactory.createSqlType(SqlTypeName.INTEGER)).nullable(false);
    fieldInfo.add("TIMESTAMP_TYPE", typeFactory.createSqlType(SqlTypeName.VARCHAR)).nullable(true);

    return fieldInfo.build();
  }

  /**
   * Parse and reformat Kafka message from consumer, to fit with row schema
   * defined as {@link #rowDataType(String)}.
   * @param message, the raw Kafka message record;
   * @return fields in the row
   */
  @Override public Object[] toRow(final ConsumerRecord<String, String> message) {
    Object[] fields = new Object[3];
    fields[0] = message.topic();
    fields[1] = message.partition();
    fields[2] = message.timestampType().name;

    return fields;
  }
}

// End KafkaRowConverterTest.java
