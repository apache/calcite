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

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.Deserializer;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.Map;

/**
 * Implementation of {@link TableFactory} for Apache Kafka. Currently an Apache Kafka
 * topic is mapping to a STREAM table.
 */
public class KafkaTableFactory implements TableFactory<KafkaStreamTable> {
  public KafkaTableFactory() {
  }

  @Override public KafkaStreamTable create(SchemaPlus schema,
      String name,
      Map<String, Object> operand,
      @Nullable RelDataType rowType) {
    final KafkaTableOptions tableOptionBuilder = new KafkaTableOptions();

    tableOptionBuilder.setBootstrapServers(
        (String) operand.getOrDefault(KafkaTableConstants.SCHEMA_BOOTSTRAP_SERVERS, null));
    tableOptionBuilder.setTopicName(
        (String) operand.getOrDefault(KafkaTableConstants.SCHEMA_TOPIC_NAME, null));

    final KafkaRowConverter rowConverter;
    if (operand.containsKey(KafkaTableConstants.SCHEMA_ROW_CONVERTER)) {
      String rowConverterClass = (String) operand.get(KafkaTableConstants.SCHEMA_ROW_CONVERTER);
      rowConverter = AvaticaUtils.instantiatePlugin(KafkaRowConverter.class, rowConverterClass);
    } else {
      rowConverter = new KafkaRowConverterImpl();
    }
    tableOptionBuilder.setRowConverter(rowConverter);

    if (operand.containsKey(KafkaTableConstants.SCHEMA_CONSUMER_PARAMS)) {
      final Map<String, String> consumerParams =
          (Map<String, String>) operand.get(KafkaTableConstants.SCHEMA_CONSUMER_PARAMS);
      checkConsumerParams(consumerParams);
      tableOptionBuilder.setConsumerParams(consumerParams);
    }
    if (operand.containsKey(KafkaTableConstants.SCHEMA_CUST_CONSUMER)) {
      String custConsumerClass = (String) operand.get(KafkaTableConstants.SCHEMA_CUST_CONSUMER);
      try {
        Class<? extends Consumer> klass =
            Class.forName(custConsumerClass, false, KafkaTableFactory.class.getClassLoader())
                .asSubclass(Consumer.class);
        tableOptionBuilder.setConsumer(
            klass.getConstructor(OffsetResetStrategy.class).newInstance(OffsetResetStrategy.NONE));
      } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException
          | InstantiationException | InvocationTargetException | ClassCastException e) {
        final String details =
            String.format(Locale.ROOT,
                "Fail to create table '%s' with configuration:\n"
                    + "'%s'\n"
                    + "KafkaCustConsumer '%s' is invalid",
                name, operand, custConsumerClass);
        throw new RuntimeException(details, e);
      }
    }

    return new KafkaStreamTable(tableOptionBuilder);
  }

  /** Rejects entries of the {@code consumer.params} operand that would make
   * the Kafka client load classes named by the model author, unless system
   * property
   * {@link CalciteSystemProperty#KAFKA_CONSUMER_PARAMS_TRUSTED} is set. Key
   * and value deserializers are allowed if the named class implements
   * {@link Deserializer}. */
  static void checkConsumerParams(Map<String, String> consumerParams) {
    if (CalciteSystemProperty.KAFKA_CONSUMER_PARAMS_TRUSTED.value()) {
      return;
    }
    for (Map.Entry<String, String> entry : consumerParams.entrySet()) {
      final String key = entry.getKey();
      if (key.equals(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
          || key.equals(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
        checkDeserializer(key, entry.getValue());
      } else if (isClassLoadingParam(key)) {
        throw new SecurityException("Consumer parameter '" + key
            + "' can make the Kafka client load and run classes named in"
            + " the model, so it is not allowed in the 'consumer.params'"
            + " operand; configure it on the operator side, or set system"
            + " property 'calcite.kafka.consumer.params.trusted' to 'true'"
            + " if models are trusted");
      }
    }
  }

  /** Returns whether a Kafka consumer configuration key causes classes
   * named in its value (or, for JAAS, in the login-module configuration
   * text) to be loaded and instantiated by the Kafka client. */
  private static boolean isClassLoadingParam(String key) {
    return key.endsWith(".class")
        || key.endsWith(".classes")
        || key.equals("sasl.jaas.config")
        || key.equals("security.providers")
        || key.equals("metric.reporters")
        || key.equals("partition.assignment.strategy");
  }

  /** Checks that a deserializer named in {@code consumer.params} implements
   * {@link Deserializer} before the Kafka client loads (and initializes) it. */
  private static void checkDeserializer(String key, String className) {
    final Class<?> klass;
    try {
      klass =
          Class.forName(className, false,
              KafkaTableFactory.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SecurityException("Deserializer class '" + className
          + "' given as consumer parameter '" + key + "' not found", e);
    }
    if (!Deserializer.class.isAssignableFrom(klass)) {
      throw new SecurityException("Class '" + className
          + "' given as consumer parameter '" + key
          + "' does not implement " + Deserializer.class.getName());
    }
  }
}
