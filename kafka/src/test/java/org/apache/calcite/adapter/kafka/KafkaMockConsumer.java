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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;

/**
 * A mock consumer to test Kafka adapter.
 */
public class KafkaMockConsumer extends MockConsumer {
  public KafkaMockConsumer(final OffsetResetStrategy offsetResetStrategy) {
    super(OffsetResetStrategy.EARLIEST);

    assign(Arrays.asList(new TopicPartition("testtopic", 0)));

    HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
    beginningOffsets.put(new TopicPartition("testtopic", 0), 0L);
    updateBeginningOffsets(beginningOffsets);

    for (int idx = 0; idx < 10; ++idx) {
      addRecord(new ConsumerRecord<byte[], byte[]>("testtopic",
          0, idx, ("mykey" + idx).getBytes(StandardCharsets.UTF_8),
          ("myvalue" + idx).getBytes(StandardCharsets.UTF_8)));
    }
  }
}

// End KafkaMockConsumer.java
