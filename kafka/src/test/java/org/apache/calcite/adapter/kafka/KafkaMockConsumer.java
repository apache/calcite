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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A mock consumer to test Kafka adapter.
 */
public class KafkaMockConsumer extends MockConsumer<byte[], byte[]> {
  private String topic = "testTopic";

  public KafkaMockConsumer(final OffsetResetStrategy offsetResetStrategy) {
    super(OffsetResetStrategy.EARLIEST);

    updatePartitions(topic,
        Arrays.asList(new PartitionInfo(topic, 0, Node.noNode(), new Node[0], new Node[0])));

    HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
    beginningOffsets.put(new TopicPartition(topic, 0), 0L);
    updateBeginningOffsets(beginningOffsets);
  }

  /**
   * Consumer calls {@link MockConsumer#rebalance(Collection)} to assign this TopicPartition.
   */
  public void doRebalance() {
    rebalance(Arrays.asList(new TopicPartition(topic, 0)));
  }

  /**
   * Calls {@link MockConsumer#addRecord(ConsumerRecord)} to add some test data,
   * and the timestamp set from 10000 to 100000.
   */
  public void sendData() {
    for (int idx = 0; idx < 10; ++idx) {
      addRecord(new ConsumerRecord<byte[], byte[]>(topic, 0,
          idx, idx * 10000, TimestampType.LOG_APPEND_TIME, -1, -1, -1,
          ("mykey" + idx).getBytes(StandardCharsets.UTF_8),
          ("myvalue" + idx).getBytes(StandardCharsets.UTF_8)));
    }
  }

  /**
   * Get the topicPartition collection for the special topic.
   *
   * @param topic topic name;
   * @return TopicPartition collection;
   */
  private Collection<TopicPartition> getTopicPartitions(String topic) {
    List<PartitionInfo> partitionInfoList = partitionsFor(topic);
    Collection<TopicPartition> partitions = new HashSet<>();
    for (PartitionInfo partitionInfo : partitionInfoList) {
      partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
    }
    return partitions;
  }

  /**
   * {@link MockConsumer} does not support offsetsForTimes, rewrite this method.
   * No matter what the requested timestamp is, just return the special OffsetAndTimestamp.
   *
   * @param timestampsToSearch the mapping from partition to the timestamp to look up;
   * @return a mapping from partition to the timestamp and offset of the first message
   * with timestamp greater than or equal to the target timestamp;
   */
  @Override public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long>
                                                                     timestampsToSearch) {
    Map<TopicPartition, OffsetAndTimestamp> fetchedOffsets = new HashMap<>();
    for (TopicPartition topicPartition : timestampsToSearch.keySet()) {
      fetchedOffsets.put(topicPartition, new OffsetAndTimestamp(5, 5000));
    }
    return fetchedOffsets;
  }

  @Override public void subscribe(Collection<String> topics) {
    super.subscribe(topics);
    doRebalance();
    sendData();
  }

  @Override public void subscribe(Collection<String> topics,
                                  final ConsumerRebalanceListener listener) {
    super.subscribe(topics, listener);
    doRebalance();
    // trigger onPartitionsAssigned method
    listener.onPartitionsAssigned(getTopicPartitions(topic));
    sendData();
  }

  @Override public void subscribe(Pattern pattern) {
    super.subscribe(pattern);
    doRebalance();
    sendData();
  }

  @Override public void subscribe(Pattern pattern, final ConsumerRebalanceListener listener) {
    super.subscribe(pattern, listener);
    doRebalance();
    // trigger onPartitionsAssigned method
    listener.onPartitionsAssigned(getTopicPartitions(topic));
    sendData();
  }
}

// End KafkaMockConsumer.java
