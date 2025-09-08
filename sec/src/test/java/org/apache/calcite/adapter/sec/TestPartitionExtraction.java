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
package org.apache.calcite.adapter.sec;

import org.apache.calcite.adapter.file.partition.PartitionDetector;

import org.junit.jupiter.api.Test;

public class TestPartitionExtraction {

  @Test void testHivePartitionExtraction() {
    String testPath = "/Volumes/T9/calcite-sec-cache/sec-parquet/cik=0000320193/filing_type=10K/year=2023/0000320193_2023-09-30_facts.parquet";

    System.out.println("Testing partition extraction for: " + testPath);

    PartitionDetector.PartitionInfo info = PartitionDetector.extractHivePartitions(testPath);

    if (info != null) {
      System.out.println("Partition columns: " + info.getPartitionColumns());
      System.out.println("Partition values: " + info.getPartitionValues());

      System.out.println("\nDetailed values:");
      for (String col : info.getPartitionColumns()) {
        System.out.println("  " + col + " = " + info.getPartitionValues().get(col));
      }
    } else {
      System.out.println("No partitions detected!");
    }
  }
}
