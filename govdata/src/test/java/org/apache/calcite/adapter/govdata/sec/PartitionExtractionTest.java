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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for partition extraction functionality.
 */
@Tag("unit")
public class PartitionExtractionTest {

  @Test void testHivePartitionExtraction() {
    String testPath = "/test/data/sec-parquet/cik=0000320193/filing_type=10K/year=2023/0000320193_2023-09-30_facts.parquet";

    PartitionDetector.PartitionInfo info = PartitionDetector.extractHivePartitions(testPath);

    assertNotNull(info, "Partition info should not be null");
    assertEquals(3, info.getPartitionColumns().size(), "Should detect 3 partition columns");
    
    // Verify partition columns
    assertEquals("0000320193", info.getPartitionValues().get("cik"));
    assertEquals("10K", info.getPartitionValues().get("filing_type"));
    assertEquals("2023", info.getPartitionValues().get("year"));
  }
}
