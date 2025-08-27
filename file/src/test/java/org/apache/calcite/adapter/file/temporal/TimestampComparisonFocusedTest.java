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
package org.apache.calcite.adapter.file.temporal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Focused test to verify timestamp comparisons work correctly
 */
@Tag("unit")
public class TimestampComparisonFocusedTest {

  private File testDir;

  @BeforeEach
  public void setUp() throws Exception {
    testDir = new File("build/test-temp/timestamp-focused");
    testDir.mkdirs();

    // Create test data
    File testFile = new File(testDir, "events.csv");
    testFile.deleteOnExit();

    java.nio.file.Files.write(testFile.toPath(),
        ("ID:int,EVENT_TIME:timestamp\n"
  +
         "1,\"2024-03-15 09:00:00\"\n"
  +
         "2,\"2024-03-15 10:00:00\"\n"
  +
         "3,\"2024-03-15 11:00:00\"\n").getBytes());
  }

  @Test public void testTimestampComparisonsWork() throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:"
        + "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'TEST',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'TEST',\n"
        + "      type: 'custom',\n"
        + "      factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
        + "      operand: {\n"
        + "        directory: '" + testDir.getAbsolutePath() + "'\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
    info.put("lex", "ORACLE");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      Statement statement = connection.createStatement();

      // Test 1: Verify data is loaded
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM \"events\"");
      assertTrue(rs.next(), "Test 1: COUNT(*) should return a row");
      int countAll = rs.getInt(1);
      assertEquals(3, countAll, "Test 1: Expected 3 rows in events, but was " + countAll);

      // Test 2: Verify ordering works
      rs = statement.executeQuery("SELECT \"id\" FROM \"events\" ORDER BY \"event_time\"");
      assertTrue(rs.next(), "Test 2: Expected first row when ordering by event_time");
      int id = rs.getInt(1);
      assertEquals(1, id, "Test 2: First id should be 1, but was " + id);
      assertTrue(rs.next(), "Test 2: Expected second row when ordering by event_time");
      id = rs.getInt(1);
      assertEquals(2, id, "Test 2: Second id should be 2, but was " + id);
      assertTrue(rs.next(), "Test 2: Expected third row when ordering by event_time");
      id = rs.getInt(1);
      assertEquals(3, id, "Test 2: Third id should be 3, but was " + id);
      assertFalse(rs.next(), "Test 2: Should be no more rows after the third");

      // Test 3: Verify we can find the middle event
      rs =
          statement.executeQuery("SELECT \"id\" FROM \"events\" E1 " +
          "WHERE EXISTS (SELECT 1 FROM \"events\" E2 WHERE E2.\"event_time\" < E1.\"event_time\") " +
          "AND EXISTS (SELECT 1 FROM \"events\" E3 WHERE E3.\"event_time\" > E1.\"event_time\")");
      assertTrue(rs.next(), "Test 3: Expected at least one middle event row");
      int middleId = rs.getInt(1);
      assertEquals(2, middleId, "Test 3: Middle event id should be 2 (10:00), but was " + middleId);
      assertFalse(rs.next(), "Test 3: Only the middle event should match");

      // Test 4: Verify MIN/MAX work
      rs =
          statement.executeQuery("SELECT " +
          "(SELECT \"id\" FROM \"events\" WHERE \"event_time\" = (SELECT MIN(\"event_time\") FROM \"events\")) AS MIN_ID, " +
          "(SELECT \"id\" FROM \"events\" WHERE \"event_time\" = (SELECT MAX(\"event_time\") FROM \"events\")) AS MAX_ID");
      assertTrue(rs.next(), "Test 4: Expected a single row with MIN_ID and MAX_ID");
      int minId = rs.getInt("MIN_ID");
      int maxId = rs.getInt("MAX_ID");
      assertEquals(1, minId, "Test 4: MIN_ID should be 1, but was " + minId);
      assertEquals(3, maxId, "Test 4: MAX_ID should be 3, but was " + maxId);

      // Test 5: Verify comparison between events
      rs =
          statement.executeQuery("SELECT COUNT(*) FROM \"events\" E1, \"events\" E2 " +
          "WHERE E1.\"id\" = 1 AND E2.\"id\" = 2 AND E1.\"event_time\" < E2.\"event_time\"");
      assertTrue(rs.next(), "Test 5: COUNT(*) should return a row");
      int lessCount = rs.getInt(1);
      assertEquals(1, lessCount, "Test 5: Expected 1 when event 1 is before event 2, but was " + lessCount);
    }
  }
}
