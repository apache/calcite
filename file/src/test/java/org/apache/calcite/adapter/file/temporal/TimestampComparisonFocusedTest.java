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

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      Statement statement = connection.createStatement();

      // Test 1: Verify data is loaded
      ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM \"EVENTS\"");
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));

      // Test 2: Verify ordering works
      rs = statement.executeQuery("SELECT ID FROM \"EVENTS\" ORDER BY EVENT_TIME");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      assertFalse(rs.next());

      // Test 3: Verify we can find the middle event
      rs =
          statement.executeQuery("SELECT ID FROM \"EVENTS\" E1 " +
          "WHERE EXISTS (SELECT 1 FROM \"EVENTS\" E2 WHERE E2.EVENT_TIME < E1.EVENT_TIME) " +
          "AND EXISTS (SELECT 1 FROM \"EVENTS\" E3 WHERE E3.EVENT_TIME > E1.EVENT_TIME)");
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1)); // Only the middle event (10:00) satisfies this
      assertFalse(rs.next());

      // Test 4: Verify MIN/MAX work
      rs =
          statement.executeQuery("SELECT " +
          "(SELECT ID FROM \"EVENTS\" WHERE EVENT_TIME = (SELECT MIN(EVENT_TIME) FROM \"EVENTS\")) AS MIN_ID, " +
          "(SELECT ID FROM \"EVENTS\" WHERE EVENT_TIME = (SELECT MAX(EVENT_TIME) FROM \"EVENTS\")) AS MAX_ID");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("MIN_ID"));
      assertEquals(3, rs.getInt("MAX_ID"));

      // Test 5: Verify comparison between events
      rs =
          statement.executeQuery("SELECT COUNT(*) FROM \"EVENTS\" E1, \"EVENTS\" E2 " +
          "WHERE E1.ID = 1 AND E2.ID = 2 AND E1.EVENT_TIME < E2.EVENT_TIME");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1)); // Event 1 is before Event 2
    }
  }
}
