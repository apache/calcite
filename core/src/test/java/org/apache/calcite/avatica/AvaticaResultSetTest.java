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
package org.apache.calcite.avatica;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link AvaticaResultSet}
 */
public class AvaticaResultSetTest {

  /**
   * A fake test driver for test.
   */
  private static final class TestDriver extends UnregisteredDriver {

    @Override protected DriverVersion createDriverVersion() {
      return new DriverVersion("test", "test 0.0.0", "test", "test 0.0.0", false, 0, 0, 0, 0);
    }

    @Override protected String getConnectStringPrefix() {
      return "jdbc:test";
    }

    @Override public Meta createMeta(AvaticaConnection connection) {
      return new AvaticaResultSetConversionsTest.TestMetaImpl(connection);
    }
  }

  @Test public void testGetRow() throws SQLException {
    Properties properties = new Properties();
    properties.setProperty("timeZone", "GMT");

    final TestDriver driver = new TestDriver();
    try (Connection connection = driver.connect("jdbc:test", properties);
         ResultSet resultSet =
             connection.createStatement().executeQuery("SELECT * FROM TABLE")) {
      assertEquals(0, resultSet.getRow());

      // checking that return values of getRow() are coherent
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getRow());
      // should be past last record
      assertFalse(resultSet.next());
      assertEquals(0, resultSet.getRow());
    }
  }
}

// End AvaticaResultSetTest.java
