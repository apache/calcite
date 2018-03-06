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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Tests for AvaticaConnection
 */
public class AvaticaConnectionTest {
  @Test
  public void testIsValid() throws SQLException {
    AvaticaConnection connection = Mockito.mock(AvaticaConnection.class,
        Mockito.CALLS_REAL_METHODS);
    try {
      connection.isValid(-1);
      Assert.fail("Connection isValid should throw SQLException on negative timeout");
    } catch (SQLException expected) {
      Assert.assertEquals("timeout is less than 0", expected.getMessage());
    }

    Mockito.when(connection.isClosed()).thenReturn(false);
    Assert.assertTrue(connection.isValid(0));

    Mockito.when(connection.isClosed()).thenReturn(true);
    Assert.assertFalse(connection.isValid(0));
  }

  @Test
  public void testNumExecuteRetries() {
    AvaticaConnection connection = Mockito.mock(AvaticaConnection.class,
        Mockito.CALLS_REAL_METHODS);

    // Bad argument should throw an exception
    try {
      connection.getNumStatementRetries(null);
      Assert.fail("Calling getNumStatementRetries with a null object should throw an exception");
    } catch (NullPointerException e) {
      // Pass
    }

    Properties props = new Properties();

    // Verify the default value
    Assert.assertEquals(Long.parseLong(AvaticaConnection.NUM_EXECUTE_RETRIES_DEFAULT),
        connection.getNumStatementRetries(props));

    // Set a non-default value
    props.setProperty(AvaticaConnection.NUM_EXECUTE_RETRIES_KEY, "10");

    // Verify that we observe that value
    Assert.assertEquals(10, connection.getNumStatementRetries(props));
  }

}

// End AvaticaConnectionTest.java
