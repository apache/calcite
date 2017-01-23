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
package org.apache.calcite.avatica.jdbc;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests covering {@link StatementInfo}.
 */
public class StatementInfoTest {

  @Test
  public void testLargeOffsets() throws Exception {
    Statement stmt = Mockito.mock(Statement.class);
    ResultSet results = Mockito.mock(ResultSet.class);

    StatementInfo info = new StatementInfo(stmt);

    Mockito.when(results.relative(Integer.MAX_VALUE)).thenReturn(true, true);
    Mockito.when(results.relative(1)).thenReturn(true);

    long offset = 1L + Integer.MAX_VALUE + Integer.MAX_VALUE;
    assertTrue(info.advanceResultSetToOffset(results, offset));

    InOrder inOrder = Mockito.inOrder(results);

    inOrder.verify(results, Mockito.times(2)).relative(Integer.MAX_VALUE);
    inOrder.verify(results).relative(1);

    assertEquals(offset, info.getPosition());
  }

  @Test
  public void testNextUpdatesPosition() throws Exception {
    Statement stmt = Mockito.mock(Statement.class);
    ResultSet results = Mockito.mock(ResultSet.class);

    StatementInfo info = new StatementInfo(stmt);
    info.setResultSet(results);

    Mockito.when(results.next()).thenReturn(true, true, true, false);

    for (int i = 0; i < 3; i++) {
      assertTrue(i + "th call of next() should return true", info.next());
      assertEquals(info.getPosition(), i + 1);
    }

    assertFalse("Expected last next() to return false", info.next());
    assertEquals(info.getPosition(), 4L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoMovement() throws Exception {
    Statement stmt = Mockito.mock(Statement.class);
    ResultSet results = Mockito.mock(ResultSet.class);

    StatementInfo info = new StatementInfo(stmt);
    info.setPosition(500);

    info.advanceResultSetToOffset(results, 400);
  }

  @Test public void testResultSetGetter() throws Exception {
    Statement stmt = Mockito.mock(Statement.class);
    ResultSet results = Mockito.mock(ResultSet.class);

    StatementInfo info = new StatementInfo(stmt);

    assertFalse("ResultSet should not be initialized", info.isResultSetInitialized());
    assertNull("ResultSet should be null", info.getResultSet());

    info.setResultSet(results);

    assertTrue("ResultSet should be initialized", info.isResultSetInitialized());
    assertEquals(results, info.getResultSet());
  }

  @Test public void testCheckPositionAfterFailedRelative() throws Exception {
    Statement stmt = Mockito.mock(Statement.class);
    ResultSet results = Mockito.mock(ResultSet.class);
    final long offset = 500;

    StatementInfo info = new StatementInfo(stmt);
    info.setResultSet(results);

    // relative() doesn't work
    Mockito.when(results.relative((int) offset)).thenThrow(new SQLFeatureNotSupportedException());
    // Should fall back to next(), 500 calls to next, 1 false
    Mockito.when(results.next()).then(new Answer<Boolean>() {
      private long invocations = 0;

      // Return true until 500, false after.
      @Override public Boolean answer(InvocationOnMock invocation) throws Throwable {
        invocations++;
        if (invocations >= offset) {
          return false;
        }
        return true;
      }
    });

    info.advanceResultSetToOffset(results, offset);

    // Verify correct position
    assertEquals(offset, info.getPosition());
    // Make sure that we actually advanced the result set
    Mockito.verify(results, Mockito.times(500)).next();
  }
}

// End StatementInfoTest.java
