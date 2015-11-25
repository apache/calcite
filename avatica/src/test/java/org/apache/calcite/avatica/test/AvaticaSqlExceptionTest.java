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
package org.apache.calcite.avatica.test;

import org.apache.calcite.avatica.AvaticaSqlException;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link AvaticaSqlException}.
 */
public class AvaticaSqlExceptionTest {

  @Test public void testGetters() {
    final String msg = "My query failed!";
    final int code = 42;
    final String sql = "SELECT foo FROM bar;";
    final String stacktrace = "My Stack Trace";
    final String server = "localhost:8765";

    AvaticaSqlException e = new AvaticaSqlException(msg, sql, code, Arrays.asList(stacktrace),
        server);
    assertTrue(e.getMessage().contains(msg));
    assertEquals(code, e.getErrorCode());
    assertEquals(sql, e.getSQLState());
    assertEquals(1, e.getStackTraces().size());
    assertEquals(stacktrace, e.getStackTraces().get(0));
    assertEquals(server, e.getRemoteServer());
  }

}

// End AvaticaSqlExceptionTest.java
