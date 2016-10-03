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
package org.apache.calcite.avatica.util;

import org.apache.calcite.avatica.util.AbstractCursor.Getter;
import org.apache.calcite.avatica.util.AbstractCursor.NumberAccessor;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

/**
 * Test logic for the NumberAccessor.
 */
public class NumberAccessorTest {

  @Test
  public void testBigDecimalZeroScale() throws SQLException {
    final BigDecimal orig = new BigDecimal(BigInteger.valueOf(137L), 1);
    NumberAccessor accessor = new AbstractCursor.NumberAccessor(
        new Getter() {
          @Override public Object getObject() {
            return orig;
          }

          @Override public boolean wasNull() {
            return false;
          }
        },
        0);

    assertEquals(orig, accessor.getBigDecimal(0));
  }

}

// End NumberAccessorTest.java
