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

import org.apache.calcite.avatica.ColumnMetaData.Rep;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link Rep}.
 */
public class RepTest {

  @Test public void testNonPrimitiveRepForType() {
    assertEquals(Rep.BOOLEAN, Rep.nonPrimitiveRepOf(SqlType.BIT));
    assertEquals(Rep.BOOLEAN, Rep.nonPrimitiveRepOf(SqlType.BOOLEAN));
    assertEquals(Rep.BYTE, Rep.nonPrimitiveRepOf(SqlType.TINYINT));
    assertEquals(Rep.SHORT, Rep.nonPrimitiveRepOf(SqlType.SMALLINT));
    assertEquals(Rep.INTEGER, Rep.nonPrimitiveRepOf(SqlType.INTEGER));
    assertEquals(Rep.LONG, Rep.nonPrimitiveRepOf(SqlType.BIGINT));
    assertEquals(Rep.DOUBLE, Rep.nonPrimitiveRepOf(SqlType.FLOAT));
    assertEquals(Rep.DOUBLE, Rep.nonPrimitiveRepOf(SqlType.DOUBLE));
    assertEquals(Rep.STRING, Rep.nonPrimitiveRepOf(SqlType.CHAR));
  }

  @Test public void testSerialRep() {
    assertEquals(Rep.BOOLEAN, Rep.serialRepOf(SqlType.BIT));
    assertEquals(Rep.BOOLEAN, Rep.serialRepOf(SqlType.BOOLEAN));
    assertEquals(Rep.BYTE, Rep.serialRepOf(SqlType.TINYINT));
    assertEquals(Rep.SHORT, Rep.serialRepOf(SqlType.SMALLINT));
    assertEquals(Rep.INTEGER, Rep.serialRepOf(SqlType.INTEGER));
    assertEquals(Rep.LONG, Rep.serialRepOf(SqlType.BIGINT));
    assertEquals(Rep.DOUBLE, Rep.serialRepOf(SqlType.FLOAT));
    assertEquals(Rep.DOUBLE, Rep.serialRepOf(SqlType.DOUBLE));
    assertEquals(Rep.INTEGER, Rep.serialRepOf(SqlType.DATE));
    assertEquals(Rep.INTEGER, Rep.serialRepOf(SqlType.TIME));
    assertEquals(Rep.LONG, Rep.serialRepOf(SqlType.TIMESTAMP));
  }
}

// End RepTest.java
