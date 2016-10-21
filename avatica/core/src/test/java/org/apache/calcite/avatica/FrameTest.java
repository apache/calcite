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

import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Common.ColumnValue;
import org.apache.calcite.avatica.proto.Common.TypedValue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests serialization of {@link Frame}.
 */
public class FrameTest {

  private static final TypedValue NUMBER_VALUE = TypedValue.newBuilder().setNumberValue(1)
      .setType(Common.Rep.LONG).build();

  private void serializeAndTestEquality(Frame frame) {
    Frame frameCopy = Frame.fromProto(frame.toProto());

    assertEquals(frame.done, frameCopy.done);
    assertEquals(frame.offset, frameCopy.offset);

    Iterable<Object> origRows = frame.rows;
    Iterable<Object> copiedRows = frameCopy.rows;

    assertEquals("Expected rows to both be null, or both be non-null",
        origRows == null, copiedRows == null);

    Iterator<Object> origIter = origRows.iterator();
    Iterator<Object> copiedIter = copiedRows.iterator();
    while (origIter.hasNext() && copiedIter.hasNext()) {
      Object orig = origIter.next();
      Object copy = copiedIter.next();

      assertEquals(orig == null, copy == null);

      // This is goofy, but it seems like an Array comes from the JDBC implementation but then
      // the resulting Frame has a List to support the Avatica typed Accessors
      assertEquals(Object[].class, orig.getClass());
      assertTrue("Expected List but got " + copy.getClass(), copy instanceof List);

      @SuppressWarnings("unchecked")
      List<Object> copyList = (List<Object>) copy;

      assertArrayEquals((Object[]) orig, copyList.toArray(new Object[0]));
    }

    assertEquals(origIter.hasNext(), copiedIter.hasNext());
  }

  @Test
  public void testEmpty() {
    serializeAndTestEquality(Frame.EMPTY);
  }

  @Test
  public void testSingleRow() {
    ArrayList<Object> rows = new ArrayList<>();
    rows.add(new Object[] {"string", Integer.MAX_VALUE, new Date().getTime()});

    Frame singleRow = new Frame(0, true, rows);

    serializeAndTestEquality(singleRow);
  }

  @Test
  public void testMultipleRows() {
    ArrayList<Object> rows = new ArrayList<>();
    rows.add(new Object[] {"string", Integer.MAX_VALUE, new Date().getTime()});
    rows.add(new Object[] {"gnirts", 0, Long.MIN_VALUE});
    rows.add(new Object[] {"", null, Long.MAX_VALUE});

    Frame singleRow = new Frame(0, true, rows);

    serializeAndTestEquality(singleRow);
  }

  @Test public void testMalformedColumnValue() {
    // Invalid ColumnValue: has an array and scalar
    final ColumnValue bothAttributesColumnValue = ColumnValue.newBuilder().setHasArrayValue(true)
        .setScalarValue(NUMBER_VALUE).build();
    // Note omission of setScalarValue(TypedValue).
    final ColumnValue neitherAttributeColumnValue = ColumnValue.newBuilder().setHasArrayValue(false)
        .build();

    try {
      Frame.validateColumnValue(bothAttributesColumnValue);
      fail("Validating the ColumnValue should have failed as it has an array and scalar");
    } catch (IllegalArgumentException e) {
      // Pass
    }

    try {
      Frame.validateColumnValue(neitherAttributeColumnValue);
      fail("Validating the ColumnValue should have failed as it has neither an array nor scalar");
    } catch (IllegalArgumentException e) {
      // Pass
    }
  }

  @Test public void testColumnValueBackwardsCompatibility() {
    // 1
    final ColumnValue oldStyleScalarValue = ColumnValue.newBuilder().addValue(NUMBER_VALUE).build();
    // [1, 1]
    final ColumnValue oldStyleArrayValue = ColumnValue.newBuilder().addValue(NUMBER_VALUE)
        .addValue(NUMBER_VALUE).build();

    assertFalse(Frame.isNewStyleColumn(oldStyleScalarValue));
    assertFalse(Frame.isNewStyleColumn(oldStyleArrayValue));

    Object scalar = Frame.parseOldStyleColumn(oldStyleScalarValue);
    assertEquals(1L, scalar);

    Object array = Frame.parseOldStyleColumn(oldStyleArrayValue);
    assertEquals(Arrays.asList(1L, 1L), array);
  }

  @Test public void testColumnValueParsing() {
    // 1
    final ColumnValue scalarValue = ColumnValue.newBuilder().setScalarValue(NUMBER_VALUE).build();
    // [1, 1]
    final ColumnValue arrayValue = ColumnValue.newBuilder().addArrayValue(NUMBER_VALUE)
        .addArrayValue(NUMBER_VALUE).setHasArrayValue(true).build();

    assertTrue(Frame.isNewStyleColumn(scalarValue));
    assertTrue(Frame.isNewStyleColumn(arrayValue));

    Object scalar = Frame.parseColumn(scalarValue);
    assertEquals(1L, scalar);

    Object array = Frame.parseColumn(arrayValue);
    assertEquals(Arrays.asList(1L, 1L), array);
  }

  @Test public void testDeprecatedValueAttributeForScalars() {
    // Create a row with schema: [VARCHAR, INTEGER, DATE]
    List<Object> rows = Collections.<Object>singletonList(new Object[] {"string", Integer.MAX_VALUE,
        new Date().getTime()});
    Meta.Frame frame = Meta.Frame.create(0, true, rows);
    // Convert it to a protobuf
    Common.Frame protoFrame = frame.toProto();
    assertEquals(1, protoFrame.getRowsCount());
    // Get that row we created
    Common.Row protoRow = protoFrame.getRows(0);
    // One row has many columns
    List<Common.ColumnValue> protoColumns = protoRow.getValueList();
    assertEquals(3, protoColumns.size());
    // Verify that the scalar value is also present in the deprecated values attributes.
    List<Common.TypedValue> deprecatedValues = protoColumns.get(0).getValueList();
    assertEquals(1, deprecatedValues.size());
    Common.TypedValue scalarValue = protoColumns.get(0).getScalarValue();
    assertEquals(deprecatedValues.get(0), scalarValue);
  }

  @Test public void testDeprecatedValueAttributeForArrays() {
    // Create a row with schema: [VARCHAR, ARRAY]
    List<Object> rows = Collections.<Object>singletonList(new Object[] {"string",
        Arrays.asList(1, 2, 3)});
    Meta.Frame frame = Meta.Frame.create(0, true, rows);
    // Convert it to a protobuf
    Common.Frame protoFrame = frame.toProto();
    assertEquals(1, protoFrame.getRowsCount());
    // Get that row we created
    Common.Row protoRow = protoFrame.getRows(0);
    // One row has many columns
    List<Common.ColumnValue> protoColumns = protoRow.getValueList();
    // We should have two columns
    assertEquals(2, protoColumns.size());
    // Fetch the ARRAY column
    Common.ColumnValue protoColumn = protoColumns.get(1);
    // We should have the 3 ARRAY elements in the array_values attribute as well as the deprecated
    // values attribute.
    List<Common.TypedValue> deprecatedValues = protoColumn.getValueList();
    assertEquals(3, deprecatedValues.size());
    assertTrue("Column 2 should have an array_value", protoColumns.get(1).getHasArrayValue());
    List<Common.TypedValue> arrayValues = protoColumns.get(1).getArrayValueList();
    assertEquals(arrayValues, deprecatedValues);
  }
}

// End FrameTest.java
