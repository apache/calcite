/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.clone;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;


/**
 * Unit test for {@link ArrayTable} and {@link ColumnLoader}.
 */
public class ArrayTableTest {
  @Test public void testPrimitiveArray() {
    long[] values = new long[]{0, 0};
    ArrayTable.BitSlicedPrimitiveArray.orLong(4, values, 0, 0x0F);
    assertEquals(0x0F, values[0]);
    ArrayTable.BitSlicedPrimitiveArray.orLong(4, values, 2, 0x0F);
    assertEquals(0xF0F, values[0]);

    values = new long[]{
        0x1213141516171819L, 0x232425262728292AL, 0x3435363738393A3BL};
    assertEquals(
        0x324, ArrayTable.BitSlicedPrimitiveArray.getLong(12, values, 9));
    assertEquals(
        0xa3b, ArrayTable.BitSlicedPrimitiveArray.getLong(12, values, 10));

    Arrays.fill(values, 0);
    for (int i = 0; i < 10; i++) {
      ArrayTable.BitSlicedPrimitiveArray.orLong(10, values, i, i);
    }

    for (int i = 0; i < 10; i++) {
      assertEquals(
          i, ArrayTable.BitSlicedPrimitiveArray.getLong(10, values, i));
    }
  }

  @Test public void testNextPowerOf2() {
    assertEquals(1, ColumnLoader.nextPowerOf2(1));
    assertEquals(2, ColumnLoader.nextPowerOf2(2));
    assertEquals(4, ColumnLoader.nextPowerOf2(3));
    assertEquals(4, ColumnLoader.nextPowerOf2(4));
    assertEquals(0x40000000, ColumnLoader.nextPowerOf2(0x3456789a));
    assertEquals(0x40000000, ColumnLoader.nextPowerOf2(0x40000000));
    // overflow
    assertEquals(0x80000000, ColumnLoader.nextPowerOf2(0x7fffffff));
    assertEquals(0x80000000, ColumnLoader.nextPowerOf2(0x7ffffffe));
  }

  @Test public void testLog2() {
    assertEquals(0, ColumnLoader.log2(0));
    assertEquals(0, ColumnLoader.log2(1));
    assertEquals(1, ColumnLoader.log2(2));
    assertEquals(2, ColumnLoader.log2(4));
    assertEquals(16, ColumnLoader.log2(65536));
    assertEquals(15, ColumnLoader.log2(65535));
    assertEquals(16, ColumnLoader.log2(65537));
    assertEquals(30, ColumnLoader.log2(Integer.MAX_VALUE));
    assertEquals(30, ColumnLoader.log2(Integer.MAX_VALUE - 1));
    assertEquals(29, ColumnLoader.log2(0x3fffffff));
    assertEquals(30, ColumnLoader.log2(0x40000000));
  }

  @Test public void testValueSetInt() {
    ArrayTable.BitSlicedPrimitiveArray representation;
    ArrayTable.Column pair;

    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(int.class);
    valueSet.add(0);
    valueSet.add(1);
    valueSet.add(10);
    pair = valueSet.freeze(0, null);
    assertTrue(
        pair.representation instanceof ArrayTable.BitSlicedPrimitiveArray);
    representation =
        (ArrayTable.BitSlicedPrimitiveArray) pair.representation;

    // unsigned 4 bit integer (values 0..15)
    assertEquals(4, representation.bitCount);
    assertFalse(representation.signed);
    assertEquals(0, representation.getInt(pair.dataSet, 0));
    assertEquals(1, representation.getInt(pair.dataSet, 1));
    assertEquals(10, representation.getInt(pair.dataSet, 2));
    assertEquals(10, representation.getObject(pair.dataSet, 2));

    // -32 takes us to 6 bit signed
    valueSet.add(-32);
    pair = valueSet.freeze(0, null);
    assertTrue(
        pair.representation instanceof ArrayTable.BitSlicedPrimitiveArray);
    representation =
        (ArrayTable.BitSlicedPrimitiveArray) pair.representation;
    assertEquals(6, representation.bitCount);
    assertTrue(representation.signed);
    assertEquals(10, representation.getInt(pair.dataSet, 2));
    assertEquals(10, representation.getObject(pair.dataSet, 2));
    assertEquals(-32, representation.getInt(pair.dataSet, 3));
    assertEquals(-32, representation.getObject(pair.dataSet, 3));

    // 63 takes us to 7 bit signed
    valueSet.add(63);
    pair = valueSet.freeze(0, null);
    assertTrue(
        pair.representation instanceof ArrayTable.BitSlicedPrimitiveArray);
    representation =
        (ArrayTable.BitSlicedPrimitiveArray) pair.representation;
    assertEquals(7, representation.bitCount);
    assertTrue(representation.signed);

    // 128 pushes us to 8 bit signed, i.e. byte
    valueSet.add(64);
    pair = valueSet.freeze(0, null);
    assertTrue(pair.representation instanceof ArrayTable.PrimitiveArray);
    ArrayTable.PrimitiveArray representation2 =
        (ArrayTable.PrimitiveArray) pair.representation;
    assertEquals(0, representation2.getInt(pair.dataSet, 0));
    assertEquals(-32, representation2.getInt(pair.dataSet, 3));
    assertEquals(-32, representation2.getObject(pair.dataSet, 3));
    assertEquals(64, representation2.getInt(pair.dataSet, 5));
    assertEquals(64, representation2.getObject(pair.dataSet, 5));
  }

  @Test public void testValueSetBoolean() {
    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(boolean.class);
    valueSet.add(0);
    valueSet.add(1);
    valueSet.add(1);
    valueSet.add(0);
    final ArrayTable.Column pair = valueSet.freeze(0, null);
    assertTrue(
        pair.representation instanceof ArrayTable.BitSlicedPrimitiveArray);
    final ArrayTable.BitSlicedPrimitiveArray representation =
        (ArrayTable.BitSlicedPrimitiveArray) pair.representation;

    assertEquals(1, representation.bitCount);
    assertEquals(0, representation.getInt(pair.dataSet, 0));
    assertEquals(1, representation.getInt(pair.dataSet, 1));
    assertEquals(1, representation.getInt(pair.dataSet, 2));
    assertEquals(0, representation.getInt(pair.dataSet, 3));
  }

  @Test public void testValueSetZero() {
    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(boolean.class);
    valueSet.add(0);
    final ArrayTable.Column pair = valueSet.freeze(0, null);
    assertTrue(pair.representation instanceof ArrayTable.Constant);
    final ArrayTable.Constant representation =
        (ArrayTable.Constant) pair.representation;

    assertEquals(0, representation.getInt(pair.dataSet, 0));
    assertEquals(1, pair.cardinality);
  }

  @Test public void testStrings() {
    ArrayTable.Column pair;

    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(String.class);
    valueSet.add("foo");
    valueSet.add("foo");
    pair = valueSet.freeze(0, null);
    assertTrue(pair.representation instanceof ArrayTable.ObjectArray);
    final ArrayTable.ObjectArray representation =
        (ArrayTable.ObjectArray) pair.representation;
    assertEquals("foo", representation.getObject(pair.dataSet, 0));
    assertEquals("foo", representation.getObject(pair.dataSet, 1));
    assertEquals(1, pair.cardinality);

    // Large number of the same string. ObjectDictionary backed by Constant.
    for (int i = 0; i < 2000; i++) {
      valueSet.add("foo");
    }
    pair = valueSet.freeze(0, null);
    final ArrayTable.ObjectDictionary representation2 =
        (ArrayTable.ObjectDictionary) pair.representation;
    assertTrue(
        representation2.representation instanceof ArrayTable.Constant);
    assertEquals("foo", representation2.getObject(pair.dataSet, 0));
    assertEquals("foo", representation2.getObject(pair.dataSet, 1000));
    assertEquals(1, pair.cardinality);

    // One different string. ObjectDictionary backed by 1-bit
    // BitSlicedPrimitiveArray
    valueSet.add("bar");
    pair = valueSet.freeze(0, null);
    final ArrayTable.ObjectDictionary representation3 =
        (ArrayTable.ObjectDictionary) pair.representation;
    assertTrue(
        representation3.representation
            instanceof ArrayTable.BitSlicedPrimitiveArray);
    final ArrayTable.BitSlicedPrimitiveArray representation4 =
        (ArrayTable.BitSlicedPrimitiveArray) representation3.representation;
    assertEquals(1, representation4.bitCount);
    assertFalse(representation4.signed);
    assertEquals("foo", representation3.getObject(pair.dataSet, 0));
    assertEquals("foo", representation3.getObject(pair.dataSet, 1000));
    assertEquals("bar", representation3.getObject(pair.dataSet, 2003));
    assertEquals(2, pair.cardinality);
  }

  @Test public void testAllNull() {
    ArrayTable.Column pair;

    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(String.class);

    valueSet.add(null);
    pair = valueSet.freeze(0, null);
    assertTrue(pair.representation instanceof ArrayTable.ObjectArray);
    final ArrayTable.ObjectArray representation =
        (ArrayTable.ObjectArray) pair.representation;
    assertNull(representation.getObject(pair.dataSet, 0));
    assertEquals(1, pair.cardinality);

    for (int i = 0; i < 3000; i++) {
      valueSet.add(null);
    }
    pair = valueSet.freeze(0, null);
    final ArrayTable.ObjectDictionary representation2 =
        (ArrayTable.ObjectDictionary) pair.representation;
    assertTrue(
        representation2.representation instanceof ArrayTable.Constant);
    assertEquals(1, pair.cardinality);
  }

  @Test public void testOneValueOneNull() {
    ArrayTable.Column pair;

    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(String.class);
    valueSet.add(null);
    valueSet.add("foo");

    pair = valueSet.freeze(0, null);
    assertTrue(pair.representation instanceof ArrayTable.ObjectArray);
    final ArrayTable.ObjectArray representation =
        (ArrayTable.ObjectArray) pair.representation;
    assertNull(representation.getObject(pair.dataSet, 0));
    assertEquals(2, pair.cardinality);

    for (int i = 0; i < 3000; i++) {
      valueSet.add(null);
    }
    pair = valueSet.freeze(0, null);
    final ArrayTable.ObjectDictionary representation2 =
        (ArrayTable.ObjectDictionary) pair.representation;
    assertEquals(
        1,
        ((ArrayTable.BitSlicedPrimitiveArray)
            representation2.representation).bitCount);
    assertEquals("foo", representation2.getObject(pair.dataSet, 1));
    assertNull(representation2.getObject(pair.dataSet, 10));
    assertEquals(2, pair.cardinality);
  }
}

// End ArrayTableTest.java
