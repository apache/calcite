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
package org.apache.calcite.adapter.clone;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link ArrayTable} and {@link ColumnLoader}.
 */
class ArrayTableTest {
  @Test void testPrimitiveArray() {
    long[] values = {0, 0};
    ArrayTable.BitSlicedPrimitiveArray.orLong(4, values, 0, 0x0F);
    assertThat(values[0], is(0x0FL));
    ArrayTable.BitSlicedPrimitiveArray.orLong(4, values, 2, 0x0F);
    assertThat(values[0], is(0xF0FL));

    values = new long[]{
        0x1213141516171819L, 0x232425262728292AL, 0x3435363738393A3BL};
    assertThat(ArrayTable.BitSlicedPrimitiveArray.getLong(12, values, 9),
        is(0x324L));
    assertThat(ArrayTable.BitSlicedPrimitiveArray.getLong(12, values, 10),
        is(0xa3bL));

    Arrays.fill(values, 0);
    for (int i = 0; i < 10; i++) {
      ArrayTable.BitSlicedPrimitiveArray.orLong(10, values, i, i);
    }

    for (int i = 0; i < 10; i++) {
      assertThat(ArrayTable.BitSlicedPrimitiveArray.getLong(10, values, i),
          is((long) i));
    }
  }

  @Test void testNextPowerOf2() {
    assertThat(ColumnLoader.nextPowerOf2(1), is(1));
    assertThat(ColumnLoader.nextPowerOf2(2), is(2));
    assertThat(ColumnLoader.nextPowerOf2(3), is(4));
    assertThat(ColumnLoader.nextPowerOf2(4), is(4));
    assertThat(ColumnLoader.nextPowerOf2(0x3456789a), is(0x40000000));
    assertThat(ColumnLoader.nextPowerOf2(0x40000000), is(0x40000000));
    // overflow
    assertThat(ColumnLoader.nextPowerOf2(0x7fffffff), is(0x80000000));
    assertThat(ColumnLoader.nextPowerOf2(0x7ffffffe), is(0x80000000));
  }

  @Test void testLog2() {
    assertThat(ColumnLoader.log2(0), is(0));
    assertThat(ColumnLoader.log2(1), is(0));
    assertThat(ColumnLoader.log2(2), is(1));
    assertThat(ColumnLoader.log2(4), is(2));
    assertThat(ColumnLoader.log2(65536), is(16));
    assertThat(ColumnLoader.log2(65535), is(15));
    assertThat(ColumnLoader.log2(65537), is(16));
    assertThat(ColumnLoader.log2(Integer.MAX_VALUE), is(30));
    assertThat(ColumnLoader.log2(Integer.MAX_VALUE - 1), is(30));
    assertThat(ColumnLoader.log2(0x3fffffff), is(29));
    assertThat(ColumnLoader.log2(0x40000000), is(30));
  }

  @Test void testValueSetInt() {
    ArrayTable.BitSlicedPrimitiveArray representation;
    ArrayTable.Column pair;

    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(int.class);
    valueSet.add(0);
    valueSet.add(1);
    valueSet.add(10);
    pair = valueSet.freeze(0, null);
    assertThat(pair.representation,
        instanceOf(ArrayTable.BitSlicedPrimitiveArray.class));
    representation =
        (ArrayTable.BitSlicedPrimitiveArray) pair.representation;

    // unsigned 4 bit integer (values 0..15)
    assertThat(representation.bitCount, is(4));
    assertFalse(representation.signed);
    assertThat(representation.getInt(pair.dataSet, 0), is(0));
    assertThat(representation.getInt(pair.dataSet, 1), is(1));
    assertThat(representation.getInt(pair.dataSet, 2), is(10));
    assertThat(representation.getObject(pair.dataSet, 2), is(10));

    // -32 takes us to 6 bit signed
    valueSet.add(-32);
    pair = valueSet.freeze(0, null);
    assertThat(pair.representation,
        instanceOf(ArrayTable.BitSlicedPrimitiveArray.class));
    representation =
        (ArrayTable.BitSlicedPrimitiveArray) pair.representation;
    assertThat(representation.bitCount, is(6));
    assertTrue(representation.signed);
    assertThat(representation.getInt(pair.dataSet, 2), is(10));
    assertThat(representation.getObject(pair.dataSet, 2), is(10));
    assertThat(representation.getInt(pair.dataSet, 3), is(-32));
    assertThat(representation.getObject(pair.dataSet, 3), is(-32));

    // 63 takes us to 7 bit signed
    valueSet.add(63);
    pair = valueSet.freeze(0, null);
    assertThat(pair.representation,
        instanceOf(ArrayTable.BitSlicedPrimitiveArray.class));
    representation =
        (ArrayTable.BitSlicedPrimitiveArray) pair.representation;
    assertThat(representation.bitCount, is(7));
    assertTrue(representation.signed);

    // 128 pushes us to 8 bit signed, i.e. byte
    valueSet.add(64);
    pair = valueSet.freeze(0, null);
    assertThat(pair.representation,
        instanceOf(ArrayTable.PrimitiveArray.class));
    ArrayTable.PrimitiveArray representation2 =
        (ArrayTable.PrimitiveArray) pair.representation;
    assertThat(representation2.getInt(pair.dataSet, 0), is(0));
    assertThat(representation2.getInt(pair.dataSet, 3), is(-32));
    assertThat(representation2.getObject(pair.dataSet, 3), is(-32));
    assertThat(representation2.getInt(pair.dataSet, 5), is(64));
    assertThat(representation2.getObject(pair.dataSet, 5), is(64));
  }

  @Test void testValueSetBoolean() {
    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(boolean.class);
    valueSet.add(0);
    valueSet.add(1);
    valueSet.add(1);
    valueSet.add(0);
    final ArrayTable.Column pair = valueSet.freeze(0, null);
    assertThat(pair.representation,
        instanceOf(ArrayTable.BitSlicedPrimitiveArray.class));
    final ArrayTable.BitSlicedPrimitiveArray representation =
        (ArrayTable.BitSlicedPrimitiveArray) pair.representation;

    assertThat(representation.bitCount, is(1));
    assertThat(representation.getInt(pair.dataSet, 0), is(0));
    assertThat(representation.getInt(pair.dataSet, 1), is(1));
    assertThat(representation.getInt(pair.dataSet, 2), is(1));
    assertThat(representation.getInt(pair.dataSet, 3), is(0));
  }

  @Test void testValueSetZero() {
    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(boolean.class);
    valueSet.add(0);
    final ArrayTable.Column pair = valueSet.freeze(0, null);
    assertThat(pair.representation, instanceOf(ArrayTable.Constant.class));
    final ArrayTable.Constant representation =
        (ArrayTable.Constant) pair.representation;

    assertThat(representation.getInt(pair.dataSet, 0), is(0));
    assertThat(pair.cardinality, is(1));
  }

  @Test void testStrings() {
    ArrayTable.Column pair;

    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(String.class);
    valueSet.add("foo");
    valueSet.add("foo");
    pair = valueSet.freeze(0, null);
    assertThat(pair.representation, instanceOf(ArrayTable.ObjectArray.class));
    final ArrayTable.ObjectArray representation =
        (ArrayTable.ObjectArray) pair.representation;
    assertThat(representation.getObject(pair.dataSet, 0), is("foo"));
    assertThat(representation.getObject(pair.dataSet, 1), is("foo"));
    assertThat(pair.cardinality, is(1));

    // Large number of the same string. ObjectDictionary backed by Constant.
    for (int i = 0; i < 2000; i++) {
      valueSet.add("foo");
    }
    pair = valueSet.freeze(0, null);
    final ArrayTable.ObjectDictionary representation2 =
        (ArrayTable.ObjectDictionary) pair.representation;
    assertThat(representation2.representation,
        instanceOf(ArrayTable.Constant.class));
    assertThat(representation2.getObject(pair.dataSet, 0), is("foo"));
    assertThat(representation2.getObject(pair.dataSet, 1000), is("foo"));
    assertThat(pair.cardinality, is(1));

    // One different string. ObjectDictionary backed by 1-bit
    // BitSlicedPrimitiveArray
    valueSet.add("bar");
    pair = valueSet.freeze(0, null);
    final ArrayTable.ObjectDictionary representation3 =
        (ArrayTable.ObjectDictionary) pair.representation;
    assertThat(representation3.representation,
        instanceOf(ArrayTable.BitSlicedPrimitiveArray.class));
    final ArrayTable.BitSlicedPrimitiveArray representation4 =
        (ArrayTable.BitSlicedPrimitiveArray) representation3.representation;
    assertThat(representation4.bitCount, is(1));
    assertFalse(representation4.signed);
    assertThat(representation3.getObject(pair.dataSet, 0), is("foo"));
    assertThat(representation3.getObject(pair.dataSet, 1000), is("foo"));
    assertThat(representation3.getObject(pair.dataSet, 2003), is("bar"));
    assertThat(pair.cardinality, is(2));
  }

  @Test void testAllNull() {
    ArrayTable.Column pair;

    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(String.class);

    valueSet.add(null);
    pair = valueSet.freeze(0, null);
    assertThat(pair.representation, instanceOf(ArrayTable.ObjectArray.class));
    final ArrayTable.ObjectArray representation =
        (ArrayTable.ObjectArray) pair.representation;
    assertNull(representation.getObject(pair.dataSet, 0));
    assertThat(pair.cardinality, is(1));

    for (int i = 0; i < 3000; i++) {
      valueSet.add(null);
    }
    pair = valueSet.freeze(0, null);
    final ArrayTable.ObjectDictionary representation2 =
        (ArrayTable.ObjectDictionary) pair.representation;
    assertThat(representation2.representation,
        instanceOf(ArrayTable.Constant.class));
    assertThat(pair.cardinality, is(1));
  }

  @Test void testOneValueOneNull() {
    ArrayTable.Column pair;

    final ColumnLoader.ValueSet valueSet =
        new ColumnLoader.ValueSet(String.class);
    valueSet.add(null);
    valueSet.add("foo");

    pair = valueSet.freeze(0, null);
    assertThat(pair.representation, instanceOf(ArrayTable.ObjectArray.class));
    final ArrayTable.ObjectArray representation =
        (ArrayTable.ObjectArray) pair.representation;
    assertNull(representation.getObject(pair.dataSet, 0));
    assertThat(pair.cardinality, is(2));

    for (int i = 0; i < 3000; i++) {
      valueSet.add(null);
    }
    pair = valueSet.freeze(0, null);
    final ArrayTable.ObjectDictionary representation2 =
        (ArrayTable.ObjectDictionary) pair.representation;
    assertThat(
        ((ArrayTable.BitSlicedPrimitiveArray)
            representation2.representation).bitCount, is(1));
    assertThat(representation2.getObject(pair.dataSet, 1), is("foo"));
    assertNull(representation2.getObject(pair.dataSet, 10));
    assertThat(pair.cardinality, is(2));
  }

  @Test void testLoadSorted() {
    final JavaTypeFactoryImpl typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType rowType =
        typeFactory.builder()
            .add("empid", typeFactory.createType(int.class))
            .add("deptno", typeFactory.createType(int.class))
            .add("name", typeFactory.createType(String.class))
            .build();
    final Enumerable<Object[]> enumerable =
        Linq4j.asEnumerable(
            Arrays.asList(
                new Object[]{100, 10, "Bill"},
                new Object[]{200, 20, "Eric"},
                new Object[]{150, 10, "Sebastian"},
                new Object[]{160, 10, "Theodore"}));
    final ColumnLoader<Object[]> loader =
        new ColumnLoader<Object[]>(typeFactory, enumerable,
            RelDataTypeImpl.proto(rowType), null);
    checkColumn(
        loader.representationValues.get(0),
        ArrayTable.RepresentationType.BIT_SLICED_PRIMITIVE_ARRAY,
        "Column(representation=BitSlicedPrimitiveArray(ordinal=0, bitCount=8, primitive=INT, signed=false), value=[100, 150, 160, 200, 0, 0, 0, 0])");
    checkColumn(
        loader.representationValues.get(1),
        ArrayTable.RepresentationType.BIT_SLICED_PRIMITIVE_ARRAY,
        "Column(representation=BitSlicedPrimitiveArray(ordinal=1, bitCount=5, primitive=INT, signed=false), value=[10, 10, 10, 20, 0, 0, 0, 0, 0, 0, 0, 0])");
    checkColumn(
        loader.representationValues.get(2),
        ArrayTable.RepresentationType.OBJECT_ARRAY,
        "Column(representation=ObjectArray(ordinal=2), value=[Bill, Sebastian, Theodore, Eric])");
  }

  /** As {@link #testLoadSorted()} but column #1 is the unique column, not
   * column #0. The algorithm needs to go back and permute the values of
   * column #0 after it discovers that column #1 is unique and sorts by it. */
  @Test void testLoadSorted2() {
    final JavaTypeFactoryImpl typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataType rowType =
        typeFactory.builder()
            .add("deptno", typeFactory.createType(int.class))
            .add("empid", typeFactory.createType(int.class))
            .add("name", typeFactory.createType(String.class))
            .build();
    final Enumerable<Object[]> enumerable =
        Linq4j.asEnumerable(
            Arrays.asList(
                new Object[]{10, 100, "Bill"},
                new Object[]{20, 200, "Eric"},
                new Object[]{30, 150, "Sebastian"},
                new Object[]{10, 160, "Theodore"}));
    final ColumnLoader<Object[]> loader =
        new ColumnLoader<Object[]>(typeFactory, enumerable,
            RelDataTypeImpl.proto(rowType), null);
    // Note that values have been sorted with {20, 200, Eric} last because the
    // value 200 is the highest value of empid, the unique column.
    checkColumn(
        loader.representationValues.get(0),
        ArrayTable.RepresentationType.BIT_SLICED_PRIMITIVE_ARRAY,
        "Column(representation=BitSlicedPrimitiveArray(ordinal=0, bitCount=5, primitive=INT, signed=false), value=[10, 30, 10, 20, 0, 0, 0, 0, 0, 0, 0, 0])");
    checkColumn(
        loader.representationValues.get(1),
        ArrayTable.RepresentationType.BIT_SLICED_PRIMITIVE_ARRAY,
        "Column(representation=BitSlicedPrimitiveArray(ordinal=1, bitCount=8, primitive=INT, signed=false), value=[100, 150, 160, 200, 0, 0, 0, 0])");
    checkColumn(
        loader.representationValues.get(2),
        ArrayTable.RepresentationType.OBJECT_ARRAY,
        "Column(representation=ObjectArray(ordinal=2), value=[Bill, Sebastian, Theodore, Eric])");
  }

  private void checkColumn(ArrayTable.Column x,
      ArrayTable.RepresentationType expectedRepresentationType,
      String expectedString) {
    assertThat(x.representation.getType(), is(expectedRepresentationType));
    assertThat(x, hasToString(expectedString));
  }
}
