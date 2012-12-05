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

import junit.framework.TestCase;

import org.eigenbase.util.Pair;

import java.util.Arrays;

/**
 * Unit test for {@link ArrayTable} and {@link ColumnLoader}.
 */
public class ArrayTableTest extends TestCase {
    public void testPrimitiveArray() {
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

    public void testNextPowerOf2() {
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

    public void testLog2() {
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

    public void testValueSetInt() {
        ArrayTable.BitSlicedPrimitiveArray representation;
        Pair<ArrayTable.Representation, Object> pair;

        final ColumnLoader.ValueSet valueSet =
            new ColumnLoader.ValueSet(int.class);
        valueSet.add(0);
        valueSet.add(1);
        valueSet.add(10);
        pair = valueSet.freeze(0);
        assertTrue(pair.left instanceof ArrayTable.BitSlicedPrimitiveArray);
        representation = (ArrayTable.BitSlicedPrimitiveArray) pair.left;

        // unsigned 4 bit integer (values 0..15)
        assertEquals(4, representation.bitCount);
        assertFalse(representation.signed);
        assertEquals(0, representation.getInt(pair.right, 0));
        assertEquals(1, representation.getInt(pair.right, 1));
        assertEquals(10, representation.getInt(pair.right, 2));
        assertEquals(10, representation.getObject(pair.right, 2));

        // -32 takes us to 6 bit signed
        valueSet.add(-32);
        pair = valueSet.freeze(0);
        assertTrue(pair.left instanceof ArrayTable.BitSlicedPrimitiveArray);
        representation = (ArrayTable.BitSlicedPrimitiveArray) pair.left;
        assertEquals(6, representation.bitCount);
        assertTrue(representation.signed);
        assertEquals(10, representation.getInt(pair.right, 2));
        assertEquals(10, representation.getObject(pair.right, 2));
        assertEquals(-32, representation.getInt(pair.right, 3));
        assertEquals(-32, representation.getObject(pair.right, 3));

        // 63 takes us to 7 bit signed
        valueSet.add(63);
        pair = valueSet.freeze(0);
        assertTrue(pair.left instanceof ArrayTable.BitSlicedPrimitiveArray);
        representation = (ArrayTable.BitSlicedPrimitiveArray) pair.left;
        assertEquals(7, representation.bitCount);
        assertTrue(representation.signed);

        // 128 pushes us to 8 bit signed, i.e. byte
        valueSet.add(64);
        pair = valueSet.freeze(0);
        assertTrue(pair.left instanceof ArrayTable.PrimitiveArray);
        ArrayTable.PrimitiveArray representation2 =
            (ArrayTable.PrimitiveArray) pair.left;
        assertEquals(0, representation2.getInt(pair.right, 0));
        assertEquals(-32, representation2.getInt(pair.right, 3));
        assertEquals(-32, representation2.getObject(pair.right, 3));
        assertEquals(64, representation2.getInt(pair.right, 5));
        assertEquals(64, representation2.getObject(pair.right, 5));
    }

    public void testValueSetBoolean() {
        final ColumnLoader.ValueSet valueSet =
            new ColumnLoader.ValueSet(boolean.class);
        valueSet.add(0);
        valueSet.add(1);
        valueSet.add(1);
        valueSet.add(0);
        final Pair<ArrayTable.Representation, Object> pair = valueSet.freeze(0);
        assertTrue(pair.left instanceof ArrayTable.BitSlicedPrimitiveArray);
        final ArrayTable.BitSlicedPrimitiveArray representation =
            (ArrayTable.BitSlicedPrimitiveArray) pair.left;

        assertEquals(1, representation.bitCount);
        assertEquals(0, representation.getInt(pair.right, 0));
        assertEquals(1, representation.getInt(pair.right, 1));
        assertEquals(1, representation.getInt(pair.right, 2));
        assertEquals(0, representation.getInt(pair.right, 3));
    }

    public void testValueSetZero() {
        final ColumnLoader.ValueSet valueSet =
            new ColumnLoader.ValueSet(boolean.class);
        valueSet.add(0);
        final Pair<ArrayTable.Representation, Object> pair = valueSet.freeze(0);
        assertTrue(pair.left instanceof ArrayTable.Constant);
        final ArrayTable.Constant representation =
            (ArrayTable.Constant) pair.left;

        assertEquals(0, representation.getInt(pair.right, 0));
    }

    public void testStrings() {
        Pair<ArrayTable.Representation, Object> pair;

        final ColumnLoader.ValueSet valueSet =
            new ColumnLoader.ValueSet(String.class);
        valueSet.add("foo");
        valueSet.add("foo");
        pair = valueSet.freeze(0);
        assertTrue(pair.left instanceof ArrayTable.ObjectArray);
        final ArrayTable.ObjectArray representation =
            (ArrayTable.ObjectArray) pair.left;
        assertEquals("foo", representation.getObject(pair.right, 0));
        assertEquals("foo", representation.getObject(pair.right, 1));

        // Large number of the same string. ObjectDictionary backed by Constant.
        for (int i = 0; i < 2000; i++) {
            valueSet.add("foo");
        }
        pair = valueSet.freeze(0);
        final ArrayTable.ObjectDictionary representation2 =
            (ArrayTable.ObjectDictionary) pair.left;
        assertTrue(
            representation2.representation instanceof ArrayTable.Constant);
        assertEquals("foo", representation2.getObject(pair.right, 0));
        assertEquals("foo", representation2.getObject(pair.right, 1000));

        // One different string. ObjectDictionary backed by 1-bit
        // BitSlicedPrimitiveArray
        valueSet.add("bar");
        pair = valueSet.freeze(0);
        final ArrayTable.ObjectDictionary representation3 =
            (ArrayTable.ObjectDictionary) pair.left;
        assertTrue(
            representation3.representation
                instanceof ArrayTable.BitSlicedPrimitiveArray);
        final ArrayTable.BitSlicedPrimitiveArray representation4 =
            (ArrayTable.BitSlicedPrimitiveArray) representation3.representation;
        assertEquals(1, representation4.bitCount);
        assertFalse(representation4.signed);
        assertEquals("foo", representation3.getObject(pair.right, 0));
        assertEquals("foo", representation3.getObject(pair.right, 1000));
        assertEquals("bar", representation3.getObject(pair.right, 2003));
    }
}

// End ArrayTableTest.java
