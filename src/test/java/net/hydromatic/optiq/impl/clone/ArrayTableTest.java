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

import java.util.Arrays;

/**
 * Unit test for {@link ArrayTable}.
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
        System.out.println(Long.toHexString(2619));
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
}

// End ArrayTableTest.java
