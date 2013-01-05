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
package net.hydromatic.linq4j.test;

import net.hydromatic.linq4j.expressions.Primitive;

import junit.framework.TestCase;


/**
 * Unit test for {@link Primitive}.
 */
public class PrimitiveTest extends TestCase {
    public void testIsAssignableFrom() {
        assertTrue(Primitive.INT.assignableFrom(Primitive.BYTE));
        assertTrue(Primitive.INT.assignableFrom(Primitive.SHORT));
        assertTrue(Primitive.INT.assignableFrom(Primitive.CHAR));
        assertTrue(Primitive.INT.assignableFrom(Primitive.INT));
        assertTrue(Primitive.INT.assignableFrom(Primitive.SHORT));
        assertFalse(Primitive.INT.assignableFrom(Primitive.LONG));

        assertTrue(Primitive.LONG.assignableFrom(Primitive.BYTE));
        assertTrue(Primitive.LONG.assignableFrom(Primitive.SHORT));
        assertTrue(Primitive.LONG.assignableFrom(Primitive.CHAR));
        assertTrue(Primitive.LONG.assignableFrom(Primitive.INT));
        assertTrue(Primitive.LONG.assignableFrom(Primitive.LONG));

        // SHORT and CHAR cannot be assigned to each other

        assertTrue(Primitive.SHORT.assignableFrom(Primitive.BYTE));
        assertTrue(Primitive.SHORT.assignableFrom(Primitive.SHORT));
        assertFalse(Primitive.SHORT.assignableFrom(Primitive.CHAR));
        assertFalse(Primitive.SHORT.assignableFrom(Primitive.INT));
        assertFalse(Primitive.SHORT.assignableFrom(Primitive.LONG));

        assertTrue(Primitive.CHAR.assignableFrom(Primitive.BYTE));
        assertFalse(Primitive.CHAR.assignableFrom(Primitive.SHORT));
        assertTrue(Primitive.CHAR.assignableFrom(Primitive.CHAR));
        assertFalse(Primitive.CHAR.assignableFrom(Primitive.INT));
        assertFalse(Primitive.CHAR.assignableFrom(Primitive.LONG));

        // cross-family assignments

        assertFalse(Primitive.BOOLEAN.assignableFrom(Primitive.INT));
        assertFalse(Primitive.INT.assignableFrom(Primitive.BOOLEAN));
    }

    public void testBox() {
        assertEquals(String.class, Primitive.box(String.class));
        assertEquals(Integer.class, Primitive.box(int.class));
        assertEquals(Integer.class, Primitive.box(Integer.class));
        assertEquals(boolean[].class, Primitive.box(boolean[].class));
    }
}

// End PrimitiveTest.java
