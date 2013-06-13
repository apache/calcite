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
package org.eigenbase.util;


import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit test for {@link Permutation}.
 */
public class PermutationTestCase {
    //~ Constructors -----------------------------------------------------------

    public PermutationTestCase() {
    }

    //~ Methods ----------------------------------------------------------------

    @Test public void testOne() {
        final Permutation perm = new Permutation(4);
        assertEquals(
            "[0, 1, 2, 3]",
            perm.toString());
        assertEquals(
            4,
            perm.size());

        perm.set(0, 2);
        assertEquals(
            "[2, 1, 0, 3]",
            perm.toString());

        perm.set(1, 0);
        assertEquals(
            "[2, 0, 1, 3]",
            perm.toString());

        final Permutation invPerm = perm.inverse();
        assertEquals(
            "[1, 2, 0, 3]",
            invPerm.toString());

        // changing perm doesn't change inverse
        perm.set(0, 0);
        assertEquals(
            "[0, 2, 1, 3]",
            perm.toString());
        assertEquals(
            "[1, 2, 0, 3]",
            invPerm.toString());
    }

    @Test public void testTwo() {
        final Permutation perm = new Permutation(new int[] { 3, 2, 0, 1 });
        assertFalse(perm.isIdentity());
        assertEquals(
            "[3, 2, 0, 1]",
            perm.toString());

        Permutation perm2 = (Permutation) perm.clone();
        assertEquals(
            "[3, 2, 0, 1]",
            perm2.toString());
        assertTrue(perm.equals(perm2));
        assertTrue(perm2.equals(perm));

        perm.set(2, 1);
        assertEquals(
            "[3, 2, 1, 0]",
            perm.toString());
        assertFalse(perm.equals(perm2));

        // clone not affected
        assertEquals(
            "[3, 2, 0, 1]",
            perm2.toString());

        perm2.set(2, 3);
        assertEquals(
            "[0, 2, 3, 1]",
            perm2.toString());
    }

    @Test public void testInsert() {
        Permutation perm = new Permutation(new int[] { 3, 0, 4, 2, 1 });
        perm.insertTarget(2);
        assertEquals(
            "[4, 0, 5, 3, 1, 2]",
            perm.toString());

        // insert at start
        perm = new Permutation(new int[] { 3, 0, 4, 2, 1 });
        perm.insertTarget(0);
        assertEquals(
            "[4, 1, 5, 3, 2, 0]",
            perm.toString());

        // insert at end
        perm = new Permutation(new int[] { 3, 0, 4, 2, 1 });
        perm.insertTarget(5);
        assertEquals(
            "[3, 0, 4, 2, 1, 5]",
            perm.toString());

        // insert into empty
        perm = new Permutation(new int[] {});
        perm.insertTarget(0);
        assertEquals(
            "[0]",
            perm.toString());
    }

    @Test public void testEmpty() {
        final Permutation perm = new Permutation(0);
        assertTrue(perm.isIdentity());
        assertEquals(
            "[]",
            perm.toString());
        assertTrue(perm.equals(perm));
        assertTrue(perm.equals(perm.inverse()));

        try {
            perm.set(1, 0);
            fail("expected exception");
        } catch (ArrayIndexOutOfBoundsException e) {
            // success
        }

        try {
            perm.set(-1, 2);
            fail("expected exception");
        } catch (ArrayIndexOutOfBoundsException e) {
            // success
        }
    }
}

// End PermutationTestCase.java
