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
package org.eigenbase.test;

import java.util.*;

import org.eigenbase.util.*;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit test for {@link Filterator}.
 */
public class FilteratorTest {
    //~ Methods ----------------------------------------------------------------

    @Test public void testOne() {
        final List<String> tomDickHarry = Arrays.asList("tom", "dick", "harry");
        final Filterator<String> filterator =
            new Filterator<String>(tomDickHarry.iterator(), String.class);

        // call hasNext twice
        assertTrue(filterator.hasNext());
        assertTrue(filterator.hasNext());
        assertEquals("tom", filterator.next());

        // call next without calling hasNext
        assertEquals("dick", filterator.next());
        assertTrue(filterator.hasNext());
        assertEquals("harry", filterator.next());
        assertFalse(filterator.hasNext());
        assertFalse(filterator.hasNext());
    }

    @Test public void testNulls() {
        // Nulls don't cause an error - but are not emitted, because they
        // fail the instanceof test.
        final List<String> tomDickHarry = Arrays.asList("paul", null, "ringo");
        final Filterator<String> filterator =
            new Filterator<String>(tomDickHarry.iterator(), String.class);
        assertEquals("paul", filterator.next());
        assertEquals("ringo", filterator.next());
        assertFalse(filterator.hasNext());
    }

    @Test public void testSubtypes() {
        final ArrayList arrayList = new ArrayList();
        final HashSet hashSet = new HashSet();
        final LinkedList linkedList = new LinkedList();
        Collection [] collections =
        {
            null,
            arrayList,
            hashSet,
            linkedList,
            null,
        };
        final Filterator<List> filterator =
            new Filterator<List>(
                Arrays.asList(collections).iterator(),
                List.class);
        assertTrue(filterator.hasNext());

        // skips null
        assertTrue(arrayList == filterator.next());

        // skips the HashSet
        assertTrue(linkedList == filterator.next());
        assertFalse(filterator.hasNext());
    }

    @Test public void testBox() {
        final Number [] numbers = { 1, 2, 3.14, 4, null, 6E23 };
        List<Integer> result = new ArrayList<Integer>();
        for (int i : Util.filter(Arrays.asList(numbers), Integer.class)) {
            result.add(i);
        }
        assertEquals("[1, 2, 4]", result.toString());
    }
}

// End FilteratorTest.java
