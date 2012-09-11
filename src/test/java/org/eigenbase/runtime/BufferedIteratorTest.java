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
package org.eigenbase.runtime;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eigenbase.test.EigenbaseTestCase;

/**
 * Unit test for {@link BufferedIterator}.
 *
 * @author jhyde
 */
public class BufferedIteratorTest
    extends EigenbaseTestCase
{
    public BufferedIteratorTest(String s)
        throws Exception
    {
        super(s);
    }

    // --------------------------------------------------------------------
    // test BufferedIterator
    public void testBufferedIterator()
    {
        String [] abc = new String[] { "a", "b", "c" };
        Iterator source = makeIterator(abc);
        BufferedIterator iterator = new BufferedIterator(source);
        assertTrue(iterator.hasNext());
        assertTrue(iterator.next().equals("a"));

        // no intervening "hasNext"
        assertTrue(iterator.next().equals("b"));

        // restart before we get to the end
        iterator.restart();
        assertTrue(iterator.hasNext());
        assertEquals(iterator, abc);
        assertTrue(!iterator.hasNext());
        assertTrue(!iterator.hasNext());
        iterator.restart();
        assertEquals(iterator, abc);
    }

    // --------------------------------------------------------------------
    // test Clonerator
    public void testClonerator()
    {
        String [] ab = new String[] { "a", "b" };
        Iterator source = makeIterator(ab);
        List list = new ArrayList();
        BufferedIterator.Clonerator clonerator =
            new BufferedIterator.Clonerator(source, list);
        assertEquals(clonerator, ab);
        assertEquals(list, ab);
    }
}

// End BufferedIteratorTest.java
