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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests serialization of {@link Frame}.
 */
public class FrameTest {

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
}

// End FrameTest.java
