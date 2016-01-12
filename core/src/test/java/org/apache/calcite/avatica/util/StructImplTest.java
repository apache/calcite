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
package org.apache.calcite.avatica.util;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.StructType;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.util.Cursor.Accessor;

import org.junit.Test;

import java.sql.Struct;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for StructImpl.
 */
public class StructImplTest {

  @Test public void structAccessor() throws Exception {
    // Define the struct type we're creating
    ColumnMetaData intMetaData = MetaImpl.columnMetaData("MY_INT", 1, int.class, false);
    ColumnMetaData stringMetaData = MetaImpl.columnMetaData("MY_STRING", 2, String.class, true);
    StructType structType = ColumnMetaData.struct(Arrays.asList(intMetaData, stringMetaData));
    // Create some structs
    Struct struct1 = new StructImpl(Arrays.<Object>asList(1, "one"));
    Struct struct2 = new StructImpl(Arrays.<Object>asList(2, "two"));
    Struct struct3 = new StructImpl(Arrays.<Object>asList(3));
    Struct struct4 = new StructImpl(Arrays.<Object>asList(4, "four", "ignored"));
    ColumnMetaData structMetaData = MetaImpl.columnMetaData("MY_STRUCT", 1, structType, false);
    List<List<Object>> rows = Arrays.asList(Collections.<Object>singletonList(struct1),
        Collections.<Object>singletonList(struct2), Collections.<Object>singletonList(struct3),
        Collections.<Object>singletonList(struct4));
    // Create four rows, each with one (struct) column
    try (Cursor cursor = new ListIteratorCursor(rows.iterator())) {
      List<Accessor> accessors = cursor.createAccessors(Collections.singletonList(structMetaData),
          Unsafe.localCalendar(), null);
      assertEquals(1, accessors.size());
      Accessor accessor = accessors.get(0);

      assertTrue(cursor.next());
      Struct s = accessor.getObject(Struct.class);
      Object[] structData = s.getAttributes();
      assertEquals(2, structData.length);
      assertEquals(1, structData[0]);
      assertEquals("one", structData[1]);

      assertTrue(cursor.next());
      s = accessor.getObject(Struct.class);
      structData = s.getAttributes();
      assertEquals(2, structData.length);
      assertEquals(2, structData[0]);
      assertEquals("two", structData[1]);

      assertTrue(cursor.next());
      s = accessor.getObject(Struct.class);
      structData = s.getAttributes();
      assertEquals(1, structData.length);
      assertEquals(3, structData[0]);

      assertTrue(cursor.next());
      s = accessor.getObject(Struct.class);
      structData = s.getAttributes();
      assertEquals(3, structData.length);
      assertEquals(4, structData[0]);
      assertEquals("four", structData[1]);
      // We didn't provide metadata, but we still expect to see it.
      assertEquals("ignored", structData[2]);
    }
  }
}

// End StructImplTest.java
