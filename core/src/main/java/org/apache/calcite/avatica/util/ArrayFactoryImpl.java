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

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.ArrayType;
import org.apache.calcite.avatica.ColumnMetaData.AvaticaType;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.ColumnMetaData.ScalarType;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.util.AbstractCursor.ArrayAccessor;
import org.apache.calcite.avatica.util.Cursor.Accessor;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Implementation of {@link ArrayImpl.Factory}.
 */
public class ArrayFactoryImpl implements ArrayImpl.Factory {
  private TimeZone timeZone;

  public ArrayFactoryImpl(TimeZone timeZone) {
    this.timeZone = Objects.requireNonNull(timeZone);
  }

  @Override public ResultSet create(AvaticaType elementType, Iterable<Object> elements)
      throws SQLException {
    // The ColumnMetaData for offset "1" in the ResultSet for an Array.
    ScalarType arrayOffsetType = ColumnMetaData.scalar(Types.INTEGER, "INTEGER", Rep.PRIMITIVE_INT);
    // Two columns (types) in the ResultSet we will create
    List<ColumnMetaData> types = Arrays.asList(ColumnMetaData.dummy(arrayOffsetType, false),
        ColumnMetaData.dummy(elementType, true));
    List<List<Object>> rows = createResultSetRowsForArrayData(elements);
    // `(List<Object>) rows` is a compile error.
    @SuppressWarnings({ "unchecked", "rawtypes" })
    List<Object> untypedRows = (List<Object>) ((List) rows);
    try (ListIteratorCursor cursor = new ListIteratorCursor(rows.iterator())) {
      final String sql = "MOCKED";
      QueryState state = new QueryState(sql);
      Meta.Signature signature = new Meta.Signature(types, sql,
          Collections.<AvaticaParameter>emptyList(), Collections.<String, Object>emptyMap(),
          Meta.CursorFactory.LIST, Meta.StatementType.SELECT);
      AvaticaResultSetMetaData resultSetMetaData = new AvaticaResultSetMetaData(null, sql,
          signature);
      Meta.Frame frame = new Meta.Frame(0, true, untypedRows);
      AvaticaResultSet resultSet = new AvaticaResultSet(null, state, signature, resultSetMetaData,
          timeZone, frame);
      resultSet.execute2(cursor, types);
      return resultSet;
    }
  }

  @Override public Array createArray(AvaticaType elementType, Iterable<Object> elements) {
    final ArrayType array = ColumnMetaData.array(elementType, elementType.name, Rep.ARRAY);
    final List<ColumnMetaData> types = Collections.singletonList(ColumnMetaData.dummy(array, true));
    // Avoid creating a new List if we already have a List
    List<Object> elementList;
    if (elements instanceof List) {
      elementList = (List<Object>) elements;
    } else {
      elementList = new ArrayList<>();
      for (Object element : elements) {
        elementList.add(element);
      }
    }
    try (ListIteratorCursor cursor = new ListIteratorCursor(createRowForArrayData(elementList))) {
      List<Accessor> accessor = cursor.createAccessors(types, Unsafe.localCalendar(), this);
      assert 1 == accessor.size();
      return new ArrayImpl(elementList, (ArrayAccessor) accessor.get(0));
    }
  }

  /**
   * Creates the row-level view over the values that will make up an Array. The Iterator has a row
   * per Array element, each row containing two columns. The second column is the array element and
   * the first column is the offset into the array of that array element (one-based, not
   * zero-based).
   *
   * <p>The ordering of the rows is not guaranteed to be in the same order as the array elements.
   *
   * <p>A list of {@code elements}:
   * <pre>[1, 2, 3]</pre>
   * <p>might be converted into
   * <pre>Iterator{ [1, 1], [2, 2], [3, 3] }</pre>
   *
   * @param elements The elements of an array.
   */
  private List<List<Object>> createResultSetRowsForArrayData(Iterable<Object> elements) {
    List<List<Object>> rows = new ArrayList<>();
    int i = 0;
    for (Object element : elements) {
      rows.add(Arrays.asList(i + 1, element));
      i++;
    }
    return rows;
  }

  /**
   * Creates an row-level view over the values that will make up an Array. The Iterator has one
   * entry which has a list that also has one entry.
   *
   * <p>A provided list of {@code elements}
   * <pre>[1, 2, 3]</pre>
   * <p>would be converted into
   * <pre>Iterator{ [ [1,2,3] ] }</pre>
   *
   * @param elements The elements of an array
   */
  private Iterator<List<Object>> createRowForArrayData(List<Object> elements) {
    // Make a "row" with one "column" (which is really a list)
    final List<Object> row = Collections.singletonList((Object) elements);
    // Make an iterator over this one "row"
    return Collections.singletonList(row).iterator();
  }
}

// End ArrayFactoryImpl.java
