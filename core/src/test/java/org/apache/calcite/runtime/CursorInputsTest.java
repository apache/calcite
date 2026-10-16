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
package org.apache.calcite.runtime;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.CursorInput;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests {@link CursorInputs}. */
class CursorInputsTest {
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  private static RelDataType rowType() {
    return new JavaTypeFactoryImpl().builder()
        .add("N", SqlTypeName.INTEGER).nullable(true)
        .add("D", SqlTypeName.DATE)
        .add("T", SqlTypeName.TIME)
        .add("TS", SqlTypeName.TIMESTAMP)
        .build();
  }

  private static List<ColumnMetaData> columns() {
    return CalcitePrepareImpl.getColumnMetaDataList(new JavaTypeFactoryImpl(), rowType());
  }

  @Test void resultSetTypesAndMetadata() throws SQLException {
    final CursorInput input =
        CursorInputs.of(rowType(), Linq4j.singletonEnumerable(new Object[] {null, 1, 1000, 1000L}));
    try (ResultSet result = CursorInputs.resultSet(input.rows(), columns(), UTC)) {
      assertEquals(ResultSet.TYPE_FORWARD_ONLY, result.getType());
      assertEquals(ResultSet.CONCUR_READ_ONLY, result.getConcurrency());
      assertEquals(4, result.getMetaData().getColumnCount());
      assertEquals(java.sql.Types.DATE, result.getMetaData().getColumnType(2));
      assertEquals("N", result.getMetaData().getColumnLabel(1));
      assertTrue(result.next());
      assertEquals(0, result.getInt("n"));
      assertTrue(result.wasNull());
      assertEquals(86400000L, result.getDate(2).getTime());
      assertEquals(1000L, result.getTime(3).getTime());
      assertEquals(1000L, result.getTimestamp(4).getTime());
      assertEquals(result.getDate(2), result.getObject(2));
      assertFalse(result.wasNull());
      assertThrows(SQLException.class, () -> result.getInt(5));
      assertFalse(result.next());
    }
  }

  @Test void usesPreparedMetadataForCustomJavaRepresentation() throws SQLException {
    final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl() {
      @Override public Type getJavaClass(RelDataType type) {
        return type.getSqlTypeName() == SqlTypeName.DATE
            ? Date.class : super.getJavaClass(type);
      }
    };
    final RelDataType rowType = typeFactory.builder().add("D", SqlTypeName.DATE).build();
    final List<ColumnMetaData> columns =
        CalcitePrepareImpl.getColumnMetaDataList(typeFactory, rowType);
    assertEquals(ColumnMetaData.Rep.JAVA_SQL_DATE, columns.get(0).type.rep);
    final Date date = new Date(86400000L);
    try (ResultSet result =
        CursorInputs.resultSet(Linq4j.singletonEnumerable(new Object[] {date}), columns, UTC)) {
      assertTrue(result.next());
      assertEquals(date, result.getDate(1));
      assertEquals(date, result.getObject(1));
    }
  }

  @Test void metadataDoesNotReadRows() throws SQLException {
    try (ResultSet result = CursorInputs.metadataOnly(columns())) {
      assertEquals(4, result.getMetaData().getColumnCount());
      assertThrows(UnsupportedOperationException.class, result::next);
    }
  }

  private static CursorInput tracking(AtomicInteger opened, AtomicInteger closed) {
    return CursorInputs.of(rowType(), new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        opened.incrementAndGet();
        return new Enumerator<Object[]>() {
          @Override public Object[] current() {
            return new Object[] {1, 0, 0, 0L};
          }

          @Override public boolean moveNext() {
            return true;
          }

          @Override public void reset() {
            throw new UnsupportedOperationException();
          }

          @Override public void close() {
            closed.incrementAndGet();
          }
        };
      }
    });
  }

  private static void next(ResultSet result) {
    try {
      assertTrue(result.next());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test void closesInputsAndReopensForEachEnumeration() {
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();
    final CursorInput input = tracking(opened, closed);
    final Enumerable<Integer> result =
        CursorInputs.enumerable(
            new CursorInput[] {input},
            Collections.singletonList(columns()),
            UTC,
            cursors -> {
              next(cursors[0]);
              return Linq4j.singletonEnumerable(1);
            });
    assertEquals(0, opened.get());
    try (Enumerator<Integer> rows = result.enumerator()) {
      assertTrue(rows.moveNext());
    }
    assertEquals(1, closed.get());
    try (Enumerator<Integer> rows = result.enumerator()) {
      assertTrue(rows.moveNext());
      assertFalse(rows.moveNext());
      assertEquals(2, closed.get());
    }
    assertEquals(2, opened.get());
    assertEquals(2, closed.get());
  }

  @Test void closesInputsWhenFunctionFails() {
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();
    final RuntimeException failure = new RuntimeException("function failed");
    final Enumerable<Object> result =
        CursorInputs.enumerable(
            new CursorInput[] {tracking(opened, closed), tracking(opened, closed)},
            Arrays.asList(columns(), columns()),
            UTC,
            cursors -> {
              next(cursors[0]);
              next(cursors[1]);
              throw failure;
            });
    assertSame(failure, assertThrows(RuntimeException.class, result::enumerator));
    assertEquals(2, closed.get());
  }

  @Test void closesInputsWhenOutputFails() {
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();
    final RuntimeException failure = new RuntimeException("output failed");
    final Enumerable<Object> result =
        CursorInputs.enumerable(
            new CursorInput[] {tracking(opened, closed)},
            Collections.singletonList(columns()),
            UTC,
            cursors -> {
              next(cursors[0]);
              return Linq4j.singletonEnumerable(1).select(value -> {
                throw failure;
              });
            });
    try (Enumerator<Object> rows = result.enumerator()) {
      assertTrue(rows.moveNext());
      assertSame(failure, assertThrows(RuntimeException.class, rows::current));
      assertEquals(1, closed.get());
    }
    assertEquals(1, closed.get());
  }

  @Test void closingBeforeReadDoesNotOpenInput() throws SQLException {
    final AtomicInteger opened = new AtomicInteger();
    final ResultSet result =
        CursorInputs.resultSet(tracking(opened, new AtomicInteger()).rows(), columns(), UTC);
    result.close();
    result.close();
    assertEquals(0, opened.get());
    assertTrue(result.isClosed());
    assertThrows(SQLException.class, result::next);
  }
}
