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
package org.apache.calcite.test;

import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TestUtil;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests external Java values in nested SQL query results. */
class NestedResultTypesTest {
  /** Calcite JDBC exposes intervals as month and millisecond counts. */
  static Stream<Arguments> values() {
    return Stream.of(
        Arguments.of("DATE '2026-01-01'", Date.valueOf("2026-01-01")),
        Arguments.of("TIME '12:34:56'", Time.valueOf("12:34:56")),
        Arguments.of("TIMESTAMP '2026-01-01 12:34:56.789'",
            Timestamp.valueOf("2026-01-01 12:34:56.789")),
        Arguments.of("INTERVAL '1-2' YEAR TO MONTH", 14),
        Arguments.of("INTERVAL '1 02:03:04.005' DAY TO SECOND", 93784005L),
        Arguments.of("X'00ABFF'", new byte[] {0, (byte) 0xab, (byte) 0xff}),
        Arguments.of("CAST(X'00ABFF' AS VARBINARY(3))",
            new byte[] {0, (byte) 0xab, (byte) 0xff}));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  void testScalar(String expression, Object expected) throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT " + expression)) {
      assertTrue(resultSet.next());
      // The typed JDBC getter is the external representation for binary values.
      assertValue(expected, expected instanceof byte[]
          ? resultSet.getBytes(1) : resultSet.getObject(1));
      assertFalse(resultSet.next());
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  void testArrayElement(String expression, Object expected) {
    check("ARRAY[" + expression + "]", Collections.singletonList(expected));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  void testRowField(String expression, Object expected) {
    check("ROW(" + expression + ", 42, CAST(NULL AS INTEGER))",
        new Object[] {expected, 42, null});
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  void testMapValue(String expression, Object expected) {
    check("MAP['value', " + expression + "]",
        Collections.singletonMap("value", expected));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  void testMapKey(String expression, Object expected) {
    check("MAP[" + expression + ", 'value']",
        Collections.singletonMap(expected, "value"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  void testArrayOfRows(String expression, Object expected) {
    check("ARRAY[ROW(" + expression + ", 42), ROW(" + expression + ", 43)]",
        Arrays.asList(new Object[] {expected, 42}, new Object[] {expected, 43}));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  void testNestedContainers(String expression, Object expected) {
    check("ROW(ARRAY[MAP['value', " + expression + "]], "
            + "MAP['rows', ARRAY[ROW(" + expression + ", 42)]])",
        new Object[] {
            Collections.singletonList(Collections.singletonMap("value", expected)),
            Collections.singletonMap("rows",
                Collections.singletonList(new Object[] {expected, 42}))});
  }

  @ParameterizedTest
  @ValueSource(strings =
      {"DATE", "TIME", "TIMESTAMP", "BINARY(3)", "INTERVAL YEAR TO MONTH", "INTERVAL DAY TO SECOND"})
  void testNullValues(String type) {
    final String value = "CAST(NULL AS " + type + ")";
    check("ROW(" + value + ", ARRAY[" + value + "], MAP['value', " + value + "])",
        new Object[] {null, Collections.singletonList(null),
            Collections.singletonMap("value", null)});
  }

  @ParameterizedTest
  @ValueSource(strings = {"DATE ARRAY", "ROW(d DATE, n INTEGER)", "MAP<VARCHAR, DATE>"})
  void testNullContainer(String type) {
    check("CAST(NULL AS " + type + ")", null);
  }

  @Test void testValuesWithoutConversion() {
    check("ROW(42, 'text', TRUE, ARRAY[1, NULL, 2], MAP['key', 3])",
        new Object[] {42, "text", true, Arrays.asList(1, null, 2),
            Collections.singletonMap("key", 3)});
  }

  @Test void testSourceContainersAreNotModified() {
    final List<Integer> array = Collections.unmodifiableList(Arrays.asList(20454, null));
    final Map<String, @Nullable Integer> map = Collections.singletonMap("date", 20454);
    final Object[] row = {20454, array, map};
    final ScannableTable table = new ResultTable(new Object[] {row}, factory -> {
      final RelDataType date =
          factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DATE), true);
      final RelDataType rowType = factory.builder()
          .add("D", date)
          .add("A", factory.createArrayType(date, -1))
          .add("M", factory.createMapType(factory.createSqlType(SqlTypeName.VARCHAR), date))
          .build();
      return factory.builder().add("R", rowType).build();
    });
    CalciteAssert.that()
        .with(connection -> {
          connection.unwrap(CalciteConnection.class).getRootSchema().add("T", table);
          return connection;
        })
        .query("SELECT * FROM \"T\"")
        .returns(resultSet -> {
          try {
            assertTrue(resultSet.next());
            // Read twice to detect conversions that mutate the stored internal values.
            for (int i = 0; i < 2; i++) {
              assertInstanceOf(Struct.class, resultSet.getObject(1)).getAttributes();
            }
            assertEquals(20454, row[0]);
            assertSame(array, row[1]);
            assertEquals(Arrays.asList(20454, null), array);
            assertSame(map, row[2]);
            assertEquals(Collections.singletonMap("date", 20454), map);
            assertFalse(resultSet.next());
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  private static void check(String expression, @Nullable Object expected) {
    CalciteAssert.that()
        .with(CalciteConnectionProperty.TIME_ZONE, TimeZone.getDefault().getID())
        .query("SELECT " + expression)
        .returns(resultSet -> {
          try {
            assertTrue(resultSet.next());
            assertValue(expected, resultSet.getObject(1));
            assertFalse(resultSet.next());
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  /** Unwraps JDBC containers only; scalar values must already be converted. */
  private static void assertValue(@Nullable Object expected, @Nullable Object actual)
      throws SQLException {
    if (expected == null) {
      assertNull(actual);
    } else if (expected instanceof byte[]) {
      assertArrayEquals((byte[]) expected, assertInstanceOf(byte[].class, actual));
    } else if (expected instanceof Object[]) {
      final Object[] attributes = actual instanceof Struct
          ? ((Struct) actual).getAttributes() : assertInstanceOf(Object[].class, actual);
      final Object[] fields = (Object[]) expected;
      assertEquals(fields.length, attributes.length);
      for (int i = 0; i < fields.length; i++) {
        assertValue(fields[i], attributes[i]);
      }
    } else if (expected instanceof List) {
      final List<?> elements = actual instanceof Array
          ? AvaticaUtils.primitiveList(((Array) actual).getArray())
          : assertInstanceOf(List.class, actual);
      final List<?> values = (List<?>) expected;
      assertEquals(values.size(), elements.size());
      for (int i = 0; i < values.size(); i++) {
        assertValue(values.get(i), elements.get(i));
      }
    } else if (expected instanceof Map) {
      final Map<?, ?> entries = assertInstanceOf(Map.class, actual);
      final Map<?, ?> values = (Map<?, ?>) expected;
      assertEquals(values.size(), entries.size());
      if (values.size() == 1) {
        // Compare binary keys by content, not by Java array identity.
        final Map.Entry<?, ?> expectedEntry = values.entrySet().iterator().next();
        final Map.Entry<?, ?> actualEntry = entries.entrySet().iterator().next();
        assertValue(expectedEntry.getKey(), actualEntry.getKey());
        assertValue(expectedEntry.getValue(), actualEntry.getValue());
      } else {
        assertEquals(values.keySet(), entries.keySet());
        for (Map.Entry<?, ?> entry : values.entrySet()) {
          assertValue(entry.getValue(), entries.get(entry.getKey()));
        }
      }
    } else {
      assertEquals(expected.getClass(), actual == null ? null : actual.getClass());
      assertEquals(expected, actual);
    }
  }

  /** Supplies shared internal values so result access cannot silently mutate them. */
  private static class ResultTable extends AbstractTable implements ScannableTable {
    private final Object[] row;
    private final Function<RelDataTypeFactory, RelDataType> type;

    ResultTable(Object[] row, Function<RelDataTypeFactory, RelDataType> type) {
      this.row = row;
      this.type = type;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return type.apply(typeFactory);
    }

    @Override public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.singletonEnumerable(row);
    }
  }
}
