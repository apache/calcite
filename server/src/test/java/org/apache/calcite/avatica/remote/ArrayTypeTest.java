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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.ArrayType;
import org.apache.calcite.avatica.ColumnMetaData.AvaticaType;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.ColumnMetaData.ScalarType;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.util.AbstractCursor.ArrayAccessor;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.avatica.util.Cursor.Accessor;
import org.apache.calcite.avatica.util.ListIteratorCursor;
import org.apache.calcite.avatica.util.Unsafe;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test class for verifying functionality with arrays.
 */
@RunWith(Parameterized.class)
public class ArrayTypeTest {
  private static final AvaticaServersForTest SERVERS = new AvaticaServersForTest();

  private final HttpServer server;
  private final String url;
  private final int port;
  @SuppressWarnings("unused")
  private final Driver.Serialization serialization;

  @Parameters(name = "{0}")
  public static List<Object[]> parameters() throws Exception {
    SERVERS.startServers();
    return SERVERS.getJUnitParameters();
  }

  public ArrayTypeTest(Serialization serialization, HttpServer server) {
    this.server = server;
    this.port = this.server.getPort();
    this.serialization = serialization;
    this.url = SERVERS.getJdbcUrl(port, serialization);
  }

  @AfterClass public static void afterClass() throws Exception {
    if (null != SERVERS) {
      SERVERS.stopServers();
    }
  }

  @Test public void simpleArrayTest() throws Exception {
    try (Connection conn = DriverManager.getConnection(url)) {
      ScalarType varcharComponent = ColumnMetaData.scalar(Types.VARCHAR, "VARCHAR", Rep.STRING);
      List<Array> varcharArrays = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        List<String> value = Collections.singletonList(Integer.toString(i));
        varcharArrays.add(createArray("VARCHAR", varcharComponent, value));
      }
      writeAndReadArrays(conn, "varchar_arrays", "VARCHAR(30)",
          varcharComponent, varcharArrays, PRIMITIVE_LIST_VALIDATOR);
    }
  }

  @Test public void booleanArrays() throws Exception {
    final Random r = new Random();
    try (Connection conn = DriverManager.getConnection(url)) {
      ScalarType component = ColumnMetaData.scalar(Types.BOOLEAN, "BOOLEAN", Rep.BOOLEAN);
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 5; i++) {
        List<Boolean> elements = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
          switch (r.nextInt(3)) {
          case 0:
            elements.add(Boolean.FALSE);
            break;
          case 1:
            elements.add(Boolean.TRUE);
            break;
          case 2:
            elements.add(null);
            break;
          default:
            fail();
          }
        }
        arrays.add(createArray("BOOLEAN", component, elements));
      }
      // Verify we can read and write the data
      writeAndReadArrays(conn, "boolean_arrays", "BOOLEAN", component, arrays,
          PRIMITIVE_LIST_VALIDATOR);
    }
  }

  @Test public void shortArrays() throws Exception {
    final Random r = new Random();
    try (Connection conn = DriverManager.getConnection(url)) {
      ScalarType component = ColumnMetaData.scalar(Types.SMALLINT, "SMALLINT", Rep.SHORT);
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 5; i++) {
        List<Short> elements = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
          short value = (short) r.nextInt(Short.MAX_VALUE);
          // 50% of the time, negate the value
          if (0 == r.nextInt(2)) {
            value *= -1;
          }
          elements.add(Short.valueOf(value));
        }
        arrays.add(createArray("SMALLINT", component, elements));
      }
      // Verify read/write
      writeAndReadArrays(conn, "short_arrays", "SMALLINT", component, arrays,
          PRIMITIVE_LIST_VALIDATOR);
    }
  }

  @Test public void shortArraysWithNull() throws Exception {
    final Random r = new Random();
    try (Connection conn = DriverManager.getConnection(url)) {
      ScalarType component = ColumnMetaData.scalar(Types.SMALLINT, "SMALLINT", Rep.SHORT);
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 5; i++) {
        List<Short> elements = new ArrayList<>();
        for (int j = 0; j < 4; j++) {
          short value = (short) r.nextInt(Short.MAX_VALUE);
          // 50% of the time, negate the value
          if (0 == r.nextInt(2)) {
            value *= -1;
          }
          elements.add(Short.valueOf(value));
        }
        elements.add(null);
        arrays.add(createArray("SMALLINT", component, elements));
      }
      // Verify read/write
      writeAndReadArrays(conn, "short_arrays", "SMALLINT", component, arrays,
          PRIMITIVE_LIST_VALIDATOR);
    }
  }

  @Test public void longArrays() throws Exception {
    final Random r = new Random();
    try (Connection conn = DriverManager.getConnection(url)) {
      ScalarType component = ColumnMetaData.scalar(Types.BIGINT, "BIGINT", Rep.LONG);
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 5; i++) {
        List<Long> elements = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
          elements.add(r.nextLong());
        }
        arrays.add(createArray("BIGINT", component, elements));
      }
      // Verify read/write
      writeAndReadArrays(conn, "long_arrays", "BIGINT", component, arrays,
          PRIMITIVE_LIST_VALIDATOR);
    }
  }

  @Test public void stringArrays() throws Exception {
    try (Connection conn = DriverManager.getConnection(url)) {
      ScalarType component = ColumnMetaData.scalar(Types.VARCHAR, "VARCHAR", Rep.STRING);
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 5; i++) {
        List<String> elements = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
          elements.add(i + "_" + j);
        }
        arrays.add(createArray("VARCHAR", component, elements));
      }
      // Verify read/write
      writeAndReadArrays(conn, "string_arrays", "VARCHAR", component, arrays,
          PRIMITIVE_LIST_VALIDATOR);
    }
  }

  @Test public void bigintArrays() throws Exception {
    final Random r = new Random();
    try (Connection conn = DriverManager.getConnection(url)) {
      ScalarType component = ColumnMetaData.scalar(Types.BIGINT, "BIGINT", Rep.LONG);
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 3; i++) {
        List<Long> elements = new ArrayList<>();
        for (int j = 0; j < 7; j++) {
          long element = r.nextLong();
          if (r.nextBoolean()) {
            element *= -1;
          }
          elements.add(element);
        }
        arrays.add(createArray("BIGINT", component, elements));
      }
      writeAndReadArrays(conn, "long_arrays", "BIGINT", component, arrays,
          PRIMITIVE_LIST_VALIDATOR);
    }
  }

  @Test public void doubleArrays() throws Exception {
    final Random r = new Random();
    try (Connection conn = DriverManager.getConnection(url)) {
      ScalarType component = ColumnMetaData.scalar(Types.DOUBLE, "DOUBLE", Rep.DOUBLE);
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 3; i++) {
        List<Double> elements = new ArrayList<>();
        for (int j = 0; j < 7; j++) {
          double element = r.nextDouble();
          if (r.nextBoolean()) {
            element *= -1;
          }
          elements.add(element);
        }
        arrays.add(createArray("DOUBLE", component, elements));
      }
      writeAndReadArrays(conn, "float_arrays", "DOUBLE", component, arrays,
          PRIMITIVE_LIST_VALIDATOR);
    }
  }

  @Test public void arraysOfByteArrays() throws Exception {
    final Random r = new Random();
    try (Connection conn = DriverManager.getConnection(url)) {
      ScalarType component = ColumnMetaData.scalar(Types.TINYINT, "TINYINT", Rep.BYTE);
      // [ Array([b, b, b]), Array([b, b, b]), ... ]
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 5; i++) {
        List<Byte> elements = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
          byte value = (byte) r.nextInt(Byte.MAX_VALUE);
          // 50% of the time, negate the value
          if (0 == r.nextInt(2)) {
            value *= -1;
          }
          elements.add(Byte.valueOf(value));
        }
        arrays.add(createArray("TINYINT", component, elements));
      }
      // Verify read/write
      writeAndReadArrays(conn, "byte_arrays", "TINYINT", component, arrays, BYTE_ARRAY_VALIDATOR);
    }
  }

  @Test public void varbinaryArrays() throws Exception {
    try (Connection conn = DriverManager.getConnection(url)) {
      ScalarType component = ColumnMetaData.scalar(Types.VARBINARY, "VARBINARY", Rep.BYTE_STRING);
      // [ Array(binary, binary, binary), Array(binary, binary, binary), ...]
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 5; i++) {
        List<byte[]> elements = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
          elements.add((i + "_" + j).getBytes(UTF_8));
        }
        arrays.add(createArray("VARBINARY", component, elements));
      }
      writeAndReadArrays(conn, "binary_arrays", "VARBINARY", component, arrays,
          BYTE_ARRAY_ARRAY_VALIDATOR);
    }
  }

  @Test public void timeArrays() throws Exception {
    try (Connection conn = DriverManager.getConnection(url)) {
      final long now = System.currentTimeMillis();
      ScalarType component = ColumnMetaData.scalar(Types.TIME, "TIME", Rep.JAVA_SQL_TIME);
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 5; i++) {
        List<Time> elements = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
          elements.add(new Time(now + i + j));
        }
        arrays.add(createArray("TIME", component, elements));
      }
      writeAndReadArrays(conn, "time_arrays", "TIME", component, arrays, new Validator<Array>() {
        @Override public void validate(Array expected, Array actual) throws SQLException {
          Object[] expectedTimes = (Object[]) expected.getArray();
          Object[] actualTimes = (Object[]) actual.getArray();
          assertEquals(expectedTimes.length, actualTimes.length);
          final Calendar cal = Unsafe.localCalendar();
          for (int i = 0;  i < expectedTimes.length; i++) {
            cal.setTime((Time) expectedTimes[i]);
            int expectedHour = cal.get(Calendar.HOUR_OF_DAY);
            int expectedMinute = cal.get(Calendar.MINUTE);
            int expectedSecond = cal.get(Calendar.SECOND);
            cal.setTime((Time) actualTimes[i]);
            assertEquals(expectedHour, cal.get(Calendar.HOUR_OF_DAY));
            assertEquals(expectedMinute, cal.get(Calendar.MINUTE));
            assertEquals(expectedSecond, cal.get(Calendar.SECOND));
          }
        }
      });
      // Ensure an array with a null element can be written/read
      Array arrayWithNull = createArray("TIME", component, Arrays.asList((Time) null));
      writeAndReadArrays(conn, "time_array_with_null", "TIME", component,
          Collections.singletonList(arrayWithNull), new Validator<Array>() {
            @Override public void validate(Array expected, Array actual) throws Exception {
              Object[] expectedArray = (Object[]) expected.getArray();
              Object[] actualArray = (Object[]) actual.getArray();
              assertEquals(1, expectedArray.length);
              assertEquals(expectedArray.length, actualArray.length);
              assertEquals(expectedArray[0], actualArray[0]);
            }
          });
    }
  }

  @Test public void dateArrays() throws Exception {
    try (Connection conn = DriverManager.getConnection(url)) {
      final long now = System.currentTimeMillis();
      ScalarType component = ColumnMetaData.scalar(Types.DATE, "DATE", Rep.JAVA_SQL_DATE);
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 5; i++) {
        List<Date> elements = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
          elements.add(new Date(now + i + j));
        }
        arrays.add(createArray("DATE", component, elements));
      }
      writeAndReadArrays(conn, "date_arrays", "DATE", component, arrays, new Validator<Array>() {
        @Override public void validate(Array expected, Array actual) throws SQLException {
          Object[] expectedDates = (Object[]) expected.getArray();
          Object[] actualDates = (Object[]) actual.getArray();
          assertEquals(expectedDates.length, actualDates.length);
          final Calendar cal = Unsafe.localCalendar();
          for (int i = 0;  i < expectedDates.length; i++) {
            cal.setTime((Date) expectedDates[i]);
            int expectedDayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
            int expectedMonth = cal.get(Calendar.MONTH);
            int expectedYear = cal.get(Calendar.YEAR);
            cal.setTime((Date) actualDates[i]);
            assertEquals(expectedDayOfMonth, cal.get(Calendar.DAY_OF_MONTH));
            assertEquals(expectedMonth, cal.get(Calendar.MONTH));
            assertEquals(expectedYear, cal.get(Calendar.YEAR));
          }
        }
      });
      // Ensure an array with a null element can be written/read
      Array arrayWithNull = createArray("DATE", component, Arrays.asList((Time) null));
      writeAndReadArrays(conn, "date_array_with_null", "DATE", component,
          Collections.singletonList(arrayWithNull), new Validator<Array>() {
            @Override public void validate(Array expected, Array actual) throws Exception {
              Object[] expectedArray = (Object[]) expected.getArray();
              Object[] actualArray = (Object[]) actual.getArray();
              assertEquals(1, expectedArray.length);
              assertEquals(expectedArray.length, actualArray.length);
              assertEquals(expectedArray[0], actualArray[0]);
            }
          });
    }
  }

  @Test public void timestampArrays() throws Exception {
    try (Connection conn = DriverManager.getConnection(url)) {
      final long now = System.currentTimeMillis();
      ScalarType component = ColumnMetaData.scalar(Types.TIMESTAMP, "TIMESTAMP",
          Rep.JAVA_SQL_TIMESTAMP);
      List<Array> arrays = new ArrayList<>();
      // Construct the data
      for (int i = 0; i < 5; i++) {
        List<Timestamp> elements = new ArrayList<>();
        for (int j = 0; j < 5; j++) {
          elements.add(new Timestamp(now + i + j));
        }
        arrays.add(createArray("TIMESTAMP", component, elements));
      }
      writeAndReadArrays(conn, "timestamp_arrays", "TIMESTAMP", component, arrays,
          new Validator<Array>() {
            @Override public void validate(Array expected, Array actual) throws SQLException {
              Object[] expectedTimestamps = (Object[]) expected.getArray();
              Object[] actualTimestamps = (Object[]) actual.getArray();
              assertEquals(expectedTimestamps.length, actualTimestamps.length);
              final Calendar cal = Unsafe.localCalendar();
              for (int i = 0;  i < expectedTimestamps.length; i++) {
                cal.setTime((Timestamp) expectedTimestamps[i]);
                int expectedDayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
                int expectedMonth = cal.get(Calendar.MONTH);
                int expectedYear = cal.get(Calendar.YEAR);
                int expectedHour = cal.get(Calendar.HOUR_OF_DAY);
                int expectedMinute = cal.get(Calendar.MINUTE);
                int expectedSecond = cal.get(Calendar.SECOND);
                int expectedMillisecond = cal.get(Calendar.MILLISECOND);
                cal.setTime((Timestamp) actualTimestamps[i]);
                assertEquals(expectedDayOfMonth, cal.get(Calendar.DAY_OF_MONTH));
                assertEquals(expectedMonth, cal.get(Calendar.MONTH));
                assertEquals(expectedYear, cal.get(Calendar.YEAR));
                assertEquals(expectedHour, cal.get(Calendar.HOUR_OF_DAY));
                assertEquals(expectedMinute, cal.get(Calendar.MINUTE));
                assertEquals(expectedSecond, cal.get(Calendar.SECOND));
                assertEquals(expectedMillisecond, cal.get(Calendar.MILLISECOND));
              }
            }
          }
      );
      // Ensure an array with a null element can be written/read
      Array arrayWithNull = createArray("TIMESTAMP", component, Arrays.asList((Timestamp) null));
      writeAndReadArrays(conn, "timestamp_array_with_null", "TIMESTAMP", component,
          Collections.singletonList(arrayWithNull), new Validator<Array>() {
            @Override public void validate(Array expected, Array actual) throws Exception {
              Object[] expectedArray = (Object[]) expected.getArray();
              Object[] actualArray = (Object[]) actual.getArray();
              assertEquals(1, expectedArray.length);
              assertEquals(expectedArray.length, actualArray.length);
              assertEquals(expectedArray[0], actualArray[0]);
            }
          });
    }
  }

  @Test public void testCreateArrayOf() throws Exception {
    try (Connection conn = DriverManager.getConnection(url)) {
      final String componentName = SqlType.INTEGER.name();
      Array a1 = conn.createArrayOf(componentName, new Object[] {1, 2, 3, 4, 5});
      Array a2 = conn.createArrayOf(componentName, new Object[] {2, 3, 4, 5, 6});
      Array a3 = conn.createArrayOf(componentName, new Object[] {3, 4, 5, 6, 7});
      AvaticaType arrayType = ColumnMetaData.array(
          ColumnMetaData.scalar(Types.INTEGER, componentName, Rep.INTEGER), "NUMBERS", Rep.ARRAY);
      writeAndReadArrays(conn, "CREATE_ARRAY_OF_INTEGERS", componentName, arrayType,
          Arrays.asList(a1, a2, a3), PRIMITIVE_LIST_VALIDATOR);
    }
  }

  /**
   * Creates a JDBC {@link Array} from a list of values.
   *
   * @param typeName the SQL type name of the elements in the array
   * @param componentType The Avatica type for the array elements
   * @param arrayValues The array elements
   * @return An Array instance for the given component and values
   */
  @SuppressWarnings("unchecked")
  private <T> Array createArray(String typeName, AvaticaType componentType, List<T> arrayValues) {
    // Make a "row" with one "column" (which is really a list)
    final List<Object> oneRow = Collections.singletonList((Object) arrayValues);
    // Make an iterator over this one "row"
    final Iterator<List<Object>> rowIterator = Collections.singletonList(oneRow).iterator();

    ArrayType array = ColumnMetaData.array(componentType, typeName, Rep.ARRAY);
    try (ListIteratorCursor cursor = new ListIteratorCursor(rowIterator)) {
      List<ColumnMetaData> types = Collections.singletonList(ColumnMetaData.dummy(array, true));
      Calendar calendar = Unsafe.localCalendar();
      List<Accessor> accessors = cursor.createAccessors(types, calendar, null);
      assertTrue("Expected at least one accessor, found " + accessors.size(),
          !accessors.isEmpty());
      ArrayAccessor arrayAccessor = (ArrayAccessor) accessors.get(0);

      return new ArrayImpl((List<Object>) arrayValues, arrayAccessor);
    }
  }

  /**
   * Creates a table, writes the arrays to the table, and then verifies that the arrays can be
   * read from that table and are equivalent to the original arrays.
   *
   * @param conn The JDBC connection
   * @param tableName The name of the table to create and use
   * @param componentType The component type of the array
   * @param scalarType The Avatica type object for the component type of the array
   * @param inputArrays The data to write and read
   */
  private void writeAndReadArrays(Connection conn, String tableName, String componentType,
      AvaticaType scalarType, List<Array> inputArrays, Validator<Array> validator)
      throws Exception {
    // Drop and create the table
    try (Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute(Unsafe.formatLocalString("DROP TABLE IF EXISTS %s", tableName)));
      String createTableSql = Unsafe.formatLocalString(
          "CREATE TABLE %s (id integer, vals %s ARRAY)", tableName, componentType);
      assertFalse(stmt.execute(createTableSql));
    }

    // Insert records, each with an array
    final String dml = Unsafe.formatLocalString("INSERT INTO %s VALUES (?, ?)", tableName);
    try (PreparedStatement stmt = conn.prepareStatement(dml)) {
      int i = 0;
      for (Array inputArray : inputArrays)  {
        stmt.setInt(1, i);
        stmt.setArray(2, inputArray);
        assertEquals(1, stmt.executeUpdate());
        i++;
      }
    }

    // Read the records
    try (Statement stmt = conn.createStatement()) {
      ResultSet results = stmt.executeQuery(
          Unsafe.formatLocalString("SELECT * FROM %s", tableName));
      assertNotNull("Expected a ResultSet", results);
      int i = 0;
      for (Array expectedArray : inputArrays) {
        assertTrue(results.next());
        assertEquals(i++, results.getInt(1));
        Array actualArray = results.getArray(2);

        validator.validate(expectedArray, actualArray);

        // TODO Fix this. See {@link AvaticaResultSet#create(ColumnMetaData.AvaticaType,Iterable)}
        //ResultSet inputResults = expectedArray.getResultSet();
        //ResultSet actualResult = actualArray.getResultSet();
      }
      assertFalse("Expected no more records", results.next());
    }
  }

  /**
   * A simple interface to validate to objects in support of type test cases
   */
  private interface Validator<T> {
    void validate(T expected, T actual) throws Exception;
  }

  private static final PrimitiveArrayValidator PRIMITIVE_LIST_VALIDATOR =
      new PrimitiveArrayValidator();
  /**
   * Validator that coerces primitive arrays into lists and comparse them.
   */
  private static class PrimitiveArrayValidator implements Validator<Array> {
    @Override public void validate(Array expected, Array actual) throws SQLException {
      assertEquals(AvaticaUtils.primitiveList(expected.getArray()),
          AvaticaUtils.primitiveList(actual.getArray()));
    }
  }

  private static final ByteArrayValidator BYTE_ARRAY_VALIDATOR = new ByteArrayValidator();
  /**
   * Validator that compares lists of bytes (the object).
   */
  private static class ByteArrayValidator implements Validator<Array> {
    @SuppressWarnings("unchecked")
    @Override public void validate(Array expected, Array actual) throws SQLException {
      // Need to compare the byte arrays.
      List<Byte> expectedArray =
          (List<Byte>) AvaticaUtils.primitiveList(expected.getArray());
      List<Byte> actualArray =
          (List<Byte>) AvaticaUtils.primitiveList(actual.getArray());
      assertEquals(expectedArray.size(), actualArray.size());

      for (int j = 0; j < expectedArray.size(); j++) {
        Byte expectedByte = expectedArray.get(j);
        Byte actualByte = actualArray.get(j);
        assertEquals(expectedByte, actualByte);
      }
    }
  }

  // Arrays of byte arrays (e.g. an Array<Varbinary>)
  private static final ByteArrayArrayValidator BYTE_ARRAY_ARRAY_VALIDATOR =
      new ByteArrayArrayValidator();
  /**
   * Validator that compares lists of byte arrays.
   */
  private static class ByteArrayArrayValidator implements Validator<Array> {
    @SuppressWarnings("unchecked")
    @Override public void validate(Array expected, Array actual) throws SQLException {
      // Need to compare the byte arrays.
      List<byte[]> expectedArray =
          (List<byte[]>) AvaticaUtils.primitiveList(expected.getArray());
      List<byte[]> actualArray =
          (List<byte[]>) AvaticaUtils.primitiveList(actual.getArray());
      assertEquals(expectedArray.size(), actualArray.size());

      for (int j = 0; j < expectedArray.size(); j++) {
        byte[] expectedBytes = expectedArray.get(j);
        byte[] actualBytes = actualArray.get(j);
        assertArrayEquals(expectedBytes, actualBytes);
      }
    }
  }
}

// End ArrayTypeTest.java
