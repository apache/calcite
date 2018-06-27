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

import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.ArrayFactoryImpl;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.StructImpl;
import org.apache.calcite.avatica.util.Unsafe;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test {@code ResultSet#getXXX} conversions.
 */
@RunWith(Parameterized.class)
public class AvaticaResultSetConversionsTest {
  /**
   * A fake test driver for test.
   */
  private static final class TestDriver extends UnregisteredDriver {

    @Override protected DriverVersion createDriverVersion() {
      return new DriverVersion("test", "test 0.0.0", "test", "test 0.0.0", false, 0, 0, 0, 0);
    }

    @Override protected String getConnectStringPrefix() {
      return "jdbc:test";
    }

    @Override public Meta createMeta(AvaticaConnection connection) {
      return new TestMetaImpl(connection);
    }
  }

  /**
   * Fake meta implementation for test driver.
   */
  public static final class TestMetaImpl extends MetaImpl {
    public TestMetaImpl(AvaticaConnection connection) {
      super(connection);
    }

    @Override public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
        long maxRowCount, PrepareCallback callback) throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public ExecuteResult prepareAndExecute(StatementHandle h, String sql,
        long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback)
        throws NoSuchStatementException {
      assertEquals("SELECT * FROM TABLE", sql);
      List<ColumnMetaData> columns = Arrays.asList(
          columnMetaData("bool", 0,
              ColumnMetaData.scalar(Types.BOOLEAN, "BOOLEAN",
                  ColumnMetaData.Rep.PRIMITIVE_BOOLEAN),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("byte", 1,
              ColumnMetaData.scalar(Types.TINYINT, "TINYINT",
                  ColumnMetaData.Rep.PRIMITIVE_BYTE),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("short", 2,
              ColumnMetaData.scalar(Types.SMALLINT, "SMALLINT",
                  ColumnMetaData.Rep.PRIMITIVE_SHORT),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("int", 3,
              ColumnMetaData.scalar(Types.INTEGER, "INTEGER",
                  ColumnMetaData.Rep.PRIMITIVE_INT),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("long", 4,
              ColumnMetaData.scalar(Types.BIGINT, "BIGINT",
                  ColumnMetaData.Rep.PRIMITIVE_LONG),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("float", 5,
              ColumnMetaData.scalar(Types.REAL, "REAL",
                  ColumnMetaData.Rep.FLOAT),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("double", 6,
              ColumnMetaData.scalar(Types.FLOAT, "FLOAT",
                  ColumnMetaData.Rep.DOUBLE),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("string", 7,
              ColumnMetaData.scalar(Types.VARCHAR, "VARCHAR",
                  ColumnMetaData.Rep.STRING),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("date", 8,
              ColumnMetaData.scalar(Types.DATE, "DATE",
                  ColumnMetaData.Rep.JAVA_SQL_DATE),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("time", 9,
              ColumnMetaData.scalar(Types.TIME, "TIME",
                  ColumnMetaData.Rep.JAVA_SQL_TIME),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("timestamp", 10,
              ColumnMetaData.scalar(Types.TIMESTAMP, "TIMESTAMP",
                  ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("array", 11,
              ColumnMetaData.array(
                  ColumnMetaData.scalar(Types.INTEGER, "INTEGER",
                      ColumnMetaData.Rep.PRIMITIVE_INT),
                  "ARRAY",
                  ColumnMetaData.Rep.ARRAY),
              DatabaseMetaData.columnNoNulls),
          columnMetaData("struct", 12,
              ColumnMetaData.struct(
                  Arrays.asList(
                      columnMetaData("int", 0,
                          ColumnMetaData.scalar(Types.INTEGER, "INTEGER",
                              ColumnMetaData.Rep.PRIMITIVE_INT),
                          DatabaseMetaData.columnNoNulls),
                      columnMetaData("bool", 1,
                          ColumnMetaData.scalar(Types.BOOLEAN, "BOOLEAN",
                              ColumnMetaData.Rep.PRIMITIVE_BOOLEAN),
                          DatabaseMetaData.columnNoNulls))),
              DatabaseMetaData.columnNoNulls));

      List<Object> row = Collections.<Object>singletonList(
          new Object[] {
              true, (byte) 1, (short) 2, 3, 4L, 5.0f, 6.0d, "testvalue",
              new Date(1476130718123L), new Time(1476130718123L),
              new Timestamp(1476130718123L),
              Arrays.asList(1, 2, 3),
              new StructImpl(Arrays.asList(42, false))
          });

      CursorFactory factory = CursorFactory.deduce(columns, null);
      Frame frame = new Frame(0, true, row);

      Signature signature = Signature.create(columns, sql,
          Collections.<AvaticaParameter>emptyList(), factory, StatementType.SELECT);
      try {
        synchronized (callback.getMonitor()) {
          callback.clear();
          callback.assign(signature, frame, -1);
        }
        callback.execute();
      } catch (SQLException e) {
        throw new RuntimeException();
      }
      MetaResultSet rs = MetaResultSet.create(h.connectionId, 0, false, signature, null);
      return new ExecuteResult(Collections.singletonList(rs));
    }

    @Override public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h,
        List<String> sqlCommands) throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public ExecuteBatchResult executeBatch(StatementHandle h,
        List<List<TypedValue>> parameterValues) throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount)
        throws NoSuchStatementException, MissingResultsException {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues,
        long maxRowCount) throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues,
        int maxRowsInFirstFrame) throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public void closeStatement(StatementHandle h) {
    }

    @Override public boolean syncResults(StatementHandle sh, QueryState state, long offset)
        throws NoSuchStatementException {
      throw new UnsupportedOperationException();
    }

    @Override public void commit(ConnectionHandle ch) {
      throw new UnsupportedOperationException();
    }

    @Override public void rollback(ConnectionHandle ch) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Base accessor test helper.
   */
  private static class AccessorTestHelper {
    protected final Getter g;

    protected AccessorTestHelper(Getter g) {
      this.g = g;
    }

    public void testGetString(ResultSet resultSet) throws SQLException {
      g.getString(resultSet);
    }

    public void testGetBoolean(ResultSet resultSet) throws SQLException {
      try {
        g.getBoolean(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetByte(ResultSet resultSet) throws SQLException {
      try {
        g.getByte(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetShort(ResultSet resultSet) throws SQLException {
      try {
        g.getShort(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetInt(ResultSet resultSet) throws SQLException {
      try {
        g.getInt(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetLong(ResultSet resultSet) throws SQLException {
      try {
        g.getLong(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetFloat(ResultSet resultSet) throws SQLException {
      try {
        g.getFloat(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetDouble(ResultSet resultSet) throws SQLException {
      try {
        g.getDouble(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetDecimal(ResultSet resultSet) throws SQLException {
      try {
        g.getBigDecimal(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetBytes(ResultSet resultSet) throws SQLException {
      try {
        g.getBytes(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetAsciiStream(ResultSet resultSet) throws SQLException {
      try {
        g.getAsciiStream(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetBinaryStream(ResultSet resultSet) throws SQLException {
      try {
        g.getBinaryStream(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetObject(ResultSet resultSet) throws SQLException {
      g.getObject(resultSet);
    }

    public void testGetCharacterStream(ResultSet resultSet) throws SQLException {
      try {
        g.getCharacterStream(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetObject(ResultSet resultSet, Map<String, Class<?>> map) throws SQLException {
      try {
        g.getObject(resultSet, map);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetRef(ResultSet resultSet) throws SQLException {
      try {
        g.getRef(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetBlob(ResultSet resultSet) throws SQLException {
      try {
        g.getBlob(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetClob(ResultSet resultSet) throws SQLException {
      try {
        g.getBlob(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetArray(ResultSet resultSet) throws SQLException {
      try {
        g.getArray(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetDate(ResultSet resultSet, Calendar calendar) throws SQLException {
      try {
        g.getDate(resultSet, calendar);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetTime(ResultSet resultSet, Calendar calendar) throws SQLException {
      try {
        g.getTime(resultSet, calendar);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetTimestamp(ResultSet resultSet, Calendar calendar) throws SQLException {
      try {
        g.getTimestamp(resultSet, calendar);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetStruct(ResultSet resultSet) throws SQLException {
      try {
        g.getStruct(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void getURL(ResultSet resultSet) throws SQLException {
      try {
        g.getURL(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetNClob(ResultSet resultSet) throws SQLException {
      try {
        g.getNClob(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetSQLXML(ResultSet resultSet) throws SQLException {
      try {
        g.getSQLXML(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetNString(ResultSet resultSet) throws SQLException {
      try {
        g.getNString(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetNCharacterStream(ResultSet resultSet) throws SQLException {
      try {
        g.getNCharacterStream(resultSet);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }
  }

  /**
   * Accessor test helper for boolean column.
   */
  private static final class BooleanAccessorTestHelper extends AccessorTestHelper {
    private BooleanAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("true", g.getString(resultSet));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, g.getBoolean(resultSet));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 1, g.getByte(resultSet));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 1, g.getShort(resultSet));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(1, g.getInt(resultSet));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(1L, g.getLong(resultSet));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(BigDecimal.ONE, g.getBigDecimal(resultSet));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(1.0f, g.getFloat(resultSet), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(1.0d, g.getDouble(resultSet), 0);
    }
  }

  /**
   * Accessor test helper for array column.
   */
  private static final class ArrayAccessorTestHelper extends AccessorTestHelper {
    private ArrayAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetArray(ResultSet resultSet) throws SQLException {
      ColumnMetaData.ScalarType intType =
          ColumnMetaData.scalar(Types.INTEGER, "INTEGER", ColumnMetaData.Rep.INTEGER);
      Array expectedArray =
          new ArrayFactoryImpl(Unsafe.localCalendar().getTimeZone()).createArray(
              intType, Arrays.asList(1, 2, 3));
      assertTrue(ArrayImpl.equalContents(expectedArray, g.getArray(resultSet)));
    }
  }

  /**
   * Accessor test helper for row column.
   */
  private static final class StructAccessorTestHelper extends AccessorTestHelper {
    private StructAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetStruct(ResultSet resultSet) throws SQLException {
      Struct expectedStruct = new StructImpl(Arrays.asList(42, false));
      assertEquals(expectedStruct, g.getStruct(resultSet));
    }
  }

  /**
   * Accessor test helper for the byte column.
   */
  private static final class ByteAccessorTestHelper extends AccessorTestHelper {
    private ByteAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("1", g.getString(resultSet));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, g.getBoolean(resultSet));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 1, g.getByte(resultSet));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 1, g.getShort(resultSet));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(1, g.getInt(resultSet));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(1L, g.getLong(resultSet));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(BigDecimal.ONE, g.getBigDecimal(resultSet));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(1.0f, g.getFloat(resultSet), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(1.0d, g.getDouble(resultSet), 0);
    }
  }

  /**
   * Accessor test helper for the short column.
   */
  private static final class ShortAccessorTestHelper extends AccessorTestHelper {
    private ShortAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("2", g.getString(resultSet));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, g.getBoolean(resultSet));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 2, g.getByte(resultSet));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 2, g.getShort(resultSet));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(2, g.getInt(resultSet));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(2L, g.getLong(resultSet));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(new BigDecimal(2), g.getBigDecimal(resultSet));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(2.0f, g.getFloat(resultSet), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(2.0d, g.getDouble(resultSet), 0);
    }
  }

  /**
   * Accessor test helper for the int column.
   */
  private static final class IntAccessorTestHelper extends AccessorTestHelper {
    private IntAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("3", g.getString(resultSet));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, g.getBoolean(resultSet));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 3, g.getByte(resultSet));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 3, g.getShort(resultSet));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(3, g.getInt(resultSet));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(3L, g.getLong(resultSet));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(new BigDecimal(3), g.getBigDecimal(resultSet));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(3.0f, g.getFloat(resultSet), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(3.0d, g.getDouble(resultSet), 0);
    }
  }

  /**
   * Accessor test helper for the long column.
   */
  private static final class LongAccessorTestHelper extends AccessorTestHelper {
    private LongAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("4", g.getString(resultSet));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, g.getBoolean(resultSet));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 4, g.getByte(resultSet));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 4, g.getShort(resultSet));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(4, g.getInt(resultSet));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(4L, g.getLong(resultSet));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(new BigDecimal(4), g.getBigDecimal(resultSet));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(4.0f, g.getFloat(resultSet), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(4.0d, g.getDouble(resultSet), 0);
    }
  }

  /**
   * accessor test helper for the float column
   */
  private static final class FloatAccessorTestHelper extends AccessorTestHelper {
    private FloatAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("5.0", g.getString(resultSet));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, g.getBoolean(resultSet));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 5, g.getByte(resultSet));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 5, g.getShort(resultSet));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(5, g.getInt(resultSet));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(5L, g.getLong(resultSet));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(new BigDecimal(5).setScale(1), g.getBigDecimal(resultSet));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(5.0f, g.getFloat(resultSet), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(5.0d, g.getDouble(resultSet), 0);
    }
  }

  /**
   * Accessor test helper for the double column.
   */
  private static final class DoubleAccessorTestHelper extends AccessorTestHelper {
    private DoubleAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("6.0", g.getString(resultSet));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, g.getBoolean(resultSet));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 6, g.getByte(resultSet));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 6, g.getShort(resultSet));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(6, g.getInt(resultSet));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(6L, g.getLong(resultSet));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(new BigDecimal(6).setScale(1), g.getBigDecimal(resultSet));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(6.0f, g.getFloat(resultSet), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(6.0d, g.getDouble(resultSet), 0);
    }
  }

  /**
   * Accessor test helper for the date column.
   */
  private static final class DateAccessorTestHelper extends AccessorTestHelper {
    private DateAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("2016-10-10", g.getString(resultSet));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, g.getBoolean(resultSet));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) -68, g.getByte(resultSet));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 17084, g.getShort(resultSet));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(17084, g.getInt(resultSet));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(17084, g.getLong(resultSet));
    }

    @Override public void testGetDate(ResultSet resultSet, Calendar calendar) throws SQLException {
      assertEquals(new Date(1476130718123L), g.getDate(resultSet, calendar));
    }
  }

  /**
   * accessor test helper for the time column
   */
  private static final class TimeAccessorTestHelper extends AccessorTestHelper {
    private TimeAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("20:18:38", g.getString(resultSet));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, g.getBoolean(resultSet));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) -85, g.getByte(resultSet));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) -20053, g.getShort(resultSet));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(73118123, g.getInt(resultSet));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(73118123, g.getLong(resultSet));
    }

    @Override public void testGetTime(ResultSet resultSet, Calendar calendar) throws SQLException {
      assertEquals(new Time(1476130718123L), g.getTime(resultSet, calendar));
    }
  }

  /**
   * accessor test helper for the timestamp column
   */
  private static final class TimestampAccessorTestHelper extends AccessorTestHelper {
    private TimestampAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("2016-10-10 20:18:38", g.getString(resultSet));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, g.getBoolean(resultSet));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) -85, g.getByte(resultSet));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 16811, g.getShort(resultSet));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(-1338031701, g.getInt(resultSet));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(1476130718123L, g.getLong(resultSet));
    }

    @Override public void testGetDate(ResultSet resultSet, Calendar calendar) throws SQLException {
      assertEquals(new Date(1476130718123L), g.getDate(resultSet, calendar));
    }

    @Override public void testGetTime(ResultSet resultSet, Calendar calendar) throws SQLException {
      // how come both are different? DST...
      //assertEquals(new Time(1476130718123L), g.getTime(label, calendar));
      assertEquals(new Time(73118123L), g.getTime(resultSet, calendar));
    }

    @Override public void testGetTimestamp(ResultSet resultSet, Calendar calendar)
        throws SQLException {
      assertEquals(new Timestamp(1476130718123L), g.getTimestamp(resultSet, calendar));
    }
  }

  /**
   * accessor test helper for the string column
   */
  private static final class StringAccessorTestHelper extends AccessorTestHelper {
    private StringAccessorTestHelper(Getter g) {
      super(g);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("testvalue", g.getString(resultSet));
    }
  }

  private static final Calendar DEFAULT_CALENDAR = DateTimeUtils.calendar();

  private static Connection connection = null;
  private static ResultSet resultSet = null;

  @BeforeClass
  public static void executeQuery() throws SQLException {
    Properties properties = new Properties();
    properties.setProperty("timeZone", "GMT");

    connection = new TestDriver().connect("jdbc:test", properties);
    resultSet = connection.createStatement().executeQuery("SELECT * FROM TABLE");
    resultSet.next(); // move to the first record
  }

  @AfterClass
  public static void cleanupQuery() throws SQLException {
    if (resultSet != null) {
      resultSet.close();
    }

    if (connection != null) {
      connection.close();
    }
  }

  @Parameters(name = "{index}: {0}")
  public static Collection<AccessorTestHelper> data() {
    return Arrays.asList(
        new BooleanAccessorTestHelper(new OrdinalGetter(1)),
        new BooleanAccessorTestHelper(new LabelGetter("bool")),
        new ByteAccessorTestHelper(new OrdinalGetter(2)),
        new ByteAccessorTestHelper(new LabelGetter("byte")),
        new ShortAccessorTestHelper(new OrdinalGetter(3)),
        new ShortAccessorTestHelper(new LabelGetter("short")),
        new IntAccessorTestHelper(new OrdinalGetter(4)),
        new IntAccessorTestHelper(new LabelGetter("int")),
        new LongAccessorTestHelper(new OrdinalGetter(5)),
        new LongAccessorTestHelper(new LabelGetter("long")),
        new FloatAccessorTestHelper(new OrdinalGetter(6)),
        new FloatAccessorTestHelper(new LabelGetter("float")),
        new DoubleAccessorTestHelper(new OrdinalGetter(7)),
        new DoubleAccessorTestHelper(new LabelGetter("double")),
        new StringAccessorTestHelper(new OrdinalGetter(8)),
        new StringAccessorTestHelper(new LabelGetter("string")),
        new DateAccessorTestHelper(new OrdinalGetter(9)),
        new DateAccessorTestHelper(new LabelGetter("date")),
        new TimeAccessorTestHelper(new OrdinalGetter(10)),
        new TimeAccessorTestHelper(new LabelGetter("time")),
        new TimestampAccessorTestHelper(new OrdinalGetter(11)),
        new TimestampAccessorTestHelper(new LabelGetter("timestamp")),
        new ArrayAccessorTestHelper(new OrdinalGetter(12)),
        new ArrayAccessorTestHelper(new LabelGetter("array")),
        new StructAccessorTestHelper(new OrdinalGetter(13)),
        new StructAccessorTestHelper(new LabelGetter("struct")));
  }

  private final AccessorTestHelper testHelper;

  public AvaticaResultSetConversionsTest(AccessorTestHelper testHelper) {
    this.testHelper = testHelper;
  }

  @Test
  public void testGetString() throws SQLException {
    testHelper.testGetString(resultSet);
  }

  @Test
  public void testGetBoolean() throws SQLException {
    testHelper.testGetBoolean(resultSet);
  }

  @Test
  public void testGetByte() throws SQLException {
    testHelper.testGetByte(resultSet);
  }

  @Test
  public void testGetShort() throws SQLException {
    testHelper.testGetShort(resultSet);
  }

  @Test
  public void testGetInt() throws SQLException {
    testHelper.testGetInt(resultSet);
  }

  @Test
  public void testGetLong() throws SQLException {
    testHelper.testGetLong(resultSet);
  }

  @Test
  public void testGetFloat() throws SQLException {
    testHelper.testGetFloat(resultSet);
  }

  @Test
  public void testGetDouble() throws SQLException {
    testHelper.testGetDouble(resultSet);
  }

  @Test
  public void testGetDecimal() throws SQLException {
    testHelper.testGetDecimal(resultSet);
  }

  @Test
  public void testGetBytes() throws SQLException {
    testHelper.testGetBytes(resultSet);
  }

  @Test
  public void testGetAsciiStream() throws SQLException {
    testHelper.testGetAsciiStream(resultSet);
  }

  @Test
  public void testGetBinaryStream() throws SQLException {
    testHelper.testGetBinaryStream(resultSet);
  }

  @Test
  public void testGetObject() throws SQLException {
    testHelper.testGetObject(resultSet);
  }

  @Test
  public void testGetCharacterStream() throws SQLException {
    testHelper.testGetCharacterStream(resultSet);
  }

  @Test
  public void testGetObjectWithMap() throws SQLException {
    testHelper.testGetObject(resultSet, Collections.<String, Class<?>>emptyMap());
  }

  @Test
  public void testGetRef() throws SQLException {
    testHelper.testGetRef(resultSet);
  }

  @Test
  public void testGetBlob() throws SQLException {
    testHelper.testGetBlob(resultSet);
  }

  @Test
  public void testGetClob() throws SQLException {
    testHelper.testGetClob(resultSet);
  }

  @Test
  public void testGetArray() throws SQLException {
    testHelper.testGetArray(resultSet);
  }

  @Test
  public void testGetDate() throws SQLException {
    testHelper.testGetDate(resultSet, DEFAULT_CALENDAR);
  }

  @Test
  public void testGetTime() throws SQLException {
    testHelper.testGetTime(resultSet, DEFAULT_CALENDAR);
  }

  @Test
  public void testGetTimestamp() throws SQLException {
    testHelper.testGetTimestamp(resultSet, DEFAULT_CALENDAR);
  }

  @Test
  public void getURL() throws SQLException {
    testHelper.getURL(resultSet);
  }

  @Test
  public void testGetNClob() throws SQLException {
    testHelper.testGetNClob(resultSet);
  }

  @Test
  public void testGetSQLXML() throws SQLException {
    testHelper.testGetSQLXML(resultSet);
  }

  @Test
  public void testGetNString() throws SQLException {
    testHelper.testGetNString(resultSet);
  }

  @Test
  public void testGetNCharacterStream() throws SQLException {
    testHelper.testGetNCharacterStream(resultSet);
  }

  /** Retrieves a value from a particular column of a result set, in
   * whatever type the caller chooses. */
  interface Getter {
    byte getByte(ResultSet r) throws SQLException;
    short getShort(ResultSet r) throws SQLException;
    int getInt(ResultSet r) throws SQLException;
    long getLong(ResultSet r) throws SQLException;
    float getFloat(ResultSet r) throws SQLException;
    double getDouble(ResultSet r) throws SQLException;
    BigDecimal getBigDecimal(ResultSet r) throws SQLException;
    boolean getBoolean(ResultSet r) throws SQLException;
    Date getDate(ResultSet r) throws SQLException;
    Date getDate(ResultSet r, Calendar c) throws SQLException;
    Time getTime(ResultSet r) throws SQLException;
    Time getTime(ResultSet r, Calendar c) throws SQLException;
    Timestamp getTimestamp(ResultSet r) throws SQLException;
    Timestamp getTimestamp(ResultSet r, Calendar c) throws SQLException;
    String getString(ResultSet r) throws SQLException;
    String getNString(ResultSet r) throws SQLException;
    byte[] getBytes(ResultSet r) throws SQLException;
    InputStream getAsciiStream(ResultSet r) throws SQLException;
    InputStream getBinaryStream(ResultSet r) throws SQLException;
    Array getArray(ResultSet r) throws SQLException;
    Struct getStruct(ResultSet r) throws SQLException;
    Object getCharacterStream(ResultSet r) throws SQLException;
    Object getNCharacterStream(ResultSet r) throws SQLException;
    Object getObject(ResultSet r) throws SQLException;
    Object getObject(ResultSet r, Map<String, Class<?>> map) throws SQLException;
    Object getRef(ResultSet r) throws SQLException;
    Object getBlob(ResultSet r) throws SQLException;
    Object getClob(ResultSet r) throws SQLException;
    Object getNClob(ResultSet r) throws SQLException;
    Object getURL(ResultSet r) throws SQLException;
    Object getSQLXML(ResultSet r) throws SQLException;
  }

  /** Retrieves the value of a column in a result set, addressing by column
   * label. */
  static class OrdinalGetter implements Getter {
    final int ordinal;

    OrdinalGetter(int ordinal) {
      this.ordinal = ordinal;
    }

    public byte getByte(ResultSet r) throws SQLException {
      return r.getByte(ordinal);
    }

    public short getShort(ResultSet r) throws SQLException {
      return r.getShort(ordinal);
    }

    public int getInt(ResultSet r) throws SQLException {
      return r.getInt(ordinal);
    }

    public long getLong(ResultSet r) throws SQLException {
      return r.getLong(ordinal);
    }

    public float getFloat(ResultSet r) throws SQLException {
      return r.getFloat(ordinal);
    }

    public double getDouble(ResultSet r) throws SQLException {
      return r.getDouble(ordinal);
    }

    public BigDecimal getBigDecimal(ResultSet r) throws SQLException {
      return r.getBigDecimal(ordinal);
    }

    public boolean getBoolean(ResultSet r) throws SQLException {
      return r.getBoolean(ordinal);
    }

    public Date getDate(ResultSet r) throws SQLException {
      return r.getDate(ordinal);
    }

    public Date getDate(ResultSet r, Calendar c) throws SQLException {
      return r.getDate(ordinal, c);
    }

    public Time getTime(ResultSet r) throws SQLException {
      return r.getTime(ordinal);
    }

    public Time getTime(ResultSet r, Calendar c) throws SQLException {
      return r.getTime(ordinal, c);
    }

    public Timestamp getTimestamp(ResultSet r) throws SQLException {
      return r.getTimestamp(ordinal);
    }

    public Timestamp getTimestamp(ResultSet r, Calendar c) throws SQLException {
      return r.getTimestamp(ordinal, c);
    }

    public String getString(ResultSet r) throws SQLException {
      return r.getString(ordinal);
    }

    public String getNString(ResultSet r) throws SQLException {
      return r.getNString(ordinal);
    }

    public byte[] getBytes(ResultSet r) throws SQLException {
      return r.getBytes(ordinal);
    }

    public InputStream getAsciiStream(ResultSet r) throws SQLException {
      return r.getAsciiStream(ordinal);
    }

    public InputStream getBinaryStream(ResultSet r) throws SQLException {
      return r.getBinaryStream(ordinal);
    }

    public Object getCharacterStream(ResultSet r) throws SQLException {
      return r.getCharacterStream(ordinal);
    }

    public Object getNCharacterStream(ResultSet r) throws SQLException {
      return r.getNCharacterStream(ordinal);
    }

    public Object getObject(ResultSet r) throws SQLException {
      return r.getObject(ordinal);
    }

    public Object getObject(ResultSet r, Map<String, Class<?>> map)
        throws SQLException {
      return r.getObject(ordinal, map);
    }

    public Object getRef(ResultSet r) throws SQLException {
      return r.getRef(ordinal);
    }

    public Object getBlob(ResultSet r) throws SQLException {
      return r.getBlob(ordinal);
    }

    public Object getClob(ResultSet r) throws SQLException {
      return r.getClob(ordinal);
    }

    public Object getNClob(ResultSet r) throws SQLException {
      return r.getNClob(ordinal);
    }

    public Object getURL(ResultSet r) throws SQLException {
      return r.getURL(ordinal);
    }

    public Object getSQLXML(ResultSet r) throws SQLException {
      return r.getSQLXML(ordinal);
    }

    public Array getArray(ResultSet r) throws SQLException {
      return r.getArray(ordinal);
    }

    public Struct getStruct(ResultSet r) throws SQLException {
      return (Struct) r.getObject(ordinal);
    }
  }

  /** Retrieves the value of a column in a result set, addressing by column
   * label. */
  static class LabelGetter implements Getter {
    final String label;

    LabelGetter(String label) {
      this.label = label;
    }

    public byte getByte(ResultSet r) throws SQLException {
      return r.getByte(label);
    }

    public short getShort(ResultSet r) throws SQLException {
      return r.getShort(label);
    }

    public int getInt(ResultSet r) throws SQLException {
      return r.getInt(label);
    }

    public long getLong(ResultSet r) throws SQLException {
      return r.getLong(label);
    }

    public float getFloat(ResultSet r) throws SQLException {
      return r.getFloat(label);
    }

    public double getDouble(ResultSet r) throws SQLException {
      return r.getDouble(label);
    }

    public BigDecimal getBigDecimal(ResultSet r) throws SQLException {
      return r.getBigDecimal(label);
    }

    public boolean getBoolean(ResultSet r) throws SQLException {
      return r.getBoolean(label);
    }

    public Date getDate(ResultSet r) throws SQLException {
      return r.getDate(label);
    }

    public Date getDate(ResultSet r, Calendar c) throws SQLException {
      return r.getDate(label, c);
    }

    public Time getTime(ResultSet r) throws SQLException {
      return r.getTime(label);
    }

    public Time getTime(ResultSet r, Calendar c) throws SQLException {
      return r.getTime(label, c);
    }

    public Timestamp getTimestamp(ResultSet r) throws SQLException {
      return r.getTimestamp(label);
    }

    public Timestamp getTimestamp(ResultSet r, Calendar c) throws SQLException {
      return r.getTimestamp(label, c);
    }

    public String getString(ResultSet r) throws SQLException {
      return r.getString(label);
    }

    public String getNString(ResultSet r) throws SQLException {
      return r.getNString(label);
    }

    public byte[] getBytes(ResultSet r) throws SQLException {
      return r.getBytes(label);
    }

    public InputStream getAsciiStream(ResultSet r) throws SQLException {
      return r.getAsciiStream(label);
    }

    public InputStream getBinaryStream(ResultSet r) throws SQLException {
      return r.getBinaryStream(label);
    }

    public Reader getCharacterStream(ResultSet r) throws SQLException {
      return r.getCharacterStream(label);
    }

    public Reader getNCharacterStream(ResultSet r) throws SQLException {
      return r.getNCharacterStream(label);
    }

    public Object getObject(ResultSet r) throws SQLException {
      return r.getObject(label);
    }

    public Object getObject(ResultSet r, Map<String, Class<?>> map)
        throws SQLException {
      return r.getObject(label, map);
    }

    public Ref getRef(ResultSet r) throws SQLException {
      return r.getRef(label);
    }

    public Blob getBlob(ResultSet r) throws SQLException {
      return r.getBlob(label);
    }

    public Clob getClob(ResultSet r) throws SQLException {
      return r.getClob(label);
    }

    public NClob getNClob(ResultSet r) throws SQLException {
      return r.getNClob(label);
    }

    public URL getURL(ResultSet r) throws SQLException {
      return r.getURL(label);
    }

    public SQLXML getSQLXML(ResultSet r) throws SQLException {
      return r.getSQLXML(label);
    }

    public Array getArray(ResultSet r) throws SQLException {
      return r.getArray(label);
    }

    public Struct getStruct(ResultSet r) throws SQLException {
      return (Struct) r.getObject(label);
    }
  }
}

// End AvaticaResultSetConversionsTest.java
