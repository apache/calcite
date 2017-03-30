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

import org.apache.calcite.avatica.ColumnMetaData.AvaticaType;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.DateTimeUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
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

    private static ColumnMetaData columnMetaData(String name, int ordinal, AvaticaType type,
        int columnNullable) {
      return new ColumnMetaData(
          ordinal, false, true, false, false,
          columnNullable,
          true, -1, name, name, null,
          0, 0, null, null, type, true, false, false,
          type.columnClassName());
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
              DatabaseMetaData.columnNoNulls));

      List<Object> row = Collections.<Object>singletonList(
          new Object[] {
            true, (byte) 1, (short) 2, 3, 4L, 5.0f, 6.0d, "testvalue",
            new Date(1476130718123L), new Time(1476130718123L),
            new Timestamp(1476130718123L)
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
   * Base accessor test helper
   */
  private static class AccessorTestHelper {
    protected final int ordinal;

    protected AccessorTestHelper(int ordinal) {
      this.ordinal = ordinal;
    }

    public void testGetString(ResultSet resultSet) throws SQLException {
      resultSet.getString(ordinal);
    }

    public void testGetBoolean(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getBoolean(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetByte(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getByte(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetShort(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getShort(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetInt(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getInt(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetLong(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getLong(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetFloat(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getFloat(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetDouble(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getDouble(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetDecimal(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getBigDecimal(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetBytes(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getBytes(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetAsciiStream(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getAsciiStream(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetBinaryStream(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getBinaryStream(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetObject(ResultSet resultSet) throws SQLException {
      resultSet.getObject(ordinal);
    }

    public void testGetCharacterStream(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getCharacterStream(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetObject(ResultSet resultSet, Map<String, Class<?>> map) throws SQLException {
      try {
        resultSet.getObject(ordinal, map);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetRef(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getRef(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetBlob(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getBlob(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetClob(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getBlob(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetArray(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getArray(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetDate(ResultSet resultSet, Calendar calendar) throws SQLException {
      try {
        resultSet.getDate(ordinal, calendar);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetTime(ResultSet resultSet, Calendar calendar) throws SQLException {
      try {
        resultSet.getTime(ordinal, calendar);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetTimestamp(ResultSet resultSet, Calendar calendar) throws SQLException {
      try {
        resultSet.getTimestamp(ordinal, calendar);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void getURL(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getURL(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetNClob(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getNClob(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetSQLXML(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getSQLXML(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetNString(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getNString(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }

    public void testGetNCharacterStream(ResultSet resultSet) throws SQLException {
      try {
        resultSet.getNCharacterStream(ordinal);
        fail("Was expecting to throw SQLDataException");
      } catch (Exception e) {
        assertThat(e, isA((Class) SQLDataException.class)); // success
      }
    }
  }

  /**
   * accessor test helper for the boolean column
   */
  private static final class BooleanAccesorTestHelper extends AccessorTestHelper {
    private BooleanAccesorTestHelper(int ordinal) {
      super(ordinal);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("true", resultSet.getString(ordinal));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, resultSet.getBoolean(ordinal));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 1, resultSet.getByte(ordinal));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 1, resultSet.getShort(ordinal));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(1, resultSet.getInt(ordinal));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(1L, resultSet.getLong(ordinal));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(BigDecimal.ONE, resultSet.getBigDecimal(ordinal));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(1.0f, resultSet.getFloat(ordinal), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(1.0d, resultSet.getDouble(ordinal), 0);
    }
  }

  /**
   * accessor test helper for the byte column
   */
  private static final class ByteAccesorTestHelper extends AccessorTestHelper {
    private ByteAccesorTestHelper(int ordinal) {
      super(ordinal);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("1", resultSet.getString(ordinal));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, resultSet.getBoolean(ordinal));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 1, resultSet.getByte(ordinal));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 1, resultSet.getShort(ordinal));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(1, resultSet.getInt(ordinal));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(1L, resultSet.getLong(ordinal));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(BigDecimal.ONE, resultSet.getBigDecimal(ordinal));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(1.0f, resultSet.getFloat(ordinal), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(1.0d, resultSet.getDouble(ordinal), 0);
    }
  }

  /**
   * accessor test helper for the short column
   */
  private static final class ShortAccessorTestHelper extends AccessorTestHelper {
    private ShortAccessorTestHelper(int ordinal) {
      super(ordinal);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("2", resultSet.getString(ordinal));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, resultSet.getBoolean(ordinal));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 2, resultSet.getByte(ordinal));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 2, resultSet.getShort(ordinal));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(2, resultSet.getInt(ordinal));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(2L, resultSet.getLong(ordinal));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(new BigDecimal(2), resultSet.getBigDecimal(ordinal));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(2.0f, resultSet.getFloat(ordinal), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(2.0d, resultSet.getDouble(ordinal), 0);
    }
  }

  /**
   * accessor test helper for the int column
   */
  private static final class IntAccessorTestHelper extends AccessorTestHelper {
    private IntAccessorTestHelper(int ordinal) {
      super(ordinal);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("3", resultSet.getString(ordinal));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, resultSet.getBoolean(ordinal));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 3, resultSet.getByte(ordinal));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 3, resultSet.getShort(ordinal));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(3, resultSet.getInt(ordinal));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(3L, resultSet.getLong(ordinal));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(new BigDecimal(3), resultSet.getBigDecimal(ordinal));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(3.0f, resultSet.getFloat(ordinal), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(3.0d, resultSet.getDouble(ordinal), 0);
    }
  }

  /**
   * accessor test helper for the long column
   */
  private static final class LongAccessorTestHelper extends AccessorTestHelper {
    private LongAccessorTestHelper(int ordinal) {
      super(ordinal);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("4", resultSet.getString(ordinal));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, resultSet.getBoolean(ordinal));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 4, resultSet.getByte(ordinal));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 4, resultSet.getShort(ordinal));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(4, resultSet.getInt(ordinal));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(4L, resultSet.getLong(ordinal));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(new BigDecimal(4), resultSet.getBigDecimal(ordinal));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(4.0f, resultSet.getFloat(ordinal), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(4.0d, resultSet.getDouble(ordinal), 0);
    }
  }

  /**
   * accessor test helper for the float column
   */
  private static final class FloatAccessorTestHelper extends AccessorTestHelper {
    private FloatAccessorTestHelper(int ordinal) {
      super(ordinal);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("5.0", resultSet.getString(ordinal));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, resultSet.getBoolean(ordinal));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 5, resultSet.getByte(ordinal));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 5, resultSet.getShort(ordinal));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(5, resultSet.getInt(ordinal));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(5L, resultSet.getLong(ordinal));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(new BigDecimal(5).setScale(1), resultSet.getBigDecimal(ordinal));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(5.0f, resultSet.getFloat(ordinal), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(5.0d, resultSet.getDouble(ordinal), 0);
    }
  }

  /**
   * accessor test helper for the double column
   */
  private static final class DoubleAccessorTestHelper extends AccessorTestHelper {
    private DoubleAccessorTestHelper(int ordinal) {
      super(ordinal);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("6.0", resultSet.getString(ordinal));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, resultSet.getBoolean(ordinal));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) 6, resultSet.getByte(ordinal));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 6, resultSet.getShort(ordinal));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(6, resultSet.getInt(ordinal));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(6L, resultSet.getLong(ordinal));
    }

    @Override public void testGetDecimal(ResultSet resultSet) throws SQLException {
      assertEquals(new BigDecimal(6).setScale(1), resultSet.getBigDecimal(ordinal));
    }

    @Override public void testGetFloat(ResultSet resultSet) throws SQLException {
      assertEquals(6.0f, resultSet.getFloat(ordinal), 0);
    }

    @Override public void testGetDouble(ResultSet resultSet) throws SQLException {
      assertEquals(6.0d, resultSet.getDouble(ordinal), 0);
    }
  }

  /**
   * accessor test helper for the date column
   */
  private static final class DateAccessorTestHelper extends AccessorTestHelper {
    private DateAccessorTestHelper(int ordinal) {
      super(ordinal);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("2016-10-10", resultSet.getString(ordinal));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, resultSet.getBoolean(ordinal));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) -68, resultSet.getByte(ordinal));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 17084, resultSet.getShort(ordinal));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(17084, resultSet.getInt(ordinal));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(17084, resultSet.getLong(ordinal));
    }

    @Override public void testGetDate(ResultSet resultSet, Calendar calendar) throws SQLException {
      assertEquals(new Date(1476130718123L), resultSet.getDate(ordinal, calendar));
    }
  }

  /**
   * accessor test helper for the time column
   */
  private static final class TimeAccessorTestHelper extends AccessorTestHelper {
    private TimeAccessorTestHelper(int ordinal) {
      super(ordinal);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("20:18:38", resultSet.getString(ordinal));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, resultSet.getBoolean(ordinal));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) -85, resultSet.getByte(ordinal));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) -20053, resultSet.getShort(ordinal));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(73118123, resultSet.getInt(ordinal));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(73118123, resultSet.getLong(ordinal));
    }

    @Override public void testGetTime(ResultSet resultSet, Calendar calendar) throws SQLException {
      assertEquals(new Time(1476130718123L), resultSet.getTime(ordinal, calendar));
    }
  }

  /**
   * accessor test helper for the timestamp column
   */
  private static final class TimestampAccessorTestHelper extends AccessorTestHelper {
    private TimestampAccessorTestHelper(int ordinal) {
      super(ordinal);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("2016-10-10 20:18:38", resultSet.getString(ordinal));
    }

    @Override public void testGetBoolean(ResultSet resultSet) throws SQLException {
      assertEquals(true, resultSet.getBoolean(ordinal));
    }

    @Override public void testGetByte(ResultSet resultSet) throws SQLException {
      assertEquals((byte) -85, resultSet.getByte(ordinal));
    }

    @Override public void testGetShort(ResultSet resultSet) throws SQLException {
      assertEquals((short) 16811, resultSet.getShort(ordinal));
    }

    @Override public void testGetInt(ResultSet resultSet) throws SQLException {
      assertEquals(-1338031701, resultSet.getInt(ordinal));
    }

    @Override public void testGetLong(ResultSet resultSet) throws SQLException {
      assertEquals(1476130718123L, resultSet.getLong(ordinal));
    }

    @Override public void testGetDate(ResultSet resultSet, Calendar calendar) throws SQLException {
      assertEquals(new Date(1476130718123L), resultSet.getDate(ordinal, calendar));
    }

    @Override public void testGetTime(ResultSet resultSet, Calendar calendar) throws SQLException {
      // how come both are different? DST...
      //assertEquals(new Time(1476130718123L), resultSet.getTime(ordinal, calendar));
      assertEquals(new Time(73118123L), resultSet.getTime(ordinal, calendar));
    }

    @Override public void testGetTimestamp(ResultSet resultSet, Calendar calendar)
        throws SQLException {
      assertEquals(new Timestamp(1476130718123L), resultSet.getTimestamp(ordinal, calendar));
    }
  }

  /**
   * accessor test helper for the string column
   */
  private static final class StringAccessorTestHelper extends AccessorTestHelper {
    private StringAccessorTestHelper(int ordinal) {
      super(ordinal);
    }

    @Override public void testGetString(ResultSet resultSet) throws SQLException {
      assertEquals("testvalue", resultSet.getString(ordinal));
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

  @Parameters
  public static Collection<AccessorTestHelper> data() {
    return Arrays.<AccessorTestHelper>asList(
        new BooleanAccesorTestHelper(1),
        new ByteAccesorTestHelper(2),
        new ShortAccessorTestHelper(3),
        new IntAccessorTestHelper(4),
        new LongAccessorTestHelper(5),
        new FloatAccessorTestHelper(6),
        new DoubleAccessorTestHelper(7),
        new StringAccessorTestHelper(8),
        new DateAccessorTestHelper(9),
        new TimeAccessorTestHelper(10),
        new TimestampAccessorTestHelper(11));
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
}

// End AvaticaResultSetConversionsTest.java
