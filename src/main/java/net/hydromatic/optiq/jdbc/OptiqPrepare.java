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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.RawEnumerable;
import net.hydromatic.linq4j.function.Function0;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.runtime.ColumnMetaData;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlNode;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;

/**
 * API for a service that prepares statements for execution.
 */
public interface OptiqPrepare {
  Function0<OptiqPrepare> DEFAULT_FACTORY =
      new Function0<OptiqPrepare>() {
        public OptiqPrepare apply() {
          return new OptiqPrepareImpl();
        }
      };

  ParseResult parse(
      Context context, String sql);

  <T> PrepareResult<T> prepareSql(
      Context context,
      String sql,
      Queryable<T> expression,
      Type elementType,
      int maxRowCount);

  <T> PrepareResult<T> prepareQueryable(
      Context context,
      Queryable<T> queryable);

  /** Context for preparing a statement. */
  interface Context {
    JavaTypeFactory getTypeFactory();

    Schema getRootSchema();

    List<String> getDefaultSchemaPath();
  }

  public static class ParseResult {
    public final String sql; // for debug
    public final SqlNode sqlNode;
    public final RelDataType rowType;

    public ParseResult(String sql, SqlNode sqlNode, RelDataType rowType) {
      super();
      this.sql = sql;
      this.sqlNode = sqlNode;
      this.rowType = rowType;
    }
  }

  public static class PrepareResult<T> {
    public final String sql; // for debug
    public final List<Parameter> parameterList;
    public final List<ColumnMetaData> columnList;
    public final RawEnumerable<T> enumerable;
    public final Class resultClazz;

    public PrepareResult(
        String sql,
        List<Parameter> parameterList,
        List<ColumnMetaData> columnList,
        RawEnumerable<T> enumerable,
        Class resultClazz) {
      super();
      this.sql = sql;
      this.parameterList = parameterList;
      this.columnList = columnList;
      this.enumerable = enumerable;
      this.resultClazz = resultClazz;
    }

    public Enumerator<T> execute() {
      return enumerable.enumerator();
    }
  }

  /**
   * Metadata for a parameter. Plus a slot to hold its value.
   */
  public static class Parameter {
    public final boolean signed;
    public final int precision;
    public final int scale;
    public final int parameterType;
    public final String typeName;
    public final String className;
    public final String name;

    Object value;

    public static final Object DUMMY_VALUE = new Object();

    public Parameter(
        boolean signed,
        int precision,
        int scale,
        int parameterType,
        String typeName,
        String className,
        String name) {
      this.signed = signed;
      this.precision = precision;
      this.scale = scale;
      this.parameterType = parameterType;
      this.typeName = typeName;
      this.className = className;
      this.name = name;
    }

    public void setByte(byte o) {
    }

    public void setValue(char o) {
    }

    public void setShort(short o) {
    }

    public void setInt(int o) {
    }

    public void setValue(long o) {
    }

    public void setValue(byte[] o) {
    }

    public void setBoolean(boolean o) {
    }

    public void setValue(Object o) {
      if (o == null) {
        o = DUMMY_VALUE;
      }
      this.value = o;
    }

    public boolean isSet() {
      return value != null;
    }

    public void setRowId(RowId x) {
    }

    public void setNString(String value) {
    }

    public void setNCharacterStream(Reader value, long length) {
    }

    public void setNClob(NClob value) {
    }

    public void setClob(Reader reader, long length) {
    }

    public void setBlob(InputStream inputStream, long length) {
    }

    public void setNClob(Reader reader, long length) {
    }

    public void setSQLXML(SQLXML xmlObject) {
    }

    public void setAsciiStream(InputStream x, long length) {
    }

    public void setBinaryStream(InputStream x, long length) {
    }

    public void setCharacterStream(Reader reader, long length) {
    }

    public void setAsciiStream(InputStream x) {
    }

    public void setBinaryStream(InputStream x) {
    }

    public void setCharacterStream(Reader reader) {
    }

    public void setNCharacterStream(Reader value) {
    }

    public void setClob(Reader reader) {
    }

    public void setBlob(InputStream inputStream) {
    }

    public void setNClob(Reader reader) {
    }

    public void setUnicodeStream(InputStream x, int length) {
    }

    public void setTimestamp(Timestamp x) {
    }

    public void setTime(Time x) {
    }

    public void setFloat(float x) {
    }

    public void setDouble(double x) {
    }

    public void setBigDecimal(BigDecimal x) {
    }

    public void setString(String x) {
    }

    public void setBytes(byte[] x) {
    }

    public void setDate(Date x, Calendar cal) {
    }

    public void setDate(Date x) {
    }

    public void setObject(Object x, int targetSqlType) {
    }

    public void setObject(Object x) {
    }

    public void setNull(int sqlType) {
    }

    public void setTime(Time x, Calendar cal) {
    }

    public void setRef(Ref x) {
    }

    public void setBlob(Blob x) {
    }

    public void setClob(Clob x) {
    }

    public void setArray(Array x) {
    }

    public void setTimestamp(Timestamp x, Calendar cal) {
    }

    public void setNull(int sqlType, String typeName) {
    }

    public void setURL(URL x) {
    }

    public void setObject(Object x, int targetSqlType, int scaleOrLength) {
    }
  }
}

// End OptiqPrepare.java
