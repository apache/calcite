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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.runtime.ResultSetEnumerable;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Utils for handling prepared statement decoration, like setting dynamic parameters
 */
public class JdbcPreparedStatementUtils {

  private JdbcPreparedStatementUtils() {
  }

  public static ResultSetEnumerable.PreparedStatementEnricher createPreparedStatementConsumer(
      Integer[] indexes,
      DataContext context) {
    return pstm -> {
      int dynamicParamIndex = 1;
      for (int index : indexes) {
        setDynamicParam(dynamicParamIndex, context.get("?" + index), pstm);
        dynamicParamIndex++;
      }
    };
  }

  private static void setDynamicParam(Integer i, Object value, PreparedStatement pstm)
      throws SQLException {
    if (value == null) {
      pstm.setObject(i, null, SqlType.ANY.id);
    } else if (value instanceof Timestamp) {
      pstm.setTimestamp(i, (Timestamp) value);
    } else if (value instanceof Time) {
      pstm.setTime(i, (Time) value);
    } else if (value instanceof String) {
      pstm.setString(i, (String) value);
    } else if (value instanceof Integer) {
      pstm.setInt(i, (Integer) value);
    } else if (value instanceof Double) {
      pstm.setDouble(i, (Double) value);
    } else if (value instanceof java.sql.Array) {
      pstm.setArray(i, (java.sql.Array) value);
    } else if (value instanceof BigDecimal) {
      pstm.setBigDecimal(i, (BigDecimal) value);
    } else if (value instanceof Boolean) {
      pstm.setBoolean(i, (Boolean) value);
    } else if (value instanceof Blob) {
      pstm.setBoolean(i, (Boolean) value);
    } else if (value instanceof Byte) {
      pstm.setByte(i, (Byte) value);
    } else if (value instanceof Clob) {
      pstm.setClob(i, (Clob) value);
    } else if (value instanceof byte[]) {
      pstm.setBytes(i, (byte[]) value);
    } else if (value instanceof Date) {
      pstm.setDate(i, (Date) value);
    } else if (value instanceof Float) {
      pstm.setFloat(i, (Float) value);
    } else if (value instanceof Long) {
      pstm.setLong(i, (Long) value);
    } else if (value instanceof NClob) {
      pstm.setNClob(i, (NClob) value);
    } else if (value instanceof Ref) {
      pstm.setRef(i, (Ref) value);
    } else if (value instanceof RowId) {
      pstm.setRowId(i, (RowId) value);
    } else if (value instanceof Short) {
      pstm.setShort(i, (Short) value);
    } else if (value instanceof URL) {
      pstm.setURL(i, (URL) value);
    } else if (value instanceof SQLXML) {
      pstm.setSQLXML(i, (SQLXML) value);
    } else {
      pstm.setObject(i, value);
    }
  }

}

// End JdbcPreparedStatementUtils.java
