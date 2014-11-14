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
package net.hydromatic.avatica;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

/**
 * Metadata for a parameter. Plus a slot to hold its value.
 */
public class AvaticaParameter {
  public final boolean signed;
  public final int precision;
  public final int scale;
  public final int parameterType;
  public final String typeName;
  public final String className;
  public final String name;

  Object value;

  /** Value that means the parameter has been set to null.
   * If {@link #value} is null, parameter has not been set. */
  public static final Object DUMMY_VALUE = new Object();

  public AvaticaParameter(
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
    this.value = o;
  }

  public void setValue(char o) {
    this.value = o;
  }

  public void setShort(short o) {
    this.value = o;
  }

  public void setInt(int o) {
    this.value = o;
  }

  public void setValue(long o) {
    this.value = o;
  }

  public void setValue(byte[] o) {
    this.value = o == null ? DUMMY_VALUE : new ByteString(o);
  }

  public void setBoolean(boolean o) {
    this.value = o;
  }

  public void setValue(Object o) {
    this.value = wrap(o);
  }

  private static Object wrap(Object o) {
    if (o == null) {
      return DUMMY_VALUE;
    }
    return o;
  }

  public boolean isSet() {
    return value != null;
  }

  public void setRowId(RowId x) {
    this.value = wrap(x);
  }

  public void setNString(String o) {
    this.value = wrap(o);
  }

  public void setNCharacterStream(Reader value, long length) {
  }

  public void setNClob(NClob value) {
    this.value = wrap(value);
  }

  public void setClob(Reader reader, long length) {
  }

  public void setBlob(InputStream inputStream, long length) {
  }

  public void setNClob(Reader reader, long length) {
  }

  public void setSQLXML(SQLXML xmlObject) {
    this.value = wrap(xmlObject);
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
    this.value = wrap(x);
  }

  public void setTime(Time x) {
    this.value = wrap(x);
  }

  public void setFloat(float x) {
    this.value = wrap(x);
  }

  public void setDouble(double x) {
    this.value = wrap(x);
  }

  public void setBigDecimal(BigDecimal x) {
    this.value = wrap(x);
  }

  public void setString(String x) {
    this.value = wrap(x);
  }

  public void setBytes(byte[] x) {
    this.value = x == null ? DUMMY_VALUE : wrap(x);
  }

  public void setDate(Date x, Calendar cal) {
  }

  public void setDate(Date x) {
    this.value = wrap(x);
  }

  public void setObject(Object x, int targetSqlType) {
  }

  public void setObject(Object x) {
    this.value = wrap(x);
  }

  public void setNull(int sqlType) {
    this.value = DUMMY_VALUE;
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

// End AvaticaParameter.java
