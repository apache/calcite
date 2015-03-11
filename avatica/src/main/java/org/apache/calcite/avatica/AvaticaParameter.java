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

import org.apache.calcite.avatica.util.ByteString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Metadata for a parameter.
 */
public class AvaticaParameter {
  public final boolean signed;
  public final int precision;
  public final int scale;
  public final int parameterType;
  public final String typeName;
  public final String className;
  public final String name;

  /** Value that means the parameter has been set to null.
   * If value is null, parameter has not been set. */
  public static final Object DUMMY_VALUE = Dummy.INSTANCE;

  @JsonCreator
  public AvaticaParameter(
      @JsonProperty("signed") boolean signed,
      @JsonProperty("precision") int precision,
      @JsonProperty("scale") int scale,
      @JsonProperty("parameterType") int parameterType,
      @JsonProperty("typeName") String typeName,
      @JsonProperty("className") String className,
      @JsonProperty("name") String name) {
    this.signed = signed;
    this.precision = precision;
    this.scale = scale;
    this.parameterType = parameterType;
    this.typeName = typeName;
    this.className = className;
    this.name = name;
  }

  public void setByte(Object[] slots, int index, byte o) {
    slots[index] = o;
  }

  public void setChar(Object[] slots, int index, char o) {
    slots[index] = o;
  }

  public void setShort(Object[] slots, int index, short o) {
    slots[index] = o;
  }

  public void setInt(Object[] slots, int index, int o) {
    slots[index] = o;
  }

  public void setLong(Object[] slots, int index, long o) {
    slots[index] = o;
  }

  public void setBoolean(Object[] slots, int index, boolean o) {
    slots[index] = o;
  }

  private static Object wrap(Object o) {
    if (o == null) {
      return DUMMY_VALUE;
    }
    return o;
  }

  public boolean isSet(Object[] slots, int index) {
    return slots[index] != null;
  }

  public void setRowId(Object[] slots, int index, RowId x) {
    slots[index] = wrap(x);
  }

  public void setNString(Object[] slots, int index, String o) {
    slots[index] = wrap(o);
  }

  public void setNCharacterStream(Object[] slots, int index, Reader value,
      long length) {
  }

  public void setNClob(Object[] slots, int index, NClob value) {
    slots[index] = wrap(value);
  }

  public void setClob(Object[] slots, int index, Reader reader, long length) {
  }

  public void setBlob(Object[] slots, int index, InputStream inputStream,
      long length) {
  }

  public void setNClob(Object[] slots, int index, Reader reader, long length) {
  }

  public void setSQLXML(Object[] slots, int index, SQLXML xmlObject) {
    slots[index] = wrap(xmlObject);
  }

  public void setAsciiStream(Object[] slots, int index, InputStream x,
      long length) {
  }

  public void setBinaryStream(Object[] slots, int index, InputStream x,
      long length) {
  }

  public void setCharacterStream(Object[] slots, int index, Reader reader,
      long length) {
  }

  public void setAsciiStream(Object[] slots, int index, InputStream x) {
  }

  public void setBinaryStream(Object[] slots, int index, InputStream x) {
  }

  public void setCharacterStream(Object[] slots, int index, Reader reader) {
  }

  public void setNCharacterStream(Object[] slots, int index, Reader value) {
  }

  public void setClob(Object[] slots, int index, Reader reader) {
  }

  public void setBlob(Object[] slots, int index, InputStream inputStream) {
  }

  public void setNClob(Object[] slots, int index, Reader reader) {
  }

  public void setUnicodeStream(Object[] slots, int index, InputStream x,
      int length) {
  }

  public void setTimestamp(Object[] slots, int index, Timestamp x) {
    slots[index] = wrap(x);
  }

  public void setTime(Object[] slots, int index, Time x) {
    slots[index] = wrap(x);
  }

  public void setFloat(Object[] slots, int index, float x) {
    slots[index] = wrap(x);
  }

  public void setDouble(Object[] slots, int index, double x) {
    slots[index] = wrap(x);
  }

  public void setBigDecimal(Object[] slots, int index, BigDecimal x) {
    slots[index] = wrap(x);
  }

  public void setString(Object[] slots, int index, String x) {
    slots[index] = wrap(x);
  }

  public void setBytes(Object[] slots, int index, byte[] x) {
    slots[index] = x == null ? DUMMY_VALUE : new ByteString(x);
  }

  public void setDate(Object[] slots, int index, Date x, Calendar cal) {
  }

  public void setDate(Object[] slots, int index, Date x) {
    slots[index] = wrap(x);
  }

  public void setObject(Object[] slots, int index, Object x,
      int targetSqlType) {
  }

  public void setObject(Object[] slots, int index, Object x) {
    slots[index] = wrap(x);
  }

  public void setNull(Object[] slots, int index, int sqlType) {
    slots[index] = DUMMY_VALUE;
  }

  public void setTime(Object[] slots, int index, Time x, Calendar cal) {
  }

  public void setRef(Object[] slots, int index, Ref x) {
  }

  public void setBlob(Object[] slots, int index, Blob x) {
  }

  public void setClob(Object[] slots, int index, Clob x) {
  }

  public void setArray(Object[] slots, int index, Array x) {
  }

  public void setTimestamp(Object[] slots, int index, Timestamp x,
      Calendar cal) {
  }

  public void setNull(Object[] slots, int index, int sqlType, String typeName) {
  }

  public void setURL(Object[] slots, int index, URL x) {
  }

  public void setObject(Object[] slots, int index, Object x, int targetSqlType,
      int scaleOrLength) {
  }

  /** Singleton value to denote parameters that have been set to null (as
   * opposed to not set).
   *
   * <p>Not a valid value for a parameter.
   *
   * <p>As an enum, it is serializable by Jackson. */
  private enum Dummy {
    INSTANCE
  }
}

// End AvaticaParameter.java
