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
import net.hydromatic.linq4j.RawEnumerable;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * API for a service that prepares statements for execution.
 *
 * @author jhyde
 */
public interface OptiqPrepare {
    PrepareResult prepare2(
        Statement statement,
        Expression expression,
        Type elementType);

    PrepareResult prepare(
        Statement statement,
        String sql,
        Expression expression,
        Type elementType);

    interface Statement {
        JavaTypeFactory getTypeFactory();

        Schema getRootSchema();

        Map getRoot();
    }

    public static class PrepareResult {
        public final String sql; // for debug
        public final List<Parameter> parameterList;
        public final List<ColumnMetaData> columnList;
        public final RawEnumerable<Object[]> enumerable;

        public PrepareResult(
            String sql,
            List<Parameter> parameterList,
            List<ColumnMetaData> columnList,
            RawEnumerable<Object[]> enumerable)
        {
            super();
            this.sql = sql;
            this.parameterList = parameterList;
            this.columnList = columnList;
            this.enumerable = enumerable;
        }

        public Enumerator<Object[]> execute() {
            return enumerable.enumerator();
        }
    }

    public static class ColumnMetaData {
        public final int ordinal; // 0-based
        public final boolean autoIncrement;
        public final boolean caseSensitive;
        public final boolean searchable;
        public final boolean currency;
        public final int nullable;
        public final boolean signed;
        public final int displaySize;
        public final String label;
        public final String columnName;
        public final String schemaName;
        public final int precision;
        public final int scale;
        public final String tableName;
        public final String catalogName;
        public final int type;
        public final String typeName;
        public final boolean readOnly;
        public final boolean writable;
        public final boolean definitelyWritable;
        public final String columnClassName;

        public ColumnMetaData(
            int ordinal,
            boolean autoIncrement,
            boolean caseSensitive,
            boolean searchable,
            boolean currency,
            int nullable,
            boolean signed,
            int displaySize,
            String label,
            String columnName,
            String schemaName,
            int precision,
            int scale,
            String tableName,
            String catalogName,
            int type,
            String typeName,
            boolean readOnly,
            boolean writable,
            boolean definitelyWritable,
            String columnClassName)
        {
            this.ordinal = ordinal;
            this.autoIncrement = autoIncrement;
            this.caseSensitive = caseSensitive;
            this.searchable = searchable;
            this.currency = currency;
            this.nullable = nullable;
            this.signed = signed;
            this.displaySize = displaySize;
            this.label = label;
            this.columnName = columnName;
            this.schemaName = schemaName;
            this.precision = precision;
            this.scale = scale;
            this.tableName = tableName;
            this.catalogName = catalogName;
            this.type = type;
            this.typeName = typeName;
            this.readOnly = readOnly;
            this.writable = writable;
            this.definitelyWritable = definitelyWritable;
            this.columnClassName = columnClassName;
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
            String name)
        {
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
