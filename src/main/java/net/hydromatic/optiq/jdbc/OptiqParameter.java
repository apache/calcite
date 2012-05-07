package net.hydromatic.optiq.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

/**
* Created with IntelliJ IDEA.
* User: jhyde
* Date: 4/15/12
* Time: 11:00 PM
* To change this template use File | Settings | File Templates.
*/
class OptiqParameter {

    final boolean signed;
    final int precision;
    final int scale;
    final int parameterType;
    final String typeName;
    final String className;
    final String name;
    private Object value;

    private static final Object DUMMY_VALUE = new Object();

    public OptiqParameter(
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

    public void zsetValue(Object o) {
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

// End OptiqParameter.java
