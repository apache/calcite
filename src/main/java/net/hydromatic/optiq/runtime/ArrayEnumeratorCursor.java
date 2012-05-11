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
package net.hydromatic.optiq.runtime;

import net.hydromatic.linq4j.Enumerator;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Implementation of {@link Cursor} on top of an
 * {@link net.hydromatic.linq4j.Enumerator} that
 * returns an array of {@link Object} for each row.
 *
 * @author jhyde
 */
public class ArrayEnumeratorCursor implements Cursor {
    private final Enumerator<Object[]> enumerator;

    /**
     * Creates an ArrayEnumeratorCursor.
     *
     * @param enumerator Enumerator
     */
    public ArrayEnumeratorCursor(Enumerator<Object[]> enumerator) {
        this.enumerator = enumerator;
    }

    public List<Accessor> createAccessors(
        boolean[] wasNull, List<Integer> types)
    {
        List<Accessor> accessors = new ArrayList<Accessor>();
        for (int type : types) {
            accessors.add(createAccessor(type, accessors.size(), wasNull));
        }
        return accessors;
    }

    private Accessor createAccessor(int type, int ordinal, boolean[] wasNull) {
        // Create an accessor appropriate to the underlying type; the accessor
        // can convert to any type in the same family.
        switch (type) {
        case Types.TINYINT:
            return new ByteAccessor(ordinal, wasNull);
        case Types.SMALLINT:
            return new ShortAccessor(ordinal, wasNull);
        case Types.INTEGER:
            return new IntAccessor(ordinal, wasNull);
        case Types.BIGINT:
            return new LongAccessor(ordinal, wasNull);
        case Types.BOOLEAN:
            return new BooleanAccessor(ordinal, wasNull);
        case Types.FLOAT:
            return new FloatAccessor(ordinal, wasNull);
        case Types.DOUBLE:
            return new DoubleAccessor(ordinal, wasNull);
        case Types.CHAR:
        case Types.VARCHAR:
            return new StringAccessor(ordinal, wasNull);
        case Types.BINARY:
        case Types.VARBINARY:
            return new BinaryAccessor(ordinal, wasNull);
        default:
            throw new RuntimeException("unknown type " + type);
        }
    }

    public boolean next() {
        return enumerator.moveNext();
    }

    class AccessorImpl implements Cursor.Accessor {
        protected final int field;
        protected final boolean[] wasNull;

        public AccessorImpl(int field, boolean[] wasNull) {
            this.field = field;
            this.wasNull = wasNull;
        }

        public String getString() {
            throw cannotConvert("String");
        }

        public boolean getBoolean() {
            throw cannotConvert("boolean");
        }

        public byte getByte() {
            throw cannotConvert("byte");
        }

        public short getShort() {
            throw cannotConvert("short");
        }

        public int getInt() {
            throw cannotConvert("int");
        }

        public long getLong() {
            throw cannotConvert("long");
        }

        public float getFloat() {
            throw cannotConvert("float");
        }

        public double getDouble() {
            throw cannotConvert("double");
        }

        public BigDecimal getBigDecimal() {
            throw cannotConvert("BigDecimal");
        }

        public BigDecimal getBigDecimal(int scale) {
            throw cannotConvert("BigDecimal with scale");
        }

        public byte[] getBytes() {
            throw cannotConvert("byte[]");
        }

        public Date getDate() {
            throw cannotConvert("Date");
        }

        public Time getTime() {
            throw cannotConvert("Time");
        }

        public Timestamp getTimestamp() {
            throw cannotConvert("Timestamp");
        }

        public InputStream getAsciiStream() {
            throw cannotConvert("InputStream (ascii)");
        }

        public InputStream getUnicodeStream() {
            throw cannotConvert("InputStream (unicode)");
        }

        public InputStream getBinaryStream() {
            throw cannotConvert("InputStream (binary)");
        }

        public Object getObject() {
            Object o = enumerator.current()[field];
            wasNull[0] = (o == null);
            return o;
        }

        public Reader getCharacterStream() {
            throw cannotConvert("Reader");
        }

        private RuntimeException cannotConvert(String targetType) {
            return new RuntimeException("cannot convert to " + targetType);
        }

        public Object getObject(Map<String, Class<?>> map) {
            throw cannotConvert("Object (with map)");
        }

        public Ref getRef() {
            throw cannotConvert("Ref");
        }

        public Blob getBlob() {
            throw cannotConvert("Blob");
        }

        public Clob getClob() {
            throw cannotConvert("Clob");
        }

        public Array getArray() {
            throw cannotConvert("Array");
        }

        public Date getDate(Calendar cal) {
            throw cannotConvert("Date (with Calendar)");
        }

        public Time getTime(Calendar cal) {
            throw cannotConvert("Time (with Calendar)");
        }

        public Timestamp getTimestamp(Calendar cal) {
            throw cannotConvert("Timestamp (with Calendar)");
        }

        public URL getURL() {
            throw cannotConvert("URL");
        }

        public NClob getNClob() {
            throw cannotConvert("NClob");
        }

        public SQLXML getSQLXML() {
            throw cannotConvert("SQLXML");
        }

        public String getNString() {
            throw cannotConvert("NString");
        }

        public Reader getNCharacterStream() {
            throw cannotConvert("NCharacterStream");
        }

        public <T> T getObject(Class<T> type) {
            throw cannotConvert("Object (with type)");
        }
    }

    /**
     * Accessor of exact numeric values. The subclass must implement the
     * {@link #getLong()} method.
     */
    private abstract class ExactNumericAccessor extends AccessorImpl {
        public ExactNumericAccessor(int field, boolean[] wasNull) {
            super(field, wasNull);
        }

        public BigDecimal getBigDecimal(int scale) {
            final long v = getLong();
            return v == 0 && wasNull[0]
                ? null
                : BigDecimal.valueOf(v)
                    .setScale(scale, RoundingMode.DOWN);
        }

        public BigDecimal getBigDecimal() {
            final long val = getLong();
            return val == 0 && wasNull[0] ? null : BigDecimal.valueOf(val);
        }

        public double getDouble() {
            return getLong();
        }

        public float getFloat() {
            return getLong();
        }

        public abstract long getLong();

        public int getInt() {
            return (int) getLong();
        }

        public short getShort() {
            return (short) getLong();
        }

        public byte getByte() {
            return (byte) getLong();
        }

        public boolean getBoolean() {
            return getLong() != 0d;
        }

        public String getString() {
            final Object o = getObject();
            return o == null ? null : String.valueOf(o);
        }
    }

    /**
     * Accessor that assumes that the underlying value is a {@link Boolean};
     * corresponds to {@link Types#BOOLEAN}.
     */
    private class BooleanAccessor extends ExactNumericAccessor {
        public BooleanAccessor(int field, boolean[] wasNull) {
            super(field, wasNull);
        }

        public boolean getBoolean() {
            Boolean o = (Boolean) getObject();
            return o != null && o;
        }

        public long getLong() {
            return getBoolean() ? 1 : 0;
        }

        public String getString() {
            Boolean o = (Boolean) getObject();
            return o == null ? null : o.toString();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a {@link Byte};
     * corresponds to {@link Types#TINYINT}.
     */
    private class ByteAccessor extends ExactNumericAccessor {
        public ByteAccessor(int field, boolean[] wasNull) {
            super(field, wasNull);
        }

        public byte getByte() {
            Byte o = (Byte) getObject();
            return o == null ? 0 : o;
        }

        public long getLong() {
            return getByte();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a {@link Short};
     * corresponds to {@link Types#SMALLINT}.
     */
    private class ShortAccessor extends ExactNumericAccessor {
        public ShortAccessor(int field, boolean[] wasNull) {
            super(field, wasNull);
        }

        public short getShort() {
            Short o = (Short) getObject();
            return o == null ? 0 : o;
        }

        public long getLong() {
            return getShort();
        }
    }

    /**
     * Accessor that assumes that the underlying value is an {@link Integer};
     * corresponds to {@link Types#INTEGER}.
     */
    private class IntAccessor extends ExactNumericAccessor {
        public IntAccessor(int field, boolean[] wasNull) {
            super(field, wasNull);
        }

        public int getInt() {
            Integer o = (Integer) getObject();
            return o == null ? 0 : o;
        }

        public long getLong() {
            return getInt();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a {@link Long};
     * corresponds to {@link Types#BIGINT}.
     */
    private class LongAccessor extends ExactNumericAccessor {
        public LongAccessor(int field, boolean[] wasNull) {
            super(field, wasNull);
        }

        public long getLong() {
            Long o = (Long) getObject();
            return o == null ? 0 : o;
        }
    }

    /**
     * Accessor of values that are {@link Double} or null.
     */
    private abstract class ApproximateNumericAccessor extends AccessorImpl {
        public ApproximateNumericAccessor(int field, boolean[] wasNull) {
            super(field, wasNull);
        }

        public BigDecimal getBigDecimal(int scale) {
            final double v = getDouble();
            return v == 0d && wasNull[0]
                ? null
                : BigDecimal.valueOf(v)
                    .setScale(scale, RoundingMode.DOWN);
        }

        public BigDecimal getBigDecimal() {
            final double v = getDouble();
            return v == 0 && wasNull[0] ? null : BigDecimal.valueOf(v);
        }

        public abstract double getDouble();

        public float getFloat() {
            return (float) getDouble();
        }

        public long getLong() {
            return (long) getDouble();
        }

        public int getInt() {
            return (int) getDouble();
        }

        public short getShort() {
            return (short) getDouble();
        }

        public byte getByte() {
            return (byte) getDouble();
        }

        public boolean getBoolean() {
            return getDouble() != 0;
        }

        public String getString() {
            final Object o = getObject();
            return o == null ? null : String.valueOf(o);
        }
    }

    /**
     * Accessor that assumes that the underlying value is a {@link Float};
     * corresponds to {@link Types#FLOAT}.
     */
    private class FloatAccessor extends ApproximateNumericAccessor {
        public FloatAccessor(int field, boolean[] wasNull) {
            super(field, wasNull);
        }

        public float getFloat() {
            Float o = (Float) getObject();
            return o == null ? 0f : o;
        }

        public double getDouble() {
            return getFloat();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a {@link Float};
     * corresponds to {@link Types#FLOAT}.
     */
    private class DoubleAccessor extends ApproximateNumericAccessor {
        public DoubleAccessor(int field, boolean[] wasNull) {
            super(field, wasNull);
        }

        public double getDouble() {
            Double o = (Double) getObject();
            return o == null ? 0d : o;
        }
    }

    /**
     * Accessor that assumes that the underlying value is a {@link String};
     * corresponds to {@link Types#CHAR} and {@link Types#VARCHAR}.
     */
    private class StringAccessor extends AccessorImpl {
        public StringAccessor(int field, boolean[] wasNull) {
            super(field, wasNull);
        }

        public String getString() {
            return (String) getObject();
        }
    }

    /**
     * Accessor that assumes that the underlying value is an array of
     * {@code byte} values;
     * corresponds to {@link Types#BINARY} and {@link Types#VARBINARY}.
     */
    private class BinaryAccessor extends AccessorImpl {
        public BinaryAccessor(int field, boolean[] wasNull) {
            super(field, wasNull);
        }

        public byte[] getBytes() {
            return (byte[]) getObject();
        }

        @Override
        public String getString() {
            byte[] bytes = getBytes();
            return bytes == null ? null : ByteString.toString(bytes);
        }
    }
}

// End ArrayEnumeratorCursor.java
