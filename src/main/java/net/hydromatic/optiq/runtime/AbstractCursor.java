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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * Base class for implementing a cursor.
 *
 * <p>Derived class needs to provide {@link Getter} and can override
 * {@link Accessor} implementations if it wishes.</p>
 *
 * @author jhyde
 */
abstract class AbstractCursor implements Cursor {
    /**
     * Slot into which each accessor should write whether the
     * value returned was null.
     */
    protected final boolean[] wasNull = {false};

    protected AbstractCursor() {
    }

    public List<Accessor> createAccessors(List<ColumnMetaData> types) {
        List<Accessor> accessors = new ArrayList<Accessor>();
        for (ColumnMetaData type : types) {
            accessors.add(createAccessor(type, accessors.size()));
        }
        return accessors;
    }

    protected Accessor createAccessor(ColumnMetaData type, int ordinal) {
        // Create an accessor appropriate to the underlying type; the accessor
        // can convert to any type in the same family.
        Getter getter = createGetter(ordinal);
        switch (type.type) {
        case Types.TINYINT:
            return new ByteAccessor(getter);
        case Types.SMALLINT:
            return new ShortAccessor(getter);
        case Types.INTEGER:
            return new IntAccessor(getter);
        case Types.BIGINT:
            return new LongAccessor(getter);
        case Types.BOOLEAN:
            return new BooleanAccessor(getter);
        case Types.FLOAT:
            return new FloatAccessor(getter);
        case Types.DOUBLE:
            return new DoubleAccessor(getter);
        case Types.DECIMAL:
            return new BigDecimalAccessor(getter);
        case Types.CHAR:
            return new FixedStringAccessor(getter, type.displaySize);
        case Types.VARCHAR:
            return new StringAccessor(getter);
        case Types.BINARY:
        case Types.VARBINARY:
            return new BinaryAccessor(getter);
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
            return new DateTimeAccessor(getter);
        case Types.JAVA_OBJECT:
            return new ObjectAccessor(getter);
        default:
            throw new RuntimeException("unknown type " + type);
        }
    }

    protected abstract Getter createGetter(int ordinal);

    public abstract boolean next();

    static class AccessorImpl implements Accessor {
        protected final Getter getter;

        public AccessorImpl(Getter getter) {
            this.getter = getter;
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
            return getter.getObject();
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
    private static abstract class ExactNumericAccessor extends AccessorImpl {
        public ExactNumericAccessor(Getter getter) {
            super(getter);
        }

        public BigDecimal getBigDecimal(int scale) {
            final long v = getLong();
            return v == 0 && getter.wasNull()
                ? null
                : BigDecimal.valueOf(v)
                    .setScale(scale, RoundingMode.DOWN);
        }

        public BigDecimal getBigDecimal() {
            final long val = getLong();
            return val == 0 && getter.wasNull()
                ? null
                : BigDecimal.valueOf(val);
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
     * corresponds to {@link java.sql.Types#BOOLEAN}.
     */
    private static class BooleanAccessor extends ExactNumericAccessor {
        public BooleanAccessor(Getter getter) {
            super(getter);
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
     * corresponds to {@link java.sql.Types#TINYINT}.
     */
    private static class ByteAccessor extends ExactNumericAccessor {
        public ByteAccessor(Getter getter) {
            super(getter);
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
     * corresponds to {@link java.sql.Types#SMALLINT}.
     */
    private static class ShortAccessor extends ExactNumericAccessor {
        public ShortAccessor(Getter getter) {
            super(getter);
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
     * corresponds to {@link java.sql.Types#INTEGER}.
     */
    private static class IntAccessor extends ExactNumericAccessor {
        public IntAccessor(Getter getter) {
            super(getter);
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
     * corresponds to {@link java.sql.Types#BIGINT}.
     */
    private static class LongAccessor extends ExactNumericAccessor {
        public LongAccessor(Getter getter) {
            super(getter);
        }

        public long getLong() {
            Long o = (Long) getObject();
            return o == null ? 0 : o;
        }
    }

    /**
     * Accessor of values that are {@link Double} or null.
     */
    private static abstract class ApproximateNumericAccessor
        extends AccessorImpl
    {
        public ApproximateNumericAccessor(Getter getter) {
            super(getter);
        }

        public BigDecimal getBigDecimal(int scale) {
            final double v = getDouble();
            return v == 0d && getter.wasNull()
                ? null
                : BigDecimal.valueOf(v)
                    .setScale(scale, RoundingMode.DOWN);
        }

        public BigDecimal getBigDecimal() {
            final double v = getDouble();
            return v == 0 && getter.wasNull() ? null : BigDecimal.valueOf(v);
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
     * corresponds to {@link java.sql.Types#FLOAT}.
     */
    private static class FloatAccessor extends ApproximateNumericAccessor {
        public FloatAccessor(Getter getter) {
            super(getter);
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
     * corresponds to {@link java.sql.Types#FLOAT}.
     */
    private static class DoubleAccessor extends ApproximateNumericAccessor {
        public DoubleAccessor(Getter getter) {
            super(getter);
        }

        public double getDouble() {
            Double o = (Double) getObject();
            return o == null ? 0d : o;
        }
    }

    /**
     * Accessor of exact numeric values. The subclass must implement the
     * {@link #getLong()} method.
     */
    private static abstract class BigNumberAccessor extends AccessorImpl {
        public BigNumberAccessor(Getter getter) {
            super(getter);
        }

        protected abstract Number getNumber();

        public double getDouble() {
            Number number = getNumber();
            return number == null ? 0d : number.doubleValue();
        }

        public float getFloat() {
            Number number = getNumber();
            return number == null ? 0f : number.floatValue();
        }

        public long getLong() {
            Number number = getNumber();
            return number == null ? 0l : number.longValue();
        }

        public int getInt() {
            Number number = getNumber();
            return number == null ? 0 : number.intValue();
        }

        public short getShort() {
            Number number = getNumber();
            return number == null ? 0 : number.shortValue();
        }

        public byte getByte() {
            Number number = getNumber();
            return number == null ? 0 : number.byteValue();
        }

        public boolean getBoolean() {
            Number number = getNumber();
            return number == null ? false : number.doubleValue() != 0;
        }

        public String getString() {
            Number number = getNumber();
            return number == null ? null : number.toString();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a {@link BigDecimal};
     * corresponds to {@link java.sql.Types#DECIMAL}.
     */
    private static class BigDecimalAccessor extends BigNumberAccessor {
        public BigDecimalAccessor(Getter getter) {
            super(getter);
        }

        protected Number getNumber() {
            return (Number) getObject();
        }

        public BigDecimal getBigDecimal(int scale) {
            return (BigDecimal) getObject();
        }

        public BigDecimal getBigDecimal() {
            return (BigDecimal) getObject();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a {@link String};
     * corresponds to {@link java.sql.Types#CHAR} and {@link java.sql.Types#VARCHAR}.
     */
    private static class StringAccessor extends AccessorImpl {
        public StringAccessor(Getter getter) {
            super(getter);
        }

        public String getString() {
            return (String) getObject();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a {@link String};
     * corresponds to {@link java.sql.Types#CHAR}.
     */
    private static class FixedStringAccessor extends StringAccessor {
        private final Spacer spacer;

        public FixedStringAccessor(Getter getter, int length) {
            super(getter);
            this.spacer = new Spacer(length);
        }

        public String getString() {
            String s = super.getString();
            if (s == null) {
                return null;
            }
            return spacer.padRight(s);
        }
    }

    /**
     * Accessor that assumes that the underlying value is an array of
     * {@code byte} values;
     * corresponds to {@link java.sql.Types#BINARY} and {@link java.sql.Types#VARBINARY}.
     */
    private static class BinaryAccessor extends AccessorImpl {
        public BinaryAccessor(Getter getter) {
            super(getter);
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

    /**
     * Accessor that assumes that the underlying value is a DATE, TIME or
     * TIMESTAMP value;
     * corresponds to {@link java.sql.Types#DATE},
     * {@link java.sql.Types#TIME} and
     * {@link java.sql.Types#TIMESTAMP}.
     */
    private static class DateTimeAccessor extends AccessorImpl {
        public DateTimeAccessor(Getter getter) {
            super(getter);
        }

        @Override
        public String getString() {
            final Object o = getObject();
            return o == null ? null : o.toString();
        }
    }

    /**
     * Accessor that assumes that the underlying value is an OBJECT;
     * corresponds to {@link java.sql.Types#JAVA_OBJECT}.
     */
    private static class ObjectAccessor extends AccessorImpl {
        public ObjectAccessor(Getter getter) {
            super(getter);
        }
    }

    protected interface Getter {
        Object getObject();

        boolean wasNull();
    }
}

// End AbstractCursor.java
