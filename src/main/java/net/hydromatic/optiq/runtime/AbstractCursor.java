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
import java.sql.Date;
import java.util.*;

/**
 * Base class for implementing a cursor.
 *
 * <p>Derived class needs to provide {@link Getter} and can override
 * {@link Accessor} implementations if it wishes.</p>
 *
 * @author jhyde
 */
public abstract class AbstractCursor implements Cursor {
    private static final int MILLIS_PER_DAY = 86400000;
    private static final Calendar LOCAL_CALENDAR = Calendar.getInstance();

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
        case Types.REAL:
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
            switch (Rep.of(type.internalClass)) {
            case PRIMITIVE_INT:
                return new DateFromIntAccessor(getter);
            case JAVA_SQL_DATE:
                return new DateAccessor(getter);
            default:
                throw new AssertionError("bad " + type.internalClass);
            }
        case Types.TIME:
            switch (Rep.of(type.internalClass)) {
            case PRIMITIVE_INT:
                return new TimeFromIntAccessor(getter);
            case JAVA_SQL_TIME:
                return new TimeAccessor(getter);
            default:
                throw new AssertionError("bad " + type.internalClass);
            }
        case Types.TIMESTAMP:
            switch (Rep.of(type.internalClass)) {
            case PRIMITIVE_LONG:
                return new TimestampFromLongAccessor(getter);
            case JAVA_SQL_TIMESTAMP:
                return new TimestampAccessor(getter);
            case JAVA_UTIL_DATE:
                return new TimestampFromUtilDateAccessor(getter);
            default:
                throw new AssertionError("bad " + type.internalClass);
            }
        case Types.JAVA_OBJECT:
        case Types.ARRAY:
        case Types.STRUCT:
        case Types.OTHER: // e.g. map
            return new ObjectAccessor(getter);
        default:
            throw new RuntimeException("unknown type " + type.type);
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
            final Object o = getObject();
            return o == null ? null : o.toString();
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
            return getDate(null);
        }

        public Time getTime() {
            return getTime(null);
        }

        public Timestamp getTimestamp() {
            return getTimestamp(null);
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
            throw cannotConvert("Date");
        }

        public Time getTime(Calendar cal) {
            throw cannotConvert("Time");
        }

        public Timestamp getTimestamp(Calendar cal) {
            throw cannotConvert("Timestamp");
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
            Integer o = (Integer) super.getObject();
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
            Long o = (Long) super.getObject();
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
            return number != null && number.doubleValue() != 0;
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
     * Accessor that assumes that the underlying value is a DATE,
     * in its default representation {@code int};
     * corresponds to {@link java.sql.Types#DATE}.
     */
    private static class DateFromIntAccessor extends IntAccessor {
        public DateFromIntAccessor(Getter getter) {
            super(getter);
        }

        @Override
        public Object getObject() {
            return getDate();
        }

        @Override
        public Date getDate(Calendar calendar) {
            int vv = getInt();
            if (vv == 0 && getter.wasNull()) {
                return null;
            }
            long v = (long) vv * MILLIS_PER_DAY;
            if (calendar != null) {
                v -= calendar.getTimeZone().getOffset(v);
            }
            return new java.sql.Date(v);
        }

        @Override
        public String getString() {
            return getDate(LOCAL_CALENDAR).toString();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a Time,
     * in its default representation {@code int};
     * corresponds to {@link java.sql.Types#TIME}.
     */
    private static class TimeFromIntAccessor extends IntAccessor {
        public TimeFromIntAccessor(Getter getter) {
            super(getter);
        }

        @Override
        public Object getObject() {
            return getTime();
        }

        @Override
        public Time getTime(Calendar calendar) {
            int v = getInt();
            if (v == 0 && getter.wasNull()) {
                return null;
            }
            if (calendar != null) {
                v -= calendar.getTimeZone().getOffset(v);
            }
            return new Time(v);
        }

        @Override
        public String getString() {
            return getTime(LOCAL_CALENDAR).toString();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a TIMESTAMP,
     * in its default representation {@code long};
     * corresponds to {@link java.sql.Types#TIMESTAMP}.
     */
    private static class TimestampFromLongAccessor extends LongAccessor {
        public TimestampFromLongAccessor(Getter getter) {
            super(getter);
        }

        @Override
        public Object getObject() {
            return getTimestamp();
        }

        @Override
        public Timestamp getTimestamp(Calendar calendar) {
            long v = getLong();
            if (v == 0L && getter.wasNull()) {
                return null;
            }
            if (calendar != null) {
                v -= calendar.getTimeZone().getOffset(v);
            }
            return new Timestamp(v);
        }

        @Override
        public String getString() {
            return getTimestamp(LOCAL_CALENDAR).toString();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a DATE,
     * represented as a java.sql.Date;
     * corresponds to {@link java.sql.Types#DATE}.
     */
    private static class DateAccessor extends ObjectAccessor {
        public DateAccessor(Getter getter) {
            super(getter);
        }

        @Override
        public Date getDate(Calendar calendar) {
            java.sql.Date date  = (Date) getObject();
            if (date == null) {
                return null;
            }
            if (calendar != null) {
                long v = date.getTime();
                v -= calendar.getTimeZone().getOffset(v);
                date = new Date(v);
            }
            return date;
        }

        @Override
        public String getString() {
            return getDate(LOCAL_CALENDAR).toString();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a TIME,
     * represented as a java.sql.Time;
     * corresponds to {@link java.sql.Types#TIME}.
     */
    private static class TimeAccessor extends ObjectAccessor {
        public TimeAccessor(Getter getter) {
            super(getter);
        }

        @Override
        public Time getTime(Calendar calendar) {
            Time date  = (Time) getObject();
            if (date == null) {
                return null;
            }
            if (calendar != null) {
                long v = date.getTime();
                v -= calendar.getTimeZone().getOffset(v);
                date = new Time(v);
            }
            return date;
        }

        @Override
        public String getString() {
            return getTime(LOCAL_CALENDAR).toString();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a TIMESTAMP,
     * represented as a java.sql.Timestamp;
     * corresponds to {@link java.sql.Types#TIMESTAMP}.
     */
    private static class TimestampAccessor extends ObjectAccessor {
        public TimestampAccessor(Getter getter) {
            super(getter);
        }

        @Override
        public Timestamp getTimestamp(Calendar calendar) {
            Timestamp date  = (Timestamp) getObject();
            if (date == null) {
                return null;
            }
            if (calendar != null) {
                long v = date.getTime();
                v -= calendar.getTimeZone().getOffset(v);
                date = new Timestamp(v);
            }
            return date;
        }

        @Override
        public String getString() {
            return getTimestamp(LOCAL_CALENDAR).toString();
        }
    }

    /**
     * Accessor that assumes that the underlying value is a TIMESTAMP,
     * represented as a java.util.Date;
     * corresponds to {@link java.sql.Types#TIMESTAMP}.
     */
    private static class TimestampFromUtilDateAccessor extends ObjectAccessor {
        public TimestampFromUtilDateAccessor(Getter getter) {
            super(getter);
        }

        @Override
        public Timestamp getTimestamp(Calendar calendar) {
            java.util.Date date  = (java.util.Date) getObject();
            if (date == null) {
                return null;
            }
            long v = date.getTime();
            if (calendar != null) {
                v -= calendar.getTimeZone().getOffset(v);
            }
            return new Timestamp(v);
        }

        @Override
        public String getString() {
            return getTimestamp(LOCAL_CALENDAR).toString();
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

    enum Rep {
        PRIMITIVE_BOOLEAN(boolean.class),
        PRIMITIVE_BYTE(byte.class),
        PRIMITIVE_CHAR(char.class),
        PRIMITIVE_SHORT(short.class),
        PRIMITIVE_INT(int.class),
        PRIMITIVE_LONG(long.class),
        PRIMITIVE_FLOAT(float.class),
        PRIMITIVE_DOUBLE(double.class),
        BOOLEAN(Boolean.class),
        BYTE(Byte.class),
        CHARACTER(Character.class),
        SHORT(Short.class),
        INTEGER(Integer.class),
        LONG(Long.class),
        FLOAT(Float.class),
        DOUBLE(Double.class),
        JAVA_SQL_TIME(Time.class),
        JAVA_SQL_TIMESTAMP(Timestamp.class),
        JAVA_SQL_DATE(java.sql.Date.class),
        JAVA_UTIL_DATE(java.util.Date.class);

        private final Class clazz;

        Rep(Class clazz) {
            this.clazz = clazz;
        }

        static Rep[] values = values();

        static Rep of(Class clazz) {
            for (Rep rep : values) {
                if (rep.clazz == clazz) {
                    return rep;
                }
            }
            throw new IllegalArgumentException("no Rep for " + clazz);
        }
    }
}

// End AbstractCursor.java
