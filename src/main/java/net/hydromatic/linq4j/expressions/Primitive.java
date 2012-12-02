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
package net.hydromatic.linq4j.expressions;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Enumeration of Java's primitive types.
 *
 * <p>There are fields for the native class (e.g. <code>int</code>, also
 * known as {@link Integer#TYPE}) and the boxing class
 * (e.g. {@link Integer}).</p>
*/
public enum Primitive {
    BOOLEAN(Boolean.TYPE, Boolean.class, true, Boolean.FALSE, Boolean.TRUE),
    BYTE(Byte.TYPE, Byte.class, true, Byte.MIN_VALUE, Byte.MAX_VALUE),
    CHARACTER(Character.TYPE, Character.class, true, Character.MIN_VALUE,
        Character.MAX_VALUE),
    SHORT(Short.TYPE, Short.class, true, Short.MIN_VALUE, Short.MAX_VALUE),
    INT(Integer.TYPE, Integer.class, true, Short.MIN_VALUE, Short.MAX_VALUE),
    LONG(Long.TYPE, Long.class, true, Long.MIN_VALUE, Long.MAX_VALUE),
    FLOAT(Float.TYPE, Float.class, false, Float.MIN_VALUE, Float.MAX_VALUE),
    DOUBLE(Double.TYPE, Double.class, false, Double.MIN_VALUE,
        Double.MAX_VALUE),
    VOID(Void.TYPE, Void.class, false, null, null),
    OTHER(null, null, false, null, null);

    public final Class primitiveClass;
    public final Class boxClass;
    public final String primitiveName; // e.g. "int"
    public final boolean fixed;
    public final Object min;
    public final Object max;

    private static final Map<Class, Primitive> PRIMITIVE_MAP =
        new HashMap<Class, Primitive>();
    private static final Map<Class, Primitive> BOX_MAP =
        new HashMap<Class, Primitive>();

    static {
        Primitive[] values = Primitive.values();
        for (Primitive value : values) {
            if (value.primitiveClass != null) {
                PRIMITIVE_MAP.put(value.primitiveClass, value);
            }
            if (value.boxClass != null) {
                BOX_MAP.put(value.boxClass, value);
            }
        }
    }

    Primitive(
        Class primitiveClass,
        Class boxClass,
        boolean fixed,
        Object min,
        Object max)
    {
        this.primitiveClass = primitiveClass;
        this.primitiveName =
            primitiveClass != null ? primitiveClass.getSimpleName() : null;
        this.boxClass = boxClass;
        this.fixed = fixed;
        this.min = min;
        this.max = max;
    }

    /** Returns the Primitive object for a given primitive class.
     *
     * <p>For example, <code>of(Long.TYPE)</code> or <code>of(long.class)</code>
     * returns {@link #LONG}. */
    public static Primitive of(Type type) {
        //noinspection SuspiciousMethodCalls
        return PRIMITIVE_MAP.get(type);
    }

    /** Returns the Primitive object for a given boxing class.
     *
     * <p>For example, <code>ofBox(java.util.Long.class)</code>
     * returns {@link #LONG}. */
    public static Primitive ofBox(Type type) {
        //noinspection SuspiciousMethodCalls
        return BOX_MAP.get(type);
    }

    /**
     * Converts a collection of boxed primitives into an array of primitives.
     *
     * @param collection Collection of boxed primitives
     * @return array of primitives
     *
     * @throws ClassCastException if any element is not of the box type
     * @throws NullPointerException if any element is null
     */
    @SuppressWarnings("unchecked")
    public Object toArray(Collection collection) {
        int i = 0;
        switch (this) {
        case DOUBLE:
            double[] doubles = new double[collection.size()];
            for (double _double : (Collection<Double>) collection) {
                doubles[i++] = _double;
            }
            return doubles;
        case FLOAT:
            float[] floats = new float[collection.size()];
            for (float _float : (Collection<Float>) collection) {
                floats[i++] = _float;
            }
            return floats;
        case INT:
            int[] ints = new int[collection.size()];
            for (int _int : (Collection<Integer>) collection) {
                ints[i++] = _int;
            }
            return ints;
        case LONG:
            long[] longs = new long[collection.size()];
            for (long _long : (Collection<Long>) collection) {
                longs[i++] = _long;
            }
            return longs;
        case SHORT:
            short[] shorts = new short[collection.size()];
            for (short _short : (Collection<Short>) collection) {
                shorts[i++] = _short;
            }
            return shorts;
        case BOOLEAN:
            boolean[] booleans = new boolean[collection.size()];
            for (boolean _boolean : (Collection<Boolean>) collection) {
                booleans[i++] = _boolean;
            }
            return booleans;
        case BYTE:
            byte[] bytes = new byte[collection.size()];
            for (byte _byte : (Collection<Byte>) collection) {
                bytes[i++] = _byte;
            }
            return bytes;
        case CHARACTER:
            char[] chars = new char[collection.size()];
            for (char _char : (Collection<Character>) collection) {
                chars[i++] = _char;
            }
            return chars;
        default:
            throw new RuntimeException("unexpected: " + this);
        }
    }

    /** Converts a collection of {@link Number} to a primitive array. */
    public Object toArray2(Collection<Number> collection) {
        int i = 0;
        switch (this) {
        case DOUBLE:
            double[] doubles = new double[collection.size()];
            for (Number number : collection) {
                doubles[i++] = number.doubleValue();
            }
            return doubles;
        case FLOAT:
            float[] floats = new float[collection.size()];
            for (Number number : collection) {
                floats[i++] = number.floatValue();
            }
            return floats;
        case INT:
            int[] ints = new int[collection.size()];
            for (Number number : collection) {
                ints[i++] = number.intValue();
            }
            return ints;
        case LONG:
            long[] longs = new long[collection.size()];
            for (Number number : collection) {
                longs[i++] = number.longValue();
            }
            return longs;
        case SHORT:
            short[] shorts = new short[collection.size()];
            for (Number number : collection) {
                shorts[i++] = number.shortValue();
            }
            return shorts;
        case BOOLEAN:
            boolean[] booleans = new boolean[collection.size()];
            for (Number number : collection) {
                booleans[i++] = number.byteValue() != 0;
            }
            return booleans;
        case BYTE:
            byte[] bytes = new byte[collection.size()];
            for (Number number : collection) {
                bytes[i++] = number.byteValue();
            }
            return bytes;
        case CHARACTER:
            char[] chars = new char[collection.size()];
            for (Number number : collection) {
                chars[i++] = (char) number.shortValue();
            }
            return chars;
        default:
            throw new RuntimeException("unexpected: " + this);
        }
    }
}

// End Primitive.java
