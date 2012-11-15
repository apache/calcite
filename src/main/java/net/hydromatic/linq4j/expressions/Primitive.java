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
import java.util.HashMap;
import java.util.Map;

/**
 * Enumeration of Java's primitive types.
 *
 * <p>There are fields for the native class (e.g. <code>int</code>, also
 * known as {@link Integer#TYPE}) and the boxing class
 * (e.g. {@link Integer}).</p>
*/
public enum Primitive {
    BOOLEAN(Boolean.TYPE, Boolean.class),
    BYTE(Byte.TYPE, Byte.class),
    CHARACTER(Character.TYPE, Character.class),
    SHORT(Short.TYPE, Short.class),
    INT(Integer.TYPE, Integer.class),
    LONG(Long.TYPE, Long.class),
    FLOAT(Float.TYPE, Float.class),
    DOUBLE(Double.TYPE, Double.class),
    VOID(Void.TYPE, Void.class),
    OTHER(null, null);

    public final Class primitiveClass;
    public final Class boxClass;
    public final String primitiveName; // e.g. "int"

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

    Primitive(Class primitiveClass, Class boxClass) {
        this.primitiveClass = primitiveClass;
        this.primitiveName =
            primitiveClass != null ? primitiveClass.getSimpleName() : null;
        this.boxClass = boxClass;
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
}

// End Primitive.java
