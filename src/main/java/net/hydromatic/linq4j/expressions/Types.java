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

import net.hydromatic.linq4j.Enumerator;

import java.lang.reflect.*;
import java.util.*;

/**
 * Utilities for converting between {@link Expression}, {@link Type} and
 * {@link Class}.
 *
 * @author jhyde
 */
public class Types {
    static final Map<Class, String> PRIMITIVES =
        new HashMap<Class, String>();

    static {
        PRIMITIVES.put(Boolean.TYPE, "Boolean");
        PRIMITIVES.put(Byte.TYPE, "Byte");
        PRIMITIVES.put(Character.TYPE, "Character");
        PRIMITIVES.put(Short.TYPE, "Short");
        PRIMITIVES.put(Integer.TYPE, "Integer");
        PRIMITIVES.put(Long.TYPE, "Long");
        PRIMITIVES.put(Float.TYPE, "Float");
        PRIMITIVES.put(Double.TYPE, "Double");
        PRIMITIVES.put(Void.TYPE, "Void");
    }

    /** Creates a type with generic parameters. */
    public static Type of(Type type, Type... typeArguments) {
        if (typeArguments.length == 0) {
            return type;
        }
        return new ParameterizedTypeImpl(type, toList(typeArguments), null);
    }

    /** Returns the component type of a {@link Collection}, {@link Iterable}
     * (including {@link net.hydromatic.linq4j.Queryable Queryable} and
     * {@link net.hydromatic.linq4j.Enumerable Enumerable}), {@link Iterator},
     * {@link Enumerator}, or an array.
     *
     * <p>Returns null if the type is not one of these.</p> */
    public static Type getComponentType(Type type) {
        Class clazz = toClass(type);
        if (clazz.isArray()) {
            return clazz.getComponentType();
        }
        if (Collection.class.isAssignableFrom(clazz)
            || Iterable.class.isAssignableFrom(clazz)
            || Iterator.class.isAssignableFrom(clazz)
            || Enumerator.class.isAssignableFrom(clazz))
        {
            if (type instanceof ParameterizedType) {
                return ((ParameterizedType) type).getActualTypeArguments()[0];
            }
            return Object.class;
        }
        return null;
    }

    /** Returns a list backed by a copy of an array. The contents of the list
     * will not change even if the contents of the array are subsequently
     * modified. */
    private static <T> List<T> toList(T[] ts) {
        switch (ts.length) {
        case 0:
            return Collections.emptyList();
        case 1:
            return Collections.singletonList(ts[0]);
        default:
            return Arrays.asList(ts.clone());
        }
    }

    static Field getField(String fieldName, Type type) {
        Field field;
        try {
            field = toClass(type).getField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(
                "while resolving field '" + fieldName + "' in class "
                + type,
                e);
        }
        return field;
    }

    public static Class toClass(Type type) {
        if (type instanceof Class) {
            return (Class) type;
        }
        if (type instanceof ParameterizedType) {
            return toClass(((ParameterizedType) type).getRawType());
        }
        throw new RuntimeException("unsupported type type " + type); // TODO:
    }

    static Class[] toClassArray(Collection<Type> types) {
        Class[] classes = new Class[types.size()];
        int i = 0;
        for (Iterator<Type> iterator = types.iterator(); iterator.hasNext();) {
            Type type = iterator.next();
            classes[i++] = toClass(type);
        }
        return classes;
    }

    static Class[] toClassArray(Iterable<Expression> arguments) {
        List<Class> classes = new ArrayList<Class>();
        for (Expression argument : arguments) {
            classes.add(toClass(argument.getType()));
        }
        return classes.toArray(new Class[classes.size()]);
    }

    static String boxClassName(Type type) {
        if (!(type instanceof Class)) {
            return type.toString();
        }
        Class clazz = (Class) type;
        if (clazz.isPrimitive()) {
            return PRIMITIVES.get(clazz);
        } else {
            return className(clazz);
        }
    }

    static String className(Type type) {
        if (!(type instanceof Class)) {
            return type.toString();
        }
        Class clazz = (Class) type;
        if (clazz.isArray()) {
            return className(clazz.getComponentType()) + "[]";
        }
        String className = clazz.getName();
        if (clazz.getPackage() == Package.getPackage("java.lang")
            && !clazz.isPrimitive())
        {
            return className.substring("java.lang.".length());
        }
        return className;
    }

    public static boolean isAssignableFrom(Class clazz, Type type) {
        return clazz.isAssignableFrom(toClass(type));
    }

    public static boolean isArray(Type type) {
        return toClass(type).isArray();
    }

    public static Field nthField(int ordinal, Type clazz) {
        return Types.toClass(clazz).getFields()[ordinal];
    }

    static boolean allAssignable(
        boolean varArgs, Class[] parameterTypes, Class[] argumentTypes)
    {
        if (varArgs) {
            if (argumentTypes.length < parameterTypes.length - 1) {
                return false;
            }
        } else {
            if (parameterTypes.length != argumentTypes.length) {
                return false;
            }
        }
        for (int i = 0; i < argumentTypes.length; i++) {
            Class parameterType =
                !varArgs || i < parameterTypes.length - 1
                    ? parameterTypes[i]
                    : Object.class;
            if (!parameterType.isAssignableFrom(argumentTypes[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Finds a method of a given name that accepts a given set of arguments.
     * Includes in its search inherited methods and methods with wider argument
     * types.
     *
     * @param clazz Class against which method is invoked
     * @param methodName Name of method
     * @param argumentTypes Types of arguments
     * @return A method with the given name that matches the arguments given
     *
     * @throws RuntimeException if method not found
     */
    public static Method lookupMethod(
        Class clazz, String methodName, Class... argumentTypes)
    {
        try {
            return clazz.getMethod(methodName, argumentTypes);
        } catch (NoSuchMethodException e) {
            for (Method method : clazz.getMethods()) {
                if (method.getName().equals(methodName)
                    && allAssignable(
                        method.isVarArgs(),
                        method.getParameterTypes(),
                        argumentTypes))
                {
                    return method;
                }
            }
            throw new RuntimeException(
                "while resolving static method '" + methodName + "' in class "
                + clazz,
                e);
        }
    }

    /**
     * Finds a constructor of a given class that accepts a given set of
     * arguments. Includes in its search methods with wider argument types.
     *
     * @param type Class against which method is invoked
     * @param argumentTypes Types of arguments
     * @return A method with the given name that matches the arguments given
     *
     * @throws RuntimeException if method not found
     */
    public static Constructor lookupConstructor(
        Type type, Class... argumentTypes)
    {
        final Class clazz = toClass(type);
        for (Constructor constructor : clazz.getConstructors()) {
            if (allAssignable(
                constructor.isVarArgs(),
                constructor.getParameterTypes(),
                argumentTypes))
            {
                return constructor;
            }
        }
        throw new RuntimeException(
            "while resolving constructor in class " + type + " with types "
            + Arrays.toString(argumentTypes));
    }

    static class ParameterizedTypeImpl implements ParameterizedType {
        private final Type rawType;
        private final List<Type> typeArguments;
        private final Type ownerType;

        ParameterizedTypeImpl(
            Type rawType, List<Type> typeArguments, Type ownerType)
        {
            super();
            this.rawType = rawType;
            this.typeArguments = typeArguments;
            this.ownerType = ownerType;
        }

        @Override
        public String toString() {
            final StringBuilder buf = new StringBuilder();
            buf.append(className(rawType));
            buf.append("<");
            int i = 0;
            for (Type typeArgument : typeArguments) {
                if (i++ > 0) {
                    buf.append(", ");
                }
                buf.append(className(typeArgument));
            }
            buf.append(">");
            return buf.toString();
        }

        public Type[] getActualTypeArguments() {
            return typeArguments.toArray(new Type[typeArguments.size()]);
        }

        public Type getRawType() {
            return rawType;
        }

        public Type getOwnerType() {
            return ownerType;
        }
    }
}

// End Types.java
