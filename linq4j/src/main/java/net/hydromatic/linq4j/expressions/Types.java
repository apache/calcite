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
 * @see Primitive
 */
public abstract class Types {
  private Types() {}

  /**
   * Creates a type with generic parameters.
   */
  public static Type of(Type type, Type... typeArguments) {
    if (typeArguments.length == 0) {
      return type;
    }
    return new ParameterizedTypeImpl(type, toList(typeArguments), null);
  }

  /**
   * Returns the element type of a {@link Collection}, {@link Iterable}
   * (including {@link net.hydromatic.linq4j.Queryable Queryable} and
   * {@link net.hydromatic.linq4j.Enumerable Enumerable}), {@link Iterator},
   * {@link Enumerator}, or an array.
   *
   * <p>Returns null if the type is not one of these.</p>
   */
  public static Type getElementType(Type type) {
    if (type instanceof ArrayType) {
      return ((ArrayType) type).getComponentType();
    }
    if (type instanceof GenericArrayType) {
      return ((GenericArrayType) type).getGenericComponentType();
    }
    Class clazz = toClass(type);
    if (clazz.isArray()) {
      return clazz.getComponentType();
    }
    if (Collection.class.isAssignableFrom(clazz)
        || Iterable.class.isAssignableFrom(clazz)
        || Iterator.class.isAssignableFrom(clazz)
        || Enumerator.class.isAssignableFrom(clazz)) {
      if (type instanceof ParameterizedType) {
        return ((ParameterizedType) type).getActualTypeArguments()[0];
      }
      return Object.class;
    }
    return null;
  }

  /**
   * Returns a list backed by a copy of an array. The contents of the list
   * will not change even if the contents of the array are subsequently
   * modified.
   */
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

  static Field getField(String fieldName, Class clazz) {
    try {
      return clazz.getField(fieldName);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(
          "Unknown field '" + fieldName + "' in class " + clazz, e);
    }
  }

  static PseudoField getField(String fieldName, Type type) {
    if (type instanceof RecordType) {
      return getRecordField(fieldName, (RecordType) type);
    } else if (type instanceof Class && ((Class) type).isArray()) {
      return getSystemField(fieldName, (Class) type);
    } else {
      return field(getField(fieldName, toClass(type)));
    }
  }

  private static RecordField getRecordField(String fieldName, RecordType type) {
    for (RecordField field : type.getRecordFields()) {
      if (field.getName().equals(fieldName)) {
        return field;
      }
    }
    throw new RuntimeException(
        "Unknown field '" + fieldName + "' in type " + type);
  }

  private static RecordField getSystemField(final String fieldName,
      final Class clazz) {
    // The "length" field of an array does not appear in Class.getFields().
    return new ArrayLengthRecordField(fieldName, clazz);
  }

  public static Class toClass(Type type) {
    if (type instanceof Class) {
      return (Class) type;
    }
    if (type instanceof ParameterizedType) {
      return toClass(((ParameterizedType) type).getRawType());
    }
    if (type instanceof TypeVariable) {
      TypeVariable typeVariable = (TypeVariable) type;
      return toClass(typeVariable.getBounds()[0]);
    }
    throw new RuntimeException("unsupported type " + type); // TODO:
  }

  static Class[] toClassArray(Collection<Type> types) {
    List<Class> classes = new ArrayList<Class>();
    for (Type type : types) {
      classes.add(toClass(type));
    }
    return classes.toArray(new Class[classes.size()]);
  }

  static Class[] toClassArray(Iterable<? extends Expression> arguments) {
    List<Class> classes = new ArrayList<Class>();
    for (Expression argument : arguments) {
      classes.add(toClass(argument.getType()));
    }
    return classes.toArray(new Class[classes.size()]);
  }

  /**
   * Returns the component type of an array.
   */
  public static Type getComponentType(Type type) {
    if (type instanceof Class) {
      return ((Class) type).getComponentType();
    }
    if (type instanceof ArrayType) {
      return ((ArrayType) type).getComponentType();
    }
    if (type instanceof GenericArrayType) {
      return ((GenericArrayType) type).getGenericComponentType();
    }
    if (type instanceof ParameterizedType) {
      return getComponentType(((ParameterizedType) type).getRawType());
    }
    if (type instanceof TypeVariable) {
      TypeVariable typeVariable = (TypeVariable) type;
      return getComponentType(typeVariable.getBounds()[0]);
    }
    return null; // not an array type
  }

  static Type getComponentTypeN(Type type) {
    for (;;) {
      final Type oldType = type;
      type = getComponentType(type);
      if (type == null) {
        return oldType;
      }
    }
  }

  /**
   * Boxes a type, if it is primitive, and returns the type name.
   * The type is abbreviated if it is in the "java.lang" package.
   *
   * <p>For example,
   * boxClassName(int) returns "Integer";
   * boxClassName(List&lt;String&gt;) returns "List&lt;String&gt;"</p>
   *
   * @param type Type
   *
   * @return Class name
   */
  static String boxClassName(Type type) {
    if (!(type instanceof Class)) {
      return type.toString();
    }
    Primitive primitive = Primitive.of(type);
    if (primitive != null) {
      return primitive.boxClass.getSimpleName();
    } else {
      return className(type);
    }
  }

  public static Type box(Type type) {
    Primitive primitive = Primitive.of(type);
    if (primitive != null) {
      return primitive.boxClass;
    } else {
      return type;
    }
  }

  public static Type unbox(Type type) {
    Primitive primitive = Primitive.ofBox(type);
    if (primitive != null) {
      return primitive.primitiveClass;
    } else {
      return type;
    }
  }

  static String className(Type type) {
    if (type instanceof ArrayType) {
      return className(((ArrayType) type).getComponentType()) + "[]";
    }
    if (!(type instanceof Class)) {
      return type.toString();
    }
    Class clazz = (Class) type;
    if (clazz.isArray()) {
      return className(clazz.getComponentType()) + "[]";
    }
    String className = clazz.getName();
    if (clazz.getPackage() == Package.getPackage("java.lang")
        && !clazz.isPrimitive()) {
      return className.substring("java.lang.".length());
    }
    return className.replace('$', '.');
  }

  public static boolean isAssignableFrom(Type type0, Type type) {
    return toClass(type0).isAssignableFrom(toClass(type));
  }

  public static boolean isArray(Type type) {
    return toClass(type).isArray();
  }

  public static Field nthField(int ordinal, Class clazz) {
    return clazz.getFields()[ordinal];
  }

  public static PseudoField nthField(int ordinal, Type clazz) {
    if (clazz instanceof RecordType) {
      RecordType recordType = (RecordType) clazz;
      return recordType.getRecordFields().get(ordinal);
    }
    return field(toClass(clazz).getFields()[ordinal]);
  }

  static boolean allAssignable(boolean varArgs, Class[] parameterTypes,
      Class[] argumentTypes) {
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
      Class
          parameterType =
          !varArgs || i < parameterTypes.length - 1
              ? parameterTypes[i]
              : Object.class;
      if (!assignableFrom(parameterType, argumentTypes[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns whether a parameter is assignable from an argument by virtue
   * of (a) sub-classing (e.g. Writer is assignable from PrintWriter) and (b)
   * up-casting (e.g. int is assignable from short).
   *
   * @param parameter Parameter type
   * @param argument Argument type
   *
   * @return Whether parameter can be assigned from argument
   */
  private static boolean assignableFrom(Class parameter, Class argument) {
    return parameter.isAssignableFrom(argument)
           || parameter.isPrimitive()
        && argument.isPrimitive()
        && Primitive.of(parameter).assignableFrom(Primitive.of(argument));
  }

  /**
   * Finds a method of a given name that accepts a given set of arguments.
   * Includes in its search inherited methods and methods with wider argument
   * types.
   *
   * @param clazz Class against which method is invoked
   * @param methodName Name of method
   * @param argumentTypes Types of arguments
   *
   * @return A method with the given name that matches the arguments given
   * @throws RuntimeException if method not found
   */
  public static Method lookupMethod(Class clazz, String methodName,
      Class... argumentTypes) {
    try {
      return clazz.getMethod(methodName, argumentTypes);
    } catch (NoSuchMethodException e) {
      for (Method method : clazz.getMethods()) {
        if (method.getName().equals(methodName) && allAssignable(
            method.isVarArgs(), method.getParameterTypes(), argumentTypes)) {
          return method;
        }
      }
      throw new RuntimeException(
          "while resolving method '" + methodName
          + Arrays.toString(argumentTypes)
          + "' in class " + clazz, e);
    }
  }

  /**
   * Finds a constructor of a given class that accepts a given set of
   * arguments. Includes in its search methods with wider argument types.
   *
   * @param type Class against which method is invoked
   * @param argumentTypes Types of arguments
   *
   * @return A method with the given name that matches the arguments given
   * @throws RuntimeException if method not found
   */
  public static Constructor lookupConstructor(Type type,
      Class... argumentTypes) {
    final Class clazz = toClass(type);
    Constructor[] constructors = clazz.getDeclaredConstructors();
    for (Constructor constructor : constructors) {
      if (allAssignable(constructor.isVarArgs(),
          constructor.getParameterTypes(), argumentTypes)) {
        return constructor;
      }
    }
    if (constructors.length == 0 && argumentTypes.length == 0) {
      Constructor[] constructors1 = clazz.getConstructors();
      try {
        return clazz.getConstructor();
      } catch (NoSuchMethodException e) {
        // ignore
      }
    }
    throw new RuntimeException(
        "while resolving constructor in class " + type + " with types " + Arrays
            .toString(argumentTypes));
  }

  public static void discard(Object o) {
    if (false) {
      discard(o);
    }
  }

  /**
   * Returns the most restrictive type that is assignable from all given
   * types.
   */
  static Type gcd(Type... types) {
    // TODO: improve this
    if (types.length == 0) {
      return Object.class;
    }
    Type best = types[0];
    Primitive bestPrimitive = Primitive.of(best);
    if (bestPrimitive != null) {
      for (int i = 1; i < types.length; i++) {
        final Primitive primitive = Primitive.of(types[i]);
        if (primitive == null) {
          return Object.class;
        }
        if (primitive.assignableFrom(bestPrimitive)) {
          bestPrimitive = primitive;
        } else if (bestPrimitive.assignableFrom(primitive)) {
          // ok
        } else if (bestPrimitive == Primitive.CHAR
                   || bestPrimitive == Primitive.BYTE) {
          // 'char' and 'byte' are problematic, because they don't
          // assign to each other. 'char' can't even assign to
          // 'short'. Before we give up, try one last time with 'int'.
          bestPrimitive = Primitive.INT;
          --i;
        } else {
          return Object.class;
        }
      }
      return bestPrimitive.primitiveClass;
    } else {
      for (int i = 1; i < types.length; i++) {
        if (types[i] != types[0]) {
          return Object.class;
        }
      }
    }
    return types[0];
  }

  /**
   * Wraps an expression in a cast if it is not already of the desired type,
   * or cannot be implicitly converted to it.
   *
   * @param returnType Desired type
   * @param expression Expression
   *
   * @return Expression of desired type
   */
  public static Expression castIfNecessary(Type returnType,
      Expression expression) {
    final Type type = expression.getType();
    if (Types.isAssignableFrom(returnType, type)) {
      return expression;
    }
    if (returnType instanceof Class
        && Number.class.isAssignableFrom((Class) returnType)
        && type instanceof Class
        && Number.class.isAssignableFrom((Class) type)) {
      // E.g.
      //   Integer foo(BigDecimal o) {
      //     return o.intValue();
      //   }
      return Expressions.unbox(expression, Primitive.ofBox(returnType));
    }
    if (Primitive.is(returnType) && !Primitive.is(type)) {
      // E.g.
      //   int foo(Object o) {
      //     return ((Integer) o).intValue();
      //   }
      return Expressions.unbox(
          Expressions.convert_(expression, Types.box(returnType)),
          Primitive.of(returnType));
    }
    if (!Primitive.is(returnType) && Primitive.is(type)) {
      // E.g.
      //   Short foo(Object o) {
      //     return (short) (int) o;
      //   }
      return Expressions.convert_(expression,
          Types.unbox(returnType));
    }
    return Expressions.convert_(expression, returnType);
  }

  public static PseudoField field(final Field field) {
    return new ReflectedPseudoField(field);
  }

  static Class arrayClass(Type type) {
    // REVIEW: Is there a way to do this without creating an instance? We
    //  just need the inverse of Class.getComponentType().
    return Array.newInstance(toClass(type), 0).getClass();
  }

  static Type arrayType(Type type, int dimension) {
    for (int i = 0; i < dimension; i++) {
      type = arrayType(type);
    }
    return type;
  }

  static Type arrayType(Type type) {
    if (type instanceof Class) {
      Class clazz = (Class) type;

      // REVIEW: Is there a way to do this without creating an instance?
      //   We just need the inverse of Class.getComponentType().
      return Array.newInstance(clazz, 0).getClass();
    }
    return new ArrayType(type);
  }

  public static Type stripGenerics(Type type) {
    if (type instanceof GenericArrayType) {
      final Type componentType =
          ((GenericArrayType) type).getGenericComponentType();
      return new ArrayType(stripGenerics(componentType));
    } else if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getRawType();
    } else {
      return type;
    }
  }

  /** Implementation of {@link ParameterizedType}. */
  static class ParameterizedTypeImpl implements ParameterizedType {
    private final Type rawType;
    private final List<Type> typeArguments;
    private final Type ownerType;

    ParameterizedTypeImpl(Type rawType, List<Type> typeArguments,
        Type ownerType) {
      super();
      this.rawType = rawType;
      this.typeArguments = typeArguments;
      this.ownerType = ownerType;
      assert rawType != null;
      for (Type typeArgument : typeArguments) {
        assert typeArgument != null;
      }
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

  /**
   * Base class for record-like types that do not mapped to (currently
   * loaded) Java {@link Class} objects. Gives the opportunity to generate
   * code that references temporary types, then generate classes for those
   * types along with the code that uses them.
   */
  public interface RecordType extends Type {
    List<RecordField> getRecordFields();

    String getName();
  }

  /**
   * Field that belongs to a record.
   */
  public interface RecordField extends PseudoField {
    boolean nullable();
  }

  /**
   * Array type.
   */
  public static class ArrayType implements Type {
    private final Type componentType;

    public ArrayType(Type componentType) {
      this.componentType = componentType;
    }

    public Type getComponentType() {
      return componentType;
    }
  }
}

// End Types.java
