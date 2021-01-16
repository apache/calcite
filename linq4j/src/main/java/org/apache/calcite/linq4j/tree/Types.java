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
package org.apache.calcite.linq4j.tree;

import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerator;

import com.google.common.primitives.Primitives;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Utilities for converting between {@link Expression}, {@link Type} and
 * {@link Class}.
 *
 * @see Primitive
 */
public abstract class Types {
  private static final FieldsOrdering FALLBACK_FIELDS_ORDERING =
      FieldsOrdering.ALPHABETICAL_AND_HIERARCHY;

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
   * (including {@link org.apache.calcite.linq4j.Queryable Queryable} and
   * {@link org.apache.calcite.linq4j.Enumerable Enumerable}), {@link Iterator},
   * {@link Enumerator}, or an array.
   *
   * <p>Returns null if the type is not one of these.</p>
   */
  public static @Nullable Type getElementType(Type type) {
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

  /** Various strategies to order fields in the derived SQL type(s). */
  public enum FieldsOrdering {
    JVM,
    ALPHABETICAL,
    ALPHABETICAL_AND_HIERARCHY,
    CONSTRUCTOR,
    EXPLICIT,
    EXPLICIT_TOLERANT
  }

  /**
   * Returns the fields of a given class.
   * @param type the target class
   * @return the list of fields of a given class.
   */
  public static List<Field> getClassFields(Class type) {
    return getClassFields(type, true);
  }

  /**
   * Returns the fields of a given class.
   * @param type the target class
   * @param useHierarchy true if the inherited fields are considered, false otherwise
   * @return the list of fields of a given class.
   */
  public static List<Field> getClassFields(Class type, boolean useHierarchy) {
    return getClassFields(type, useHierarchy, FieldsOrdering.JVM, null);
  }

  /**
   * Returns the fields of a given class.
   * @param type the target class
   * @param useHierarchy true if the inherited fields are considered, false otherwise
   * @param fieldsOrdering the strategy for the fields ordering
   * @param classFieldsMap a mapping from class to fields
   * @return the list of fields of a given class.
   */
  public static List<Field> getClassFields(Class type, boolean useHierarchy,
      FieldsOrdering fieldsOrdering, @Nullable Map<Class, List<Field>> classFieldsMap) {
    if (type.equals(MetaImpl.MetaTable.class)) {
      return Arrays.asList(type.getFields());
    }

    switch (fieldsOrdering) {
    case JVM:
      return _getClassFields(useHierarchy ? type.getFields() : type.getDeclaredFields());
    case ALPHABETICAL:
      List<Field> fieldList =
          _getClassFields(useHierarchy ? type.getFields() : type.getDeclaredFields());
      fieldList.sort(Comparator.comparing(Field::getName));
      return fieldList;
    case ALPHABETICAL_AND_HIERARCHY:
      return Types.getCandidateClasses(type, useHierarchy).stream()
          .map(
              t -> Types._getClassFields(t.getDeclaredFields(),
              Comparator.comparing(Field::getName)))
          .flatMap(Collection::stream)
          .collect(Collectors.toList());
    case CONSTRUCTOR:
      return getFieldsFromConstructors(type, useHierarchy);
    case EXPLICIT:
    case EXPLICIT_TOLERANT:
      if (classFieldsMap == null) {
        throw new IllegalStateException("classFieldsMap cannot be null with field ordering \""
            + fieldsOrdering.name() + "\"");
      }

      List<Field> classFields = classFieldsMap.get(type);
      if (classFields == null) {
        throw new IllegalArgumentException("Missing fields declaration for type \"" + type
            + "\" with field ordering \"" + fieldsOrdering.name() + "\"");
      }

      if (fieldsOrdering == Types.FieldsOrdering.EXPLICIT) {
        checkFields(type, classFieldsMap, new HashSet<>());
      }

      return classFields;
    default:
      throw new IllegalArgumentException("Unknown fields ordering: " + fieldsOrdering);
    }
  }

  private static List<Class> getCandidateClasses(Class cls, boolean useHierarchy) {
    final List<Class> list = new ArrayList<>();

    if (!useHierarchy) {
      list.add(cls);
    } else {
      Class clazz = cls;
      while (clazz != null && clazz != Object.class) {
        list.add(0, clazz);
        clazz = clazz.getSuperclass();
      }
    }

    return list;
  }

  private static List<Field> _getClassFields(Field[] fields) {
    // no-op comparator
    return _getClassFields(fields, (f1, f2) -> 0);
  }

  private static List<Field> _getClassFields(Field[] fields, Comparator<Field> comparator) {
    return Arrays.stream(fields)
        .filter(f -> Modifier.isPublic(f.getModifiers()))
        .filter(f -> !Modifier.isStatic(f.getModifiers()))
        .sorted(comparator)
        .collect(Collectors.toList());
  }

  private static void checkFields(
      Class type, Map<Class, List<Field>> classFieldsMap, Set<Class> visitedClasses) {
    if (visitedClasses.contains(type)) {
      return;
    }

    if (!classFieldsMap.containsKey(type)) {
      throw new IllegalArgumentException("No list of fields for " + type);
    }

    List<Field> allClassFields = Types.getClassFields(type);
    List<Field> classFields = classFieldsMap.get(type);
    if (!classFields.containsAll(allClassFields)
        || !allClassFields.containsAll(classFields)) {
      List<Field> missingFields = new ArrayList<>(allClassFields);
      missingFields.removeAll(classFields);
      throw new IllegalArgumentException(
          "Incomplete list of fields is not compatible with \""
              + Types.FieldsOrdering.EXPLICIT.name()
              + "\" field ordering, provided \"" + classFields
              + "\",\nwhile the full list of class fields is \"" + allClassFields
              + "\",\nmissing: " + missingFields);
    }

    visitedClasses.add(type);

    for (Field f : allClassFields) {
      Class fieldType = f.getType();
      if (fieldType.isArray()) {
        checkFields(fieldType.getComponentType(), classFieldsMap, visitedClasses);
      } else if (fieldType == Collection.class) {
        checkFields(fieldType.getTypeParameters()[0].getClass(), classFieldsMap, visitedClasses);
      } else if (fieldType == Map.class) {
        TypeVariable[] typeVariable = fieldType.getTypeParameters();
        checkFields(typeVariable[0].getClass(), classFieldsMap, visitedClasses);
        checkFields(typeVariable[1].getClass(), classFieldsMap, visitedClasses);
      } else if ((!fieldType.isPrimitive()
          && !Primitives.isWrapperType(fieldType)
          && !fieldType.equals(String.class))
          && !fieldType.isInterface()) {
        checkFields(fieldType, classFieldsMap, visitedClasses);
      }
    }
  }

  private static List<Field> getFieldsFromConstructors(Class type, boolean useHierarchy) {
    Set<Field> allFieldsSet = new HashSet<>(
        Types.getClassFields(type, useHierarchy, FieldsOrdering.JVM, null));
    List<List<Field>> candidatesList = new ArrayList<>();

    for (Constructor<?> constructor: type.getDeclaredConstructors()) {
      List<Field> fields = new ArrayList<>();
      for (Parameter param : constructor.getParameters()) {
        // fail fast and use default ordering, if one param has no name, none has
        if (!param.isNamePresent()) {
          return getClassFields(type, useHierarchy, FALLBACK_FIELDS_ORDERING, null);
        }
        try {
          Field f = type.getField(param.getName());

          if (!allFieldsSet.contains(f)) {
            continue;
          }
          if (f.getType().isAssignableFrom(param.getType())) {
            fields.add(f);
          } else {
            fields.clear();
            break; // try another constructor, if any
          }
        } catch (NoSuchFieldException e) {
          fields.clear();
          break; // try another constructor, if any
        }
      }
      if (!fields.isEmpty()) {
        candidatesList.add(fields);
      }
    }

    final List<Field> fields = candidatesList.stream().max(Comparator.comparingInt(List::size))
        .orElse(getClassFields(type, useHierarchy, FALLBACK_FIELDS_ORDERING, null));

    // if even the "longest" constructor does not cover all fields, keep its ordering and fill
    // in the rest following the "fallback" ordering
    if (!fields.containsAll(allFieldsSet)) {
      List<Field> allFields =
          getClassFields(type, useHierarchy, FALLBACK_FIELDS_ORDERING, null);
      allFields.removeAll(fields);
      fields.addAll(allFields);
    }

    return fields;
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
    List<Class> classes = new ArrayList<>();
    for (Type type : types) {
      classes.add(toClass(type));
    }
    return classes.toArray(new Class[0]);
  }

  public static Class[] toClassArray(Iterable<? extends Expression> arguments) {
    List<Class> classes = new ArrayList<>();
    for (Expression argument : arguments) {
      classes.add(toClass(argument.getType()));
    }
    return classes.toArray(new Class[0]);
  }

  /**
   * Returns the component type of an array.
   */
  public static @Nullable Type getComponentType(Type type) {
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
      Type componentType = getComponentType(type);
      if (componentType == null) {
        return type;
      }
      type = componentType;
    }
  }

  public static Type box(Type type) {
    return Primitive.box(type);
  }

  public static Type unbox(Type type) {
    return Primitive.unbox(type);
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
    if (!clazz.isPrimitive()
        && clazz.getPackage() != null
        && clazz.getPackage().getName().equals("java.lang")) {
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

  public static boolean allAssignable(boolean varArgs, Class[] parameterTypes,
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
  @SuppressWarnings("nullness")
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
      throw new RuntimeException("while resolving method '" + methodName
          + Arrays.toString(argumentTypes) + "' in class " + clazz, e);
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

  public static Field lookupField(Type type, String name) {
    final Class clazz = toClass(type);
    try {
      return clazz.getField(name);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("while resolving field in class " + type);
    }
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
      return requireNonNull(bestPrimitive.primitiveClass);
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
    if (!needTypeCast(type, returnType)) {
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
      return Expressions.unbox(expression, requireNonNull(Primitive.ofBox(returnType)));
    }
    if (Primitive.is(returnType) && !Primitive.is(type)) {
      // E.g.
      //   int foo(Object o) {
      //     return ((Integer) o).intValue();
      //   }
      return Expressions.unbox(
          Expressions.convert_(expression, Types.box(returnType)),
          requireNonNull(Primitive.of(returnType)));
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

  /**
   * When trying to cast/convert a {@code Type} to another {@code Type},
   * it is necessary to pre-check whether the cast operation is needed.
   * We summarize general exceptions, including:
   *
   * <ol>
   *   <li>target Type {@code toType} equals with original Type {@code fromType}</li>
   *   <li>target Type can be assignable from original Type</li>
   *   <li>target Type is an instance of {@code RecordType},
   *   since the mapping Java Class might not generated yet</li>
   * </ol>
   *
   * @param fromType original type
   * @param toType   target type
   * @return Whether a cast operation is needed
   */
  public static boolean needTypeCast(Type fromType, Type toType) {
    return !(fromType.equals(toType)
        || toType instanceof RecordType
        || isAssignableFrom(toType, fromType));
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
    private final @Nullable Type ownerType;

    ParameterizedTypeImpl(Type rawType, List<Type> typeArguments,
        @Nullable Type ownerType) {
      super();
      this.rawType = rawType;
      this.typeArguments = typeArguments;
      this.ownerType = ownerType;
      assert rawType != null;
      for (Type typeArgument : typeArguments) {
        assert typeArgument != null;
      }
    }

    @Override public String toString() {
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

    @Override public Type[] getActualTypeArguments() {
      return typeArguments.toArray(new Type[0]);
    }

    @Override public Type getRawType() {
      return rawType;
    }

    @Override public @Nullable Type getOwnerType() {
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
    private final boolean componentIsNullable;
    private final long maximumCardinality;

    public ArrayType(Type componentType, boolean componentIsNullable,
        long maximumCardinality) {
      this.componentType = componentType;
      this.componentIsNullable = componentIsNullable;
      this.maximumCardinality = Math.max(maximumCardinality, -1L);
    }

    public ArrayType(Type componentType) {
      this(componentType, !Primitive.is(componentType), -1L);
    }

    /** Returns the type of elements in the array. */
    public Type getComponentType() {
      return componentType;
    }

    /** Returns whether elements in the array may be null. */
    public boolean componentIsNullable() {
      return componentIsNullable;
    }

    /** Returns the maximum cardinality; -1 if there is no maximum. */
    public long maximumCardinality() {
      return maximumCardinality;
    }
  }

  /**
   * Map type.
   */
  public static class MapType implements Type {
    private final Type keyType;
    private final boolean keyIsNullable;
    private final Type valueType;
    private final boolean valueIsNullable;

    public MapType(Type keyType, boolean keyIsNullable,
        Type valueType, boolean valueIsNullable) {
      this.keyType = keyType;
      this.keyIsNullable = keyIsNullable;
      this.valueType = valueType;
      this.valueIsNullable = valueIsNullable;
    }

    /** Returns the type of keys. */
    public Type getKeyType() {
      return keyType;
    }

    /** Returns whether keys may be null. */
    public boolean keyIsNullable() {
      return keyIsNullable;
    }

    /** Returns the type of values. */
    public Type getValueType() {
      return valueType;
    }

    /** Returns whether values may be null. */
    public boolean valueIsNullable() {
      return valueIsNullable;
    }

  }
}
