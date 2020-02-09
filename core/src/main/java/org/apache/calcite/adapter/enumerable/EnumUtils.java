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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.ConstantUntypedNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.runtime.SortedMultiMap;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Utilities for generating programs in the Enumerable (functional)
 * style.
 */
public class EnumUtils {

  private EnumUtils() {}

  static final boolean BRIDGE_METHODS = true;

  static final List<ParameterExpression> NO_PARAMS =
      ImmutableList.of();

  static final List<Expression> NO_EXPRS =
      ImmutableList.of();

  public static final List<String> LEFT_RIGHT =
      ImmutableList.of("left", "right");

  /** Declares a method that overrides another method. */
  public static MethodDeclaration overridingMethodDecl(Method method,
      Iterable<ParameterExpression> parameters,
      BlockStatement body) {
    return Expressions.methodDecl(
        method.getModifiers() & ~Modifier.ABSTRACT,
        method.getReturnType(),
        method.getName(),
        parameters,
        body);
  }

  static Type javaClass(
      JavaTypeFactory typeFactory, RelDataType type) {
    final Type clazz = typeFactory.getJavaClass(type);
    return clazz instanceof Class ? clazz : Object[].class;
  }

  static List<Type> fieldTypes(
      final JavaTypeFactory typeFactory,
      final List<? extends RelDataType> inputTypes) {
    return new AbstractList<Type>() {
      public Type get(int index) {
        return EnumUtils.javaClass(typeFactory, inputTypes.get(index));
      }
      public int size() {
        return inputTypes.size();
      }
    };
  }

  static List<RelDataType> fieldRowTypes(
      final RelDataType inputRowType,
      final List<? extends RexNode> extraInputs,
      final List<Integer> argList) {
    final List<RelDataTypeField> inputFields = inputRowType.getFieldList();
    return new AbstractList<RelDataType>() {
      public RelDataType get(int index) {
        final int arg = argList.get(index);
        return arg < inputFields.size()
            ? inputFields.get(arg).getType()
            : extraInputs.get(arg - inputFields.size()).getType();
      }
      public int size() {
        return argList.size();
      }
    };
  }

  static Expression joinSelector(JoinRelType joinType, PhysType physType,
      List<PhysType> inputPhysTypes) {
    // A parameter for each input.
    final List<ParameterExpression> parameters = new ArrayList<>();

    // Generate all fields.
    final List<Expression> expressions = new ArrayList<>();
    final int outputFieldCount = physType.getRowType().getFieldCount();
    for (Ord<PhysType> ord : Ord.zip(inputPhysTypes)) {
      final PhysType inputPhysType =
          ord.e.makeNullable(joinType.generatesNullsOn(ord.i));
      // If input item is just a primitive, we do not generate specialized
      // primitive apply override since it won't be called anyway
      // Function<T> always operates on boxed arguments
      final ParameterExpression parameter =
          Expressions.parameter(Primitive.box(inputPhysType.getJavaRowType()),
              EnumUtils.LEFT_RIGHT.get(ord.i));
      parameters.add(parameter);
      if (expressions.size() == outputFieldCount) {
        // For instance, if semi-join needs to return just the left inputs
        break;
      }
      final int fieldCount = inputPhysType.getRowType().getFieldCount();
      for (int i = 0; i < fieldCount; i++) {
        Expression expression =
            inputPhysType.fieldReference(parameter, i,
                physType.getJavaFieldType(expressions.size()));
        if (joinType.generatesNullsOn(ord.i)) {
          expression =
              Expressions.condition(
                  Expressions.equal(parameter, Expressions.constant(null)),
                  Expressions.constant(null),
                  expression);
        }
        expressions.add(expression);
      }
    }
    return Expressions.lambda(
        Function2.class,
        physType.record(expressions),
        parameters);
  }

  /**
   * In Calcite, {@code java.sql.Date} and {@code java.sql.Time} are
   * stored as {@code Integer} type, {@code java.sql.Timestamp} is
   * stored as {@code Long} type.
   */
  static Expression toInternal(Expression operand, Type targetType) {
    return toInternal(operand, operand.getType(), targetType);
  }

  private static Expression toInternal(Expression operand,
      Type fromType, Type targetType) {
    if (fromType == java.sql.Date.class) {
      if (targetType == int.class) {
        return Expressions.call(BuiltInMethod.DATE_TO_INT.method, operand);
      } else if (targetType == Integer.class) {
        return Expressions.call(BuiltInMethod.DATE_TO_INT_OPTIONAL.method, operand);
      }
    } else if (fromType == java.sql.Time.class) {
      if (targetType == int.class) {
        return Expressions.call(BuiltInMethod.TIME_TO_INT.method, operand);
      } else if (targetType == Integer.class) {
        return Expressions.call(BuiltInMethod.TIME_TO_INT_OPTIONAL.method, operand);
      }
    } else if (fromType == java.sql.Timestamp.class) {
      if (targetType == long.class) {
        return Expressions.call(BuiltInMethod.TIMESTAMP_TO_LONG.method, operand);
      } else if (targetType == Long.class) {
        return Expressions.call(BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL.method, operand);
      }
    }
    return operand;
  }

  /** Converts from internal representation to JDBC representation used by
   * arguments of user-defined functions. For example, converts date values from
   * {@code int} to {@link java.sql.Date}. */
  private static Expression fromInternal(Expression operand, Type targetType) {
    return fromInternal(operand, operand.getType(), targetType);
  }

  private static Expression fromInternal(Expression operand,
      Type fromType, Type targetType) {
    if (operand == ConstantUntypedNull.INSTANCE) {
      return operand;
    }
    if (!(operand.getType() instanceof Class)) {
      return operand;
    }
    if (Types.isAssignableFrom(targetType, fromType)) {
      return operand;
    }
    if (targetType == java.sql.Date.class) {
      // E.g. from "int" or "Integer" to "java.sql.Date",
      // generate "SqlFunctions.internalToDate".
      if (isA(fromType, Primitive.INT)) {
        return Expressions.call(BuiltInMethod.INTERNAL_TO_DATE.method, operand);
      }
    } else if (targetType == java.sql.Time.class) {
      // E.g. from "int" or "Integer" to "java.sql.Time",
      // generate "SqlFunctions.internalToTime".
      if (isA(fromType, Primitive.INT)) {
        return Expressions.call(BuiltInMethod.INTERNAL_TO_TIME.method, operand);
      }
    } else if (targetType == java.sql.Timestamp.class) {
      // E.g. from "long" or "Long" to "java.sql.Timestamp",
      // generate "SqlFunctions.internalToTimestamp".
      if (isA(fromType, Primitive.LONG)) {
        return Expressions.call(BuiltInMethod.INTERNAL_TO_TIMESTAMP.method, operand);
      }
    }
    if (Primitive.is(operand.type)
        && Primitive.isBox(targetType)) {
      // E.g. operand is "int", target is "Long", generate "(long) operand".
      return Expressions.convert_(operand,
          Primitive.ofBox(targetType).primitiveClass);
    }
    return operand;
  }

  static List<Expression> fromInternal(Class<?>[] targetTypes,
      List<Expression> expressions) {
    final List<Expression> list = new ArrayList<>();
    if (targetTypes.length == expressions.size()) {
      for (int i = 0; i < expressions.size(); i++) {
        list.add(fromInternal(expressions.get(i), targetTypes[i]));
      }
    } else {
      int j = 0;
      for (int i = 0; i < expressions.size(); i++) {
        Class<?> type;
        if (!targetTypes[j].isArray()) {
          type = targetTypes[j];
          j++;
        } else {
          type = targetTypes[j].getComponentType();
        }
        list.add(fromInternal(expressions.get(i), type));
      }
    }
    return list;
  }

  static Type fromInternal(Type type) {
    if (type == java.sql.Date.class || type == java.sql.Time.class) {
      return int.class;
    }
    if (type == java.sql.Timestamp.class) {
      return long.class;
    }
    return type;
  }

  private static Type toInternal(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case DATE:
    case TIME:
      return type.isNullable() ? Integer.class : int.class;
    case TIMESTAMP:
      return type.isNullable() ? Long.class : long.class;
    default:
      return null; // we don't care; use the default storage type
    }
  }

  static List<Type> internalTypes(List<? extends RexNode> operandList) {
    return Util.transform(operandList, node -> toInternal(node.getType()));
  }

  /**
   * Convert {@code operand} to target type {@code toType}.
   *
   * @param operand The expression to convert
   * @param toType  Target type
   * @return A new expression with type {@code toType} or original if there
   * is no need to convert
   */
  public static Expression convert(Expression operand, Type toType) {
    final Type fromType = operand.getType();
    return EnumUtils.convert(operand, fromType, toType);
  }

  /**
   * Convert {@code operand} to target type {@code toType}.
   *
   * @param operand  The expression to convert
   * @param fromType Field type
   * @param toType   Target type
   * @return A new expression with type {@code toType} or original if there
   * is no need to convert
   */
  public static Expression convert(Expression operand, Type fromType,
      Type toType) {
    if (!Types.needTypeCast(fromType, toType)) {
      return operand;
    }
    // E.g. from "Short" to "int".
    // Generate "x.intValue()".
    final Primitive toPrimitive = Primitive.of(toType);
    final Primitive toBox = Primitive.ofBox(toType);
    final Primitive fromBox = Primitive.ofBox(fromType);
    final Primitive fromPrimitive = Primitive.of(fromType);
    final boolean fromNumber = fromType instanceof Class
        && Number.class.isAssignableFrom((Class) fromType);
    if (fromType == String.class) {
      if (toPrimitive != null) {
        switch (toPrimitive) {
        case CHAR:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
          // Generate "SqlFunctions.toShort(x)".
          return Expressions.call(
              SqlFunctions.class,
              "to" + SqlFunctions.initcap(toPrimitive.primitiveName),
              operand);
        default:
          // Generate "Short.parseShort(x)".
          return Expressions.call(
              toPrimitive.boxClass,
              "parse" + SqlFunctions.initcap(toPrimitive.primitiveName),
              operand);
        }
      }
      if (toBox != null) {
        switch (toBox) {
        case CHAR:
          // Generate "SqlFunctions.toCharBoxed(x)".
          return Expressions.call(
              SqlFunctions.class,
              "to" + SqlFunctions.initcap(toBox.primitiveName) + "Boxed",
              operand);
        default:
          // Generate "Short.valueOf(x)".
          return Expressions.call(
              toBox.boxClass,
              "valueOf",
              operand);
        }
      }
    }
    if (toPrimitive != null) {
      if (fromPrimitive != null) {
        // E.g. from "float" to "double"
        return Expressions.convert_(
            operand, toPrimitive.primitiveClass);
      }
      if (fromNumber || fromBox == Primitive.CHAR) {
        // Generate "x.shortValue()".
        return Expressions.unbox(operand, toPrimitive);
      } else {
        // E.g. from "Object" to "short".
        // Generate "SqlFunctions.toShort(x)"
        return Expressions.call(
            SqlFunctions.class,
            "to" + SqlFunctions.initcap(toPrimitive.primitiveName),
            operand);
      }
    } else if (fromNumber && toBox != null) {
      // E.g. from "Short" to "Integer"
      // Generate "x == null ? null : Integer.valueOf(x.intValue())"
      return Expressions.condition(
          Expressions.equal(operand, RexImpTable.NULL_EXPR),
          RexImpTable.NULL_EXPR,
          Expressions.box(
              Expressions.unbox(operand, toBox),
              toBox));
    } else if (fromPrimitive != null && toBox != null) {
      // E.g. from "int" to "Long".
      // Generate Long.valueOf(x)
      // Eliminate primitive casts like Long.valueOf((long) x)
      if (operand instanceof UnaryExpression) {
        UnaryExpression una = (UnaryExpression) operand;
        if (una.nodeType == ExpressionType.Convert
            && Primitive.of(una.getType()) == toBox) {
          Primitive origin = Primitive.of(una.expression.type);
          if (origin != null && toBox.assignableFrom(origin)) {
            return Expressions.box(una.expression, toBox);
          }
        }
      }
      if (fromType == toBox.primitiveClass) {
        return Expressions.box(operand, toBox);
      }
      // E.g., from "int" to "Byte".
      // Convert it first and generate "Byte.valueOf((byte)x)"
      // Because there is no method "Byte.valueOf(int)" in Byte
      return Expressions.box(
          Expressions.convert_(operand, toBox.primitiveClass),
          toBox);
    }
    // Convert datetime types to internal storage type:
    // 1. java.sql.Date -> int or Integer
    // 2. java.sql.Time -> int or Integer
    // 3. java.sql.Timestamp -> long or Long
    if (representAsInternalType(fromType)) {
      final Expression internalTypedOperand =
          toInternal(operand, fromType, toType);
      if (operand != internalTypedOperand) {
        return internalTypedOperand;
      }
    }
    // Convert internal storage type to datetime types:
    // 1. int or Integer -> java.sql.Date
    // 2. int or Integer -> java.sql.Time
    // 3. long or Long -> java.sql.Timestamp
    if (representAsInternalType(toType)) {
      final Expression originTypedOperand =
          fromInternal(operand, fromType, toType);
      if (operand != originTypedOperand) {
        return originTypedOperand;
      }
    }
    if (toType == BigDecimal.class) {
      if (fromBox != null) {
        // E.g. from "Integer" to "BigDecimal".
        // Generate "x == null ? null : new BigDecimal(x.intValue())"
        return Expressions.condition(
            Expressions.equal(operand, RexImpTable.NULL_EXPR),
            RexImpTable.NULL_EXPR,
            Expressions.new_(
                BigDecimal.class,
                Expressions.unbox(operand, fromBox)));
      }
      if (fromPrimitive != null) {
        // E.g. from "int" to "BigDecimal".
        // Generate "new BigDecimal(x)"
        return Expressions.new_(BigDecimal.class, operand);
      }
      // E.g. from "Object" to "BigDecimal".
      // Generate "x == null ? null : SqlFunctions.toBigDecimal(x)"
      return Expressions.condition(
          Expressions.equal(operand, RexImpTable.NULL_EXPR),
          RexImpTable.NULL_EXPR,
          Expressions.call(
              SqlFunctions.class,
              "toBigDecimal",
              operand));
    } else if (toType == String.class) {
      if (fromPrimitive != null) {
        switch (fromPrimitive) {
        case DOUBLE:
        case FLOAT:
          // E.g. from "double" to "String"
          // Generate "SqlFunctions.toString(x)"
          return Expressions.call(
              SqlFunctions.class,
              "toString",
              operand);
        default:
          // E.g. from "int" to "String"
          // Generate "Integer.toString(x)"
          return Expressions.call(
              fromPrimitive.boxClass,
              "toString",
              operand);
        }
      } else if (fromType == BigDecimal.class) {
        // E.g. from "BigDecimal" to "String"
        // Generate "SqlFunctions.toString(x)"
        return Expressions.condition(
            Expressions.equal(operand, RexImpTable.NULL_EXPR),
            RexImpTable.NULL_EXPR,
            Expressions.call(
                SqlFunctions.class,
                "toString",
                operand));
      } else {
        Expression result;
        try {
          // Avoid to generate code like:
          // "null.toString()" or "(xxx) null.toString()"
          if (operand instanceof ConstantExpression) {
            ConstantExpression ce = (ConstantExpression) operand;
            if (ce.value == null) {
              return Expressions.convert_(operand, toType);
            }
          }
          // Try to call "toString()" method
          // E.g. from "Integer" to "String"
          // Generate "x == null ? null : x.toString()"
          result = Expressions.condition(
              Expressions.equal(operand, RexImpTable.NULL_EXPR),
              RexImpTable.NULL_EXPR,
              Expressions.call(operand, "toString"));
        } catch (RuntimeException e) {
          // For some special cases, e.g., "BuiltInMethod.LESSER",
          // its return type is generic ("Comparable"), which contains
          // no "toString()" method. We fall through to "(String)x".
          return Expressions.convert_(operand, toType);
        }
        return result;
      }
    }
    return Expressions.convert_(operand, toType);
  }

  private static boolean isA(Type fromType, Primitive primitive) {
    return Primitive.of(fromType) == primitive
        || Primitive.ofBox(fromType) == primitive;
  }

  private static boolean representAsInternalType(Type type) {
    return type == java.sql.Date.class
        || type == java.sql.Time.class
        || type == java.sql.Timestamp.class;
  }

  /**
   * In {@link org.apache.calcite.sql.type.SqlTypeAssignmentRule},
   * some rules decide whether one type can be assignable to another type.
   * Based on these rules, a function can accept arguments with assignable types.
   *
   * <p>For example, a function with Long type operand can accept Integer as input.
   * See {@code org.apache.calcite.sql.SqlUtil#filterRoutinesByParameterType()} for details.
   *
   * <p>During query execution, some of the assignable types need explicit conversion
   * to the target types. i.e., Decimal expression should be converted to Integer
   * before it is assigned to the Integer type Lvalue(In Java, Decimal can not be assigned to
   * Integer directly).
   *
   * @param targetTypes Formal operand types declared for the function arguments
   * @param arguments Input expressions to the function
   * @return Input expressions with probable type conversion
   */
  static List<Expression> convertAssignableTypes(Class<?>[] targetTypes,
      List<Expression> arguments) {
    final List<Expression> list = new ArrayList<>();
    if (targetTypes.length == arguments.size()) {
      for (int i = 0; i < arguments.size(); i++) {
        list.add(convertAssignableType(arguments.get(i), targetTypes[i]));
      }
    } else {
      int j = 0;
      for (Expression argument: arguments) {
        Class<?> type;
        if (!targetTypes[j].isArray()) {
          type = targetTypes[j];
          j++;
        } else {
          type = targetTypes[j].getComponentType();
        }
        list.add(convertAssignableType(argument, type));
      }
    }
    return list;
  }

  /**
   * Handle decimal type specifically with explicit type conversion
   */
  private static Expression convertAssignableType(
      Expression argument, Type targetType) {
    if (targetType != BigDecimal.class) {
      return argument;
    }
    return convert(argument, targetType);
  }

  public static MethodCallExpression call(Class clazz, String methodName,
      List<? extends Expression> arguments) {
    return call(clazz, methodName, arguments, null);
  }

  /**
   * The powerful version of {@code org.apache.calcite.linq4j.tree.Expressions#call(
   * Type, String, Iterable<? extends Expression>)}. Try best effort to convert the
   * accepted arguments to match parameter type.
   *
   * @param clazz Class against which method is invoked
   * @param methodName Name of method
   * @param arguments argument expressions
   * @param targetExpression target expression
   *
   * @return MethodCallExpression that call the given name method
   * @throws RuntimeException if no suitable method found
   */
  public static MethodCallExpression call(Class clazz, String methodName,
       List<? extends Expression> arguments, Expression targetExpression) {
    Class[] argumentTypes = Types.toClassArray(arguments);
    try {
      Method candidate = clazz.getMethod(methodName, argumentTypes);
      return Expressions.call(targetExpression, candidate, arguments);
    } catch (NoSuchMethodException e) {
      for (Method method : clazz.getMethods()) {
        if (method.getName().equals(methodName)) {
          final boolean varArgs = method.isVarArgs();
          final Class[] parameterTypes = method.getParameterTypes();
          if (Types.allAssignable(varArgs, parameterTypes, argumentTypes)) {
            return Expressions.call(targetExpression, method, arguments);
          }
          // fall through
          final List<? extends Expression> typeMatchedArguments =
              matchMethodParameterTypes(varArgs, parameterTypes, arguments);
          if (typeMatchedArguments != null) {
            return Expressions.call(targetExpression, method, typeMatchedArguments);
          }
        }
      }
      throw new RuntimeException("while resolving method '" + methodName
          + Arrays.toString(argumentTypes) + "' in class " + clazz, e);
    }
  }

  private static List<? extends Expression> matchMethodParameterTypes(boolean varArgs,
      Class[] parameterTypes, List<? extends Expression> arguments) {
    if ((varArgs  && arguments.size() < parameterTypes.length - 1)
        || (!varArgs && arguments.size() != parameterTypes.length)) {
      return null;
    }
    final List<Expression> typeMatchedArguments = new ArrayList<>();
    for (int i = 0; i < arguments.size(); i++) {
      Class parameterType =
          !varArgs || i < parameterTypes.length - 1
              ? parameterTypes[i]
              : Object.class;
      final Expression typeMatchedArgument =
          matchMethodParameterType(arguments.get(i), parameterType);
      if (typeMatchedArgument == null) {
        return null;
      }
      typeMatchedArguments.add(typeMatchedArgument);
    }
    return typeMatchedArguments;
  }

  /**
   * Match an argument expression to method parameter type with best effort
   * @param argument Argument Expression
   * @param parameter Parameter type
   * @return Converted argument expression that matches the parameter type.
   *         Returns null if it is impossible to match.
   */
  private static Expression matchMethodParameterType(
      Expression argument, Class parameter) {
    Type argumentType = argument.getType();
    if (Types.isAssignableFrom(parameter, argumentType)) {
      return argument;
    }
    // Object.class is not assignable from primitive types,
    // but the method with Object parameters can accept primitive types.
    // E.g., "array(Object... args)" in SqlFunctions
    if (parameter == Object.class
        && Primitive.of(argumentType) != null) {
      return argument;
    }
    // Convert argument with Object.class type to parameter explicitly
    if (argumentType == Object.class
        && Primitive.of(argumentType) == null) {
      return convert(argument, parameter);
    }
    // assignable types that can be accepted with explicit conversion
    if (parameter == BigDecimal.class
        && Primitive.ofBoxOr(argumentType) != null) {
      return convert(argument, parameter);
    }
    return null;
  }

  /** Transforms a JoinRelType to Linq4j JoinType. **/
  static JoinType toLinq4jJoinType(JoinRelType joinRelType) {
    switch (joinRelType) {
    case INNER:
      return JoinType.INNER;
    case LEFT:
      return JoinType.LEFT;
    case RIGHT:
      return JoinType.RIGHT;
    case FULL:
      return JoinType.FULL;
    case SEMI:
      return JoinType.SEMI;
    case ANTI:
      return JoinType.ANTI;
    }
    throw new IllegalStateException(
        "Unable to convert " + joinRelType + " to Linq4j JoinType");
  }

  /** Returns a predicate expression based on a join condition. **/
  static Expression generatePredicate(
      EnumerableRelImplementor implementor,
      RexBuilder rexBuilder,
      RelNode left,
      RelNode right,
      PhysType leftPhysType,
      PhysType rightPhysType,
      RexNode condition) {
    final BlockBuilder builder = new BlockBuilder();
    final ParameterExpression left_ =
        Expressions.parameter(leftPhysType.getJavaRowType(), "left");
    final ParameterExpression right_ =
        Expressions.parameter(rightPhysType.getJavaRowType(), "right");
    final RexProgramBuilder program =
        new RexProgramBuilder(
            implementor.getTypeFactory().builder()
                .addAll(left.getRowType().getFieldList())
                .addAll(right.getRowType().getFieldList())
                .build(),
            rexBuilder);
    program.addCondition(condition);
    builder.add(
        Expressions.return_(null,
            RexToLixTranslator.translateCondition(program.getProgram(),
                implementor.getTypeFactory(),
                builder,
                new RexToLixTranslator.InputGetterImpl(
                    ImmutableList.of(Pair.of(left_, leftPhysType),
                        Pair.of(right_, rightPhysType))),
                implementor.allCorrelateVariables,
                implementor.getConformance())));
    return Expressions.lambda(Predicate2.class, builder.toBlock(), left_, right_);
  }

  /**
   * Generates a window selector which appends attribute of the window based on
   * the parameters.
   *
   * Note that it only works for batch scenario. E.g. all data is known and there is no late data.
   */
  static Expression tumblingWindowSelector(
      PhysType inputPhysType,
      PhysType outputPhysType,
      Expression wmColExpr,
      Expression intervalExpr) {
    // Generate all fields.
    final List<Expression> expressions = new ArrayList<>();
    // If input item is just a primitive, we do not generate specialized
    // primitive apply override since it won't be called anyway
    // Function<T> always operates on boxed arguments
    final ParameterExpression parameter =
        Expressions.parameter(Primitive.box(inputPhysType.getJavaRowType()), "_input");
    final int fieldCount = inputPhysType.getRowType().getFieldCount();
    for (int i = 0; i < fieldCount; i++) {
      Expression expression =
          inputPhysType.fieldReference(parameter, i,
              outputPhysType.getJavaFieldType(expressions.size()));
      expressions.add(expression);
    }
    final Expression wmColExprToLong = EnumUtils.convert(wmColExpr, long.class);
    final Expression shiftExpr = Expressions.constant(1, long.class);

    // Find the fixed window for a timestamp given a window size and return the window start.
    // Fixed windows counts from timestamp 0.
    // wmMillis / intervalMillis * intervalMillis
    expressions.add(
        Expressions.multiply(
            Expressions.divide(
                wmColExprToLong,
                intervalExpr),
            intervalExpr));

    // Find the fixed window for a timestamp given a window size and return the window end.
    // Fixed windows counts from timestamp 0.

    // (wmMillis / sizeMillis + 1L) * sizeMillis;
    expressions.add(
        Expressions.multiply(
            Expressions.add(
                Expressions.divide(
                    wmColExprToLong,
                    intervalExpr),
                shiftExpr),
            intervalExpr));

    return Expressions.lambda(
        Function1.class,
        outputPhysType.record(expressions),
        parameter);
  }

  /**
   * Creates enumerable implementation that applies sessionization to elements from the input
   * enumerator based on a specified key. Elements are windowed into sessions separated by
   * periods with no input for at least the duration specified by gap parameter.
   */
  public static Enumerable<Object[]> sessionize(Enumerator<Object[]> inputEnumerator,
      int indexOfWatermarkedColumn, int indexOfKeyColumn, long gap) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new SessionizationEnumerator(inputEnumerator,
            indexOfWatermarkedColumn, indexOfKeyColumn, gap);
      }
    };
  }

  private static class SessionizationEnumerator implements Enumerator<Object[]> {
    private final Enumerator<Object[]> inputEnumerator;
    private final int indexOfWatermarkedColumn;
    private final int indexOfKeyColumn;
    private final long gap;
    private LinkedList<Object[]> list;
    private boolean initialized;

    /**
     * Note that it only works for batch scenario. E.g. all data is known and there is no
     * late data.
     *
     * @param inputEnumerator the enumerator to provide an array of objects as input
     * @param indexOfWatermarkedColumn the index of timestamp column upon which a watermark is built
     * @param indexOfKeyColumn the index of column that acts as grouping key
     * @param gap gap parameter
     */
    SessionizationEnumerator(Enumerator<Object[]> inputEnumerator,
        int indexOfWatermarkedColumn, int indexOfKeyColumn, long gap) {
      this.inputEnumerator = inputEnumerator;
      this.indexOfWatermarkedColumn = indexOfWatermarkedColumn;
      this.indexOfKeyColumn = indexOfKeyColumn;
      this.gap = gap;
      list = new LinkedList<>();
      initialized = false;
    }

    @Override public Object[] current() {
      if (!initialized) {
        initialize();
        initialized = true;
      }
      return list.pollFirst();
    }

    @Override public boolean moveNext() {
      return initialized ? list.size() > 0 : inputEnumerator.moveNext();
    }

    @Override public void reset() {
      list.clear();
      inputEnumerator.reset();
      initialized = false;
    }

    @Override public void close() {
      list.clear();
      inputEnumerator.close();
      initialized = false;
    }

    private void initialize() {
      List<Object[]> elements = new ArrayList<>();
      // initialize() will be called when inputEnumerator.moveNext() is true,
      // thus firstly should take the current element.
      elements.add(inputEnumerator.current());
      // sessionization needs to see all data.
      while (inputEnumerator.moveNext()) {
        elements.add(inputEnumerator.current());
      }

      Map<Object, SortedMultiMap<Pair<Long, Long>, Object[]>> sessionKeyMap = new HashMap<>();
      for (Object[] element : elements) {
        sessionKeyMap.putIfAbsent(element[indexOfKeyColumn], new SortedMultiMap<>());
        Pair initWindow = computeInitWindow(
            SqlFunctions.toLong(element[indexOfWatermarkedColumn]), gap);
        sessionKeyMap.get(element[indexOfKeyColumn]).putMulti(initWindow, element);
      }

      // merge per key session windows if there is any overlap between windows.
      for (Map.Entry<Object, SortedMultiMap<Pair<Long, Long>, Object[]>> perKeyEntry
          : sessionKeyMap.entrySet()) {
        Map<Pair<Long, Long>, List<Object[]>> finalWindowElementsMap = new HashMap<>();
        Pair<Long, Long> currentWindow = null;
        List<Object[]> tempElementList = new ArrayList<>();
        for (Map.Entry<Pair<Long, Long>, List<Object[]>> sessionEntry
            : perKeyEntry.getValue().entrySet()) {
          // check the next window can be merged.
          if (currentWindow == null || !isOverlapped(currentWindow, sessionEntry.getKey())) {
            // cannot merge window as there is no overlap
            if (currentWindow != null) {
              finalWindowElementsMap.put(currentWindow, new ArrayList<>(tempElementList));
            }

            currentWindow = sessionEntry.getKey();
            tempElementList.clear();
            tempElementList.addAll(sessionEntry.getValue());
          } else {
            // merge windows.
            currentWindow = mergeWindows(currentWindow, sessionEntry.getKey());
            // merge elements in windows.
            tempElementList.addAll(sessionEntry.getValue());
          }
        }

        if (!tempElementList.isEmpty()) {
          finalWindowElementsMap.put(currentWindow, new ArrayList<>(tempElementList));
        }

        // construct final results from finalWindowElementsMap.
        for (Map.Entry<Pair<Long, Long>, List<Object[]>> finalWindowElementsEntry
            : finalWindowElementsMap.entrySet()) {
          for (Object[] element : finalWindowElementsEntry.getValue()) {
            Object[] curWithWindow = new Object[element.length + 2];
            System.arraycopy(element, 0, curWithWindow, 0, element.length);
            curWithWindow[element.length] = finalWindowElementsEntry.getKey().left;
            curWithWindow[element.length + 1] = finalWindowElementsEntry.getKey().right;
            list.offer(curWithWindow);
          }
        }
      }
    }

    private boolean isOverlapped(Pair<Long, Long> a, Pair<Long, Long> b) {
      return !(b.left >= a.right);
    }

    private Pair<Long, Long> mergeWindows(Pair<Long, Long> a, Pair<Long, Long> b) {
      return new Pair<>(a.left <= b.left ? a.left : b.left, a.right >= b.right ? a.right : b.right);
    }

    private Pair<Long, Long> computeInitWindow(long ts, long gap) {
      return new Pair<>(ts, ts + gap);
    }
  }

  /**
   * Create enumerable implementation that applies hopping on each element from the input
   * enumerator and produces at least one element for each input element.
   */
  public static Enumerable<Object[]> hopping(Enumerator<Object[]> inputEnumerator,
      int indexOfWatermarkedColumn, long emitFrequency, long intervalSize) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new HopEnumerator(inputEnumerator,
            indexOfWatermarkedColumn, emitFrequency, intervalSize);
      }
    };
  }

  private static class HopEnumerator implements Enumerator<Object[]> {
    private final Enumerator<Object[]> inputEnumerator;
    private final int indexOfWatermarkedColumn;
    private final long emitFrequency;
    private final long intervalSize;
    private LinkedList<Object[]> list;

    /**
     * Note that it only works for batch scenario. E.g. all data is known and there is no late data.
     *
     * @param inputEnumerator the enumerator to provide an array of objects as input
     * @param indexOfWatermarkedColumn the index of timestamp column upon which a watermark is built
     * @param slide sliding size
     * @param intervalSize window size
     */
    HopEnumerator(Enumerator<Object[]> inputEnumerator,
        int indexOfWatermarkedColumn, long slide, long intervalSize) {
      this.inputEnumerator = inputEnumerator;
      this.indexOfWatermarkedColumn = indexOfWatermarkedColumn;
      this.emitFrequency = slide;
      this.intervalSize = intervalSize;
      list = new LinkedList<>();
    }

    public Object[] current() {
      if (list.size() > 0) {
        return takeOne();
      } else {
        Object[] current = inputEnumerator.current();
        List<Pair> windows = hopWindows(SqlFunctions.toLong(current[indexOfWatermarkedColumn]),
            emitFrequency, intervalSize);
        for (Pair window : windows) {
          Object[] curWithWindow = new Object[current.length + 2];
          System.arraycopy(current, 0, curWithWindow, 0, current.length);
          curWithWindow[current.length] = window.left;
          curWithWindow[current.length + 1] = window.right;
          list.offer(curWithWindow);
        }
        return takeOne();
      }
    }

    public boolean moveNext() {
      return list.size() > 0 || inputEnumerator.moveNext();
    }

    public void reset() {
      inputEnumerator.reset();
      list.clear();
    }

    public void close() {
    }

    private Object[] takeOne() {
      return list.pollFirst();
    }
  }

  private static List<Pair> hopWindows(long tsMillis, long periodMillis, long sizeMillis) {
    ArrayList<Pair> ret = new ArrayList<>(Math.toIntExact(sizeMillis / periodMillis));
    long lastStart = tsMillis - ((tsMillis + periodMillis) % periodMillis);
    for (long start = lastStart;
         start > tsMillis - sizeMillis;
         start -= periodMillis) {
      ret.add(new Pair(start, start + sizeMillis));
    }
    return ret;
  }

  /**
   * Apply tumbling per row from the enumerable input.
   */
  public static <TSource, TResult> Enumerable<TResult> tumbling(
      Enumerable<TSource> inputEnumerable,
      Function1<TSource, TResult> outSelector) {
    return new AbstractEnumerable<TResult>() {
      // Applies tumbling on each element from the input enumerator and produces
      // exactly one element for each input element.
      @Override public Enumerator<TResult> enumerator() {
        return new Enumerator<TResult>() {
          Enumerator<TSource> inputs = inputEnumerable.enumerator();

          public TResult current() {
            return outSelector.apply(inputs.current());
          }

          public boolean moveNext() {
            return inputs.moveNext();
          }

          public void reset() {
            inputs.reset();
          }

          public void close() {
          }
        };
      }
    };
  }
}
