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
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ConstantUntypedNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

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

  /** Converts from internal representation to JDBC representation used by
   * arguments of user-defined functions. For example, converts date values from
   * {@code int} to {@link java.sql.Date}. */
  static Expression fromInternal(Expression e, Class<?> targetType) {
    if (e == ConstantUntypedNull.INSTANCE) {
      return e;
    }
    if (!(e.getType() instanceof Class)) {
      return e;
    }
    if (targetType.isAssignableFrom((Class) e.getType())) {
      return e;
    }
    if (targetType == java.sql.Date.class) {
      return Expressions.call(BuiltInMethod.INTERNAL_TO_DATE.method, e);
    }
    if (targetType == java.sql.Time.class) {
      return Expressions.call(BuiltInMethod.INTERNAL_TO_TIME.method, e);
    }
    if (targetType == java.sql.Timestamp.class) {
      return Expressions.call(BuiltInMethod.INTERNAL_TO_TIMESTAMP.method, e);
    }
    if (Primitive.is(e.type)
        && Primitive.isBox(targetType)) {
      // E.g. e is "int", target is "Long", generate "(long) e".
      return Expressions.convert_(e,
          Primitive.ofBox(targetType).primitiveClass);
    }
    return e;
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

  static Type toInternal(RelDataType type) {
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

  static Expression enforce(final Type storageType,
      final Expression e) {
    if (storageType != null && e.type != storageType) {
      if (e.type == java.sql.Date.class) {
        if (storageType == int.class) {
          return Expressions.call(BuiltInMethod.DATE_TO_INT.method, e);
        }
        if (storageType == Integer.class) {
          return Expressions.call(BuiltInMethod.DATE_TO_INT_OPTIONAL.method, e);
        }
      } else if (e.type == java.sql.Time.class) {
        if (storageType == int.class) {
          return Expressions.call(BuiltInMethod.TIME_TO_INT.method, e);
        }
        if (storageType == Integer.class) {
          return Expressions.call(BuiltInMethod.TIME_TO_INT_OPTIONAL.method, e);
        }
      } else if (e.type == java.sql.Timestamp.class) {
        if (storageType == long.class) {
          return Expressions.call(BuiltInMethod.TIMESTAMP_TO_LONG.method, e);
        }
        if (storageType == Long.class) {
          return Expressions.call(BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL.method, e);
        }
      }
    }
    return e;
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
}

// End EnumUtils.java
