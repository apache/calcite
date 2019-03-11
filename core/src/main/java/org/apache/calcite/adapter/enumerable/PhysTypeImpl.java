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
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.calcite.adapter.enumerable.EnumUtils.overridingMethodDecl;

/** Implementation of {@link PhysType}. */
public class PhysTypeImpl implements PhysType {
  private final JavaTypeFactory typeFactory;
  private final RelDataType rowType;
  private final Type javaRowClass;
  private final List<Class> fieldClasses = new ArrayList<>();
  final JavaRowFormat format;

  /** Creates a PhysTypeImpl. */
  PhysTypeImpl(
      JavaTypeFactory typeFactory,
      RelDataType rowType,
      Type javaRowClass,
      JavaRowFormat format) {
    this.typeFactory = typeFactory;
    this.rowType = rowType;
    this.javaRowClass = javaRowClass;
    this.format = format;
    for (RelDataTypeField field : rowType.getFieldList()) {
      Type fieldType = typeFactory.getJavaClass(field.getType());
      fieldClasses.add(fieldType instanceof Class ? (Class) fieldType : Object[].class);
    }
  }

  public static PhysType of(
      JavaTypeFactory typeFactory,
      RelDataType rowType,
      JavaRowFormat format) {
    return of(typeFactory, rowType, format, true);
  }

  public static PhysType of(
      JavaTypeFactory typeFactory,
      RelDataType rowType,
      JavaRowFormat format,
      boolean optimize) {
    if (optimize) {
      format = format.optimize(rowType);
    }
    final Type javaRowClass = format.javaRowClass(typeFactory, rowType);
    return new PhysTypeImpl(typeFactory, rowType, javaRowClass, format);
  }

  static PhysType of(
      final JavaTypeFactory typeFactory,
      Type javaRowClass) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    if (javaRowClass instanceof Types.RecordType) {
      final Types.RecordType recordType = (Types.RecordType) javaRowClass;
      for (Types.RecordField field : recordType.getRecordFields()) {
        builder.add(field.getName(), typeFactory.createType(field.getType()));
      }
    }
    RelDataType rowType = builder.build();
    // Do not optimize if there are 0 or 1 fields.
    return new PhysTypeImpl(typeFactory, rowType, javaRowClass,
        JavaRowFormat.CUSTOM);
  }

  public JavaRowFormat getFormat() {
    return format;
  }

  public PhysType project(List<Integer> integers, JavaRowFormat format) {
    return project(integers, false, format);
  }

  public PhysType project(List<Integer> integers, boolean indicator,
      JavaRowFormat format) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int index : integers) {
      builder.add(rowType.getFieldList().get(index));
    }
    if (indicator) {
      final RelDataType booleanType =
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.BOOLEAN), false);
      for (int index : integers) {
        builder.add("i$" + rowType.getFieldList().get(index).getName(),
            booleanType);
      }
    }
    RelDataType projectedRowType = builder.build();
    return of(typeFactory, projectedRowType, format.optimize(projectedRowType));
  }

  public Expression generateSelector(
      ParameterExpression parameter,
      List<Integer> fields) {
    return generateSelector(parameter, fields, format);
  }

  public Expression generateSelector(
      ParameterExpression parameter,
      List<Integer> fields,
      JavaRowFormat targetFormat) {
    // Optimize target format
    switch (fields.size()) {
    case 0:
      targetFormat = JavaRowFormat.LIST;
      break;
    case 1:
      targetFormat = JavaRowFormat.SCALAR;
      break;
    }
    final PhysType targetPhysType =
        project(fields, targetFormat);
    switch (format) {
    case SCALAR:
      return Expressions.call(BuiltInMethod.IDENTITY_SELECTOR.method);
    default:
      return Expressions.lambda(Function1.class,
          targetPhysType.record(fieldReferences(parameter, fields)), parameter);
    }
  }

  public Expression generateSelector(final ParameterExpression parameter,
      final List<Integer> fields, List<Integer> usedFields,
      JavaRowFormat targetFormat) {
    final PhysType targetPhysType =
        project(fields, true, targetFormat);
    final List<Expression> expressions = new ArrayList<>();
    for (Ord<Integer> ord : Ord.zip(fields)) {
      final Integer field = ord.e;
      if (usedFields.contains(field)) {
        expressions.add(fieldReference(parameter, field));
      } else {
        final Primitive primitive =
            Primitive.of(targetPhysType.fieldClass(ord.i));
        expressions.add(
            Expressions.constant(
                primitive != null ? primitive.defaultValue : null));
      }
    }
    for (Integer field : fields) {
      expressions.add(Expressions.constant(!usedFields.contains(field)));
    }
    return Expressions.lambda(Function1.class,
        targetPhysType.record(expressions), parameter);
  }

  public Pair<Type, List<Expression>> selector(
      ParameterExpression parameter,
      List<Integer> fields,
      JavaRowFormat targetFormat) {
    // Optimize target format
    switch (fields.size()) {
    case 0:
      targetFormat = JavaRowFormat.LIST;
      break;
    case 1:
      targetFormat = JavaRowFormat.SCALAR;
      break;
    }
    final PhysType targetPhysType =
        project(fields, targetFormat);
    switch (format) {
    case SCALAR:
      return Pair.of(parameter.getType(), ImmutableList.of(parameter));
    default:
      return Pair.of(targetPhysType.getJavaRowType(),
          fieldReferences(parameter, fields));
    }
  }

  public List<Expression> accessors(Expression v1, List<Integer> argList) {
    final List<Expression> expressions = new ArrayList<>();
    for (int field : argList) {
      expressions.add(
          Types.castIfNecessary(
              fieldClass(field),
              fieldReference(v1, field)));
    }
    return expressions;
  }

  public PhysType makeNullable(boolean nullable) {
    if (!nullable) {
      return this;
    }
    return new PhysTypeImpl(typeFactory,
        typeFactory.createTypeWithNullability(rowType, true),
        Primitive.box(javaRowClass), format);
  }

  @SuppressWarnings("deprecation")
  public Expression convertTo(Expression exp, PhysType targetPhysType) {
    return convertTo(exp, targetPhysType.getFormat());
  }

  public Expression convertTo(Expression exp, JavaRowFormat targetFormat) {
    if (format == targetFormat) {
      return exp;
    }
    final ParameterExpression o_ =
        Expressions.parameter(javaRowClass, "o");
    final int fieldCount = rowType.getFieldCount();
    // The conversion must be strict so optimizations of the targetFormat should not be performed
    // by the code that follows. If necessary the target format can be optimized before calling
    // this method.
    PhysType targetPhysType = PhysTypeImpl.of(typeFactory, rowType, targetFormat, false);
    final Expression selector;
    switch (targetPhysType.getFormat()) {
    case SCALAR:
      selector = Expressions.call(BuiltInMethod.IDENTITY_SELECTOR.method);
      break;
    default:
      selector = Expressions.lambda(Function1.class,
          targetPhysType.record(fieldReferences(o_, Util.range(fieldCount))), o_);
    }
    return Expressions.call(exp, BuiltInMethod.SELECT.method, selector);
  }

  public Pair<Expression, Expression> generateCollationKey(
      final List<RelFieldCollation> collations) {
    final Expression selector;
    if (collations.size() == 1) {
      RelFieldCollation collation = collations.get(0);
      ParameterExpression parameter =
          Expressions.parameter(javaRowClass, "v");
      selector =
          Expressions.lambda(
              Function1.class,
              fieldReference(parameter, collation.getFieldIndex()),
              parameter);
      return Pair.of(selector,
          Expressions.call(
              BuiltInMethod.NULLS_COMPARATOR.method,
              Expressions.constant(
                  collation.nullDirection
                      == RelFieldCollation.NullDirection.FIRST),
              Expressions.constant(
                  collation.getDirection()
                      == RelFieldCollation.Direction.DESCENDING)));
    }
    selector =
        Expressions.call(BuiltInMethod.IDENTITY_SELECTOR.method);

    // int c;
    // c = Utilities.compare(v0, v1);
    // if (c != 0) return c; // or -c if descending
    // ...
    // return 0;
    BlockBuilder body = new BlockBuilder();
    final ParameterExpression parameterV0 =
        Expressions.parameter(javaRowClass, "v0");
    final ParameterExpression parameterV1 =
        Expressions.parameter(javaRowClass, "v1");
    final ParameterExpression parameterC =
        Expressions.parameter(int.class, "c");
    final int mod = collations.size() == 1 ? Modifier.FINAL : 0;
    body.add(Expressions.declare(mod, parameterC, null));
    for (RelFieldCollation collation : collations) {
      final int index = collation.getFieldIndex();
      Expression arg0 = fieldReference(parameterV0, index);
      Expression arg1 = fieldReference(parameterV1, index);
      switch (Primitive.flavor(fieldClass(index))) {
      case OBJECT:
        arg0 = Types.castIfNecessary(Comparable.class, arg0);
        arg1 = Types.castIfNecessary(Comparable.class, arg1);
      }
      final boolean nullsFirst =
          collation.nullDirection
              == RelFieldCollation.NullDirection.FIRST;
      final boolean descending =
          collation.getDirection()
              == RelFieldCollation.Direction.DESCENDING;
      final Method method = (fieldNullable(index)
          ? (nullsFirst ^ descending
              ? BuiltInMethod.COMPARE_NULLS_FIRST
              : BuiltInMethod.COMPARE_NULLS_LAST)
          : BuiltInMethod.COMPARE).method;
      body.add(
          Expressions.statement(
              Expressions.assign(
                  parameterC,
                  Expressions.call(method.getDeclaringClass(),
                      method.getName(),
                      arg0,
                      arg1))));
      body.add(
          Expressions.ifThen(
              Expressions.notEqual(
                  parameterC, Expressions.constant(0)),
              Expressions.return_(
                  null,
                  descending
                      ? Expressions.negate(parameterC)
                      : parameterC)));
    }
    body.add(
        Expressions.return_(null, Expressions.constant(0)));

    final List<MemberDeclaration> memberDeclarations =
        Expressions.list(
            Expressions.methodDecl(
                Modifier.PUBLIC,
                int.class,
                "compare",
                ImmutableList.of(
                    parameterV0, parameterV1),
                body.toBlock()));

    if (EnumerableRules.BRIDGE_METHODS) {
      final ParameterExpression parameterO0 =
          Expressions.parameter(Object.class, "o0");
      final ParameterExpression parameterO1 =
          Expressions.parameter(Object.class, "o1");
      BlockBuilder bridgeBody = new BlockBuilder();
      bridgeBody.add(
          Expressions.return_(
              null,
              Expressions.call(
                  Expressions.parameter(
                      Comparable.class, "this"),
                  BuiltInMethod.COMPARATOR_COMPARE.method,
                  Expressions.convert_(
                      parameterO0,
                      javaRowClass),
                  Expressions.convert_(
                      parameterO1,
                      javaRowClass))));
      memberDeclarations.add(
          overridingMethodDecl(
              BuiltInMethod.COMPARATOR_COMPARE.method,
              ImmutableList.of(parameterO0, parameterO1),
              bridgeBody.toBlock()));
    }
    return Pair.of(selector,
        Expressions.new_(Comparator.class,
            ImmutableList.of(),
            memberDeclarations));
  }

  public Expression generateComparator(RelCollation collation) {
    // int c;
    // c = Utilities.compare(v0, v1);
    // if (c != 0) return c; // or -c if descending
    // ...
    // return 0;
    BlockBuilder body = new BlockBuilder();
    final Type javaRowClass = Primitive.box(this.javaRowClass);
    final ParameterExpression parameterV0 =
        Expressions.parameter(javaRowClass, "v0");
    final ParameterExpression parameterV1 =
        Expressions.parameter(javaRowClass, "v1");
    final ParameterExpression parameterC =
        Expressions.parameter(int.class, "c");
    final int mod =
        collation.getFieldCollations().size() == 1 ? Modifier.FINAL : 0;
    body.add(Expressions.declare(mod, parameterC, null));
    for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
      final int index = fieldCollation.getFieldIndex();
      Expression arg0 = fieldReference(parameterV0, index);
      Expression arg1 = fieldReference(parameterV1, index);
      switch (Primitive.flavor(fieldClass(index))) {
      case OBJECT:
        arg0 = Types.castIfNecessary(Comparable.class, arg0);
        arg1 = Types.castIfNecessary(Comparable.class, arg1);
      }
      final boolean nullsFirst =
          fieldCollation.nullDirection
              == RelFieldCollation.NullDirection.FIRST;
      final boolean descending =
          fieldCollation.getDirection()
              == RelFieldCollation.Direction.DESCENDING;
      body.add(
          Expressions.statement(
              Expressions.assign(
                  parameterC,
                  Expressions.call(
                      Utilities.class,
                      fieldNullable(index)
                          ? (nullsFirst != descending
                          ? "compareNullsFirst"
                          : "compareNullsLast")
                          : "compare",
                      arg0,
                      arg1))));
      body.add(
          Expressions.ifThen(
              Expressions.notEqual(
                  parameterC, Expressions.constant(0)),
              Expressions.return_(
                  null,
                  descending
                      ? Expressions.negate(parameterC)
                      : parameterC)));
    }
    body.add(
        Expressions.return_(null, Expressions.constant(0)));

    final List<MemberDeclaration> memberDeclarations =
        Expressions.list(
            Expressions.methodDecl(
                Modifier.PUBLIC,
                int.class,
                "compare",
                ImmutableList.of(parameterV0, parameterV1),
                body.toBlock()));

    if (EnumerableRules.BRIDGE_METHODS) {
      final ParameterExpression parameterO0 =
          Expressions.parameter(Object.class, "o0");
      final ParameterExpression parameterO1 =
          Expressions.parameter(Object.class, "o1");
      BlockBuilder bridgeBody = new BlockBuilder();
      bridgeBody.add(
          Expressions.return_(
              null,
              Expressions.call(
                  Expressions.parameter(
                      Comparable.class, "this"),
                  BuiltInMethod.COMPARATOR_COMPARE.method,
                  Expressions.convert_(
                      parameterO0,
                      javaRowClass),
                  Expressions.convert_(
                      parameterO1,
                      javaRowClass))));
      memberDeclarations.add(
          overridingMethodDecl(
              BuiltInMethod.COMPARATOR_COMPARE.method,
              ImmutableList.of(parameterO0, parameterO1),
              bridgeBody.toBlock()));
    }
    return Expressions.new_(
        Comparator.class,
        ImmutableList.of(),
        memberDeclarations);
  }

  public RelDataType getRowType() {
    return rowType;
  }

  public Expression record(List<Expression> expressions) {
    return format.record(javaRowClass, expressions);
  }

  public Type getJavaRowType() {
    return javaRowClass;
  }

  public Type getJavaFieldType(int index) {
    return format.javaFieldClass(typeFactory, rowType, index);
  }

  public PhysType component(int fieldOrdinal) {
    final RelDataTypeField field = rowType.getFieldList().get(fieldOrdinal);
    return PhysTypeImpl.of(typeFactory,
        toStruct(field.getType().getComponentType()), format, false);
  }

  public PhysType field(int ordinal) {
    final RelDataTypeField field = rowType.getFieldList().get(ordinal);
    final RelDataType type = field.getType();
    return PhysTypeImpl.of(typeFactory, toStruct(type), format, false);
  }

  private RelDataType toStruct(RelDataType type) {
    if (type.isStruct()) {
      return type;
    }
    return typeFactory.builder()
        .add(SqlUtil.deriveAliasFromOrdinal(0), type)
        .build();
  }

  public Expression comparer() {
    return format.comparer();
  }

  private List<Expression> fieldReferences(
      final Expression parameter, final List<Integer> fields) {
    return new AbstractList<Expression>() {
      public Expression get(int index) {
        return fieldReference(parameter, fields.get(index));
      }

      public int size() {
        return fields.size();
      }
    };
  }

  public Class fieldClass(int field) {
    return fieldClasses.get(field);
  }

  public boolean fieldNullable(int field) {
    return rowType.getFieldList().get(field).getType().isNullable();
  }

  public Expression generateAccessor(
      List<Integer> fields) {
    ParameterExpression v1 =
        Expressions.parameter(javaRowClass, "v1");
    switch (fields.size()) {
    case 0:
      return Expressions.lambda(
          Function1.class,
          Expressions.field(
              null,
              BuiltInMethod.COMPARABLE_EMPTY_LIST.field),
          v1);
    case 1:
      int field0 = fields.get(0);

      // new Function1<Employee, Res> {
      //    public Res apply(Employee v1) {
      //        return v1.<fieldN>;
      //    }
      // }
      Class returnType = fieldClasses.get(field0);
      Expression fieldReference =
          Types.castIfNecessary(
              returnType,
              fieldReference(v1, field0));
      return Expressions.lambda(
          Function1.class,
          fieldReference,
          v1);
    default:
      // new Function1<Employee, List> {
      //    public List apply(Employee v1) {
      //        return Arrays.asList(
      //            new Object[] {v1.<fieldN>, v1.<fieldM>});
      //    }
      // }
      Expressions.FluentList<Expression> list = Expressions.list();
      for (int field : fields) {
        list.add(fieldReference(v1, field));
      }
      switch (list.size()) {
      case 2:
        return Expressions.lambda(
            Function1.class,
            Expressions.call(
                List.class,
                null,
                BuiltInMethod.LIST2.method,
                list),
            v1);
      case 3:
        return Expressions.lambda(
            Function1.class,
            Expressions.call(
                List.class,
                null,
                BuiltInMethod.LIST3.method,
                list),
            v1);
      case 4:
        return Expressions.lambda(
            Function1.class,
            Expressions.call(
                List.class,
                null,
                BuiltInMethod.LIST4.method,
                list),
            v1);
      case 5:
        return Expressions.lambda(
            Function1.class,
            Expressions.call(
                List.class,
                null,
                BuiltInMethod.LIST5.method,
                list),
            v1);
      case 6:
        return Expressions.lambda(
            Function1.class,
            Expressions.call(
                List.class,
                null,
                BuiltInMethod.LIST6.method,
                list),
            v1);
      default:
        return Expressions.lambda(
            Function1.class,
            Expressions.call(
                List.class,
                null,
                BuiltInMethod.LIST_N.method,
                Expressions.newArrayInit(
                    Comparable.class,
                    list)),
            v1);
      }
    }
  }

  public Expression fieldReference(
      Expression expression, int field) {
    return fieldReference(expression, field, null);
  }

  public Expression fieldReference(
      Expression expression, int field, Type storageType) {
    Type fieldType;
    if (storageType == null) {
      storageType = fieldClass(field);
      fieldType = null;
    } else {
      fieldType = fieldClass(field);
      if (fieldType != java.sql.Date.class) {
        fieldType = null;
      }
    }
    return format.field(expression, field, fieldType, storageType);
  }
}

// End PhysTypeImpl.java
