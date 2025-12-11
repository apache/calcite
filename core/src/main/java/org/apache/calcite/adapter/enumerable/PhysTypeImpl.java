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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.calcite.adapter.enumerable.EnumUtils.generateCollatorExpression;
import static org.apache.calcite.adapter.enumerable.EnumUtils.overridingMethodDecl;

import static java.util.Objects.requireNonNull;

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

  @Override public JavaRowFormat getFormat() {
    return format;
  }

  @Override public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  @Override public PhysType project(List<Integer> integers, JavaRowFormat format) {
    return project(integers, false, format);
  }

  @Override public PhysType project(List<Integer> integers, boolean indicator,
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

  @Override public Expression generateSelector(
      ParameterExpression parameter,
      List<Integer> fields) {
    return generateSelector(parameter, fields, format);
  }

  @Override public Expression generateSelector(
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
    default:
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

  @Override public Expression generateSelector(final ParameterExpression parameter,
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

  @Override public Pair<Type, List<Expression>> selector(
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
    default:
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

  @Override public List<Expression> accessors(Expression v1, List<Integer> argList) {
    final List<Expression> expressions = new ArrayList<>();
    for (int field : argList) {
      expressions.add(
          EnumUtils.convert(
              fieldReference(v1, field),
              fieldClass(field)));
    }
    return expressions;
  }

  @Override public PhysType makeNullable(boolean nullable) {
    if (!nullable) {
      return this;
    }
    return new PhysTypeImpl(typeFactory,
        typeFactory.createTypeWithNullability(rowType, true),
        Primitive.box(javaRowClass), format);
  }

  @SuppressWarnings("deprecation")
  @Override public Expression convertTo(Expression exp, PhysType targetPhysType) {
    return convertTo(exp, targetPhysType.getFormat());
  }

  @Override public Expression convertTo(Expression exp, JavaRowFormat targetFormat) {
    if (format == targetFormat) {
      return exp;
    }
    final ParameterExpression o_ =
        Expressions.parameter(javaRowClass, "o");
    final int fieldCount = rowType.getFieldCount();
    // The conversion must be strict so optimizations of the targetFormat should not be performed
    // by the code that follows. If necessary the target format can be optimized before calling
    // this method.
    PhysType targetPhysType =
        PhysTypeImpl.of(typeFactory, rowType, targetFormat, false);
    final Expression selector =
        Expressions.lambda(Function1.class,
            targetPhysType.record(fieldReferences(o_, Util.range(fieldCount))),
            o_);
    return Expressions.call(exp, BuiltInMethod.SELECT.method, selector);
  }

  @Override public Pair<Expression, Expression> generateCollationKey(
      final List<RelFieldCollation> collations) {
    final Expression selector;
    if (collations.size() == 1) {
      RelFieldCollation collation = collations.get(0);
      RelDataType fieldType = rowType.getFieldList() == null || rowType.getFieldList().isEmpty()
          ? rowType
          : rowType.getFieldList().get(collation.getFieldIndex()).getType();
      Expression fieldComparator = generateCollatorExpression(fieldType.getCollation());
      ParameterExpression parameter =
          Expressions.parameter(javaRowClass, "v");
      selector =
          Expressions.lambda(
              Function1.class,
              fieldReference(parameter, collation.getFieldIndex()),
              parameter);
      return Pair.of(selector,
          Expressions.call(
              fieldComparator == null ? BuiltInMethod.NULLS_COMPARATOR.method
                  : BuiltInMethod.NULLS_COMPARATOR2.method,
              Expressions.list(
                  (Expression) Expressions.constant(
                      collation.nullDirection
                          == RelFieldCollation.NullDirection.FIRST),
                  Expressions.constant(
                      collation.direction
                          == RelFieldCollation.Direction.DESCENDING))
                  .appendIfNotNull(fieldComparator)));
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
      final RelDataType fieldType = rowType.getFieldList().get(index).getType();
      final Expression fieldComparator = generateCollatorExpression(fieldType.getCollation());
      Expression arg0 = fieldReference(parameterV0, index);
      Expression arg1 = fieldReference(parameterV1, index);
      switch (Primitive.flavor(fieldClass(index))) {
      case OBJECT:
        arg0 = EnumUtils.convert(arg0, Comparable.class);
        arg1 = EnumUtils.convert(arg1, Comparable.class);
        break;
      default:
        break;
      }
      final boolean nullsFirst =
          collation.nullDirection
              == RelFieldCollation.NullDirection.FIRST;
      final boolean descending =
          collation.getDirection()
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
                      Expressions.list(
                          arg0,
                          arg1)
                          .appendIfNotNull(fieldComparator)))));
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

  @Override public Expression generateComparator(RelCollation collation) {
    return this.generateComparator(collation, fieldCollation -> {
      final int index = fieldCollation.getFieldIndex();
      final boolean nullsFirst =
          fieldCollation.nullDirection
              == RelFieldCollation.NullDirection.FIRST;
      final boolean descending =
          fieldCollation.getDirection()
              == RelFieldCollation.Direction.DESCENDING;
      return fieldNullable(index)
          ? (nullsFirst != descending
          ? "compareNullsFirst"
          : "compareNullsLast")
          : "compare";
    });
  }

  private Expression generateComparator(RelCollation collation,
      Function1<RelFieldCollation, String> compareMethodNameFunction) {
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
      final RelDataType fieldType = rowType.getFieldList().get(index).getType();
      final Expression fieldComparator = generateCollatorExpression(fieldType.getCollation());
      Expression arg0 = fieldReference(parameterV0, index);
      Expression arg1 = fieldReference(parameterV1, index);
      switch (Primitive.flavor(fieldClass(index))) {
      case OBJECT:
        arg0 = EnumUtils.convert(arg0, Comparable.class);
        arg1 = EnumUtils.convert(arg1, Comparable.class);
        break;
      default:
        break;
      }
      final boolean descending =
          fieldCollation.getDirection()
              == RelFieldCollation.Direction.DESCENDING;
      body.add(
          Expressions.statement(
              Expressions.assign(
                  parameterC,
                  Expressions.call(
                      Utilities.class,
                      compareMethodNameFunction.apply(fieldCollation),
                      Expressions.list(
                          arg0,
                          arg1)
                          .appendIfNotNull(fieldComparator)))));
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

  @Override public Expression generateMergeJoinComparator(RelCollation collation) {
    return this.generateComparator(collation, fieldCollation -> {
      // merge join keys must be sorted in ascending order, nulls last
      assert fieldCollation.nullDirection == RelFieldCollation.NullDirection.LAST;
      assert fieldCollation.getDirection() == RelFieldCollation.Direction.ASCENDING;
      return fieldNullable(fieldCollation.getFieldIndex())
          ? "compareNullsLastForMergeJoin"
          : "compare";
    });
  }

  @Override public RelDataType getRowType() {
    return rowType;
  }

  @Override public Expression record(List<Expression> expressions) {
    return format.record(javaRowClass, expressions);
  }

  @Override public Type getJavaRowType() {
    return javaRowClass;
  }

  @Override public Type getJavaFieldType(int index) {
    return format.javaFieldClass(typeFactory, rowType, index);
  }

  @Override public PhysType component(int fieldOrdinal) {
    final RelDataTypeField field = rowType.getFieldList().get(fieldOrdinal);
    RelDataType componentType =
        requireNonNull(field.getType().getComponentType(),
            () -> "field.getType().getComponentType() for " + field);
    return PhysTypeImpl.of(typeFactory,
        toStruct(componentType), format, false);
  }

  @Override public PhysType field(int ordinal) {
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

  @Override public @Nullable Expression comparer() {
    return format.comparer();
  }

  private List<Expression> fieldReferences(
      final Expression parameter, final List<Integer> fields) {
    return new AbstractList<Expression>() {
      @Override public Expression get(int index) {
        return fieldReference(parameter, fields.get(index));
      }

      @Override public int size() {
        return fields.size();
      }
    };
  }

  @Override public Class fieldClass(int field) {
    return fieldClasses.get(field);
  }

  @Override public boolean fieldNullable(int field) {
    return rowType.getFieldList().get(field).getType().isNullable();
  }

  @Override public Expression generateAccessor(
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
          EnumUtils.convert(
              fieldReference(v1, field0),
              returnType);
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
      return Expressions.lambda(Function1.class, getListExpression(list), v1);
    }
  }

  private static Expression getListExpressionAllowSingleElement(
      Expressions.FluentList<Expression> list) {
    assert list.size() > 0;

    if (list.size() == 1) {
      return Expressions.call(
          List.class,
          null,
          BuiltInMethod.LIST1.method,
          list);
    } else {
      return getListExpression(list);
    }
  }

  private static Expression getListExpression(Expressions.FluentList<Expression> list) {
    assert list.size() >= 2;

    switch (list.size()) {
    case 2:
      return Expressions.call(
              List.class,
              null,
              BuiltInMethod.LIST2.method,
              list);
    case 3:
      return Expressions.call(
              List.class,
              null,
              BuiltInMethod.LIST3.method,
              list);
    case 4:
      return Expressions.call(
              List.class,
              null,
              BuiltInMethod.LIST4.method,
              list);
    case 5:
      return Expressions.call(
              List.class,
              null,
              BuiltInMethod.LIST5.method,
              list);
    case 6:
      return Expressions.call(
              List.class,
              null,
              BuiltInMethod.LIST6.method,
              list);
    default:
      return Expressions.call(
              List.class,
              null,
              BuiltInMethod.LIST_N.method,
              Expressions.newArrayInit(Comparable.class, list));
    }
  }

  @Override public Expression generateAccessorWithoutNulls(List<Integer> fields) {
    if (fields.size() < 2) {
      return generateAccessor(fields);
    }

    ParameterExpression v1 = Expressions.parameter(javaRowClass, "v1");
    Expressions.FluentList<Expression> list = Expressions.list();
    for (int field : fields) {
      list.add(fieldReference(v1, field));
    }

    // (v1.<field0> == null)
    //   ? null
    //   : (v1.<field1> == null)
    //     ? null;
    //     : ...
    //         : FlatLists.of(...);
    Expression exp = getListExpression(list);
    for (int i = list.size() - 1; i >= 0; i--) {
      exp =
          Expressions.condition(
              Expressions.equal(list.get(i), Expressions.constant(null)),
              Expressions.constant(null),
              exp);
    }
    return Expressions.lambda(Function1.class, exp, v1);
  }

  @Override public Expression generateNullAwareAccessor(
      List<Integer> fields,
      List<Boolean> nullExclusionFlags) {
    assert fields.size() == nullExclusionFlags.size();
    ParameterExpression v1 = Expressions.parameter(javaRowClass, "v1");
    if (fields.isEmpty()) {
      return Expressions.lambda(
          Function1.class,
          Expressions.field(
              null,
              BuiltInMethod.COMPARABLE_EMPTY_LIST.field),
          v1);
    }
    Expressions.FluentList<Expression> list = Expressions.list();
    for (int field : fields) {
      list.add(fieldReference(v1, field));
    }

    // in the HashJoin key selector scenario, when there is exactly one join key and it is
    // null-safe, a row whose join key is null must still be correctly recognized and extracted.
    // Therefore, when list.size() == 1, this method returns a list containing a single
    // element (which may be null) rather than returning the element directly.
    Expression exp = getListExpressionAllowSingleElement(list);
    for (int i = list.size() - 1; i >= 0; i--) {
      if (nullExclusionFlags.get(i)) {
        exp =
            Expressions.condition(
                Expressions.equal(list.get(i), Expressions.constant(null)),
                Expressions.constant(null),
                exp);
      }
    }
    return Expressions.lambda(Function1.class, exp, v1);
  }

  @Override public Expression fieldReference(
      Expression expression, int field) {
    return fieldReference(expression, field, null);
  }

  @Override public Expression fieldReference(
      Expression expression, int field, @Nullable Type storageType) {
    Type fieldType;
    if (storageType == null) {
      storageType = fieldClass(field);
      fieldType = null;
    } else {
      fieldType = fieldClass(field);
      if (fieldType != java.sql.Date.class
          && fieldType != java.sql.Time.class
          && fieldType != java.sql.Timestamp.class) {
        fieldType = null;
      }
    }
    return format.field(expression, field, fieldType, storageType);
  }
}
