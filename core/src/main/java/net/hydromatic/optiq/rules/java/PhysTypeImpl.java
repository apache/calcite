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
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.runtime.Utilities;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.util.Pair;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;

/** Implementation of {@link PhysType}. */
public class PhysTypeImpl implements PhysType {
  private final JavaTypeFactory typeFactory;
  private final RelDataType rowType;
  private final Type javaRowClass;
  private final List<Class> fieldClasses = new ArrayList<Class>();
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
      fieldClasses.add(
          JavaRules.EnumUtil.javaRowClass(typeFactory, field.getType()));
    }
  }

  public static PhysType of(
      JavaTypeFactory typeFactory,
      RelDataType rowType,
      JavaRowFormat format) {
    final JavaRowFormat format2 = format.optimize(rowType);
    final Type javaRowClass = format2.javaRowClass(typeFactory, rowType);
    return new PhysTypeImpl(
        typeFactory, rowType, javaRowClass, format2);
  }

  static PhysType of(
      final JavaTypeFactory typeFactory,
      Type javaRowClass) {
    final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
    if (javaRowClass instanceof Types.RecordType) {
      final Types.RecordType recordType = (Types.RecordType) javaRowClass;
      for (Types.RecordField field : recordType.getRecordFields()) {
        builder.add(field.getName(), typeFactory.createType(field.getType()));
      }
    }
    RelDataType rowType = builder.build();
    // Do not optimize if there are 0 or 1 fields.
    return new PhysTypeImpl(
        typeFactory, rowType, javaRowClass, JavaRowFormat.CUSTOM);
  }

  public JavaRowFormat getFormat() {
    return format;
  }

  public PhysType project(List<Integer> integers, JavaRowFormat format) {
    RelDataType projectedRowType =
        typeFactory.createStructType(
            Lists.transform(integers,
                new Function<Integer, RelDataTypeField>() {
                  public RelDataTypeField apply(Integer index) {
                    return rowType.getFieldList().get(index);
                  }
                }));
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
      return Expressions.call(BuiltinMethod.IDENTITY_SELECTOR.method);
    default:
      return Expressions.lambda(Function1.class,
          targetPhysType.record(fieldReferences(parameter, fields)),
          parameter);
    }
  }

  public Expression selector(
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
      return parameter;
    default:
      return targetPhysType.record(fieldReferences(parameter, fields));
    }
  }

  public List<Expression> accessors(Expression v1, List<Integer> argList) {
    final List<Expression> expressions = new ArrayList<Expression>();
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
      return Pair.<Expression, Expression>of(
          selector,
          Expressions.call(
              BuiltinMethod.NULLS_COMPARATOR.method,
              Expressions.constant(
                  collation.nullDirection
                      == RelFieldCollation.NullDirection.FIRST),
              Expressions.constant(
                  collation.getDirection()
                      == RelFieldCollation.Direction.DESCENDING)));
    }
    selector =
        Expressions.call(BuiltinMethod.IDENTITY_SELECTOR.method);

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
      body.add(
          Expressions.statement(
              Expressions.assign(
                  parameterC,
                  Expressions.call(
                      Utilities.class,
                      fieldNullable(index)
                          ? (nullsFirst
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
        Expressions.<MemberDeclaration>list(
            Expressions.methodDecl(
                Modifier.PUBLIC,
                int.class,
                "compare",
                ImmutableList.of(
                    parameterV0, parameterV1),
                body.toBlock()));

    if (JavaRules.BRIDGE_METHODS) {
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
                  BuiltinMethod.COMPARATOR_COMPARE.method,
                  Expressions.convert_(
                      parameterO0,
                      javaRowClass),
                  Expressions.convert_(
                      parameterO1,
                      javaRowClass))));
      memberDeclarations.add(
          JavaRules.EnumUtil.overridingMethodDecl(
              BuiltinMethod.COMPARATOR_COMPARE.method,
              ImmutableList.of(parameterO0, parameterO1),
              bridgeBody.toBlock()));
    }
    return Pair.<Expression, Expression>of(
        selector,
        Expressions.new_(
            Comparator.class,
            Collections.<Expression>emptyList(),
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
        Expressions.<MemberDeclaration>list(
            Expressions.methodDecl(
                Modifier.PUBLIC,
                int.class,
                "compare",
                ImmutableList.of(parameterV0, parameterV1),
                body.toBlock()));

    if (JavaRules.BRIDGE_METHODS) {
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
                  BuiltinMethod.COMPARATOR_COMPARE.method,
                  Expressions.convert_(
                      parameterO0,
                      javaRowClass),
                  Expressions.convert_(
                      parameterO1,
                      javaRowClass))));
      memberDeclarations.add(
          JavaRules.EnumUtil.overridingMethodDecl(
              BuiltinMethod.COMPARATOR_COMPARE.method,
              ImmutableList.of(parameterO0, parameterO1),
              bridgeBody.toBlock()));
    }
    return Expressions.new_(
        Comparator.class,
        Collections.<Expression>emptyList(),
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
              Collections.class,
              "EMPTY_LIST"),
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
                BuiltinMethod.LIST2.method,
                list),
            v1);
      case 3:
        return Expressions.lambda(
            Function1.class,
            Expressions.call(
                List.class,
                null,
                BuiltinMethod.LIST3.method,
                list),
            v1);
      default:
        return Expressions.lambda(
            Function1.class,
            Expressions.call(
                List.class,
                null,
                BuiltinMethod.ARRAYS_AS_LIST.method,
                Expressions.newArrayInit(
                    Object.class,
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
    if (storageType == null) {
      storageType = fieldClass(field);
    }
    return format.field(expression, field, storageType);
  }
}

// End PhysTypeImpl.java
