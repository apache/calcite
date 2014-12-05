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

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.ConditionalStatement;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewArrayExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.linq4j.tree.Visitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Subclass of {@link org.apache.calcite.plan.RelImplementor} for relational
 * operators of {@link EnumerableConvention} calling convention.
 */
public class EnumerableRelImplementor extends JavaRelImplementor {
  public final Map<String, Object> map;
  private final Map<String, RexToLixTranslator.InputGetter> corrVars =
      Maps.newHashMap();

  protected final Function1<String, RexToLixTranslator.InputGetter>
  allCorrelateVariables =
    new Function1<String, RexToLixTranslator.InputGetter>() {
      public RexToLixTranslator.InputGetter apply(String name) {
        return getCorrelVariableGetter(name);
      }
    };

  public EnumerableRelImplementor(RexBuilder rexBuilder,
      Map<String, Object> internalParameters) {
    super(rexBuilder);
    this.map = internalParameters;
  }

  public EnumerableRel.Result visitChild(
      EnumerableRel parent,
      int ordinal,
      EnumerableRel child,
      EnumerableRel.Prefer prefer) {
    if (parent != null) {
      assert child == parent.getInputs().get(ordinal);
    }
    return child.implement(this, prefer);
  }

  public ClassDeclaration implementRoot(EnumerableRel rootRel,
      EnumerableRel.Prefer prefer) {
    final EnumerableRel.Result result = rootRel.implement(this, prefer);
    List<MemberDeclaration> memberDeclarations =
        new ArrayList<MemberDeclaration>();
    declareSyntheticClasses(result.block, memberDeclarations);

    // The following is a workaround to
    // http://jira.codehaus.org/browse/JANINO-169. Otherwise we'd remove the
    // member variable, rename the "root0" parameter as "root", and reference it
    // directly from inner classes.
    final ParameterExpression root0_ =
        Expressions.parameter(Modifier.FINAL, DataContext.class, "root0");
    final BlockStatement block = Expressions.block(
        Iterables.concat(
            ImmutableList.of(
                Expressions.statement(
                    Expressions.assign(DataContext.ROOT, root0_))),
            result.block.statements));
    memberDeclarations.add(
        Expressions.fieldDecl(0, DataContext.ROOT, null));

    memberDeclarations.add(
        Expressions.methodDecl(
            Modifier.PUBLIC,
            Enumerable.class,
            BuiltInMethod.BINDABLE_BIND.method.getName(),
            Expressions.list(root0_),
            block));
    memberDeclarations.add(Expressions.methodDecl(Modifier.PUBLIC,
        Type.class,
        BuiltInMethod.TYPED_GET_ELEMENT_TYPE.method.getName(),
        Collections.<ParameterExpression>emptyList(),
        Blocks.toFunctionBlock(Expressions.return_(null,
            Expressions.constant(result.physType.getJavaRowType())))));
    return Expressions.classDecl(Modifier.PUBLIC,
        "Baz",
        null,
        Collections.<Type>singletonList(Bindable.class),
        memberDeclarations);
  }

  private void declareSyntheticClasses(
      BlockStatement block,
      List<MemberDeclaration> memberDeclarations) {
    final LinkedHashSet<Type> types = new LinkedHashSet<Type>();
    block.accept(new TypeFinder(types));
    for (Type type : types) {
      if (type instanceof JavaTypeFactoryImpl.SyntheticRecordType) {
        memberDeclarations.add(
            classDecl((JavaTypeFactoryImpl.SyntheticRecordType) type));
      }
    }
  }

  private ClassDeclaration classDecl(
      JavaTypeFactoryImpl.SyntheticRecordType type) {
    ClassDeclaration classDeclaration =
        Expressions.classDecl(
            Modifier.PUBLIC | Modifier.STATIC,
            type.getName(),
            null,
            ImmutableList.<Type>of(Serializable.class),
            new ArrayList<MemberDeclaration>());

    // For each field:
    //   public T0 f0;
    //   ...
    for (Types.RecordField field : type.getRecordFields()) {
      classDeclaration.memberDeclarations.add(
          Expressions.fieldDecl(
              field.getModifiers(),
              Expressions.parameter(
                  field.getType(), field.getName()),
              null));
    }

    // Constructor:
    //   Foo(T0 f0, ...) { this.f0 = f0; ... }
    final BlockBuilder blockBuilder = new BlockBuilder();
    final List<ParameterExpression> parameters =
        new ArrayList<ParameterExpression>();
    final ParameterExpression thisParameter =
        Expressions.parameter(type, "this");
    for (Types.RecordField field : type.getRecordFields()) {
      final ParameterExpression parameter =
          Expressions.parameter(field.getType(), field.getName());
      parameters.add(parameter);
      blockBuilder.add(
          Expressions.statement(
              Expressions.assign(
                  Expressions.field(
                      thisParameter,
                      field),
                  parameter)));
    }
    classDeclaration.memberDeclarations.add(
        Expressions.constructorDecl(
            Modifier.PUBLIC,
            type,
            parameters,
            blockBuilder.toBlock()));

    // equals method():
    //   public boolean equals(Object o) {
    //       if (this == o) return true;
    //       if (!(o instanceof MyClass)) return false;
    //       final MyClass that = (MyClass) o;
    //       return this.f0 == that.f0
    //         && equal(this.f1, that.f1)
    //         ...
    //   }
    final BlockBuilder blockBuilder2 = new BlockBuilder();
    final ParameterExpression thatParameter =
        Expressions.parameter(type, "that");
    final ParameterExpression oParameter =
        Expressions.parameter(Object.class, "o");
    blockBuilder2.add(
        Expressions.ifThen(
            Expressions.equal(thisParameter, oParameter),
            Expressions.return_(null, Expressions.constant(true))));
    blockBuilder2.add(
        Expressions.ifThen(
            Expressions.not(
                Expressions.typeIs(oParameter, type)),
            Expressions.return_(null, Expressions.constant(false))));
    blockBuilder2.add(
        Expressions.declare(
            Modifier.FINAL,
            thatParameter,
            Expressions.convert_(oParameter, type)));
    List<Expression> conditions = new ArrayList<Expression>();
    for (Types.RecordField field : type.getRecordFields()) {
      conditions.add(
          Primitive.is(field.getType())
              ? Expressions.equal(
                  Expressions.field(thisParameter, field.getName()),
                  Expressions.field(thatParameter, field.getName()))
              : Expressions.call(
                  Utilities.class,
                  "equal",
                  Expressions.field(thisParameter, field.getName()),
                  Expressions.field(thatParameter, field.getName())));
    }
    blockBuilder2.add(
        Expressions.return_(null, Expressions.foldAnd(conditions)));
    classDeclaration.memberDeclarations.add(
        Expressions.methodDecl(
            Modifier.PUBLIC,
            boolean.class,
            "equals",
            Collections.singletonList(oParameter),
            blockBuilder2.toBlock()));

    // hashCode method:
    //   public int hashCode() {
    //     int h = 0;
    //     h = hash(h, f0);
    //     ...
    //     return h;
    //   }
    final BlockBuilder blockBuilder3 = new BlockBuilder();
    final ParameterExpression hParameter =
        Expressions.parameter(int.class, "h");
    final ConstantExpression constantZero =
        Expressions.constant(0);
    blockBuilder3.add(
        Expressions.declare(0, hParameter, constantZero));
    for (Types.RecordField field : type.getRecordFields()) {
      blockBuilder3.add(
          Expressions.statement(
              Expressions.assign(
                  hParameter,
                  Expressions.call(
                      Utilities.class,
                      "hash",
                      ImmutableList.of(
                          hParameter,
                          Expressions.field(thisParameter, field))))));
    }
    blockBuilder3.add(
        Expressions.return_(null, hParameter));
    classDeclaration.memberDeclarations.add(
        Expressions.methodDecl(
            Modifier.PUBLIC,
            int.class,
            "hashCode",
            Collections.<ParameterExpression>emptyList(),
            blockBuilder3.toBlock()));

    // compareTo method:
    //   public int compareTo(MyClass that) {
    //     int c;
    //     c = compare(this.f0, that.f0);
    //     if (c != 0) return c;
    //     ...
    //     return 0;
    //   }
    final BlockBuilder blockBuilder4 = new BlockBuilder();
    final ParameterExpression cParameter =
        Expressions.parameter(int.class, "c");
    final int mod = type.getRecordFields().size() == 1 ? Modifier.FINAL : 0;
    blockBuilder4.add(
        Expressions.declare(mod, cParameter, null));
    final ConditionalStatement conditionalStatement =
        Expressions.ifThen(
            Expressions.notEqual(cParameter, constantZero),
            Expressions.return_(null, cParameter));
    for (Types.RecordField field : type.getRecordFields()) {
      MethodCallExpression compareCall;
      try {
        compareCall = Expressions.call(
            Utilities.class,
            field.nullable() ? "compareNullsLast" : "compare",
            Expressions.field(thisParameter, field),
            Expressions.field(thatParameter, field));
      } catch (RuntimeException e) {
        if (e.getCause() instanceof NoSuchMethodException) {
          // Just ignore the field in compareTo
          // "create synthetic record class" blindly creates compareTo for
          // all the fields, however not all the records will actually be used
          // as sorting keys (e.g. temporary state for aggregate calculation).
          // In those cases it is fine if we skip the problematic fields.
          continue;
        }
        throw e;
      }
      blockBuilder4.add(
          Expressions.statement(
              Expressions.assign(
                  cParameter,
                  compareCall)));
      blockBuilder4.add(conditionalStatement);
    }
    blockBuilder4.add(
        Expressions.return_(null, constantZero));
    classDeclaration.memberDeclarations.add(
        Expressions.methodDecl(
            Modifier.PUBLIC,
            int.class,
            "compareTo",
            Collections.singletonList(thatParameter),
            blockBuilder4.toBlock()));

    // toString method:
    //   public String toString() {
    //     return "{f0=" + f0
    //       + ", f1=" + f1
    //       ...
    //       + "}";
    //   }
    final BlockBuilder blockBuilder5 = new BlockBuilder();
    Expression expression5 = null;
    for (Types.RecordField field : type.getRecordFields()) {
      if (expression5 == null) {
        expression5 =
            Expressions.constant("{" + field.getName() + "=");
      } else {
        expression5 =
            Expressions.add(
                expression5,
                Expressions.constant(", " + field.getName() + "="));
      }
      expression5 =
          Expressions.add(
              expression5,
              Expressions.field(thisParameter, field.getName()));
    }
    expression5 =
        expression5 == null
            ? Expressions.constant("{}")
            : Expressions.add(
                expression5,
                Expressions.constant("}"));
    blockBuilder5.add(
        Expressions.return_(
            null,
            expression5));
    classDeclaration.memberDeclarations.add(
        Expressions.methodDecl(
            Modifier.PUBLIC,
            String.class,
            "toString",
            Collections.<ParameterExpression>emptyList(),
            blockBuilder5.toBlock()));

    return classDeclaration;
  }

  public Expression register(Queryable queryable) {
    return register(queryable, queryable.getClass());
  }

  public ParameterExpression register(Object o, Class clazz) {
    final String name = "v" + map.size();
    map.put(name, o);
    return Expressions.variable(clazz, name);
  }

  public Expression stash(RelNode child, Class clazz) {
    final ParameterExpression x = register(child, clazz);
    final Expression e = Expressions.call(getRootExpression(),
        BuiltInMethod.DATA_CONTEXT_GET.method, Expressions.constant(x.name));
    return Expressions.convert_(e, clazz);
  }

  public void registerCorrelVariable(final String name,
      final ParameterExpression pe,
      final BlockBuilder corrBlock, final PhysType physType) {
    corrVars.put(name, new RexToLixTranslator.InputGetter() {
      public Expression field(BlockBuilder list, int index, Type storageType) {
        Expression fieldReference =
            physType.fieldReference(pe, index, storageType);
        return corrBlock.append(name + "_" + index, fieldReference);
      }
    });
  }

  public void clearCorrelVariable(String name) {
    assert corrVars.containsKey(name) : "Correlation variable " + name
        + " should be defined";
    corrVars.remove(name);
  }

  public RexToLixTranslator.InputGetter getCorrelVariableGetter(String name) {
    assert corrVars.containsKey(name) : "Correlation variable " + name
        + " should be defined";
    return corrVars.get(name);
  }

  public EnumerableRel.Result result(PhysType physType, BlockStatement block) {
    return new EnumerableRel.Result(
        block, physType, ((PhysTypeImpl) physType).format);
  }

  /** Visitor that finds types in an {@link Expression} tree. */
  private static class TypeFinder extends Visitor {
    private final LinkedHashSet<Type> types;

    TypeFinder(LinkedHashSet<Type> types) {
      this.types = types;
    }

    @Override public Expression visit(
        NewExpression newExpression,
        List<Expression> arguments,
        List<MemberDeclaration> memberDeclarations) {
      types.add(newExpression.type);
      return super.visit(
          newExpression,
          arguments,
          memberDeclarations);
    }

    @Override public Expression visit(NewArrayExpression newArrayExpression,
        int dimension, Expression bound, List<Expression> expressions) {
      Type type = newArrayExpression.type;
      for (;;) {
        final Type componentType = Types.getComponentType(type);
        if (componentType == null) {
          break;
        }
        type = componentType;
      }
      types.add(type);
      return super.visit(newArrayExpression, dimension, bound, expressions);
    }
  }
}

// End EnumerableRelImplementor.java
