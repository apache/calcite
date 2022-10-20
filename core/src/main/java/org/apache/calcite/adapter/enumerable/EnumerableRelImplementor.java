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
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.ConditionalStatement;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.GotoStatement;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewArrayExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.apache.calcite.linq4j.tree.VisitorImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Equivalence;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Subclass of {@link org.apache.calcite.plan.RelImplementor} for relational
 * operators of {@link EnumerableConvention} calling convention.
 */
public class EnumerableRelImplementor extends JavaRelImplementor {
  public final Map<String, Object> map;
  private final Map<String, RexToLixTranslator.InputGetter> corrVars =
      new HashMap<>();
  private static final Equivalence<Object> IDENTITY = Equivalence.identity();
  // A combination of IdentityHashMap + LinkedHashMap to ensure deterministic order
  private final Map<Equivalence.Wrapper<Object>, ParameterExpression> stashedParameters =
      new LinkedHashMap<>();

  @SuppressWarnings("methodref.receiver.bound.invalid")
  protected final Function1<String, RexToLixTranslator.InputGetter> allCorrelateVariables =
      this::getCorrelVariableGetter;

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
    EnumerableRel.Result result;
    try {
      result = rootRel.implement(this, prefer);
    } catch (RuntimeException e) {
      IllegalStateException ex = new IllegalStateException("Unable to implement "
          + RelOptUtil.toString(rootRel, SqlExplainLevel.ALL_ATTRIBUTES));
      ex.addSuppressed(e);
      throw ex;
    }
    switch (prefer) {
    case ARRAY:
      if (result.physType.getFormat() == JavaRowFormat.ARRAY
          && rootRel.getRowType().getFieldCount() == 1) {
        BlockBuilder bb = new BlockBuilder();
        Expression e = null;
        for (Statement statement : result.block.statements) {
          if (statement instanceof GotoStatement) {
            e = bb.append("v",
                requireNonNull(((GotoStatement) statement).expression, "expression"));
          } else {
            bb.add(statement);
          }
        }
        if (e != null) {
          bb.add(
              Expressions.return_(null,
                  Expressions.call(null, BuiltInMethod.SLICE0.method, e)));
        }
        result = new EnumerableRel.Result(bb.toBlock(), result.physType,
            JavaRowFormat.SCALAR);
      }
      break;
    default:
      break;
    }

    final List<MemberDeclaration> memberDeclarations = new ArrayList<>();
    new TypeRegistrar(memberDeclarations).go(result);

    // This creates the following code
    // final Integer v1stashed = (Integer) root.get("v1stashed")
    // It is convenient for passing non-literal "compile-time" constants
    final Collection<Statement> stashed =
        Collections2.transform(stashedParameters.values(),
            input -> Expressions.declare(Modifier.FINAL, input,
                Expressions.convert_(
                    Expressions.call(DataContext.ROOT,
                        BuiltInMethod.DATA_CONTEXT_GET.method,
                        Expressions.constant(input.name)),
                    input.type)));

    final BlockStatement block = Expressions.block(
        Iterables.concat(
            stashed,
            result.block.statements));
    memberDeclarations.add(
        Expressions.methodDecl(
            Modifier.PUBLIC,
            Enumerable.class,
            BuiltInMethod.BINDABLE_BIND.method.getName(),
            Expressions.list(DataContext.ROOT),
            block));
    memberDeclarations.add(
        Expressions.methodDecl(Modifier.PUBLIC, Class.class,
            BuiltInMethod.TYPED_GET_ELEMENT_TYPE.method.getName(),
            ImmutableList.of(),
            Blocks.toFunctionBlock(
                Expressions.return_(null,
                    Expressions.constant(result.physType.getJavaRowType())))));
    return Expressions.classDecl(Modifier.PUBLIC,
        "Baz",
        null,
        Collections.singletonList(Bindable.class),
        memberDeclarations);
  }

  private static ClassDeclaration classDecl(
      JavaTypeFactoryImpl.SyntheticRecordType type) {
    ClassDeclaration classDeclaration =
        Expressions.classDecl(
            Modifier.PUBLIC | Modifier.STATIC,
            type.getName(),
            null,
            ImmutableList.of(Serializable.class),
            new ArrayList<>());

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
    final List<ParameterExpression> parameters = new ArrayList<>();
    final ParameterExpression thisParameter =
        Expressions.parameter(type, "this");

    // Here a constructor without parameter is used because the generated
    // code could cause error if number of fields is too large.
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
    final List<Expression> conditions = new ArrayList<>();
    for (Types.RecordField field : type.getRecordFields()) {
      conditions.add(
          Primitive.is(field.getType())
              ? Expressions.equal(
                  Expressions.field(thisParameter, field.getName()),
                  Expressions.field(thatParameter, field.getName()))
              : Expressions.call(BuiltInMethod.OBJECTS_EQUAL.method,
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
      final Method method = BuiltInMethod.HASH.method;
      blockBuilder3.add(
          Expressions.statement(
              Expressions.assign(
                  hParameter,
                  Expressions.call(
                      method.getDeclaringClass(),
                      method.getName(),
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
            Collections.emptyList(),
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
        final Method method = (field.nullable()
            ? BuiltInMethod.COMPARE_NULLS_LAST
            : BuiltInMethod.COMPARE).method;
        compareCall = Expressions.call(method.getDeclaringClass(),
            method.getName(),
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
            Collections.emptyList(),
            blockBuilder5.toBlock()));

    return classDeclaration;
  }

  /**
   * Stashes a value for the executor. Given values are de-duplicated if
   * identical (see {@link java.util.IdentityHashMap}).
   *
   * <p>For instance, to pass {@code ArrayList} to your method, you can use
   * {@code Expressions.call(method, implementor.stash(arrayList))}.
   *
   * <p>For simple literals (strings, numbers) the result is equivalent to
   * {@link org.apache.calcite.linq4j.tree.Expressions#constant(Object, java.lang.reflect.Type)}.
   *
   * <p>Note: the input value is held in memory as long as the statement
   * is alive. If you are using just a subset of its content, consider creating
   * a slimmer holder.
   *
   * @param input Value to be stashed
   * @param clazz Java class type of the value when it is used
   * @param <T> Java class type of the value when it is used
   * @return Expression that will represent {@code input} in runtime
   */
  public <T> Expression stash(T input, Class<? super T> clazz) {
    // Well-known final classes that can be used as literals
    if (input == null
        || input instanceof String
        || input instanceof Boolean
        || input instanceof Byte
        || input instanceof Short
        || input instanceof Integer
        || input instanceof Long
        || input instanceof Float
        || input instanceof Double) {
      return Expressions.constant(input, clazz);
    }
    final Equivalence.Wrapper<Object> key = IDENTITY.wrap(input);
    ParameterExpression cached = stashedParameters.get(key);
    if (cached != null) {
      return cached;
    }
    // "stashed" avoids name clash since this name will be used as the variable
    // name at the very start of the method.
    final String name = "v" + map.size() + "stashed";
    final ParameterExpression x = Expressions.variable(clazz, name);
    map.put(name, input);
    stashedParameters.put(key, x);
    return x;
  }

  public void registerCorrelVariable(final String name,
      final ParameterExpression pe,
      final BlockBuilder corrBlock, final PhysType physType) {
    corrVars.put(name, (list, index, storageType) -> {
      Expression fieldReference =
          physType.fieldReference(pe, index, storageType);
      return corrBlock.append(name + "_" + index, fieldReference);
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

  @Override public SqlConformance getConformance() {
    return (SqlConformance) map.getOrDefault("_conformance",
        SqlConformanceEnum.DEFAULT);
  }

  /** Visitor that finds types in an {@link Expression} tree. */
  @VisibleForTesting
  static class TypeFinder extends VisitorImpl<Void> {
    private final Collection<Type> types;

    TypeFinder(Collection<Type> types) {
      this.types = types;
    }

    @Override public Void visit(NewExpression newExpression) {
      types.add(newExpression.type);
      return super.visit(newExpression);
    }

    @Override public Void visit(NewArrayExpression newArrayExpression) {
      Type type = newArrayExpression.type;
      for (;;) {
        final Type componentType = Types.getComponentType(type);
        if (componentType == null) {
          break;
        }
        type = componentType;
      }
      types.add(type);
      return super.visit(newArrayExpression);
    }

    @Override public Void visit(ConstantExpression constantExpression) {
      final Object value = constantExpression.value;
      if (value instanceof Type) {
        types.add((Type) value);
      }
      if (value == null) {
        // null literal
        Type type = constantExpression.getType();
        types.add(type);
      }
      return super.visit(constantExpression);
    }

    @Override public Void visit(FunctionExpression functionExpression) {
      final List<ParameterExpression> list = functionExpression.parameterList;
      for (ParameterExpression pe : list) {
        types.add(pe.getType());
      }
      if (functionExpression.body == null) {
        return super.visit(functionExpression);
      }
      types.add(functionExpression.body.getType());
      return super.visit(functionExpression);
    }

    @Override public Void visit(UnaryExpression unaryExpression) {
      if (unaryExpression.nodeType == ExpressionType.Convert) {
        types.add(unaryExpression.getType());
      }
      return super.visit(unaryExpression);
    }
  }

  /** Adds a declaration of each synthetic type found in a code block. */
  private static class TypeRegistrar {
    private final List<MemberDeclaration> memberDeclarations;
    private final Set<Type> seen = new HashSet<>();

    TypeRegistrar(List<MemberDeclaration> memberDeclarations) {
      this.memberDeclarations = memberDeclarations;
    }

    private void register(Type type) {
      if (!seen.add(type)) {
        return;
      }
      if (type instanceof JavaTypeFactoryImpl.SyntheticRecordType) {
        memberDeclarations.add(
            classDecl((JavaTypeFactoryImpl.SyntheticRecordType) type));
      }
      if (type instanceof ParameterizedType) {
        for (Type type1 : ((ParameterizedType) type).getActualTypeArguments()) {
          register(type1);
        }
      }
    }

    public void go(EnumerableRel.Result result) {
      final Set<Type> types = new LinkedHashSet<>();
      result.block.accept(new TypeFinder(types));
      types.add(result.physType.getJavaRowType());
      for (Type type : types) {
        register(type);
      }
    }
  }
}
