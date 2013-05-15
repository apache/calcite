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
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import net.hydromatic.optiq.runtime.Executable;
import net.hydromatic.optiq.runtime.Utilities;

import org.eigenbase.rel.RelImplementorImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelImplementor;
import org.eigenbase.rex.RexBuilder;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Subclass of {@link RelImplementor} for relational operators
 * of one of the {@link EnumerableConvention} calling
 * conventions.
 *
 * @author jhyde
 */
public class EnumerableRelImplementor extends RelImplementorImpl {
    public Map<String, Queryable> map = new LinkedHashMap<String, Queryable>();

    public EnumerableRelImplementor(RexBuilder rexBuilder) {
        super(rexBuilder);
    }

    public BlockExpression visitChild(
        EnumerableRel parent,
        int ordinal,
        EnumerableRel child)
    {
        return (BlockExpression) super.visitChild(parent, ordinal, child);
    }

    public BlockExpression visitChildInternal(RelNode child, int ordinal) {
        return ((EnumerableRel) child).implement(this);
    }

    public ClassDeclaration implementRoot(EnumerableRel rootRel) {
        final BlockExpression implement = rootRel.implement(this);
        List<MemberDeclaration> memberDeclarations =
            new ArrayList<MemberDeclaration>();
        declareSyntheticClasses(implement, memberDeclarations);

        ParameterExpression root =
            Expressions.parameter(Modifier.FINAL, DataContext.class, "root");
        memberDeclarations.add(
            Expressions.methodDecl(
                Modifier.PUBLIC,
                Enumerable.class,
                BuiltinMethod.EXECUTABLE_EXECUTE.method.getName(),
                Expressions.list(root),
                implement));
        memberDeclarations.add(
            Expressions.methodDecl(
                Modifier.PUBLIC,
                Type.class,
                BuiltinMethod.TYPED_GET_ELEMENT_TYPE.method.getName(),
                Collections.<ParameterExpression>emptyList(),
                Blocks.toFunctionBlock(
                    Expressions.return_(
                        null,
                        Expressions.constant(
                            rootRel.getPhysType().getJavaRowType())))));
        return Expressions.classDecl(
            Modifier.PUBLIC,
            "Baz",
            null,
            Collections.<Type>singletonList(Executable.class),
            memberDeclarations);
    }

    private void declareSyntheticClasses(
        BlockExpression implement,
        List<MemberDeclaration> memberDeclarations)
    {
        final LinkedHashSet<Type> types = new LinkedHashSet<Type>();
        implement.accept(new TypeFinder(types));
        for (Type type : types) {
            if (type instanceof JavaTypeFactoryImpl.SyntheticRecordType) {
                memberDeclarations.add(
                    classDecl((JavaTypeFactoryImpl.SyntheticRecordType) type));
            }
        }
    }

    private ClassDeclaration classDecl(
        JavaTypeFactoryImpl.SyntheticRecordType type)
    {
        ClassDeclaration classDeclaration =
            Expressions.classDecl(
                Modifier.PUBLIC | Modifier.STATIC,
                type.getName(),
                null,
                Collections.<Type>emptyList(),
                new ArrayList<MemberDeclaration>());

        // For each field:
        //   public T0 f0;
        //   ...
        for (Types.RecordField field : type.getRecordFields()) {
            classDeclaration.memberDeclarations.add(
                Expressions.fieldDecl(
                    Modifier.PUBLIC,
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
                            Arrays.<Expression>asList(
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
        blockBuilder4.add(
            Expressions.declare(
                0, cParameter, null));
        final ConditionalStatement conditionalStatement =
            Expressions.ifThen(
                Expressions.notEqual(cParameter, constantZero),
                Expressions.return_(null, cParameter));
        for (Types.RecordField field : type.getRecordFields()) {
            blockBuilder4.add(
                Expressions.statement(
                    Expressions.assign(
                        cParameter,
                        Expressions.call(
                            Utilities.class,
                            field.nullable() ? "compareNullsLast" : "compare",
                            Expressions.field(thisParameter, field),
                            Expressions.field(thatParameter, field)))));
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
        String name = "v" + map.size();
        map.put(name, queryable);
        return Expressions.variable(queryable.getClass(), name);
    }

    private static class TypeFinder extends Visitor {
        private final LinkedHashSet<Type> types;

        TypeFinder(LinkedHashSet<Type> types) {
            this.types = types;
        }

        @Override
        public Expression visit(
            NewExpression newExpression,
            List<Expression> arguments,
            List<MemberDeclaration> memberDeclarations)
        {
            types.add(newExpression.type);
            return super.visit(
                newExpression,
                arguments,
                memberDeclarations);
        }

        @Override
        public Expression visit(
            NewArrayExpression newArrayExpression,
            List<Expression> expressions)
        {
            Type type = newArrayExpression.type;
            for (;;) {
                final Type componentType = Types.getComponentType(type);
                if (componentType == null) {
                    break;
                }
                type = componentType;
            }
            types.add(type);
            return super.visit(
                newArrayExpression,
                expressions);
        }
    }
}

// End EnumerableRelImplementor.java
