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

import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.runtime.SqlFunctions;

import org.eigenbase.rel.Aggregation;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;

import static org.eigenbase.sql.fun.SqlStdOperatorTable.*;

/**
 * Translates {@link org.eigenbase.rex.RexNode REX expressions} to
 * {@link Expression linq4j expressions}.
 *
 * @author jhyde
 */
public class RexToLixTranslator {
    public static final Map<Method, SqlOperator> JAVA_TO_SQL_METHOD_MAP =
        Util.<Method, SqlOperator>mapOf(
            findMethod(String.class, "toUpperCase"), upperFunc,
            findMethod(
                SqlFunctions.class, "substring", String.class, Integer.TYPE,
                Integer.TYPE),
            substringFunc,
            findMethod(SqlFunctions.class, "charLength", String.class),
            characterLengthFunc,
            findMethod(SqlFunctions.class, "charLength", String.class),
            charLengthFunc);

    final JavaTypeFactory typeFactory;
    final RexBuilder builder;
    private final RexProgram program;
    private final RexToLixTranslator.InputGetter inputGetter;

    private BlockBuilder list;

    private static Method findMethod(
        Class<?> clazz, String name, Class... parameterTypes)
    {
        try {
            return clazz.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private RexToLixTranslator(
        RexProgram program,
        JavaTypeFactory typeFactory,
        InputGetter inputGetter,
        BlockBuilder list)
    {
        this.program = program;
        this.typeFactory = typeFactory;
        this.inputGetter = inputGetter;
        this.list = list;
        this.builder = new RexBuilder(typeFactory);
    }

    /**
     * Translates a {@link RexProgram} to a sequence of expressions and
     * declarations.
     *
     * @param program Program to be translated
     * @param typeFactory Type factory
     * @param list List of statements, populated with declarations
     * @param inputGetter Generates expressions for inputs
     * @return Sequence of expressions, optional condition
     */
    public static List<Expression> translateProjects(
        RexProgram program,
        JavaTypeFactory typeFactory,
        BlockBuilder list,
        InputGetter inputGetter)
    {
        return new RexToLixTranslator(program, typeFactory, inputGetter, list)
            .translate(program.getProjectList());
    }

    Expression translate(RexNode expr) {
        Expression expression = translate0(expr);
        assert expression != null;
        return list.append("v", expression);
    }

    Expression translateCast(RexNode expr, RelDataType type) {
        Expression operand = translate(expr);
        return convert(
            operand,
            typeFactory.getJavaClass(type));
    }

    private Expression translate0(RexNode expr) {
        if (expr instanceof RexInputRef) {
            final int index = ((RexInputRef) expr).getIndex();
            return inputGetter.field(list, index);
        }
        if (expr instanceof RexLocalRef) {
            return translate(
                program.getExprList().get(((RexLocalRef) expr).getIndex()));
        }
        if (expr instanceof RexLiteral) {
            return translateLiteral(expr, null, typeFactory);
        }
        if (expr instanceof RexCall) {
            final RexCall call = (RexCall) expr;
            final SqlOperator operator = call.getOperator();
            RexImpTable.CallImplementor implementor =
                RexImpTable.INSTANCE.get(operator);
            if (implementor != null) {
                return implementor.implement(this, call);
            }
        }
        switch (expr.getKind()) {
        default:
            throw new RuntimeException(
                "cannot translate expression " + expr);
        }
    }

    /** Translates a literal. */
    public static Expression translateLiteral(
        RexNode expr,
        RelDataType type,
        JavaTypeFactory typeFactory)
    {
        Object o = ((RexLiteral) expr).getValue3();
        Type javaClass;
        if (type == null) {
            javaClass = typeFactory.getJavaClass(expr.getType());
        } else {
            if (type.getSqlTypeName() == SqlTypeName.VARCHAR) {
                o = SqlFunctions.rtrim(o.toString());
            }
            javaClass = typeFactory.getJavaClass(type);
        }
        if (javaClass == BigDecimal.class) {
            return Expressions.new_(
                BigDecimal.class,
                Arrays.<Expression>asList(
                    Expressions.constant(
                        o.toString())));
        }
        return Expressions.constant(o, javaClass);
    }

    List<Expression> translateList(List<RexNode> operandList) {
        final List<Expression> list = new ArrayList<Expression>();
        for (RexNode rex : operandList) {
            list.add(translate(rex));
        }
        return list;
    }

    private List<Expression> translate(
        List<RexLocalRef> rexList)
    {
        List<Expression> translateds = new ArrayList<Expression>();
        for (RexNode rexExpr : rexList) {
            translateds.add(translate(rexExpr));
        }
        return translateds;
    }

    public static Expression translateCondition(
        RexProgram program,
        JavaTypeFactory typeFactory,
        BlockBuilder list,
        InputGetter inputGetter)
    {
        if (program.getCondition() == null) {
            return Expressions.constant(true);
        }
        List<Expression> x =
            new RexToLixTranslator(program, typeFactory, inputGetter, list)
                .translate(
                    Collections.singletonList(program.getCondition()));
        assert x.size() == 1;
        return x.get(0);
    }

    public static Expression translateAggregate(
        Expression grouping,
        Aggregation aggregation,
        Expression accessor)
    {
        final RexImpTable.AggregateImplementor implementor =
            RexImpTable.INSTANCE.aggMap.get(aggregation);
        if (aggregation == countOperator) {
            // FIXME: count(x) and count(distinct x) don't work currently
            accessor = null;
        }
        if (implementor != null) {
            return implementor.implementAggregate(grouping, accessor);
        }
        throw new AssertionError("unknown agg " + aggregation);
    }

    public static Expression convert(Expression operand, Type toType) {
        final Type fromType = operand.getType();
        if (fromType.equals(toType)) {
            return operand;
        }
        // E.g. from "Short" to "int".
        // Generate "x.intValue()".
        final Primitive toPrimitive = Primitive.of(toType);
        final Primitive fromBox = Primitive.ofBox(fromType);
        final Primitive fromPrimitive = Primitive.of(fromType);
        if (toPrimitive != null) {
            if (fromPrimitive != null) {
                // E.g. from "float" to "double"
                return Expressions.convert_(
                    operand, toPrimitive.primitiveClass);
            }
            if (fromBox == null
                && !(fromType instanceof Class
                     && Number.class.isAssignableFrom((Class) fromType)))
            {
                // E.g. from "Object" to "short".
                // Generate "(Short) x".
                return Expressions.convert_(operand, toPrimitive.boxClass);
            }
            return Expressions.call(
                operand, toPrimitive.primitiveName + "Value");
        }
        return Expressions.convert_(operand, toType);
    }

    /** Translates a field of an input to an expression. */
    public interface InputGetter {
        Expression field(BlockBuilder list, int index);
    }

    /** Implementation of {@link InputGetter} that calls
     * {@link PhysType#fieldReference}. */
    public static class InputGetterImpl implements InputGetter {
        private List<Pair<Expression, PhysType>> inputs;

        public InputGetterImpl(List<Pair<Expression, PhysType>> inputs) {
            this.inputs = inputs;
        }

        public Expression field(BlockBuilder list, int index) {
            final Pair<Expression, PhysType> input = inputs.get(0);
            final PhysType physType = input.right;
            final Expression left = list.append("current" + index, input.left);
            return physType.fieldReference(left, index);
        }
    }
}

// End RexToLixTranslator.java
