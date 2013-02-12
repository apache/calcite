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
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.BasicSqlType;
import org.eigenbase.util.*;

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

    private static final long MILLIS_IN_DAY = 24 * 60 * 60 * 1000;

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

    /** Translates a boolean expression such that null values become false. */
    Expression translateCondition(RexNode expr) {
        return translate(
            builder.makeCall(
                SqlStdOperatorTable.isTrueOperator, expr));
    }

    Expression translate(RexNode expr) {
        return translate(expr, expr.getType().isNullable());
    }

    Expression translate(RexNode expr, boolean mayBeNull) {
        Expression expression = translate0(expr, mayBeNull);
        assert expression != null;
        return list.append("v", expression);
    }

    Expression translateCast(RexNode expr, RelDataType type) {
        // It's only possible for the result to be null if both expression and
        // target type are nullable. We assume that the caller did not make a
        // mistake. If expression looks nullable, caller WILL have checked that
        // expression is not null before calling us.
        final boolean mayBeNull =
            expr.getType().isNullable() && type.isNullable();
        Expression operand = translate(expr, mayBeNull);
        final Expression convert;
        switch (expr.getType().getSqlTypeName()) {
        case DATE:
            convert = RexImpTable.optimize2(
                operand,
                Expressions.call(
                    SqlFunctions.class,
                    "dateToString",
                    operand));
            break;
        default:
            convert = convert(operand, typeFactory.getJavaClass(type));
        }
        switch (type.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
            if (type instanceof BasicSqlType
                && type.getPrecision() >= 0)
            {
                return RexImpTable.optimize2(
                    convert,
                    Expressions.call(
                        SqlFunctions.class,
                        "truncate",
                        convert,
                        Expressions.constant(type.getPrecision())));
            }
        }
        return convert;
    }

    /** Translates an expression that is not in the cache.
     *
     * @param expr Expression
     * @param mayBeNull If false, if expression is definitely not null at
     *   runtime. Therefore we can optimize. For example, we can cast to int
     *   using x.intValue().
     * @return Translated expression
     */
    private Expression translate0(RexNode expr, boolean mayBeNull) {
        if (expr instanceof RexInputRef) {
            final int index = ((RexInputRef) expr).getIndex();
            return inputGetter.field(list, index);
        }
        if (expr instanceof RexLocalRef) {
            return translate(
                program.getExprList().get(((RexLocalRef) expr).getIndex()),
                mayBeNull);
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
                return implementor.implement(this, call, mayBeNull);
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
        Type javaClass = typeFactory.getJavaClass(expr.getType());
        final RexLiteral literal = (RexLiteral) expr;
        switch (literal.getType().getSqlTypeName()) {
        case DECIMAL:
            assert javaClass == BigDecimal.class;
            return Expressions.new_(
                BigDecimal.class,
                Arrays.<Expression>asList(
                    Expressions.constant(literal.getValue().toString())));
        case DATE:
            return Expressions.constant(
                (int) (((Calendar) literal.getValue()).getTimeInMillis()
                       / MILLIS_IN_DAY),
                javaClass);
        case TIME:
            return Expressions.constant(
                (int) (((Calendar) literal.getValue()).getTimeInMillis()
                       % MILLIS_IN_DAY),
                javaClass);
        case TIMESTAMP:
            return Expressions.constant(
                ((Calendar) literal.getValue()).getTimeInMillis(), javaClass);
        case CHAR:
        case VARCHAR:
            final NlsString nlsString = (NlsString) literal.getValue();
            return Expressions.constant(
                nlsString == null ? null : nlsString.getValue(), javaClass);
        default:
            return Expressions.constant(literal.getValue(), javaClass);
        }
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
        final RexToLixTranslator translator =
            new RexToLixTranslator(
                program, typeFactory, inputGetter, list);
        return translator.translateCondition(program.getCondition());
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
        final Primitive toBox = Primitive.ofBox(toType);
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
        } else if (fromBox != null && toBox != null) {
            // E.g. from "Short" to "Integer"
            // Generate "x == null ? null : Integer.valueOf(x.intValue())"
          return Expressions.condition(
              Expressions.equal(operand, Expressions.constant(null)),
              Expressions.constant(null),
              Expressions.call(
                  toBox.boxClass,
                  "valueOf",
                  Expressions.call(operand, toBox.primitiveName + "Value")));
        } else if (fromBox != null && toType == BigDecimal.class) {
          // E.g. from "Integer" to "BigDecimal".
          // Generate "x == null ? null : new BigDecimal(x.intValue())"
          return Expressions.condition(
              Expressions.equal(operand, Expressions.constant(null)),
              Expressions.constant(null),
              Expressions.new_(
                  BigDecimal.class,
                  Arrays.<Expression>asList(
                      Expressions.call(
                          operand, fromBox.primitiveClass + "Value"))));
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
                // Generate "x.toString()"
              return Expressions.condition(
                  Expressions.equal(operand, Expressions.constant(null)),
                  Expressions.constant(null),
                  Expressions.call(
                      SqlFunctions.class,
                      "toString",
                      operand));
            } else {
              // E.g. from "BigDecimal" to "String"
              // Generate "x == null ? null : x.toString()"
              return Expressions.condition(
                  Expressions.equal(operand, Expressions.constant(null)),
                  Expressions.constant(null),
                  Expressions.call(
                      operand,
                      "toString"));
            }
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
