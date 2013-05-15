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

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.runtime.SqlFunctions;

import org.eigenbase.rel.Aggregation;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
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
    private final BlockBuilder list;
    private final Map<RexNode, Boolean> exprNullableMap;

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
        this(
            program, typeFactory, inputGetter, list,
            Collections.<RexNode, Boolean>emptyMap(),
            new RexBuilder(typeFactory));
    }

    private RexToLixTranslator(
        RexProgram program,
        JavaTypeFactory typeFactory,
        InputGetter inputGetter,
        BlockBuilder list,
        Map<RexNode, Boolean> exprNullableMap,
        RexBuilder builder)
    {
        this.program = program;
        this.typeFactory = typeFactory;
        this.inputGetter = inputGetter;
        this.list = list;
        this.exprNullableMap = exprNullableMap;
        this.builder = builder;
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
            .translateList(program.getProjectList());
    }

    Expression translate(RexNode expr) {
        final RexImpTable.NullAs nullAs =
            RexImpTable.NullAs.of(isNullable(expr));
        return translate(expr, nullAs);
    }

    Expression translate(RexNode expr, RexImpTable.NullAs nullAs) {
        Expression expression = translate0(expr, nullAs);
        assert expression != null;
        return list.append("v", expression);
    }

    Expression translateCast(
        RelDataType sourceType,
        RelDataType targetType,
        Expression operand)
    {
        Expression convert = null;
        switch (targetType.getSqlTypeName()) {
        case BOOLEAN:
            switch (sourceType.getSqlTypeName()) {
            case CHAR:
            case VARCHAR:
                convert = Expressions.call(
                    BuiltinMethod.STRING_TO_BOOLEAN.method,
                    operand);
            }
            break;
        case CHAR:
        case VARCHAR:
            switch (sourceType.getSqlTypeName()) {
            case DATE:
                convert = RexImpTable.optimize2(
                    operand,
                    Expressions.call(
                        BuiltinMethod.UNIX_DATE_TO_STRING.method,
                        operand));
                break;
            case TIME:
                convert = RexImpTable.optimize2(
                    operand,
                    Expressions.call(
                        BuiltinMethod.UNIX_TIME_TO_STRING.method,
                        operand));
                break;
            case TIMESTAMP:
                convert = RexImpTable.optimize2(
                    operand,
                    Expressions.call(
                        BuiltinMethod.UNIX_TIMESTAMP_TO_STRING.method,
                        operand));
                break;
            case BOOLEAN:
                convert = RexImpTable.optimize2(
                    operand,
                    Expressions.call(
                        BuiltinMethod.BOOLEAN_TO_STRING.method,
                        operand));
                break;
            }
        }
        if (convert == null) {
            convert = convert(operand, typeFactory.getJavaClass(targetType));
        }
        // Going from CHAR(n), trim.
        switch (sourceType.getSqlTypeName()) {
        case CHAR:
            switch (targetType.getSqlTypeName()) {
            case VARCHAR:
                convert = Expressions.call(
                    BuiltinMethod.TRIM.method, convert);
            }
        }
        // Going from anything to CHAR(n) or VARCHAR(n), make sure value is no
        // longer than n.
        truncate:
        switch (targetType.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
            final int targetPrecision = targetType.getPrecision();
            if (targetPrecision >= 0) {
                switch (sourceType.getSqlTypeName()) {
                case CHAR:
                case VARCHAR:
                    // If this is a widening cast, no need to truncate.
                    final int sourcePrecision = sourceType.getPrecision();
                    if (sourcePrecision >= 0
                        && sourcePrecision <= targetPrecision)
                    {
                        break truncate;
                    }
                default:
                    convert =
                        Expressions.call(
                            BuiltinMethod.TRUNCATE.method,
                            convert,
                            Expressions.constant(targetPrecision));
                }
            }
            break;
        case TIMESTAMP:
            int targetScale = targetType.getScale();
            if (targetScale == RelDataType.SCALE_NOT_SPECIFIED) {
                targetScale = 0;
            }
            if (targetScale < sourceType.getScale()) {
                convert =
                    Expressions.call(
                        BuiltinMethod.ROUND_LONG.method,
                        convert,
                        Expressions.constant(
                            (long) Math.pow(10, 3 - targetScale)));
            }
            break;
        }
        return convert;
    }

    /** Translates an expression that is not in the cache.
     *
     * @param expr Expression
     * @param nullAs If false, if expression is definitely not null at
     *   runtime. Therefore we can optimize. For example, we can cast to int
     *   using x.intValue().
     * @return Translated expression
     */
    private Expression translate0(RexNode expr, RexImpTable.NullAs nullAs) {
        if (nullAs == RexImpTable.NullAs.NULL && !expr.getType().isNullable()) {
            nullAs = RexImpTable.NullAs.NOT_POSSIBLE;
        }
        if (expr instanceof RexInputRef) {
            final int index = ((RexInputRef) expr).getIndex();
            Expression x = inputGetter.field(list, index);
            return list.append(
                "v",
                nullAs.handle(
                    list.append("v", x)));
        }
        if (expr instanceof RexLocalRef) {
            return translate(
                program.getExprList().get(((RexLocalRef) expr).getIndex()),
                nullAs);
        }
        if (expr instanceof RexLiteral) {
            return translateLiteral(
                expr,
                nullifyType(
                    expr.getType(),
                    isNullable(expr)
                    && nullAs != RexImpTable.NullAs.NOT_POSSIBLE),
                typeFactory,
                nullAs);
        }
        if (expr instanceof RexCall) {
            final RexCall call = (RexCall) expr;
            final SqlOperator operator = call.getOperator();
            RexImpTable.CallImplementor implementor =
                RexImpTable.INSTANCE.get(operator);
            if (implementor != null) {
                return implementor.implement(this, call, nullAs);
            }
        }
        switch (expr.getKind()) {
        default:
            throw new RuntimeException(
                "cannot translate expression " + expr);
        }
    }

    /** Translates a literal.
     *
     * @throws AlwaysNull if literal is null but {@code nullAs} is
     * {@link net.hydromatic.optiq.rules.java.RexImpTable.NullAs#NOT_POSSIBLE}.
     */
    public static Expression translateLiteral(
        RexNode expr,
        RelDataType type,
        JavaTypeFactory typeFactory,
        RexImpTable.NullAs nullAs)
    {
        Type javaClass = typeFactory.getJavaClass(type);
        final RexLiteral literal = (RexLiteral) expr;
        Comparable value = literal.getValue();
        if (value == null) {
            switch (nullAs) {
            case TRUE:
            case IS_NULL:
                return RexImpTable.TRUE_EXPR;
            case FALSE:
            case IS_NOT_NULL:
                return RexImpTable.FALSE_EXPR;
            case NOT_POSSIBLE:
                throw new AlwaysNull();
            }
        } else {
            switch (nullAs) {
            case IS_NOT_NULL:
                return RexImpTable.TRUE_EXPR;
            case IS_NULL:
                return RexImpTable.FALSE_EXPR;
            }
        }
        final Object value2;
        switch (literal.getType().getSqlTypeName()) {
        case DECIMAL:
            assert javaClass == BigDecimal.class;
            return Expressions.new_(
                BigDecimal.class,
                Arrays.<Expression>asList(
                    Expressions.constant(value.toString())));
        case DATE:
            value2 =
                (int) (((Calendar) value).getTimeInMillis() / MILLIS_IN_DAY);
            break;
        case TIME:
            value2 =
                (int) (((Calendar) value).getTimeInMillis() % MILLIS_IN_DAY);
            break;
        case TIMESTAMP:
            value2 = ((Calendar) value).getTimeInMillis();
            break;
        case CHAR:
        case VARCHAR:
            value2 = value == null ? null : ((NlsString) value).getValue();
            break;
        default:
            final Primitive primitive = Primitive.ofBoxOr(javaClass);
            if (primitive != null && value instanceof Number) {
                value2 = primitive.number((Number) value);
            } else {
                value2 = value;
            }
        }
        return Expressions.constant(value2, javaClass);
    }

    public List<Expression> translateList(
        List<RexNode> operandList,
        RexImpTable.NullAs nullAs)
    {
        final List<Expression> list = new ArrayList<Expression>();
        for (RexNode rex : operandList) {
            list.add(translate(rex, nullAs));
        }
        return list;
    }

    List<Expression> translateList(List<? extends RexNode> operandList) {
        final List<Expression> list = new ArrayList<Expression>();
        for (RexNode rex : operandList) {
            final Expression translate = translate(rex);
            list.add(translate);
            if (!isNullable(rex)) {
                assert !Primitive.isBox(translate.getType())
                    : "Not-null boxed primitive should come back as primitive: "
                      + rex + ", " + translate.getType();
            }
        }
        return list;
    }

    public static Expression translateCondition(
        RexProgram program,
        JavaTypeFactory typeFactory,
        BlockBuilder list,
        InputGetter inputGetter)
    {
        if (program.getCondition() == null) {
            return RexImpTable.TRUE_EXPR;
        }
        final RexToLixTranslator translator =
            new RexToLixTranslator(program, typeFactory, inputGetter, list);
        return translator.translate(
            program.getCondition(),
            RexImpTable.NullAs.FALSE);
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
        if (fromType == String.class) {
            if (toPrimitive != null) {
                switch (toPrimitive) {
                case CHAR:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                    // Generate "SqlFunctions.parseChar(x)".
                    return Expressions.call(
                        SqlFunctions.class,
                        "parse"
                        + SqlFunctions.initcap(toPrimitive.primitiveName),
                        operand);
                default:
                    // Generate "Short.parseShort(x)".
                    return Expressions.call(
                        toPrimitive.boxClass,
                        "parse"
                        + SqlFunctions.initcap(toPrimitive.primitiveName),
                        operand);
                }
            }
            if (toBox != null) {
                switch (toBox) {
                case CHAR:
                    // Generate "SqlFunctions.charValueOf(x)".
                    return Expressions.call(
                        SqlFunctions.class,
                        toBox.primitiveName + "ValueOf",
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
            if (fromBox == null
                && !(fromType instanceof Class
                     && Number.class.isAssignableFrom((Class) fromType)))
            {
                // E.g. from "Object" to "short".
                // Generate "((Short) x).shortValue()".
                operand = Expressions.convert_(operand, toPrimitive.boxClass);
                // fall through
            }
            // Generate "x.shortValue()".
            return Expressions.unbox(operand, toPrimitive);
        } else if (fromBox != null && toBox != null) {
            // E.g. from "Short" to "Integer"
            // Generate "x == null ? null : Integer.valueOf(x.intValue())"
            return Expressions.condition(
                Expressions.equal(operand, RexImpTable.NULL_EXPR),
                RexImpTable.NULL_EXPR,
                Expressions.box(
                    Expressions.unbox(operand, toBox),
                    toBox));
        } else if (fromBox != null && toType == BigDecimal.class) {
            // E.g. from "Integer" to "BigDecimal".
            // Generate "x == null ? null : new BigDecimal(x.intValue())"
            return Expressions.condition(
                Expressions.equal(operand, RexImpTable.NULL_EXPR),
                RexImpTable.NULL_EXPR,
                Expressions.new_(
                    BigDecimal.class,
                    Arrays.<Expression>asList(
                        Expressions.unbox(operand, fromBox))));
        } else if (fromPrimitive != null && toType == BigDecimal.class) {
            // E.g. from "int" to "BigDecimal".
            // Generate "new BigDecimal(x)"
            return Expressions.new_(
                BigDecimal.class, Collections.singletonList(operand));
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
                    Expressions.equal(operand, RexImpTable.NULL_EXPR),
                    RexImpTable.NULL_EXPR,
                    Expressions.call(
                        SqlFunctions.class,
                        "toString",
                        operand));
            } else {
                // E.g. from "BigDecimal" to "String"
                // Generate "x == null ? null : x.toString()"
                return Expressions.condition(
                    Expressions.equal(operand, RexImpTable.NULL_EXPR),
                    RexImpTable.NULL_EXPR,
                    Expressions.call(
                        operand,
                        "toString"));
            }
        }
        return Expressions.convert_(operand, toType);
    }

    private static <T> T elvis(T t0, T t1) {
        return t0 != null ? t0 : t1;
    }

    private static <T> T elvis(T t0, T t1, T t2) {
        return t0 != null ? t0 : t1 != null ? t1 : t2;
    }

    public Expression translateConstructor(
        List<RexNode> operandList, SqlKind kind)
    {
        switch (kind) {
        case MAP_VALUE_CONSTRUCTOR:
            Expression map =
                list.append(
                    "map",
                    Expressions.new_(LinkedHashMap.class));
            for (int i = 0; i < operandList.size(); i++) {
                RexNode key = operandList.get(i++);
                RexNode value = operandList.get(i);
                list.add(
                    Expressions.statement(
                        Expressions.call(
                            map,
                            BuiltinMethod.MAP_PUT.method,
                            Expressions.box(translate(key)),
                            Expressions.box(translate(value)))));
            }
            return map;
        case ARRAY_VALUE_CONSTRUCTOR:
            Expression lyst =
                list.append(
                    "list",
                    Expressions.new_(ArrayList.class));
            for (RexNode value : operandList) {
                list.add(
                    Expressions.statement(
                        Expressions.call(
                            lyst,
                            BuiltinMethod.LIST_ADD.method,
                            Expressions.box(translate(value)))));
            }
            return lyst;
        default:
            throw new AssertionError("unexpected: " + kind);
        }
    }

    /** Returns whether an expression is nullable. Even if its type says it is
     * nullable, if we have previously generated a check to make sure that it is
     * not null, we will say so.
     *
     * <p>For example, {@code WHERE a == b} translates to
     * {@code a != null && b != null && a.equals(b)}. When translating the
     * 3rd part of the disjunction, we already know a and b are not null.</p>
     *
     * @param e Expression
     * @return Whether expression is nullable in the current translation context
     */
    public boolean isNullable(RexNode e) {
        final Boolean b = exprNullableMap.get(e);
        if (b != null) {
            return b;
        }
        return e.getType().isNullable();
    }

    /** Creates a read-only copy of this translator that records that a given
     * expression is nullable. */
    public RexToLixTranslator setNullable(RexNode e, boolean nullable) {
        // TODO: use linked-list, to avoid copying whole map & translator
        // each time
        final Map<RexNode, Boolean> map =
            new HashMap<RexNode, Boolean>(exprNullableMap);
        map.put(e, nullable);
        return new RexToLixTranslator(
            program, typeFactory, inputGetter, list, map, builder);
    }

    public RelDataType nullifyType(RelDataType type, boolean nullable) {
        final Primitive primitive = javaPrimitive(type);
        if (primitive != null) {
            return typeFactory.createJavaType(primitive.primitiveClass);
        } else {
            return typeFactory.createTypeWithNullability(type, nullable);
        }
    }

    private Primitive javaPrimitive(RelDataType type) {
        if (type instanceof RelDataTypeFactoryImpl.JavaType) {
            return Primitive.ofBox(
                ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass());
        }
      return null;
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

    /** Thrown in the unusual (but not erroneous) situation where the expression
     * we are translating is the null literal but we have already checked that
     * it is not null. It is easier to throw (and caller will always handle)
     * than to check exhaustively beforehand. */
    static class AlwaysNull extends RuntimeException {
        AlwaysNull() {
        }
    }
}

// End RexToLixTranslator.java
