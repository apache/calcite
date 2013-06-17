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
package org.eigenbase.rex;

import java.math.*;

import java.nio.*;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.SqlIntervalQualifier.TimeUnit;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.DateTimeUtil;

import net.hydromatic.optiq.runtime.SqlFunctions;


/**
 * Factory for row expressions.
 *
 * <p>Some common literal values (NULL, TRUE, FALSE, 0, 1, '') are cached.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 23, 2003
 */
public class RexBuilder
{
    /** Special operator that accesses an unadvertised field of an input record.
     * This operator cannot be used in SQL queries; it is introduced temporarily
     * during sql-to-rel translation, then replaced during the process that
     * trims unwanted fields. */
    public static final SqlSpecialOperator GET_OPERATOR =
        new SqlSpecialOperator("_get", SqlKind.OTHER_FUNCTION);

    //~ Instance fields --------------------------------------------------------

    protected final RelDataTypeFactory typeFactory;
    private final RexLiteral booleanTrue;
    private final RexLiteral booleanFalse;
    private final RexLiteral charEmpty;
    private final RexLiteral constantNull;
    private final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RexBuilder.
     *
     * @param typeFactory Type factory
     */
    public RexBuilder(RelDataTypeFactory typeFactory)
    {
        this.typeFactory = typeFactory;
        this.booleanTrue =
            makeLiteral(
                Boolean.TRUE,
                typeFactory.createSqlType(SqlTypeName.BOOLEAN),
                SqlTypeName.BOOLEAN);
        this.booleanFalse =
            makeLiteral(
                Boolean.FALSE,
                typeFactory.createSqlType(SqlTypeName.BOOLEAN),
                SqlTypeName.BOOLEAN);
        this.charEmpty =
            makeLiteral(
                new NlsString("", null, null),
                typeFactory.createSqlType(SqlTypeName.CHAR, 0),
                SqlTypeName.CHAR);
        this.constantNull =
            makeLiteral(
                null,
                typeFactory.createSqlType(SqlTypeName.NULL),
                SqlTypeName.NULL);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns this RexBuilder's type factory
     *
     * @return type factory
     */
    public RelDataTypeFactory getTypeFactory()
    {
        return typeFactory;
    }

    /**
     * Returns this RexBuilder's operator table
     *
     * @return operator table
     */
    public SqlStdOperatorTable getOpTab()
    {
        return opTab;
    }

    /**
     * Creates an expression accessing a given named field from a record.
     *
     * @param expr Expression yielding a record
     * @param fieldName Name of field in record
     *
     * @return Expression accessing a given named field
     */
    public RexNode makeFieldAccess(
        RexNode expr,
        String fieldName)
    {
        final RelDataType type = expr.getType();
        final RelDataTypeField field = type.getField(fieldName);
        if (field == null) {
            throw Util.newInternal(
                "Type '" + type + "' has no field '"
                + fieldName + "'");
        }
        return makeFieldAccessInternal(expr, field);
    }

    /**
     * Creates an expression accessing a field with a given ordinal from a
     * record.
     *
     * @param expr Expression yielding a record
     * @param i Ordinal of field
     *
     * @return Expression accessing given field
     */
    public RexNode makeFieldAccess(
        RexNode expr,
        int i)
    {
        final RelDataType type = expr.getType();
        final RelDataTypeField [] fields = type.getFields();
        if ((i < 0) || (i >= fields.length)) {
            throw Util.newInternal(
                "Field ordinal " + i + " is invalid for "
                + " type '" + type + "'");
        }
        return makeFieldAccessInternal(expr, fields[i]);
    }

    /**
     * Creates an expression accessing a given field from a record.
     *
     * @param expr Expression yielding a record
     * @param field Field
     *
     * @return Expression accessing given field
     */
    private RexNode makeFieldAccessInternal(
        RexNode expr,
        final RelDataTypeField field)
    {
        if (expr instanceof RexRangeRef) {
            RexRangeRef range = (RexRangeRef) expr;
            if (field.getIndex() < 0) {
                return makeCall(
                    field.getType(),
                    GET_OPERATOR,
                    expr,
                    makeLiteral(field.getName()));
            }
            return new RexInputRef(
                range.getOffset() + field.getIndex(),
                field.getType());
        }
        return new RexFieldAccess(expr, field);
    }

    /**
     * Creates a call with an array of arguments and a predetermined type.
     */
    public RexNode makeCall(
        RelDataType returnType,
        SqlOperator op,
        RexNode ... exprs)
    {
        return new RexCall(
            returnType,
            op,
            exprs);
    }

    /**
     * Creates a call with an array of arguments.
     *
     * <p>This is the fundamental method called by all of the other <code>
     * makeCall</code> methods. If you derive a class from {@link RexBuilder},
     * this is the only method you need to override.</p>
     */
    public RexNode makeCall(
        SqlOperator op,
        RexNode ... exprs)
    {
        // TODO jvs 12-Jun-2010:  Find a better place for this;
        // it surely does not belong here.
        if (op == SqlStdOperatorTable.andOperator
            && exprs.length == 2
            && exprs[0].equals(exprs[1]))
        {
            // Avoid generating 'AND(x, x)'; this can cause plan explosions if a
            // relnode is its own child and is merged with itself.
            return exprs[0];
        }

        final RelDataType type = deriveReturnType(op, typeFactory, exprs);
        return new RexCall(type, op, exprs);
    }

    /**
     * Creates a call with a list of arguments.
     *
     * <p>Equivalent to <code>makeCall(op, exprList.toArray(new
     * RexNode[exprList.size()]))</code>.
     */
    public final RexNode makeCall(
        SqlOperator op,
        List<? extends RexNode> exprList)
    {
        return makeCall(op, exprList.toArray(new RexNode[exprList.size()]));
    }

    /**
     * Derives the return type of a call to an operator.
     *
     * @param op the operator being called
     * @param typeFactory factory for return type
     * @param exprs actual operands
     *
     * @return derived type
     */
    public RelDataType deriveReturnType(
        SqlOperator op,
        RelDataTypeFactory typeFactory,
        RexNode [] exprs)
    {
        return op.inferReturnType(new RexCallBinding(typeFactory, op, exprs));
    }

    /**
     * Creates a reference to an aggregate call, checking for repeated calls.
     */
    public RexNode addAggCall(
        AggregateCall aggCall,
        int groupCount,
        List<AggregateCall> aggCalls,
        Map<AggregateCall, RexNode> aggCallMapping)
    {
        RexNode rex = aggCallMapping.get(aggCall);
        if (rex == null) {
            int index = aggCalls.size() + groupCount;
            aggCalls.add(aggCall);
            rex = makeInputRef(aggCall.getType(), index);
            aggCallMapping.put(aggCall, rex);
        }
        return rex;
    }

    /**
     * Creates a call to a windowed agg.
     */
    public RexNode makeOver(
        RelDataType type,
        SqlAggFunction operator,
        RexNode [] exprs,
        RexNode [] partitionKeys,
        RexNode [] orderKeys,
        SqlNode lowerBound,
        SqlNode upperBound,
        boolean physical,
        boolean allowPartial,
        boolean nullWhenCountZero)
    {
        assert operator != null;
        assert exprs != null;
        assert partitionKeys != null;
        assert orderKeys != null;
        final RexWindow window =
            makeWindow(
                partitionKeys,
                orderKeys,
                lowerBound,
                upperBound,
                physical);
        // Windowed aggregates are nullable, because the window might be empty.
        type = typeFactory.createTypeWithNullability(type, true);
        final RexOver over = new RexOver(type, operator, exprs, window);
        RexNode result = over;

        // This should be correct but need time to go over test results.
        // Also want to look at combing with section below.
        if (nullWhenCountZero) {
            final RelDataType bigintType = getTypeFactory().createSqlType(
                SqlTypeName.BIGINT);
            result = makeCall(
                SqlStdOperatorTable.caseOperator,
                makeCall(
                    SqlStdOperatorTable.greaterThanOperator,
                    new RexOver(
                        bigintType,
                        SqlStdOperatorTable.countOperator,
                        exprs,
                        window),
                    makeLiteral(
                        new BigDecimal(0),
                        bigintType,
                        SqlTypeName.DECIMAL)),
                over,
                makeCast(over.getType(), constantNull()));
        }
        if (!allowPartial) {
            Util.permAssert(physical, "DISALLOW PARTIAL over RANGE");
            final RelDataType bigintType = getTypeFactory().createSqlType(
                SqlTypeName.BIGINT);
            // todo: read bound
            result =
                makeCall(
                    SqlStdOperatorTable.caseOperator,
                    makeCall(
                        SqlStdOperatorTable.greaterThanOrEqualOperator,
                        new RexOver(
                            bigintType,
                            SqlStdOperatorTable.countOperator,
                            RexNode.EMPTY_ARRAY,
                            window),
                        makeLiteral(
                            new BigDecimal(2),
                            bigintType,
                            SqlTypeName.DECIMAL)),
                    result,
                    constantNull);
        }
        return result;
    }

    /**
     * Creates a window specification.
     *
     * @param partitionKeys Partition keys
     * @param orderKeys Order keys
     * @param lowerBound Lower bound
     * @param upperBound Upper bound
     * @param physical Whether physical. True if row-based, false if range-based
     * @return window specification
     */
    public RexWindow makeWindow(
        RexNode[] partitionKeys,
        RexNode[] orderKeys,
        SqlNode lowerBound,
        SqlNode upperBound,
        boolean physical)
    {
        return new RexWindow(
            partitionKeys,
            orderKeys,
            lowerBound,
            upperBound,
            physical);
    }

    /**
     * Creates a constant for the SQL <code>NULL</code> value.
     */
    public RexLiteral constantNull()
    {
        return constantNull;
    }

    /**
     * Creates an expression referencing a correlation variable.
     *
     * @param type Type of variable
     * @param name Name of variable
     *
     * @return Correlation variable
     */
    public RexNode makeCorrel(
        RelDataType type,
        String name)
    {
        return new RexCorrelVariable(name, type);
    }

    /**
     * Creates an invocation of the NEW operator.
     *
     * @param type Type to be instantiated
     * @param exprs Arguments to NEW operator
     *
     * @return Expression invoking NEW operator
     */
    public RexNode makeNewInvocation(
        RelDataType type,
        RexNode [] exprs)
    {
        return new RexCall(
            type,
            SqlStdOperatorTable.newOperator,
            exprs);
    }

    /**
     * Creates a call to the CAST operator, expanding if possible.
     *
     * @param type Type to cast to
     * @param exp Expression being cast
     *
     * @return Call to CAST operator
     */
    public RexNode makeCast(
        RelDataType type,
        RexNode exp)
    {
        final SqlTypeName sqlType = type.getSqlTypeName();
        if (exp instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) exp;
            Comparable value = literal.getValue();
            if (RexLiteral.valueMatchesType(value, sqlType, false)
                && (!(value instanceof NlsString)
                    || (type.getPrecision()
                       >= ((NlsString) value).getValue().length())))
            {
                switch (literal.getTypeName()) {
                case CHAR:
                    if (value instanceof NlsString) {
                        value = ((NlsString) value).rtrim();
                    }
                    break;
                case TIMESTAMP:
                case TIME:
                    final Calendar calendar = (Calendar) value;
                    int scale = type.getScale();
                    if (scale == RelDataType.SCALE_NOT_SPECIFIED) {
                        scale = 0;
                    }
                    calendar.setTimeInMillis(
                        SqlFunctions.round(
                            calendar.getTimeInMillis(),
                            SqlFunctions.power(10, 3 - scale)));
                    break;
                }
                return makeLiteral(value, type, literal.getTypeName());
            }
        } else if (SqlTypeUtil.isInterval(type)
            && SqlTypeUtil.isExactNumeric(exp.getType()))
        {
            return makeCastExactToInterval(type, exp);
        } else if (SqlTypeUtil.isExactNumeric(type)
            && SqlTypeUtil.isInterval(exp.getType()))
        {
            return makeCastIntervalToExact(type, exp);
        } else if (sqlType == SqlTypeName.BOOLEAN
            && SqlTypeUtil.isExactNumeric(exp.getType()))
        {
            return makeCastExactToBoolean(type, exp);
        } else if (exp.getType().getSqlTypeName()  == SqlTypeName.BOOLEAN
            && SqlTypeUtil.isExactNumeric(type))
        {
            return makeCastBooleanToExact(type, exp);
        }
        return makeAbstractCast(type, exp);
    }

    private RexNode makeCastExactToBoolean(RelDataType toType, RexNode exp)
    {
        return makeCall(
            toType,
            SqlStdOperatorTable.notEqualsOperator,
            exp,
            makeZeroLiteral(exp.getType()));
    }

    private RexNode makeCastBooleanToExact(RelDataType toType, RexNode exp)
    {
        final RexNode casted = makeCall(
            SqlStdOperatorTable.caseOperator,
            exp,
            makeExactLiteral(new BigDecimal(1), toType),
            makeZeroLiteral(toType));
        if (!exp.getType().isNullable()) {
            return casted;
        }
        return makeCall(
            toType,
            SqlStdOperatorTable.caseOperator,
            makeCall(SqlStdOperatorTable.isNotNullOperator, exp),
            casted,
            makeNullLiteral(toType.getSqlTypeName()));
    }

    private RexNode makeCastIntervalToExact(RelDataType toType, RexNode exp)
    {
        IntervalSqlType intervalType = (IntervalSqlType) exp.getType();
        TimeUnit endUnit = intervalType.getIntervalQualifier().getEndUnit();
        if (endUnit == null) {
            endUnit = intervalType.getIntervalQualifier().getStartUnit();
        }
        int scale = 0;
        if (endUnit == TimeUnit.SECOND) {
            scale = Math.min(
                intervalType.getIntervalQualifier()
                .getFractionalSecondPrecision(), 3);
        }
        BigDecimal multiplier = BigDecimal.valueOf(endUnit.multiplier)
            .divide(BigDecimal.TEN.pow(scale));
        RexNode value = decodeIntervalOrDecimal(exp);
        if (multiplier.longValue() != 1) {
            value = makeCall(
                SqlStdOperatorTable.divideIntegerOperator,
                value, makeBigintLiteral(multiplier));
        }
        if (scale > 0) {
            RelDataType decimalType =
                getTypeFactory().createSqlType(
                    SqlTypeName.DECIMAL,
                    scale + intervalType.getPrecision(),
                    scale);
            value = encodeIntervalOrDecimal(value, decimalType, false);
        }
        return ensureType(toType, value, false);
    }

    private RexNode makeCastExactToInterval(RelDataType toType, RexNode exp)
    {
        IntervalSqlType intervalType = (IntervalSqlType) toType;
        TimeUnit endUnit = intervalType.getIntervalQualifier().getEndUnit();
        if (endUnit == null) {
            endUnit = intervalType.getIntervalQualifier().getStartUnit();
        }
        int scale = 0;
        if (endUnit == TimeUnit.SECOND) {
            scale = Math.min(
                intervalType.getIntervalQualifier()
                .getFractionalSecondPrecision(), 3);
        }
        BigDecimal multiplier = BigDecimal.valueOf(endUnit.multiplier)
            .divide(BigDecimal.TEN.pow(scale));
        RelDataType decimalType =
            getTypeFactory().createSqlType(
                SqlTypeName.DECIMAL,
                scale + intervalType.getPrecision(),
                scale);
        RexNode value = decodeIntervalOrDecimal(
            ensureType(decimalType, exp, true));
        if (multiplier.longValue() != 1) {
            value = makeCall(
                SqlStdOperatorTable.multiplyOperator,
                value, makeExactLiteral(multiplier));
        }
        return encodeIntervalOrDecimal(value, toType, false);
    }

    /**
     * Casts a decimal's integer representation to a decimal node. If the
     * expression is not the expected integer type, then it is casted first.
     *
     * <p>An overflow check may be requested to ensure the internal value
     * does not exceed the maximum value of the decimal type.
     *
     * @param value integer representation of decimal
     * @param type type integer will be reinterpreted as
     * @param checkOverflow indicates whether an overflow check is required
     * when reinterpreting this particular value as the decimal type. A
     * check usually not required for arithmetic, but is often required for
     * rounding and explicit casts.
     *
     * @return the integer reinterpreted as an opaque decimal type
     */
    public RexNode encodeIntervalOrDecimal(
        RexNode value,
        RelDataType type,
        boolean checkOverflow)
    {
        RelDataType bigintType =
            typeFactory.createSqlType(
                SqlTypeName.BIGINT);
        RexNode cast = ensureType(bigintType, value, true);
        return makeReinterpretCast(
            type,
            cast,
            makeLiteral(checkOverflow));
    }

    /**
     * Retrieves an interval or decimal node's integer representation
     *
     * @param node the interval or decimal value as an opaque type
     *
     * @return an integer representation of the decimal value
     */
    public RexNode decodeIntervalOrDecimal(RexNode node)
    {
        assert (SqlTypeUtil.isDecimal(node.getType())
                || SqlTypeUtil.isInterval(node.getType()));
        RelDataType bigintType =
            typeFactory.createSqlType(
                SqlTypeName.BIGINT);
        return makeReinterpretCast(
            matchNullability(bigintType, node),
            node,
            makeLiteral(false));
    }

    /**
     * Creates a call to the CAST operator.
     *
     * @param type Type to cast to
     * @param exp Expression being cast
     *
     * @return Call to CAST operator
     */
    public RexNode makeAbstractCast(
        RelDataType type,
        RexNode exp)
    {
        return new RexCall(
            type,
            SqlStdOperatorTable.castFunc,
            new RexNode[] { exp });
    }

    /**
     * Makes a reinterpret cast.
     *
     * @param type type returned by the cast
     * @param exp expression to be casted
     * @param checkOverflow whether an overflow check is required
     *
     * @return a RexCall with two operands and a special return type
     */
    public RexNode makeReinterpretCast(
        RelDataType type,
        RexNode exp,
        RexNode checkOverflow)
    {
        RexNode [] args;
        if ((checkOverflow != null) && checkOverflow.isAlwaysTrue()) {
            args = new RexNode[] { exp, checkOverflow };
        } else {
            args = new RexNode[] { exp };
        }
        return new RexCall(
            type,
            SqlStdOperatorTable.reinterpretOperator,
            args);
    }

    /**
     * Makes an expression which converts a value of type T to a value of type T
     * NOT NULL, or throws if the value is NULL. If the expression is already
     * NOT NULL, does nothing.
     */
    public RexNode makeNotNullCast(RexNode expr)
    {
        RelDataType type = expr.getType();
        if (!type.isNullable()) {
            return expr;
        }
        RelDataType typeNotNull =
            getTypeFactory().createTypeWithNullability(type, false);
        return new RexCall(
            typeNotNull,
            SqlStdOperatorTable.castFunc,
            new RexNode[] {
                expr
            });
    }

    /**
     * Creates a reference to all the fields in the row. That is, the whole row
     * as a single record object.
     *
     * @param rowType Type of the input row.
     */
    public RexNode makeRangeReference(RelDataType rowType)
    {
        return new RexRangeRef(rowType, 0);
    }

    /**
     * Creates a reference to all the fields in the row.
     *
     * <p>For example, if the input row has type <code>T{f0,f1,f2,f3,f4}</code>
     * then <code>makeRangeReference(T{f0,f1,f2,f3,f4}, S{f3,f4}, 3)</code> is
     * an expression which yields the last 2 fields.
     *
     * @param type Type of the resulting range record.
     * @param offset Index of first field.
     * @param nullable Whether the record is nullable.
     */
    public RexNode makeRangeReference(
        RelDataType type,
        int offset,
        boolean nullable)
    {
        if (nullable && !type.isNullable()) {
            type =
                typeFactory.createTypeWithNullability(
                    type,
                    nullable);
        }
        return new RexRangeRef(type, offset);
    }

    /**
     * Creates a reference to a given field of the input record.
     *
     * @param type Type of field
     * @param i Ordinal of field
     *
     * @return Reference to field
     */
    public RexInputRef makeInputRef(
        RelDataType type,
        int i)
    {
        type = SqlTypeUtil.addCharsetAndCollation(type, typeFactory);
        return new RexInputRef(i, type);
    }

    /**
     * Creates a literal representing a flag.
     *
     * @param flag Flag value; must be either a {@link
     * org.eigenbase.util14.Enum14.Value} or a {@link Enum}, and hence a {@link
     * Comparable}.
     */
    public RexLiteral makeFlag(
        Object flag)
    {
        assert flag != null;
        assert (flag instanceof EnumeratedValues.Value)
            || (flag instanceof Enum);
        assert flag instanceof Comparable;
        return makeLiteral(
            (Comparable) flag,
            typeFactory.createSqlType(SqlTypeName.SYMBOL),
            SqlTypeName.SYMBOL);
    }

    /**
     * Internal method to create a call to a literal. Code outside this package
     * should call one of the type-specific methods such as {@link
     * #makeDateLiteral(Calendar)}, {@link #makeLiteral(boolean)}, {@link
     * #makeLiteral(String)}.
     *
     * @param o Value of literal, must be appropriate for the type
     * @param type Type of literal
     * @param typeName SQL type of literal
     *
     * @return Literal
     */
    protected RexLiteral makeLiteral(
        Comparable o,
        RelDataType type,
        SqlTypeName typeName)
    {
        // All literals except NULL have NOT NULL types.
        type = typeFactory.createTypeWithNullability(type, o == null);
        if (typeName == SqlTypeName.CHAR) {
            // Character literals must have a charset and collation. Populate
            // from the type if necessary.
            assert o instanceof NlsString;
            NlsString nlsString = (NlsString) o;
            if ((nlsString.getCollation() == null)
                || (nlsString.getCharset() == null))
            {
                assert type.getSqlTypeName() == SqlTypeName.CHAR;
                assert type.getCharset().name() != null;
                assert type.getCollation() != null;
                o = new NlsString(
                    nlsString.getValue(),
                    type.getCharset().name(),
                    type.getCollation());
            }
        }
        return new RexLiteral(o, type, typeName);
    }

    /**
     * Creates a boolean literal.
     */
    public RexLiteral makeLiteral(boolean b)
    {
        return b ? booleanTrue : booleanFalse;
    }

    /**
     * Creates a numeric literal.
     */
    public RexLiteral makeExactLiteral(BigDecimal bd)
    {
        RelDataType relType;
        int scale = bd.scale();
        long l = bd.unscaledValue().longValue();
        assert ((scale >= 0) && (scale <= SqlTypeName.MAX_NUMERIC_SCALE));
        assert (BigDecimal.valueOf(l, scale).equals(bd));
        if (scale == 0) {
            if ((l >= Integer.MIN_VALUE) && (l <= Integer.MAX_VALUE)) {
                relType = typeFactory.createSqlType(SqlTypeName.INTEGER);
            } else {
                relType = typeFactory.createSqlType(SqlTypeName.BIGINT);
            }
        } else {
            int precision = bd.unscaledValue().toString().length();
            relType =
                typeFactory.createSqlType(
                    SqlTypeName.DECIMAL,
                    scale,
                    precision);
        }
        return makeExactLiteral(bd, relType);
    }

    /**
     * Creates a BIGINT literal.
     */
    public RexLiteral makeBigintLiteral(BigDecimal bd)
    {
        RelDataType bigintType =
            typeFactory.createSqlType(
                SqlTypeName.BIGINT);
        return makeLiteral(bd, bigintType, SqlTypeName.DECIMAL);
    }

    /**
     * Creates a numeric literal.
     */
    public RexLiteral makeExactLiteral(BigDecimal bd, RelDataType type)
    {
        return makeLiteral(
            bd, type, SqlTypeName.DECIMAL);
    }

    /**
     * Creates a byte array literal.
     */
    public RexLiteral makeBinaryLiteral(byte [] byteArray)
    {
        return makeLiteral(
            ByteBuffer.wrap(byteArray),
            typeFactory.createSqlType(
                SqlTypeName.BINARY,
                byteArray.length),
            SqlTypeName.BINARY);
    }

    /**
     * Creates a double-precision literal.
     */
    public RexLiteral makeApproxLiteral(BigDecimal bd)
    {
        // Validator should catch if underflow is allowed
        // If underflow is allowed, let underflow become zero
        if (bd.doubleValue() == 0) {
            bd = BigDecimal.ZERO;
        }
        return makeApproxLiteral(
            bd, typeFactory.createSqlType(SqlTypeName.DOUBLE));
    }

    /**
     * Creates an approximate numeric literal (double or float).
     *
     * @param bd literal value
     * @param type approximate numeric type
     *
     * @return new literal
     */
    public RexLiteral makeApproxLiteral(BigDecimal bd, RelDataType type)
    {
        assert (SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(
            type.getSqlTypeName()));
        return makeLiteral(
            bd,
            type,
            SqlTypeName.DOUBLE);
    }

    /**
     * Creates a character string literal.
     *
     * @pre s != null
     */
    public RexLiteral makeLiteral(String s)
    {
        return makePreciseStringLiteral(s);
    }

    /**
     * Creates a character string literal with type CHAR and default charset and
     * collation.
     *
     * @param s String value
     *
     * @return Character string literal
     */
    protected RexLiteral makePreciseStringLiteral(String s)
    {
        Util.pre(s != null, "s != null");
        if (s.equals("")) {
            return charEmpty;
        } else {
            return makeLiteral(
                new NlsString(s, null, null),
                typeFactory.createSqlType(
                    SqlTypeName.CHAR,
                    s.length()),
                SqlTypeName.CHAR);
        }
    }

    /**
     * Ensures expression is interpreted as a specified type. The returned
     * expression may be wrapped with a cast.
     *
     * @param type desired type
     * @param node expression
     * @param matchNullability whether to correct nullability of specified
     * type to match the expression; this usually should be true, except for
     * explicit casts which can override default nullability
     *
     * @return a casted expression or the original expression
     */
    public RexNode ensureType(
        RelDataType type,
        RexNode node,
        boolean matchNullability)
    {
        RelDataType targetType = type;
        if (matchNullability) {
            targetType = matchNullability(type, node);
        }
        if (!node.getType().equals(targetType)) {
            return makeCast(targetType, node);
        }
        return node;
    }

    /**
     * Ensures that a type's nullability matches a value's nullability.
     */
    public RelDataType matchNullability(
        RelDataType type,
        RexNode value)
    {
        boolean typeNullability = type.isNullable();
        boolean valueNullability = value.getType().isNullable();
        if (typeNullability != valueNullability) {
            return getTypeFactory().createTypeWithNullability(
                type,
                valueNullability);
        }
        return type;
    }

    /**
     * Creates a character string literal from an {@link NlsString}.
     *
     * <p>If the string's charset and collation are not set, uses the system
     * defaults.
     *
     * @pre str != null
     */
    public RexLiteral makeCharLiteral(NlsString str)
    {
        Util.pre(str != null, "str != null");
        RelDataType type = SqlUtil.createNlsStringType(typeFactory, str);
        return makeLiteral(str, type, SqlTypeName.CHAR);
    }

    /**
     * Creates a Date literal.
     *
     * @pre date != null
     */
    public RexLiteral makeDateLiteral(Calendar date)
    {
        Util.pre(date != null, "date != null");
        return makeLiteral(
            date,
            typeFactory.createSqlType(SqlTypeName.DATE),
            SqlTypeName.DATE);
    }

    /**
     * Creates a Time literal.
     *
     * @pre time != null
     */
    public RexLiteral makeTimeLiteral(
        Calendar time,
        int precision)
    {
        Util.pre(time != null, "time != null");
        return makeLiteral(
            time,
            typeFactory.createSqlType(SqlTypeName.TIME, precision),
            SqlTypeName.TIME);
    }

    /**
     * Creates a Timestamp literal.
     *
     * @pre timestamp != null
     */
    public RexLiteral makeTimestampLiteral(
        Calendar timestamp,
        int precision)
    {
        Util.pre(timestamp != null, "timestamp != null");
        return makeLiteral(
            timestamp,
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP, precision),
            SqlTypeName.TIMESTAMP);
    }

    /**
     * Creates an interval literal.
     */
    public RexLiteral makeIntervalLiteral(
        SqlIntervalQualifier intervalQualifier)
    {
        Util.pre(intervalQualifier != null, "intervalQualifier != null");
        return makeLiteral(
            null,
            typeFactory.createSqlIntervalType(intervalQualifier),
            intervalQualifier.isYearMonth() ? SqlTypeName.INTERVAL_YEAR_MONTH
            : SqlTypeName.INTERVAL_DAY_TIME);
    }

    /**
     * Creates an interval literal.
     */
    public RexLiteral makeIntervalLiteral(
        long l,
        SqlIntervalQualifier intervalQualifier)
    {
        return makeLiteral(
            new BigDecimal(l),
            typeFactory.createSqlIntervalType(intervalQualifier),
            intervalQualifier.isYearMonth() ? SqlTypeName.INTERVAL_YEAR_MONTH
            : SqlTypeName.INTERVAL_DAY_TIME);
    }

    /**
     * Creates a reference to a dynamic parameter
     *
     * @param type Type of dynamic parameter
     * @param index Index of dynamic parameter
     *
     * @return Expression referencing dynamic parameter
     */
    public RexDynamicParam makeDynamicParam(
        RelDataType type,
        int index)
    {
        return new RexDynamicParam(type, index);
    }

    /**
     * Creates an expression corresponding to a null literal, cast to a specific
     * type and precision
     *
     * @param typeName name of the type that the null will be cast to
     * @param precision precision of the type
     *
     * @return created expression
     */
    public RexNode makeNullLiteral(SqlTypeName typeName, int precision)
    {
        RelDataType type =
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(typeName, precision),
                true);
        return makeCast(
            type,
            constantNull());
    }

    /**
     * Creates a literal whose value is NULL, with a particular type.
     *
     * <p>The typing is necessary because RexNodes are strictly typed. For
     * example, in the Rex world the <code>NULL</code> parameter to <code>
     * SUBSTRING(NULL FROM 2 FOR 4)</code> must have a valid VARCHAR type so
     * that the result type can be determined.
     *
     * @param typeName Type to cast NULL to
     *
     * @return NULL literal of given type
     */
    public RexNode makeNullLiteral(SqlTypeName typeName)
    {
        RelDataType type =
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(typeName),
                true);
        return makeCast(
            type,
            constantNull());
    }

    /**
     * Creates a copy of an expression, which may have been created using a
     * different RexBuilder and/or {@link RelDataTypeFactory}, using this
     * RexBuilder.
     *
     * @param expr Expression
     *
     * @return Copy of expression
     *
     * @see RelDataTypeFactory#copyType(RelDataType)
     */
    public RexNode copy(RexNode expr)
    {
        return expr.accept(new RexCopier(this));
    }

    /**
     * Creates a literal of the default value for the given type.
     *
     * @see #makeZeroLiteral(org.eigenbase.reltype.RelDataType, boolean)
     *
     * @param type Type
     * @return Simple literal
     */
    public RexLiteral makeZeroLiteral(RelDataType type)
    {
        return (RexLiteral) makeZeroLiteral(type, false);
    }

    /**
     * Creates an expression of the default value for the given type, casting if
     * necessary to ensure that the expression is the exact type.
     *
     * <p>This value is:</p>
     *
     * <ul>
     * <li>0 for numeric types;
     * <li>FALSE for BOOLEAN;
     * <li>The epoch for TIMESTAMP and DATE;
     * <li>Midnight for TIME;
     * <li>The empty string for string types (CHAR, BINARY, VARCHAR, VARBINARY).
     * </ul>
     *
     * @param type Type
     * @param allowCast Whether to allow a cast. If false, value is always a
     *    {@link RexLiteral} but may not be the exact type
     * @return Simple literal, or cast simple literal
     */
    public RexNode makeZeroLiteral(RelDataType type, boolean allowCast)
    {
        if (type.isNullable()) {
            type = typeFactory.createTypeWithNullability(type, false);
        }
        RexLiteral literal;
        switch (type.getSqlTypeName()) {
        case CHAR:
            return makeCharLiteral(
                new NlsString(Util.spaces(type.getPrecision()), null, null));
        case VARCHAR:
            literal = makeCharLiteral(new NlsString("", null, null));
            if (allowCast) {
                return makeCast(type, literal);
            } else {
                return literal;
            }
        case BINARY:
            return makeBinaryLiteral(new byte[type.getPrecision()]);
        case VARBINARY:
            literal = makeBinaryLiteral(new byte[0]);
            if (allowCast) {
                return makeCast(type, literal);
            } else {
                return literal;
            }
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
            return makeExactLiteral(BigDecimal.ZERO, type);
        case FLOAT:
        case REAL:
        case DOUBLE:
            return makeApproxLiteral(BigDecimal.ZERO, type);
        case BOOLEAN:
            return booleanFalse;
        case TIME:
            return makeTimeLiteral(
                DateTimeUtil.zeroCalendar, type.getPrecision());
        case DATE:
            return makeDateLiteral(DateTimeUtil.zeroCalendar);
        case TIMESTAMP:
            return makeTimestampLiteral(
                DateTimeUtil.zeroCalendar, type.getPrecision());
        default:
            throw Util.unexpected(type.getSqlTypeName());
        }
    }
}

// End RexBuilder.java
