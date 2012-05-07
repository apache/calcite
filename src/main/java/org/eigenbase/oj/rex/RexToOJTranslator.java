/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.oj.rex;

import java.math.*;

import java.nio.*;

import java.util.*;
import java.util.List;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

/**
 * Converts expressions in logical format ({@link RexNode}) into OpenJava code.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RexToOJTranslator
    implements RexVisitor<Expression>
{
    //~ Instance fields --------------------------------------------------------

    private final JavaRelImplementor implementor;
    private final RelNode contextRel;
    private final OJRexImplementorTable implementorTable;

    // TODO jvs 16-Oct-2006:  Eliminate this now that RexVisitor
    // can return values.
    private Expression translatedExpr;

    /**
     * Program which the expression is part of.
     *
     * <ul>
     * <li>If this field is set, the expression is interpreted in terms of
     * output fields of the program.
     * <li>If this field is not set, the expression is interpreted in terms of
     * the inputs to the calculator.</li>
     * </ul>
     */
    private RexProgram program;

    private final Stack<RexProgram> programStack = new Stack<RexProgram>();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a translator.
     *
     * @param implementor implementation context
     * @param contextRel relational expression which is the context for the
     * row-expressions which are to be translated
     * @param implementorTable table of implementation functors for Rex
     * operators; if null, {@link OJRexImplementorTableImpl#instance} is used
     */
    public RexToOJTranslator(
        JavaRelImplementor implementor,
        RelNode contextRel,
        OJRexImplementorTable implementorTable)
    {
        if (implementorTable == null) {
            implementorTable = OJRexImplementorTableImpl.instance();
        }

        this.implementor = implementor;
        this.contextRel = contextRel;
        this.implementorTable = implementorTable;
    }

    //~ Methods ----------------------------------------------------------------

    protected Expression setTranslation(Expression expr)
    {
        return translatedExpr = expr;
    }

    protected Expression getTranslation()
    {
        return translatedExpr;
    }

    /**
     * Returns the current program.
     *
     * @see #pushProgram(RexProgram)
     * @see #popProgram(RexProgram)
     */
    public RexProgram getProgram()
    {
        return program;
    }

    protected OJRexImplementorTable getImplementorTable()
    {
        return implementorTable;
    }

    public JavaRelImplementor getRelImplementor()
    {
        return implementor;
    }

    public RelNode getContextRel()
    {
        return contextRel;
    }

    public RelDataTypeFactory getTypeFactory()
    {
        return contextRel.getCluster().getTypeFactory();
    }

    // implement RexVisitor
    public Expression visitLocalRef(RexLocalRef localRef)
    {
        assert program != null;
        if (isInputRef(localRef)) {
            return translateInput(localRef.getIndex());
        } else {
            // It's a reference to a common sub-expression. Recursively
            // translate that expression.
            return setTranslation(translateSubExpression(localRef));
        }
    }

    /**
     * Translates a common subexpression.
     *
     * @param localRef common subexpression to be translated
     *
     * @return translation
     */
    public Expression translateSubExpression(RexLocalRef localRef)
    {
        final RexNode expr = program.getExprList().get(localRef.getIndex());
        assert expr.getType() == localRef.getType();
        return translateRexNode(expr);
    }

    /**
     * Tests whether a RexLocalRef refers to an input.
     *
     * @param localRef reference to test
     *
     * @return true if an input reference; false if a reference to a common
     * subexpression
     */
    protected boolean isInputRef(RexLocalRef localRef)
    {
        final int index = localRef.getIndex();
        return program.getInputRowType().isStruct()
            && (index < program.getInputRowType().getFields().length);
    }

    // implement RexVisitor
    public Expression visitInputRef(RexInputRef inputRef)
    {
        final int index = inputRef.getIndex();
        if (program != null) {
            // Lookup the expression.
            final RexNode expanded = program.getExprList().get(index);
            assert expanded.getType() == inputRef.getType();

            // Unset program because the new expression is in terms of the
            // program's inputs. This also prevents infinite expansion.
            pushProgram(null);
            expanded.accept(this);
            popProgram(null);
            return null;
        }
        return translateInput(index);
    }

    private Expression translateInput(final int index)
    {
        WhichInputResult inputAndCol = whichInput(index, contextRel);
        if (inputAndCol == null) {
            throw Util.newInternal("input not found");
        }
        final Variable v = implementor.findInputVariable(inputAndCol.input);
        RelDataType rowType = inputAndCol.input.getRowType();
        final RelDataTypeField field =
            rowType.getFields()[inputAndCol.fieldIndex];
        final String javaFieldName =
            Util.toJavaId(
                field.getName(),
                inputAndCol.fieldIndex);
        return setTranslation(new FieldAccess(v, javaFieldName));
    }

    // implement RexVisitor
    public Expression visitLiteral(RexLiteral literal)
    {
        // Refer to RexLiteral.valueMatchesType for the type/value combinations
        // we need to handle here.
        final Object value = literal.getValue();
        Calendar calendar;
        long timeInMillis;
        switch (literal.getTypeName()) {
        case NULL:
            setTranslation(Literal.constantNull());
            break;
        case CHAR:
            Literal lit = Literal.makeLiteral(((NlsString) value).getValue());

            // Replace non-ASCII characters with Java Unicode escape
            // sequences to avoid encoding glitches in the generated
            // Java code.
            String s = lit.toString();
            int n = s.length();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < n; ++i) {
                char c = s.charAt(i);
                int v = (int) c;
                if (v < 128) {
                    sb.append(c);
                } else {
                    sb.append("\\u");
                    sb.append(String.format("%1$04X", v));
                }
            }
            lit = new Literal(Literal.STRING, sb.toString());
            setTranslation(lit);
            break;
        case BOOLEAN:
            setTranslation(Literal.makeLiteral((Boolean) value));
            break;
        case DECIMAL:
            BigDecimal bd = (BigDecimal) value;
            if (bd.scale() == 0) {
                // Honor the requested type (if long) to prevent
                // unexpected overflow during arithmetic.
                SqlTypeName type = literal.getType().getSqlTypeName();
                long longValue = bd.longValue();
                switch (type) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    setTranslation(Literal.makeLiteral((int) longValue));
                    break;
                default:
                    setTranslation(Literal.makeLiteral(longValue));
                    break;
                }
                break;
            }

            // represent decimals with unscaled value
            long unscaled = bd.unscaledValue().longValue();
            setTranslation(Literal.makeLiteral(unscaled));
            break;
        case DOUBLE:
            if (literal.getType().getSqlTypeName() == SqlTypeName.REAL) {
                setTranslation(
                    Literal.makeLiteral(((BigDecimal) value).floatValue()));
            } else {
                setTranslation(
                    Literal.makeLiteral(((BigDecimal) value).doubleValue()));
            }
            break;
        case BINARY:
            setTranslation(
                convertByteArrayLiteral(((ByteBuffer) value).array()));
            break;
        case DATE:
        case TIME:
        case TIMESTAMP:
            calendar = (Calendar) value;
            timeInMillis = calendar.getTimeInMillis();
            setTranslation(Literal.makeLiteral(timeInMillis));
            break;
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
            BigDecimal interval = (BigDecimal) value;
            setTranslation(Literal.makeLiteral(interval.longValue()));
            break;
        case SYMBOL:
            SqlLiteral.SqlSymbol ord = (SqlLiteral.SqlSymbol) value;
            setTranslation(Literal.makeLiteral(ord.ordinal()));
            break;
        default:
            throw Util.newInternal(
                "Bad literal value " + value + " ("
                + value.getClass() + "); breaches "
                + "post-condition on RexLiteral.getValue()");
        }
        return null;
    }

    // implement RexVisitor
    public Expression visitCall(RexCall call)
    {
        final Expression callExpr = convertCallAndOperands(call);
        return setTranslation(callExpr);
    }

    protected Expression convertCallAndOperands(
        RexCall call)
    {
        List<Expression> exprs = new ArrayList<Expression>();
        for (RexNode operand : call.getOperands()) {
            exprs.add(translateRexNode(operand));
        }
        return convertCall(call, exprs);
    }

    /**
     * Converts a call after its operands have already been translated.
     *
     * @param call call to be translated
     * @param operandExprList translated operands
     *
     * @return converted call
     */
    protected Expression convertCall(
        RexCall call,
        List<Expression> operandExprList)
    {
        OJRexImplementor implementor = implementorTable.get(call.getOperator());
        if (implementor == null) {
            throw Util.needToImplement(call);
        }
        final Expression[] operandExprs2 =
            operandExprList.toArray(new Expression[operandExprList.size()]);
        return implementor.implement(this, call, operandExprs2);
    }

    // implement RexVisitor
    public Expression visitOver(RexOver over)
    {
        throw Util.needToImplement("Row-expression RexOver");
    }

    // implement RexVisitor
    public Expression visitCorrelVariable(RexCorrelVariable correlVariable)
    {
        throw Util.needToImplement("Row-expression RexCorrelVariable");
    }

    // implement RexVisitor
    public Expression visitDynamicParam(RexDynamicParam dynamicParam)
    {
        throw Util.needToImplement("Row-expression RexDynamicParam");
    }

    // implement RexVisitor
    public Expression visitRangeRef(RexRangeRef rangeRef)
    {
        final WhichInputResult inputAndCol =
            whichInput(
                rangeRef.getOffset(),
                contextRel);
        if (inputAndCol == null) {
            throw Util.newInternal("input not found");
        }
        final RelDataType inputRowType = inputAndCol.input.getRowType();

        // Simple case is if the range refers to every field of the
        // input. Return the whole input.
        final Variable inputExpr =
            implementor.findInputVariable(inputAndCol.input);
        final RelDataType rangeType = rangeRef.getType();
        if ((inputAndCol.fieldIndex == 0) && (rangeType == inputRowType)) {
            return setTranslation(inputExpr);
        }

        // More complex case is if the range refers to a subset of
        // the input's fields. Generate "new Type(argN,...,argM)".
        final RelDataTypeField [] rangeFields = rangeType.getFields();
        final RelDataTypeField [] inputRowFields = inputRowType.getFields();
        final ExpressionList args = new ExpressionList();
        for (int i = 0; i < rangeFields.length; i++) {
            String fieldName =
                inputRowFields[inputAndCol.fieldIndex + i].getName();
            final String javaFieldName = Util.toJavaId(fieldName, i);
            args.add(new FieldAccess(inputExpr, javaFieldName));
        }
        return setTranslation(
            new AllocationExpression(
                OJUtil.typeToOJClass(
                    rangeType,
                    getTypeFactory()),
                args));
    }

    // implement RexVisitor
    public Expression visitFieldAccess(RexFieldAccess fieldAccess)
    {
        final String javaFieldName =
            Util.toJavaId(
                fieldAccess.getName(),
                fieldAccess.getField().getIndex());
        return setTranslation(
            new FieldAccess(
                translateRexNode(fieldAccess.getReferenceExpr()),
                javaFieldName));
    }

    /**
     * Translates an expression into a Java expression. If the program has
     * previously been set via {@link #pushProgram(RexProgram)}, the expression
     * is interpreted in terms of the <em>output</em> fields of the program.
     * Suppose that the program is
     *
     * <blockquote>
     * <pre>
     *   exprs: {$0, $1, $0 + $1}
     *   projectRefs: {$0, $2}
     *   conditionRef: null</pre>
     * </blockquote>
     *
     * and the expression is <code>$1 + 5</code>. This would be expanded to
     * <code>(a + b) + 5</code>, because output field $1 of the program is
     * defined to be the expression <code>$0 + $1</code> in terms of the input
     * fields.
     *
     * <p/>Sometimes a calculator expression is defined in terms of simpler
     * calculator expressions. If this is the case, those expressions will be
     * successively evaluated and assigned to variables. If a variable with the
     * appropriate value is already in scope, it will be used.
     *
     * <p/>If the program is not present, no mapping occurs.
     *
     * @param node Expression to be translated.
     *
     * @return Java translation of expression
     */
    public Expression translateRexNode(RexNode node)
    {
        if (node instanceof JavaRowExpression) {
            return ((JavaRowExpression) node).getExpression();
        } else {
            node.accept(this);
            Expression expr = translatedExpr;
            this.translatedExpr = null;
            return expr;
        }
    }

    protected ArrayInitializer convertByteArrayLiteralToInitializer(
        byte [] bytes)
    {
        ExpressionList byteList = new ExpressionList();
        for (int i = 0; i < bytes.length; ++i) {
            byteList.add(Literal.makeLiteral(bytes[i]));
        }
        return new ArrayInitializer(byteList);
    }

    protected Expression convertByteArrayLiteral(byte [] bytes)
    {
        return new ArrayAllocationExpression(
            TypeName.forOJClass(OJSystem.BYTE),
            new ExpressionList(null),
            convertByteArrayLiteralToInitializer(bytes));
    }

    public boolean canConvertCall(RexCall call)
    {
        OJRexImplementor implementor = implementorTable.get(call.getOperator());
        if (implementor == null) {
            return false;
        }
        return implementor.canImplement(call);
    }

    /**
     * Returns the ordinal of the input relational expression which a given
     * column ordinal comes from.
     *
     * <p>For example, if <code>rel</code> has inputs <code>I(a, b, c)</code>
     * and <code>J(d, e)</code>, then <code>whichInput(0, rel)</code> returns 0
     * (column a), <code>whichInput(2, rel)</code> returns 0 (column c), <code>
     * whichInput(3, rel)</code> returns 1 (column d).</p>
     *
     * @param fieldIndex Index of field
     * @param rel Relational expression
     *
     * @return a {@link WhichInputResult} if found, otherwise null.
     */
    private static WhichInputResult whichInput(
        int fieldIndex,
        RelNode rel)
    {
        assert fieldIndex >= 0;
        final List<RelNode> inputs = rel.getInputs();
        for (
            int inputIndex = 0, firstFieldIndex = 0;
            inputIndex < inputs.size();
            inputIndex++)
        {
            RelNode input = inputs.get(inputIndex);

            // Index of first field in next input. Special case if this
            // input has no fields: it's ambiguous (we could be looking
            // at the first field of the next input) but we allow it.
            final int fieldCount = input.getRowType().getFieldList().size();
            final int lastFieldIndex = firstFieldIndex + fieldCount;
            if ((lastFieldIndex > fieldIndex)
                || ((fieldCount == 0) && (lastFieldIndex == fieldIndex)))
            {
                final int fieldIndex2 = fieldIndex - firstFieldIndex;
                return new WhichInputResult(input, inputIndex, fieldIndex2);
            }
            firstFieldIndex = lastFieldIndex;
        }
        return null;
    }

    /**
     * Generates code for an assignment.
     *
     * <p>NOTE: This method is only implemented in translators which can
     * generate sequences of statements. The default implementation of this
     * method throws {@link UnsupportedOperationException}.
     *
     * @param lhsField target field
     * @param lhs target field as OpenJava
     * @param rhs the source expression (as RexNode)
     */
    public void translateAssignment(
        RelDataTypeField lhsField,
        Expression lhs,
        RexNode rhs)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a sub-translator to deal with a sub-block.
     *
     * <p>The default implementation simply returns this translator. Other
     * implementations may create a new translator which contains the
     * expression-to-variable mappings of the sub-block.
     *
     * @param stmtList Sub-block to generate code into
     *
     * @return A translator
     */
    public RexToOJTranslator push(StatementList stmtList)
    {
        return this;
    }

    /**
     * Sets the current program. The previous program will be restored when
     * {@link #popProgram} is called. The program may be null.
     *
     * @param program New current program
     */
    public void pushProgram(RexProgram program)
    {
        programStack.push(program);
        this.program = program;
    }

    /**
     * Restores the current program to the one before {@link
     * #pushProgram(RexProgram)} was called.
     *
     * @param program The program most recently pushed
     */
    public void popProgram(RexProgram program)
    {
        Util.pre(program == this.program, "mismatched push/pop");
        assert programStack.pop() == program;
        if (programStack.isEmpty()) {
            this.program = null;
        } else {
            this.program = programStack.lastElement();
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Result of call to {@link RexToOJTranslator#whichInput}, contains the
     * input relational expression, its index, and the index of the field within
     * that relational expression.
     */
    private static class WhichInputResult
    {
        final RelNode input;
        final int inputIndex;
        final int fieldIndex;

        WhichInputResult(
            RelNode input,
            int inputIndex,
            int fieldIndex)
        {
            this.input = input;
            this.inputIndex = inputIndex;
            this.fieldIndex = fieldIndex;
        }
    }
}

// End RexToOJTranslator.java
