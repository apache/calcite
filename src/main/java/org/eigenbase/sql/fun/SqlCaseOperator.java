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
package org.eigenbase.sql.fun;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;


/**
 * An operator describing a <code>CASE</code>, <code>NULLIF</code> or <code>
 * COALESCE</code> expression. All of these forms are normalized at parse time
 * to a to a simple <code>CASE</code> statement like this:
 *
 * <blockquote><code>
 * <pre>CASE
 *   WHEN &lt;when expression_0&gt; THEN &lt;then expression_0&gt;
 *   WHEN &lt;when expression_1&gt; THEN &lt;then expression_1&gt;
 *   ...
 *   WHEN &lt;when expression_N&gt; THEN &ltthen expression_N&gt;
 *   ELSE &lt;else expression&gt;
 * END</pre>
 * </code></blockquote>
 *
 * The switched form of the <code>CASE</code> statement is normalized to the
 * simple form by inserting calls to the <code>=</code> operator. For example,
 * <blockquote<code>
 * <pre>CASE x + y
 *   WHEN 1 THEN 'fee'
 *   WHEN 2 THEN 'fie'
 *   ELSE 'foe'
 * END</pre>
 * </code></blockquote>
 *
 * becomes
 *
 * <blockquote><code>
 * <pre>CASE
 * WHEN Equals(x + y, 1) THEN 'fee'
 * WHEN Equals(x + y, 2) THEN 'fie'
 * ELSE 'foe'
 * END</pre>
 * </code></blockquote>
 *
 * <p>REVIEW jhyde 2004/3/19 Does <code>Equals</code> handle NULL semantics
 * correctly?</p>
 *
 * <p><code>COALESCE(x, y, z)</code> becomes
 *
 * <blockquote><code>
 * <pre>CASE
 * WHEN x IS NOT NULL THEN x
 * WHEN y IS NOT NULL THEN y
 * ELSE z
 * END</pre>
 * </code></blockquote>
 * </p>
 *
 * <p><code>NULLIF(x, -1)</code> becomes
 *
 * <blockquote><code>
 * <pre>CASE
 * WHEN x = -1 THEN NULL
 * ELSE x
 * END</pre>
 * </code></blockquote>
 * </p>
 *
 * <p>Note that some of these normalizations cause expressions to be duplicated.
 * This may make it more difficult to write optimizer rules (because the rules
 * will have to deduce that expressions are equivalent). It also requires that
 * some part of the planning process (probably the generator of the calculator
 * program) does common sub-expression elimination.</p>
 *
 * <p>REVIEW jhyde 2004/3/19. Expanding expressions at parse time has some other
 * drawbacks. It is more difficult to give meaningful validation errors: given
 * <code>COALESCE(DATE '2004-03-18', 3.5)</code>, do we issue a type-checking
 * error against a <code>CASE</code> operator? Second, I'd like to use the
 * {@link SqlNode} object model to generate SQL to send to 3rd-party databases,
 * but there's now no way to represent a call to COALESCE or NULLIF. All in all,
 * it would be better to have operators for COALESCE, NULLIF, and both simple
 * and switched forms of CASE, then translate to simple CASE when building the
 * {@link org.eigenbase.rex.RexNode} tree.</p>
 *
 * <p>The arguments are physically represented as follows:
 *
 * <ul>
 * <li>The <i>when</i> expressions are stored in a {@link SqlNodeList}
 * whenList.</li>
 * <li>The <i>then</i> expressions are stored in a {@link SqlNodeList}
 * thenList.</li>
 * <li>The <i>else</i> expression is stored as a regular {@link SqlNode}.</li>
 * </ul>
 * </p>
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Mar 14, 2004
 */
public class SqlCaseOperator
    extends SqlOperator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final SqlWriter.FrameType CaseFrameType =
        SqlWriter.FrameTypeEnum.create("CASE");

    //~ Constructors -----------------------------------------------------------

    public SqlCaseOperator()
    {
        super(
            "CASE",
            SqlKind.CASE,
            MaxPrec,
            true,
            null,
            SqlTypeStrategies.otiReturnType,
            null);
    }

    //~ Methods ----------------------------------------------------------------

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        final SqlCase sqlCase = (SqlCase) call;
        final SqlNodeList whenOperands = sqlCase.getWhenOperands();
        final SqlNodeList thenOperands = sqlCase.getThenOperands();
        final SqlNode elseOperand = sqlCase.getElseOperand();
        for (SqlNode operand : whenOperands) {
            operand.validateExpr(validator, operandScope);
        }
        for (SqlNode operand : thenOperands) {
            operand.validateExpr(validator, operandScope);
        }
        if (elseOperand != null) {
            elseOperand.validateExpr(validator, operandScope);
        }
    }

    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call)
    {
        // Do not try to derive the types of the operands. We will do that
        // later, top down.
        return validateOperands(validator, scope, call);
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        SqlCase caseCall = (SqlCase) callBinding.getCall();
        SqlNodeList whenList = caseCall.getWhenOperands();
        SqlNodeList thenList = caseCall.getThenOperands();
        assert (whenList.size() == thenList.size());

        // checking that search conditions are ok...
        for (SqlNode node : whenList) {
            // should throw validation error if something wrong...
            RelDataType type =
                callBinding.getValidator().deriveType(
                    callBinding.getScope(),
                    node);
            if (!SqlTypeUtil.inBooleanFamily(type)) {
                if (throwOnFailure) {
                    throw callBinding.newError(
                        EigenbaseResource.instance().ExpectedBoolean.ex());
                }
                return false;
            }
        }

        boolean foundNotNull = false;
        for (SqlNode node : thenList) {
            if (!SqlUtil.isNullLiteral(node, false)) {
                foundNotNull = true;
            }
        }

        if (!SqlUtil.isNullLiteral(
                caseCall.getElseOperand(),
                false))
        {
            foundNotNull = true;
        }

        if (!foundNotNull) {
            // according to the sql standard we can not have all of the THEN
            // statements and the ELSE returning null
            if (throwOnFailure) {
                throw callBinding.newError(
                    EigenbaseResource.instance().MustNotNullInElse.ex());
            }
            return false;
        }
        return true;
    }

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        // REVIEW jvs 4-June-2005:  can't these be unified?
        if (!(opBinding instanceof SqlCallBinding)) {
            return inferTypeFromOperands(
                opBinding.getTypeFactory(),
                opBinding.collectOperandTypes());
        }
        return inferTypeFromValidator((SqlCallBinding) opBinding);
    }

    private RelDataType inferTypeFromValidator(
        SqlCallBinding callBinding)
    {
        SqlCase caseCall = (SqlCase) callBinding.getCall();
        SqlNodeList thenList = caseCall.getThenOperands();
        ArrayList<SqlNode> nullList = new ArrayList<SqlNode>();
        List<RelDataType> argTypes = new ArrayList<RelDataType>();
        for (SqlNode node : thenList) {
            argTypes.add(
                callBinding.getValidator().deriveType(
                    callBinding.getScope(), node));
            if (SqlUtil.isNullLiteral(node, false)) {
                nullList.add(node);
            }
        }
        SqlNode elseOp = caseCall.getElseOperand();
        argTypes.add(
            callBinding.getValidator().deriveType(
                callBinding.getScope(), caseCall.getElseOperand()));
        if (SqlUtil.isNullLiteral(elseOp, false)) {
            nullList.add(elseOp);
        }

        RelDataType ret =
            callBinding.getTypeFactory().leastRestrictive(
                argTypes);
        if (null == ret) {
            throw callBinding.newValidationError(
                EigenbaseResource.instance().IllegalMixingOfTypes.ex());
        }
        for (SqlNode node : nullList) {
            callBinding.getValidator().setValidatedNodeType(node, ret);
        }
        return ret;
    }

    private RelDataType inferTypeFromOperands(
        RelDataTypeFactory typeFactory,
        List<RelDataType> argTypes)
    {
        assert (argTypes.size() % 2) == 1 : "odd number of arguments expected: "
            + argTypes.size();
        assert argTypes.size() > 1 : argTypes.size();
        List<RelDataType> thenTypes = new ArrayList<RelDataType>();
        for (int j = 1; j < (argTypes.size() - 1); j += 2) {
            thenTypes.add(argTypes.get(j));
        }

        thenTypes.add(Util.last(argTypes));
        return typeFactory.leastRestrictive(thenTypes);
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRanges.any();
    }

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode ... operands)
    {
        assert functionQualifier == null;
        return new SqlCase(this, operands, pos);
    }

    /**
     * Creates a call to the switched form of the case operator, viz:
     *
     * <blockquote><code>CASE caseIdentifier<br/>
     * WHEN whenList[0] THEN thenList[0]<br/>
     * WHEN whenList[1] THEN thenList[1]<br/>
     * ...<br/>
     * ELSE elseClause<br/>
     * END</code></blockquote>
     */
    public SqlCase createSwitchedCall(
        SqlParserPos pos,
        SqlNode caseIdentifier,
        SqlNodeList whenList,
        SqlNodeList thenList,
        SqlNode elseClause)
    {
        if (null != caseIdentifier) {
            List<SqlNode> list = whenList.getList();
            for (int i = 0; i < list.size(); i++) {
                SqlNode e = list.get(i);
                list.set(
                    i,
                    SqlStdOperatorTable.equalsOperator.createCall(
                        pos,
                        caseIdentifier,
                        e));
            }
        }

        if (null == elseClause) {
            elseClause = SqlLiteral.createNull(pos);
        }

        return (SqlCase) createCall(
            pos,
            whenList,
            thenList,
            elseClause);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame =
            writer.startList(CaseFrameType, "CASE", "END");
        SqlNodeList whenList = (SqlNodeList) operands[SqlCase.WHEN_OPERANDS];
        SqlNodeList thenList = (SqlNodeList) operands[SqlCase.THEN_OPERANDS];
        assert whenList.size() == thenList.size();
        for (Pair<SqlNode, SqlNode> pair : Pair.zip(whenList, thenList)) {
            writer.sep("WHEN");
            pair.left.unparse(writer, 0, 0);
            writer.sep("THEN");
            pair.right.unparse(writer, 0, 0);
        }

        writer.sep("ELSE");
        final SqlNode elseOperand = operands[SqlCase.ELSE_OPERAND];
        elseOperand.unparse(writer, 0, 0);
        writer.endList(frame);
    }
}

// End SqlCaseOperator.java
