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
package org.eigenbase.sql;

import java.util.*;

import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;


/**
 * An operator describing a query. (Not a query itself.)
 *
 * <p>Operands are:
 *
 * <ul>
 * <li>0: distinct ({@link SqlLiteral})</li>
 * <li>1: selectClause ({@link SqlNodeList})</li>
 * <li>2: fromClause ({@link SqlCall} to "join" operator)</li>
 * <li>3: whereClause ({@link SqlNode})</li>
 * <li>4: havingClause ({@link SqlNode})</li>
 * <li>5: groupClause ({@link SqlNode})</li>
 * <li>6: windowClause ({@link SqlNodeList})</li>
 * <li>7: orderClause ({@link SqlNode})</li>
 * </ul>
 * </p>
 */
public class SqlSelectOperator
    extends SqlOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlSelectOperator()
    {
        super(
            "SELECT",
            SqlKind.SELECT,
            2,
            true,
            SqlTypeStrategies.rtiScope,
            null,
            null);
    }

    //~ Methods ----------------------------------------------------------------

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
        return new SqlSelect(this, operands, pos);
    }

    /**
     * Creates a call to the <code>SELECT</code> operator.
     *
     * @param keywordList List of keywords such DISTINCT and ALL, or null
     * @param selectList The SELECT clause, or null if empty
     * @param fromClause The FROM clause
     * @param whereClause The WHERE clause, or null if not present
     * @param groupBy The GROUP BY clause, or null if not present
     * @param having The HAVING clause, or null if not present
     * @param windowDecls The WINDOW clause, or null if not present
     * @param orderBy The ORDER BY clause, or null if not present
     * @param pos The parser position, or {@link SqlParserPos#ZERO} if not
     * specified; must not be null.
     *
     * @return A {@link SqlSelect}, never null
     */
    public SqlSelect createCall(
        SqlNodeList keywordList,
        SqlNodeList selectList,
        SqlNode fromClause,
        SqlNode whereClause,
        SqlNode groupBy,
        SqlNode having,
        SqlNodeList windowDecls,
        SqlNode orderBy,
        SqlParserPos pos)
    {
        if (keywordList == null) {
            keywordList = new SqlNodeList(pos);
        }
        if (windowDecls == null) {
            windowDecls = new SqlNodeList(pos);
        }
        return (SqlSelect) createCall(
            pos,
            keywordList,
            selectList,
            fromClause,
            whereClause,
            groupBy,
            having,
            windowDecls,
            orderBy);
    }

    public <R> void acceptCall(
        SqlVisitor<R> visitor,
        SqlCall call,
        boolean onlyExpressions,
        SqlBasicVisitor.ArgHandler<R> argHandler)
    {
        if (onlyExpressions) {
            // None of the arguments to the SELECT operator are expressions.
            return;
        } else {
            super.acceptCall(visitor, call, onlyExpressions, argHandler);
        }
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame selectFrame =
            writer.startList(SqlWriter.FrameTypeEnum.Select);
        writer.sep("SELECT");
        final SqlNodeList keywords =
            (SqlNodeList) operands[SqlSelect.KEYWORDS_OPERAND];
        for (int i = 0; i < keywords.size(); i++) {
            final SqlNode keyword = keywords.get(i);
            keyword.unparse(writer, 0, 0);
        }
        SqlNode selectClause = operands[SqlSelect.SELECT_OPERAND];
        if (selectClause == null) {
            selectClause =
                new SqlIdentifier(
                    "*",
                    SqlParserPos.ZERO);
        }
        final SqlWriter.Frame selectListFrame =
            writer.startList(SqlWriter.FrameTypeEnum.SelectList);
        unparseListClause(writer, selectClause);
        writer.endList(selectListFrame);

        writer.sep("FROM");
        SqlNode fromClause = operands[SqlSelect.FROM_OPERAND];

        // for FROM clause, use precedence just below join operator to make
        // sure that an unjoined nested select will be properly
        // parenthesized
        final SqlWriter.Frame fromFrame =
            writer.startList(SqlWriter.FrameTypeEnum.FromList);
        fromClause.unparse(
            writer,
            SqlStdOperatorTable.joinOperator.getLeftPrec() - 1,
            SqlStdOperatorTable.joinOperator.getRightPrec() - 1);
        writer.endList(fromFrame);

        SqlNode whereClause = operands[SqlSelect.WHERE_OPERAND];

        if (whereClause != null) {
            writer.sep("WHERE");

            if (!writer.isAlwaysUseParentheses()) {
                SqlNode node = whereClause;

                // decide whether to split on ORs or ANDs
                SqlKind whereSepKind = SqlKind.AND;
                if ((node instanceof SqlCall)
                    && ((SqlCall) node).getKind() == SqlKind.OR)
                {
                    whereSepKind = SqlKind.OR;
                }

                // unroll whereClause
                ArrayList<SqlNode> list = new ArrayList<SqlNode>(0);
                while (
                    (node instanceof SqlCall)
                    && (((SqlCall) node).getKind() == whereSepKind))
                {
                    list.add(0, ((SqlCall) node).getOperands()[1]);
                    node = ((SqlCall) node).getOperands()[0];
                }
                list.add(0, node);

                // unparse in a WhereList frame
                final SqlWriter.Frame whereFrame =
                    writer.startList(SqlWriter.FrameTypeEnum.WhereList);
                unparseListClause(
                    writer,
                    new SqlNodeList(
                        list,
                        whereClause.getParserPosition()),
                    whereSepKind);
                writer.endList(whereFrame);
            } else {
                whereClause.unparse(writer, 0, 0);
            }
        }
        SqlNodeList groupClause =
            (SqlNodeList) operands[SqlSelect.GROUP_OPERAND];
        if (groupClause != null) {
            writer.sep("GROUP BY");
            final SqlWriter.Frame groupFrame =
                writer.startList(SqlWriter.FrameTypeEnum.GroupByList);
            if (groupClause.getList().isEmpty()) {
                final SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.Simple, "(", ")");
                writer.endList(frame);
            } else {
                unparseListClause(writer, groupClause);
            }
            writer.endList(groupFrame);
        }
        SqlNode havingClause = operands[SqlSelect.HAVING_OPERAND];
        if (havingClause != null) {
            writer.sep("HAVING");
            havingClause.unparse(writer, 0, 0);
        }
        SqlNodeList windowDecls =
            (SqlNodeList) operands[SqlSelect.WINDOW_OPERAND];
        if (windowDecls.size() > 0) {
            writer.sep("WINDOW");
            final SqlWriter.Frame windowFrame =
                writer.startList(SqlWriter.FrameTypeEnum.WindowDeclList);
            for (int i = 0; i < windowDecls.size(); i++) {
                SqlNode windowDecl = windowDecls.get(i);
                writer.sep(",");
                windowDecl.unparse(writer, 0, 0);
            }
            writer.endList(windowFrame);
        }
        SqlNode orderClause = operands[SqlSelect.ORDER_OPERAND];
        if (orderClause != null) {
            writer.sep("ORDER BY");
            final SqlWriter.Frame orderFrame =
                writer.startList(SqlWriter.FrameTypeEnum.OrderByList);
            unparseListClause(writer, orderClause);
            writer.endList(orderFrame);
        }
        writer.endList(selectFrame);
    }

    public boolean argumentMustBeScalar(int ordinal)
    {
        return ordinal == SqlSelect.WHERE_OPERAND;
    }
}

// End SqlSelectOperator.java
