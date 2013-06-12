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

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * <code>SqlJoinOperator</code> describes the syntax of the SQL <code>
 * JOIN</code> operator. Since there is only one such operator, this class is
 * almost certainly a singleton.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 19, 2003
 */
public class SqlJoinOperator
    extends SqlOperator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final SqlWriter.FrameType UsingFrameType =
        SqlWriter.FrameTypeEnum.create("USING");

    //~ Enums ------------------------------------------------------------------

    /**
     * Enumerates the types of condition in a join expression.
     */
    public enum ConditionType
        implements SqlLiteral.SqlSymbol
    {
        /**
         * Join clause has no condition, for example "FROM EMP, DEPT"
         */
        None,

        /**
         * Join clause has an ON condition, for example "FROM EMP JOIN DEPT ON
         * EMP.DEPTNO = DEPT.DEPTNO"
         */
        On,

        /**
         * Join clause has a USING condition, for example "FROM EMP JOIN DEPT
         * USING (DEPTNO)"
         */
        Using;

        /** Creates a parse-tree node representing an occurrence of this join
         * type at a particular position in the parsed text. */
        public SqlLiteral symbol(SqlParserPos pos) {
            return SqlLiteral.createSymbol(this, pos);
        }
    }

    /**
     * Enumerates the types of join.
     */
    public enum JoinType
        implements SqlLiteral.SqlSymbol
    {
        /**
         * Inner join.
         */
        Inner,

        /**
         * Full outer join.
         */
        Full,

        /**
         * Cross join (also known as Cartesian product).
         */
        Cross,

        /**
         * Left outer join.
         */
        Left,

        /**
         * Right outer join.
         */
        Right,

        /**
         * Comma join: the good old-fashioned SQL <code>FROM</code> clause,
         * where table expressions are specified with commas between them, and
         * join conditions are specified in the <code>WHERE</code> clause.
         */
        Comma;

        /** Creates a parse-tree node representing an occurrence of this
         * condition type keyword at a particular position in the parsed
         * text. */
        public SqlLiteral symbol(SqlParserPos pos) {
            return SqlLiteral.createSymbol(this, pos);
        }
    }

    //~ Constructors -----------------------------------------------------------

    public SqlJoinOperator()
    {
        super("JOIN", SqlKind.JOIN, 16, true, null, null, null);
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
        assert (operands[SqlJoin.IS_NATURAL_OPERAND] instanceof SqlLiteral);
        final SqlLiteral isNatural =
            (SqlLiteral) operands[SqlJoin.IS_NATURAL_OPERAND];
        assert (isNatural.getTypeName() == SqlTypeName.BOOLEAN);
        assert operands[SqlJoin.CONDITION_TYPE_OPERAND] != null
            : "precondition: operands[CONDITION_TYPE_OPERAND] != null";
        assert (operands[SqlJoin.CONDITION_TYPE_OPERAND] instanceof SqlLiteral)
            && (SqlLiteral.symbolValue(operands[SqlJoin.CONDITION_TYPE_OPERAND])
                instanceof ConditionType);
        assert operands[SqlJoin.TYPE_OPERAND] != null
            : "precondition: operands[TYPE_OPERAND] != null";
        assert (operands[SqlJoin.TYPE_OPERAND] instanceof SqlLiteral)
            && (SqlLiteral.symbolValue(operands[SqlJoin.TYPE_OPERAND])
                instanceof JoinType);
        return new SqlJoin(this, operands, pos);
    }

    public SqlCall createCall(
        SqlNode left,
        SqlLiteral isNatural,
        SqlLiteral joinType,
        SqlNode right,
        SqlLiteral conditionType,
        SqlNode condition,
        SqlParserPos pos)
    {
        return createCall(
            pos,
            left,
            isNatural,
            joinType,
            right,
            conditionType,
            condition);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlNode left = operands[SqlJoin.LEFT_OPERAND];

        left.unparse(
            writer,
            leftPrec,
            getLeftPrec());
        String natural = "";
        if (SqlLiteral.booleanValue(operands[SqlJoin.IS_NATURAL_OPERAND])) {
            natural = "NATURAL ";
        }
        final SqlJoinOperator.JoinType joinType =
            (JoinType) SqlLiteral.symbolValue(operands[SqlJoin.TYPE_OPERAND]);
        switch (joinType) {
        case Comma:
            writer.sep(",", true);
            break;
        case Cross:
            writer.sep(natural + "CROSS JOIN");
            break;
        case Full:
            writer.sep(natural + "FULL JOIN");
            break;
        case Inner:
            writer.sep(natural + "INNER JOIN");
            break;
        case Left:
            writer.sep(natural + "LEFT JOIN");
            break;
        case Right:
            writer.sep(natural + "RIGHT JOIN");
            break;
        default:
            throw Util.unexpected(joinType);
        }
        final SqlNode right = operands[SqlJoin.RIGHT_OPERAND];
        right.unparse(
            writer,
            getRightPrec(),
            rightPrec);
        final SqlNode condition = operands[SqlJoin.CONDITION_OPERAND];
        if (condition != null) {
            final SqlJoinOperator.ConditionType conditionType =
                (ConditionType) SqlLiteral.symbolValue(
                    operands[SqlJoin.CONDITION_TYPE_OPERAND]);
            switch (conditionType) {
            case Using:

                // No need for an extra pair of parens -- the condition is a
                // list. The result is something like "USING (deptno, gender)".
                writer.keyword("USING");
                assert condition instanceof SqlNodeList;
                final SqlWriter.Frame frame =
                    writer.startList(UsingFrameType, "(", ")");
                condition.unparse(writer, 0, 0);
                writer.endList(frame);
                break;
            case On:
                writer.keyword("ON");
                condition.unparse(writer, leftPrec, rightPrec);
                break;
            default:
                throw Util.unexpected(conditionType);
            }
        }
    }
}

// End SqlJoinOperator.java
