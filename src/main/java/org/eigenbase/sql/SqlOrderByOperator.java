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
package org.eigenbase.sql;

/**
 * SqlOrderByOperator is used to represent an ORDER BY on a query other than a
 * SELECT (e.g. VALUES or UNION). It is a purely syntactic operator, and is
 * eliminated by SqlValidator.performUnconditionalRewrites and replaced with the
 * ORDER_OPERAND of SqlSelect.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlOrderByOperator
    extends SqlSpecialOperator
{
    //~ Static fields/initializers ---------------------------------------------

    // constants representing operand positions
    public static final int QUERY_OPERAND = 0;
    public static final int ORDER_OPERAND = 1;

    //~ Constructors -----------------------------------------------------------

    public SqlOrderByOperator()
    {
        // NOTE:  make precedence lower then SELECT to avoid extra parens
        super("ORDER BY", SqlKind.ORDER_BY, 0);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Postfix;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        assert (operands.length == 2);
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.OrderBy);
        operands[QUERY_OPERAND].unparse(
            writer,
            getLeftPrec(),
            getRightPrec());
        writer.sep(getName());
        final SqlWriter.Frame listFrame =
            writer.startList(SqlWriter.FrameTypeEnum.OrderByList);
        unparseListClause(writer, operands[ORDER_OPERAND]);
        writer.endList(listFrame);
        writer.endList(frame);
    }
}

// End SqlOrderByOperator.java
