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
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * A <code>SqlInsert</code> is a node of a parse tree which represents an INSERT
 * statement.
 */
public class SqlInsert
    extends SqlCall
{
    //~ Static fields/initializers ---------------------------------------------

    // constants representing operand positions
    public static final int KEYWORDS_OPERAND = 0;
    public static final int TARGET_TABLE_OPERAND = 1;
    public static final int SOURCE_OPERAND = 2;
    public static final int TARGET_COLUMN_LIST_OPERAND = 3;
    public static final int OPERAND_COUNT = 4;

    //~ Constructors -----------------------------------------------------------

    public SqlInsert(
        SqlSpecialOperator operator,
        SqlNodeList keywords,
        SqlIdentifier targetTable,
        SqlNode source,
        SqlNodeList columnList,
        SqlParserPos pos)
    {
        super(
            operator,
            new SqlNode[OPERAND_COUNT],
            pos);

        Util.pre(keywords != null, "keywords != null");

        operands[KEYWORDS_OPERAND] = keywords;
        operands[TARGET_TABLE_OPERAND] = targetTable;
        operands[SOURCE_OPERAND] = source;
        operands[TARGET_COLUMN_LIST_OPERAND] = columnList;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the identifier for the target table of the insertion
     */
    public SqlIdentifier getTargetTable()
    {
        return (SqlIdentifier) operands[TARGET_TABLE_OPERAND];
    }

    /**
     * @return the source expression for the data to be inserted
     */
    public SqlNode getSource()
    {
        return operands[SOURCE_OPERAND];
    }

    /**
     * @return the list of target column names, or null for all columns in the
     * target table
     */
    public SqlNodeList getTargetColumnList()
    {
        return (SqlNodeList) operands[TARGET_COLUMN_LIST_OPERAND];
    }

    public final SqlNode getModifierNode(SqlInsertKeyword modifier)
    {
        final SqlNodeList keywords = (SqlNodeList) operands[KEYWORDS_OPERAND];
        for (int i = 0; i < keywords.size(); i++) {
            SqlInsertKeyword keyword =
                (SqlInsertKeyword) SqlLiteral.symbolValue(keywords.get(i));
            if (keyword == modifier) {
                return keywords.get(i);
            }
        }
        return null;
    }

    // implement SqlNode
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.startList(SqlWriter.FrameTypeEnum.Select);
        writer.sep("INSERT INTO");
        getTargetTable().unparse(
            writer,
            getOperator().getLeftPrec(),
            getOperator().getRightPrec());
        if (getTargetColumnList() != null) {
            getTargetColumnList().unparse(
                writer,
                getOperator().getLeftPrec(),
                getOperator().getRightPrec());
        }
        writer.newlineAndIndent();
        getSource().unparse(
            writer,
            getOperator().getLeftPrec(),
            getOperator().getRightPrec());
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        validator.validateInsert(this);
    }
}

// End SqlInsert.java
