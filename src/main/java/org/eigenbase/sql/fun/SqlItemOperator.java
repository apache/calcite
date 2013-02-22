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

import java.util.List;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.*;

/**
 * The item operator {@code [ ... ]}, used to access a given element of an
 * array or map. For example, {@code myArray[3]} or {@code "myMap['foo']"}.
 */
class SqlItemOperator extends SqlSpecialOperator {

    private static final SqlSingleOperandTypeChecker ARRAY_OR_MAP =
        SqlTypeStrategies.or(
            SqlTypeStrategies.family(SqlTypeFamily.ARRAY),
            SqlTypeStrategies.family(SqlTypeFamily.MAP));

    public SqlItemOperator() {
        super("ITEM", SqlKind.OTHER_FUNCTION, 4, true, null, null, null);
    }

    @Override
    public int getRightPrec() {
        return 100;
    }

    @Override
    public int reduceExpr(int ordinal, List<Object> list) {
        SqlNode left = (SqlNode) list.get(ordinal - 1);
        SqlNode right = (SqlNode) list.get(ordinal + 1);
        final SqlParserUtil.ToTreeListItem treeListItem =
            (SqlParserUtil.ToTreeListItem) list.get(ordinal);
        SqlParserUtil.replaceSublist(
            list,
            ordinal - 1,
            ordinal + 2,
            createCall(
                left.getParserPosition()
                    .plus(right.getParserPosition())
                    .plus(treeListItem.getPos()),
            left,
            right));
        return ordinal - 1;
    }

    @Override
    public void unparse(
        SqlWriter writer, SqlNode[] operands, int leftPrec, int rightPrec)
    {
        operands[0].unparse(writer, leftPrec, 0);
        final SqlWriter.Frame frame = writer.startList("[", "]");
        operands[1].unparse(writer, 0, 0);
        writer.endList(frame);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        if (!ARRAY_OR_MAP.checkSingleOperandType(
                callBinding, callBinding.getCall().operands[0], 0,
                throwOnFailure))
        {
            return false;
        }
        final RelDataType operandType = callBinding.getOperandType(0);
        final FamilyOperandTypeChecker checker;
        switch (operandType.getSqlTypeName()) {
        case ARRAY:
            checker = SqlTypeStrategies.family(SqlTypeFamily.INTEGER);
            break;
        default:
        case MAP:
            checker = SqlTypeStrategies.family(
                operandType.getKeyType().getSqlTypeName().getFamily());
            break;
        }
        return checker.checkSingleOperandType(
            callBinding, callBinding.getCall().operands[1], 0, throwOnFailure);
    }

    @Override
    public String getAllowedSignatures(String name) {
        return "<ARRAY>[<INTEGER>]\n"
            + "<MAP>[<VALUE>]";
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        final RelDataType operandType = opBinding.getOperandType(0);
        switch (operandType.getSqlTypeName()) {
        case ARRAY:
            return operandType.getComponentType();
        case MAP:
            return operandType.getValueType();
        default:
            throw new AssertionError();
        }
    }
}

// End SqlItemOperator.java
