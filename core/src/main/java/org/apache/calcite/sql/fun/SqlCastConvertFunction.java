/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * MSSQL CONVERT function
 * of the form {@code CONVERT ( data_type [ ( length ) ] , expression [ , style ] )}.
 * <ul>
 * <b>Important notes:</b>
 * <li>'style' parameter is ignored.</li>
 *
 * <p><li>This is just a wrapper around CAST, and hence acts like CAST</li>
 * </ul>
 *
 *
 * @see SqlCastFunction
 */
public class SqlCastConvertFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlCastConvertFunction() {
    super("CONVERT",
        SqlKind.CAST,
        null,
        InferTypes.FIRST_KNOWN,
        null,
        SqlFunctionCategory.SYSTEM);
  }

  //~ Methods ----------------------------------------------------------------

  /** Switches places of 1st and 2nd operand in SqlCallBinding, and delegates to SqlCastFunction's
   * inferReturnType
   */
  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    assert opBinding.getOperandCount() == 2 || opBinding.getOperandCount() == 3;
    // guaranteed to be a SqlCallBinding
    SqlCallBinding callBinding = (SqlCallBinding) opBinding;
    SqlCall reorderedCall = createCall(
        callBinding.getCall().getParserPosition(),
        callBinding.operand(1), callBinding.operand(0));
    SqlCallBinding reorderedCallBinding = new SqlCallBinding(
        callBinding.getValidator(), callBinding.getScope(), reorderedCall);

    return SqlStdOperatorTable.CAST.inferReturnType(reorderedCallBinding);
  }


  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 3);
  }

  @Override public SqlNode rewriteCall(final SqlValidator validator, final SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();

    if (operands.size() != 2 && operands.size() != 3) {
      // invalidArgCount accepts int only, so picked 2 to show how many min args needed
      throw validator.newValidationError(call, RESOURCE.invalidArgCount(getName(), 2));
    }

    SqlParserPos pos = call.getParserPosition();
    return SqlStdOperatorTable.CAST.createCall(pos,
        operands.get(1),
        operands.get(0));
  }
}
