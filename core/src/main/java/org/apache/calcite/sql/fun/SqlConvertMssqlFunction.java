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
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import java.text.Collator;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A <i>partial</i> implementation of MSSQL CONVERT function
 * of the form {@code CONVERT ( data_type [ ( length ) ] , expression [ , style ] )}.
 * <ul>
 * <b>Important notes:</b>
 * <li>'style' parameter is ignored.</li><p>
 * <li>This is just a wrapper around CAST, and hence acts like CAST</li>
 * </ul>
 *
 *
 * @see SqlCastFunction
 */
public class SqlConvertMssqlFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlConvertMssqlFunction() {
    super("CONVERT",
        SqlKind.CAST,
        null,
        InferTypes.FIRST_KNOWN,
        null,
        SqlFunctionCategory.SYSTEM);
  }

  //~ Methods ----------------------------------------------------------------

  // Pretty much a copy of SqlCastFunction's inferReturnType
  // with operand idxs reordered
  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    assert opBinding.getOperandCount() == 2 || opBinding.getOperandCount() == 3;
    // guaranteed to be a SqlCallBinding
    SqlCallBinding callBinding = (SqlCallBinding) opBinding;
    SqlCallBinding reordered = getReorderedCallBinding(callBinding);
    return SqlStdOperatorTable.CAST.inferReturnType(reordered);

  }


  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 3);
  }

  /* CONVERT is a normal function
  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }
   */

  /* CONVERT is a normal function
  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 0;
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, 0, 0);
    writer.sep("AS");
    if (call.operand(1) instanceof SqlIntervalQualifier) {
      writer.sep("INTERVAL");
    }
    call.operand(1).unparse(writer, 0, 0);
    writer.endFunCall(frame);
  }
  */


  @Override public SqlNode rewriteCall(final SqlValidator validator, final SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();

    if (operands.size() != 2 && operands.size() != 3 ) {
      // invalidArgCount accepts int only, so picked 2 to show how many min args needed
      throw validator.newValidationError(call, RESOURCE.invalidArgCount(getName(), 2));
    }

    SqlParserPos pos = call.getParserPosition();
    return SqlStdOperatorTable.CAST.createCall(pos,
        operands.get(1),
        operands.get(0));
  }

  /**
   * Given a SqlCallBinding for MSSQL CONVERT, gets first two operands (data type, expr) and reorders
   * into (expr, data type) for the purposes of delegation logic to SqlCastFunction.
   * @param callBinding
   * @return Reordered SqlCallBinding
   */
  private SqlCallBinding getReorderedCallBinding(SqlCallBinding callBinding) {
    SqlCall reorderedCall = createCall(
        callBinding.getCall().getParserPosition(),
        callBinding.operand(1), callBinding.operand(0)
    );

    return new SqlCallBinding(
        callBinding.getValidator(), callBinding.getScope(), reorderedCall
    );
  }
}
