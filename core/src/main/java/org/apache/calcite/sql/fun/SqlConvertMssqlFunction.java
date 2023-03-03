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
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import java.text.Collator;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * SqlCastFunction. Note that the std functions are really singleton objects,
 * because they always get fetched via the StdOperatorTable. So you can't store
 * any local info in the class and hence the return type data is maintained in
 * operand[1] through the validation phase.
 *
 * <p>Can be used for both {@link SqlCall} and
 * {@link org.apache.calcite.rex.RexCall}.
 * Note that the {@code SqlCall} has two operands (expression and type),
 * while the {@code RexCall} has one operand (expression) and the type is
 * obtained from {@link org.apache.calcite.rex.RexNode#getType()}.
 *
 * @see SqlCastOperator
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
    // TODO: Clarify with reviewers
    // guaranteed to be a SqlCallBinding
    SqlCallBinding callBinding = (SqlCallBinding) opBinding;
    SqlCallBinding reordered = getReorderedCallBinding(callBinding);
    return SqlStdOperatorTable.CAST.inferReturnType(reordered);

  }

  /* CONVERT is a normal function
  @Override public String getSignatureTemplate(final int operandsCount) {
    assert operandsCount == 2;
    return "{0}({1} AS {2})";
  }
  */

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2,3);
  }

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {

    // check if style code is actually an integer
    if (callBinding.getOperandCount() == 3) {
      SqlNode styleCode = callBinding.operand(2);
      RelDataType styleCodeType =
          callBinding.getValidator().getValidatedNodeType(styleCode);

      if (SqlTypeUtil.isIntType(styleCodeType)) {
        return false;
      }
    }

    // for first two, delegate to CAST
    SqlCallBinding reordered = getReorderedCallBinding(callBinding);
    return SqlStdOperatorTable.CAST.checkOperandTypes(reordered, throwOnFailure);
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

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    SqlCallBinding reordered = getReorderedCallBinding((SqlCallBinding) call);
    return SqlStdOperatorTable.CAST.getMonotonicity(reordered);
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

    SqlCallBinding reorderedCallBinding = new SqlCallBinding(
        callBinding.getValidator(), callBinding.getScope(), reorderedCall
    );

    return reorderedCallBinding;
  }
}
