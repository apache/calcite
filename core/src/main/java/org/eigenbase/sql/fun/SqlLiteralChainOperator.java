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
import org.eigenbase.util.*;

/**
 * Internal operator, by which the parser represents a continued string literal.
 *
 * <p>The string fragments are {@link SqlLiteral} objects, all of the same type,
 * collected as the operands of an {@link SqlCall} using this operator. After
 * validation, the fragments will be concatenated into a single literal.
 *
 * <p>For a chain of {@link org.eigenbase.sql.SqlCharStringLiteral} objects, a
 * {@link SqlCollation} object is attached only to the head of the chain.
 */
public class SqlLiteralChainOperator extends SqlInternalOperator {
  //~ Constructors -----------------------------------------------------------

  SqlLiteralChainOperator() {
    super(
        "$LiteralChain",
        SqlKind.LITERAL_CHAIN,
        80,
        true,

        // precedence tighter than the * and || operators
        SqlTypeStrategies.rtiFirstArgType,
        SqlTypeStrategies.otiFirstKnown,
        SqlTypeStrategies.otcVariadic);
  }

  //~ Methods ----------------------------------------------------------------

  // all operands must be the same type
  private boolean argTypesValid(
      SqlCallBinding callBinding) {
    if (callBinding.getOperandCount() < 2) {
      return true; // nothing to compare
    }
    SqlNode operand = callBinding.getCall().operands[0];
    RelDataType firstType =
        callBinding.getValidator().deriveType(
            callBinding.getScope(),
            operand);
    for (int i = 1; i < callBinding.getCall().operands.length; i++) {
      operand = callBinding.getCall().operands[i];
      RelDataType otherType =
          callBinding.getValidator().deriveType(
              callBinding.getScope(),
              operand);
      if (!SqlTypeUtil.sameNamedType(firstType, otherType)) {
        return false;
      }
    }
    return true;
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (!argTypesValid(callBinding)) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }
    return true;
  }

  // Result type is the same as all the args, but its size is the
  // total size.
  // REVIEW mb 8/8/04: Possibly this can be achieved by combining
  // the strategy useFirstArgType with a new transformer.
  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    // Here we know all the operands have the same type,
    // which has a size (precision), but not a scale.
    RelDataType ret = opBinding.getOperandType(0);
    SqlTypeName typeName = ret.getSqlTypeName();
    assert typeName.allowsPrecNoScale()
        : "LiteralChain has impossible operand type "
        + typeName;
    int size = 0;
    for (RelDataType type : opBinding.collectOperandTypes()) {
      size += type.getPrecision();
      assert (type.getSqlTypeName().equals(typeName));
    }
    return opBinding.getTypeFactory().createSqlType(typeName, size);
  }

  public String getAllowedSignatures(String opName) {
    return opName + "(...)";
  }

  public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    // per the SQL std, each string fragment must be on a different line
    for (int i = 1; i < call.operands.length; i++) {
      SqlParserPos prevPos = call.operands[i - 1].getParserPosition();
      final SqlNode operand = call.operands[i];
      SqlParserPos pos = operand.getParserPosition();
      if (pos.getLineNum() <= prevPos.getLineNum()) {
        throw validator.newValidationError(
            operand,
            EigenbaseResource.instance().StringFragsOnSameLine.ex());
      }
    }
  }

  public void unparse(
      SqlWriter writer,
      SqlNode[] rands,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startList("", "");
    SqlCollation collation = null;
    for (int i = 0; i < rands.length; i++) {
      SqlLiteral rand = (SqlLiteral) rands[i];
      if (i > 0) {
        // SQL:2003 says there must be a newline between string
        // fragments.
        writer.newlineAndIndent();
      }
      if (rand instanceof SqlCharStringLiteral) {
        NlsString nls = ((SqlCharStringLiteral) rand).getNlsString();
        if (i == 0) {
          collation = nls.getCollation();

          // print with prefix
          writer.literal(nls.asSql(true, false));
        } else {
          // print without prefix
          writer.literal(nls.asSql(false, false));
        }
      } else if (i == 0) {
        // print with prefix
        rand.unparse(writer, leftPrec, rightPrec);
      } else {
        // print without prefix
        if (rand.getTypeName() == SqlTypeName.BINARY) {
          BitString bs = (BitString) rand.getValue();
          writer.literal("'" + bs.toHexString() + "'");
        } else {
          writer.literal("'" + rand.toValue() + "'");
        }
      }
    }
    if (collation != null) {
      collation.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }

  /**
   * Concatenates the operands of a call to this operator.
   */
  public static SqlLiteral concatenateOperands(SqlCall call) {
    assert call.operands.length > 0;
    assert call.operands[0] instanceof SqlLiteral : call.operands[0]
        .getClass();
    final List<SqlNode> operandList = Arrays.asList(call.operands);
    SqlLiteral[] fragments =
        operandList.toArray(new SqlLiteral[operandList.size()]);
    return SqlUtil.concatenateLiterals(fragments);
  }
}

// End SqlLiteralChainOperator.java
