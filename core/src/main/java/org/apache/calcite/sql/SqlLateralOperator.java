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
package org.apache.calcite.sql;

import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;


/**
 * An operator describing a LATERAL specification.
 */
public class SqlLateralOperator extends SqlSpecialOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlLateralOperator(
      String name,
      SqlKind kind,
      int pred,
      boolean isLeftAssoc,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    super(
        name,
        kind,
        pred,
        isLeftAssoc,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker);
  }

  //~ Methods ----------------------------------------------------------------

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    if (call.operandCount() > 0
            && call.getOperandList().get(0).getKind() == SqlKind.COLLECTION_TABLE) {
      // do not create ( ) around the following TABLE clause
      writer.print(getName());
      writer.print(" ");
      final SqlWriter.Frame frame =
              writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL);
      final SqlLiteral quantifier = call.getFunctionQuantifier();
      if (quantifier != null) {
        quantifier.unparse(writer, 0, 0);
      }
      if (call.operandCount() == 0) {
        switch (call.getOperator().getSyntax()) {
        case FUNCTION_STAR:
          writer.sep("*");
        }
      }
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    } else {
      SqlUtil.unparseFunctionSyntax(this, writer, call);
    }
  }
}

// End SqlLateralOperator.java
