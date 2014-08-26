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
package org.eigenbase.sql.fun;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;

/**
 * The <code>POSITION</code> function.
 */
public class SqlPositionFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  // FIXME jvs 25-Jan-2009:  POSITION should verify that
  // params are all same character set, like OVERLAY does implicitly
  // as part of rtiDyadicStringSumPrecision

  public SqlPositionFunction() {
    super(
        "POSITION",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        OperandTypes.STRING_SAME_SAME,
        SqlFunctionCategory.NUMERIC);
  }

  //~ Methods ----------------------------------------------------------------

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep("IN");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(frame);
  }

  public String getSignatureTemplate(final int operandsCount) {
    assert operandsCount == 2;
    return "{0}({1} IN {2})";
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    // check that the two operands are of same type.
    return OperandTypes.SAME_SAME.checkOperandTypes(
        callBinding, throwOnFailure)
        && super.checkOperandTypes(callBinding, throwOnFailure);
  }
}

// End SqlPositionFunction.java
