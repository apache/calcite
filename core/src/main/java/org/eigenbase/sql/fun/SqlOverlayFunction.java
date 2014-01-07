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

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;

/**
 * The <code>OVERLAY</code> function.
 */
public class SqlOverlayFunction extends SqlFunction {
  //~ Static fields/initializers ---------------------------------------------

  private static final SqlOperandTypeChecker otcCustom =
      SqlTypeStrategies.or(
          SqlTypeStrategies.otcStringX2Int,
          SqlTypeStrategies.otcStringX2IntX2);

  //~ Constructors -----------------------------------------------------------

  public SqlOverlayFunction() {
    super(
        "OVERLAY",
        SqlKind.OTHER_FUNCTION,
        SqlTypeStrategies.rtiNullableVaryingDyadicStringSumPrecision,
        null,
        otcCustom,
        SqlFunctionCategory.String);
  }

  //~ Methods ----------------------------------------------------------------

  public void unparse(
      SqlWriter writer,
      SqlNode[] operands,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    operands[0].unparse(writer, leftPrec, rightPrec);
    writer.sep("PLACING");
    operands[1].unparse(writer, leftPrec, rightPrec);
    writer.sep("FROM");
    operands[2].unparse(writer, leftPrec, rightPrec);
    if (4 == operands.length) {
      writer.sep("FOR");
      operands[3].unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(frame);
  }

  public String getSignatureTemplate(final int operandsCount) {
    switch (operandsCount) {
    case 3:
      return "{0}({1} PLACING {2} FROM {3})";
    case 4:
      return "{0}({1} PLACING {2} FROM {3} FOR {4})";
    }
    assert (false);
    return null;
  }
}

// End SqlOverlayFunction.java
