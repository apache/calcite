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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;

/**
 * SqlCursorConstructor defines the non-standard CURSOR(&lt;query&gt;)
 * constructor.
 */
public class SqlCursorConstructor extends SqlSpecialOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlCursorConstructor() {
    super(
        "CURSOR",
        SqlKind.CURSOR, MDX_PRECEDENCE,
        false,
        ReturnTypes.CURSOR,
        null,
        OperandTypes.ANY);
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    SqlSelect subSelect = (SqlSelect) call.operands[0];
    validator.declareCursor(subSelect, scope);
    subSelect.validateExpr(validator, scope);
    RelDataType type = super.deriveType(validator, scope, call);
    return type;
  }

  public void unparse(
      SqlWriter writer,
      SqlNode[] operands,
      int leftPrec,
      int rightPrec) {
    writer.keyword("CURSOR");
    final SqlWriter.Frame frame = writer.startList("(", ")");
    assert operands.length == 1;
    operands[0].unparse(writer, leftPrec, rightPrec);
    writer.endList(frame);
  }

  public boolean argumentMustBeScalar(int ordinal) {
    return false;
  }
}

// End SqlCursorConstructor.java
