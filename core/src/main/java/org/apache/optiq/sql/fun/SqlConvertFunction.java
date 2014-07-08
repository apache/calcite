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
package org.apache.optiq.sql.fun;

import org.apache.optiq.sql.*;

/**
 * Common base for the <code>CONVERT</code> and <code>TRANSLATE</code>
 * functions.
 */
public class SqlConvertFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  protected SqlConvertFunction(String name) {
    super(
        name,
        SqlKind.OTHER_FUNCTION,
        null,
        null,
        null,
        SqlFunctionCategory.STRING);
  }

  //~ Methods ----------------------------------------------------------------

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep("USING");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(frame);
  }

  public String getSignatureTemplate(final int operandsCount) {
    switch (operandsCount) {
    case 2:
      return "{0}({1} USING {2})";
    }
    assert false;
    return null;
  }
}

// End SqlConvertFunction.java
