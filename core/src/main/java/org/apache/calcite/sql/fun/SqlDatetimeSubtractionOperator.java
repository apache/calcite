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
import org.eigenbase.sql.validate.SqlMonotonicity;
import org.eigenbase.sql.validate.SqlValidatorScope;

/**
 * A special operator for the subtraction of two DATETIMEs. The format of
 * DATETIME substraction is:
 *
 * <blockquote><code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")"
 * &lt;interval qualifier&gt;</code></blockquote>
 *
 * <p>This operator is special since it needs to hold the
 * additional interval qualifier specification.</p>
 */
public class SqlDatetimeSubtractionOperator extends SqlSpecialOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlDatetimeSubtractionOperator() {
    super(
        "-",
        SqlKind.MINUS,
        40,
        true,
        ReturnTypes.ARG2_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.MINUS_DATE_OPERATOR);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startList("(", ")");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep("-");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.endList(frame);
    call.operand(2).unparse(writer, leftPrec, rightPrec);
  }

  public SqlMonotonicity getMonotonicity(
      SqlCall call,
      SqlValidatorScope scope) {
    return SqlStdOperatorTable.MINUS.getMonotonicity(call, scope);
  }
}

// End SqlDatetimeSubtractionOperator.java
