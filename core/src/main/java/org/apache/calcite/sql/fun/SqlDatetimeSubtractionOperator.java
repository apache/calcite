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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlMonotonicity;

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

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return SqlStdOperatorTable.MINUS.getMonotonicity(call);
  }
}

// End SqlDatetimeSubtractionOperator.java
