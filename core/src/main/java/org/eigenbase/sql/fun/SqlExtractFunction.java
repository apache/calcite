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
import org.eigenbase.util.*;

/**
 * The SQL <code>EXTRACT</code> operator. Extracts a specified field value from
 * a DATETIME or an INTERVAL. E.g.<br>
 * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code>
 * 23</code>
 */
public class SqlExtractFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  // SQL2003, Part 2, Section 4.4.3 - extract returns a exact numeric
  // TODO: Return type should be decimal for seconds
  public SqlExtractFunction() {
    super(
        "EXTRACT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        null,
        OperandTypes.INTERVALINTERVAL_INTERVALDATETIME,
        SqlFunctionCategory.SYSTEM);
  }

  //~ Methods ----------------------------------------------------------------

  public String getSignatureTemplate(int operandsCount) {
    Util.discard(operandsCount);
    return "{0}({1} FROM {2})";
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep("FROM");
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(frame);
  }
}

// End SqlExtractFunction.java
