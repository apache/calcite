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
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * A special operator for the subtraction of two DATETIMEs. The format of
 * DATETIME subtraction is:
 *
 * <blockquote><code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")"
 * &lt;interval qualifier&gt;</code></blockquote>
 *
 * <p>This operator is special since it needs to hold the
 * additional interval qualifier specification, when in {@link SqlCall} form.
 * In {@link org.apache.calcite.rex.RexNode} form, it has only two parameters,
 * and the return type describes the desired type of interval.
 *
 * <p>When being used for BigQuery's {@code TIMESTAMP_SUB}, {@code TIME_SUB},
 * and {@code DATE_SUB} operators, this operator subtracts an interval value
 * from a timestamp value. The return type differs due to differing number of
 * parameters and ordering. This is accounted for by passing in a
 * {@link SqlReturnTypeInference} which is passed in by
 * the standard {@link SqlStdOperatorTable#MINUS_DATE MINUS_DATE}
 * and the library {@link SqlInternalOperators#MINUS_DATE2 MINUS_DATE2}
 * operators at their respective initializations.
 */
public class SqlDatetimeSubtractionOperator extends SqlSpecialOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlDatetimeSubtractionOperator(String name,
      SqlReturnTypeInference returnTypeInference) {
    super("-", SqlKind.MINUS, 40, true, returnTypeInference,
        InferTypes.FIRST_KNOWN, OperandTypes.MINUS_DATE_OPERATOR);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    writer.getDialect().unparseSqlDatetimeArithmetic(
        writer, call, SqlKind.MINUS, leftPrec, rightPrec);
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return SqlStdOperatorTable.MINUS.getMonotonicity(call);
  }
}
