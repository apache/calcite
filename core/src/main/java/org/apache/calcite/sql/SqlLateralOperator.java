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

import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * An operator describing a LATERAL specification.
 */
public class SqlLateralOperator extends SqlSpecialOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlLateralOperator(SqlKind kind) {
    super(kind.name(), kind, 200, true, ReturnTypes.ARG0, null,
        OperandTypes.ANY);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    if (call.operandCount() == 1
        && (call.getOperandList().get(0).getKind() == SqlKind.COLLECTION_TABLE
            || call.getOperandList().get(0).getKind() == SqlKind.SNAPSHOT
            || call.getOperandList().get(0).getKind() == SqlKind.SELECT
            || call.getOperandList().get(0).getKind() == SqlKind.AS)) {

      // do not create ( ) around the following TABLE clause
      writer.keyword(getName());
      call.operand(0).unparse(writer, 0, 0);
    } else {
      SqlUtil.unparseFunctionSyntax(this, writer, call);
    }
  }
}

// End SqlLateralOperator.java
