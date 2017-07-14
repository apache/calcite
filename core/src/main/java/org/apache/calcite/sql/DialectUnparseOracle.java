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

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.fun.SqlFloorFunction;

/**
 * <code>DialectUnparseOracle</code> defines how a <code>SqlOperator</code> should be unparsed
 * for execution against a Oracle database. It reverts to the unparse method of the operator
 * if this database's implementation is standard.
 */
public class DialectUnparseOracle extends SqlDialect.DefaultDialectUnparser {
  public void unparseCall(
      SqlOperator operator,
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    switch (operator.getKind()) {
    case FLOOR:
      if (call.operandCount() != 2) {
        operator.unparse(writer, call, leftPrec, rightPrec);
        return;
      }

      final SqlLiteral timeUnitNode = call.operand(1);
      final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

      SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
          timeUnitNode.getParserPosition());
      SqlFloorFunction.unparseDatetimeFunction(writer, call2, "TRUNC", true);
      break;

    default:
      operator.unparse(writer, call, leftPrec, rightPrec);
    }
  }
}

// End DialectUnparseOracle.java
