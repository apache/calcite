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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import javax.annotation.Nullable;

/**
 * Converts Calcite SUBSTRING call to Druid Expression when possible
 */
public class SubstringOperatorConversion implements DruidSqlOperatorConverter {
  @Override public SqlOperator calciteOperator() {
    return SqlStdOperatorTable.SUBSTRING;
  }

  @Nullable
  @Override public String toDruidExpression(RexNode rexNode, RelDataType rowType,
      DruidQuery query) {
    final RexCall call = (RexCall) rexNode;
    final String arg = DruidExpressions.toDruidExpression(
        call.getOperands().get(0), rowType, query);
    if (arg == null) {
      return null;
    }

    final int index = RexLiteral.intValue(call.getOperands().get(1)) - 1;
    // SQL is 1-indexed, Druid is 0-indexed.
    final int length;
    if (call.getOperands().size() > 2) {
      //case substring from index with length
      length = RexLiteral.intValue(call.getOperands().get(2));
    } else {
      //case substring from index to the end
      length = -1;
    }
    return DruidQuery.format("substring(%s, %s, %s)",
        arg,
        DruidExpressions.numberLiteral(index),
        DruidExpressions.numberLiteral(length));
  }
}

// End SubstringOperatorConversion.java
