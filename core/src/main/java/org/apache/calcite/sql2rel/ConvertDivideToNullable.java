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
package org.apache.calcite.sql2rel;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlShuttle;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Converts a RelNode tree such that division uses nullable division, which
 * produces NULL on division by zero.  Since there is no type information available,
 * we convert all instances of DIVIDE and MOD operators.  However, operators
 * acting on floating point values or intervals should be converted back.
 */
public class ConvertDivideToNullable extends SqlShuttle {
  public ConvertDivideToNullable() {  }

  @Override public @Nullable SqlNode visit(SqlCall call) {
    SqlNode node = super.visit(call);
    if (node instanceof SqlBasicCall) {
      SqlBasicCall newCall = (SqlBasicCall) node;
      SqlOperator operator = newCall.getOperator();
      switch (operator.getKind()) {
      case MOD:
        operator = SqlStdOperatorTable.MOD_0_NULL;
        break;
      case DIVIDE:
        operator = SqlStdOperatorTable.DIVIDE_0_NULL;
        break;
      default:
        break;
      }
      newCall.setOperator(operator);
      return newCall;
    }
    return node;
  }
}
