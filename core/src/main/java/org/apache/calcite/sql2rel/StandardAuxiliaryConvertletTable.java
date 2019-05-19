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

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlGroupedWindowFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.HOP_END;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.HOP_START;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SESSION_END;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SESSION_START;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TUMBLE_END;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.TUMBLE_START;

/**
 * Standard implementation of {@link }.
 */
public class StandardAuxiliaryConvertletTable
    extends ReflectiveAuxiliaryConvertletTable {

  /** Singleton instance. */
  public static final AuxiliaryConvertletTable INSTANCE =
      new StandardAuxiliaryConvertletTable();

  //~ Constructors -----------------------------------------------------------

  private StandardAuxiliaryConvertletTable() {
    super();

    // Register convertlets
    registerGroupWindowOp(HOP_START);
    registerGroupWindowOp(HOP_END);
    registerGroupWindowOp(SESSION_START);
    registerGroupWindowOp(SESSION_END);
    registerGroupWindowOp(TUMBLE_START);
    registerGroupWindowOp(TUMBLE_END);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates and registers a convertlet for an operator in which
   * the SQL and Rex representations are structurally equivalent.
   *
   * @param op operator instance
   */
  private void registerGroupWindowOp(SqlOperator op) {
    registerOp(op, new GroupWindowAuxiliaryConverter(op));
  }

  /** Standard group window convertlet implementation.
   *
   * It contains converter for a group window function (e.g. TUMBLE)
   * into an expression for an auxiliary group function (e.g. TUMBLE_START).
   *
   * @see SqlStdOperatorTable#TUMBLE
   */
  private class GroupWindowAuxiliaryConverter implements AuxiliaryConvertlet {
    private final SqlGroupedWindowFunction op;

    GroupWindowAuxiliaryConverter(SqlOperator op) {
      this.op = (SqlGroupedWindowFunction) op;
    }

    public RexNode convert(RexBuilder rexBuilder, RexNode groupCall,
                           RexNode e) {
      switch (op.getKind()) {
      case TUMBLE_START:
      case HOP_START:
      case SESSION_START:
      case SESSION_END:
        return e;
      case TUMBLE_END:
        return rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS, e,
            ((RexCall) groupCall).operands.get(1));
      case HOP_END:
        return rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS, e,
            ((RexCall) groupCall).operands.get(2));
      default:
        throw new AssertionError("unknown: " + op);
      }
    }
  }
}

// End StandardAuxiliaryConvertletTable.java
