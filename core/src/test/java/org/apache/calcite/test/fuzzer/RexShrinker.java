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
package org.apache.calcite.test.fuzzer;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Random;

/**
 * Reduces {@link RexNode} by removing random bits of it.
 */
public class RexShrinker extends RexShuttle {
  private final Random r;
  private final RexBuilder rexBuilder;
  private boolean didWork;

  RexShrinker(Random r, RexBuilder rexBuilder) {
    this.r = r;
    this.rexBuilder = rexBuilder;
  }

  @Override public RexNode visitCall(RexCall call) {
    RelDataType type = call.getType();
    if (didWork || r.nextInt(100) > 80) {
      return super.visitCall(call);
    }
    if (r.nextInt(100) < 10 && !call.operands.isEmpty()) {
      // Replace with its argument
      RexNode node = call.operands.get(r.nextInt(call.operands.size()));
      if (node.getType().equals(type)) {
        return node;
      }
    }
    if (r.nextInt(100) < 10) {
      // Replace with simple value
      RexNode res = null;
      switch (r.nextInt(type.isNullable() ? 3 : 2)) {
      case 0:
        if (type.getSqlTypeName() == SqlTypeName.BOOLEAN) {
          res = rexBuilder.makeLiteral(true);
        } else if (type.getSqlTypeName() == SqlTypeName.INTEGER) {
          res = rexBuilder.makeLiteral(1, type, true);
        }
        break;
      case 1:
        if (type.getSqlTypeName() == SqlTypeName.BOOLEAN) {
          res = rexBuilder.makeLiteral(false);
        } else if (type.getSqlTypeName() == SqlTypeName.INTEGER) {
          res = rexBuilder.makeLiteral(0, type, true);
        }
        break;
      case 2:
        res = rexBuilder.makeNullLiteral(type);
      }
      if (res != null) {
        didWork = true;
        if (!res.getType().equals(type)) {
          return rexBuilder.makeCast(call.getParserPosition(), type, res);
        }
        return res;
      }
    }
    int operandSize = call.operands.size();
    SqlKind kind = call.getKind();
    if ((kind == SqlKind.AND || kind == SqlKind.OR) && operandSize > 2
        || kind == SqlKind.COALESCE) {
      // Trim random item
      if (operandSize == 1) {
        return call.operands.get(0);
      }
      ArrayList<RexNode> newOperands = new ArrayList<>(call.operands);
      newOperands.remove(r.nextInt(operandSize));
      if (newOperands.size() == 1) {
        return call.operands.get(0);
      }
      didWork = true;
      return call.clone(type, newOperands);
    }
    if ((kind == SqlKind.MINUS_PREFIX || kind == SqlKind.PLUS_PREFIX)
        && r.nextInt(100) < 10) {
      didWork = true;
      return call.operands.get(0);
    }
    if (kind == SqlKind.CASE) {
      ArrayList<RexNode> newOperands = new ArrayList<>(call.operands);
      int indexToRemove = r.nextInt(newOperands.size() - 1) & 0xfffe;
      // remove case branch
      newOperands.remove(indexToRemove);
      newOperands.remove(indexToRemove);
      didWork = true;
      if (newOperands.size() == 1) {
        return newOperands.get(0);
      }
      return call.clone(type, newOperands);
    }
    return super.visitCall(call);
  }
}
