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

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import java.math.BigDecimal;

/**
 * Base class for binary operators such as addition, subtraction, and
 * multiplication which are monotonic for the patterns <code>m op c</code> and
 * <code>c op m</code> where m is any monotonic expression and c is a constant.
 */
public class SqlMonotonicBinaryOperator extends SqlBinaryOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlMonotonicBinaryOperator(
      String name,
      SqlKind kind,
      int prec,
      boolean isLeftAssoc,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    super(
        name,
        kind,
        prec,
        isLeftAssoc,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    final SqlMonotonicity mono0 = call.getOperandMonotonicity(0);
    final SqlMonotonicity mono1 = call.getOperandMonotonicity(1);

    // constant <op> constant --> constant
    if ((mono1 == SqlMonotonicity.CONSTANT)
        && (mono0 == SqlMonotonicity.CONSTANT)) {
      return SqlMonotonicity.CONSTANT;
    }

    // monotonic <op> constant
    if (mono1 == SqlMonotonicity.CONSTANT) {
      // mono0 + constant --> mono0
      // mono0 - constant --> mono0
      if (getName().equals("-")
          || getName().equals("+")) {
        return mono0;
      }
      assert getName().equals("*");
      BigDecimal value = call.getOperandLiteralValue(1, BigDecimal.class);
      switch (value == null ? 1 : value.signum()) {
      case -1:
        // mono0 * negative constant --> reverse mono0
        return mono0.reverse();

      case 0:
        // mono0 * 0 --> constant (zero)
        return SqlMonotonicity.CONSTANT;

      default:
        // mono0 * positive constant --> mono0
        return mono0;
      }
    }

    // constant <op> mono
    if (mono0 == SqlMonotonicity.CONSTANT) {
      if (getName().equals("-")) {
        // constant - mono1 --> reverse mono1
        return mono1.reverse();
      }
      if (getName().equals("+")) {
        // constant + mono1 --> mono1
        return mono1;
      }
      assert getName().equals("*");
      if (!call.isOperandNull(0, true)) {
        BigDecimal value = call.getOperandLiteralValue(0, BigDecimal.class);
        switch (value == null ? 1 : value.signum()) {
        case -1:
          // negative constant * mono1 --> reverse mono1
          return mono1.reverse();

        case 0:
          // 0 * mono1 --> constant (zero)
          return SqlMonotonicity.CONSTANT;

        default:
          // positive constant * mono1 --> mono1
          return mono1;
        }
      }
    }

    // strictly asc + strictly asc --> strictly asc
    //   e.g. 2 * orderid + 3 * orderid
    //     is strictly increasing if orderid is strictly increasing
    // asc + asc --> asc
    //   e.g. 2 * orderid + 3 * orderid
    //     is increasing if orderid is increasing
    // asc + desc --> not monotonic
    //   e.g. 2 * orderid + (-3 * orderid) is not monotonic

    if (getName().equals("+")) {
      if (mono0 == mono1) {
        return mono0;
      } else if (mono0.unstrict() == mono1.unstrict()) {
        return mono0.unstrict();
      } else {
        return SqlMonotonicity.NOT_MONOTONIC;
      }
    }
    if (getName().equals("-")) {
      if (mono0 == mono1.reverse()) {
        return mono0;
      } else if (mono0.unstrict() == mono1.reverse().unstrict()) {
        return mono0.unstrict();
      } else {
        return SqlMonotonicity.NOT_MONOTONIC;
      }
    }
    if (getName().equals("*")) {
      return SqlMonotonicity.NOT_MONOTONIC;
    }

    return super.getMonotonicity(call);
  }
}

// End SqlMonotonicBinaryOperator.java
