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
package org.apache.calcite.rex;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.apiguardian.api.API;

import java.util.List;

/**
 * Context required to normalize a row-expression.
 *
 * <p>Currently, only simple normalization is supported, such as:
 *
 * <ul>
 *   <li>$2 = $1 &rarr; $1 = $2</li>
 *   <li>$2 &gt; $1 &rarr; $1 &lt; $2</li>
 *   <li>1.23 = $1 &rarr; $1 = 1.23</li>
 *   <li>OR(OR(udf($1), $2), $3) &rarr; OR($3, OR($2, udf($1)))</li>
 * </ul>
 *
 * <p>In the future, this component may extend to support more normalization cases
 * for general promotion. e.g. the strategy to decide which operand is more complex
 * should be more smart.
 *
 * <p>There is no one normalization strategy that works for all cases, and no consensus about what
 * the desired strategies should be. So by default, the normalization is disabled. We do force
 * normalization when computing the digest of {@link RexCall}s during planner planning.
 */
public class RexNormalize {

  private RexNormalize() {}

  /**
   * Normalizes the variables of a rex call.
   *
   * @param operator The operator
   * @param operands The operands
   *
   * @return normalized variables of the call or the original
   * if there is no need to normalize
   */
  @API(since = "1.24", status = API.Status.EXPERIMENTAL)
  public static Pair<SqlOperator, List<RexNode>> normalize(
      SqlOperator operator,
      List<RexNode> operands) {
    final Pair<SqlOperator, List<RexNode>> original = Pair.of(operator, operands);
    if (!allowsNormalize()) {
      return original;
    }
    // Quick check.
    if (operands.size() != 2) {
      return original;
    }

    final RexNode operand0 = operands.get(0);
    final RexNode operand1 = operands.get(1);

    // If arguments are the same, then we normalize < vs >
    // '<' == 60, '>' == 62, so we prefer <.
    final SqlKind kind = operator.getKind();
    final SqlKind reversedKind = kind.reverse();
    final int x = reversedKind.compareTo(kind);
    if (x < 0) {
      return Pair.of(
          SqlStdOperatorTable.reverse(operator),
          ImmutableList.of(operand1, operand0));
    }
    if (x > 0) {
      return original;
    }

    if (!isSymmetricalCall(operator, operand0, operand1)) {
      return original;
    }

    if (reorderOperands(operand0, operand1) < 0) {
      // $0=$1 is the same as $1=$0, so we make sure the digest is the same for them.

      // When $1 > $0 is normalized, the operation needs to be flipped
      // so we sort arguments first, then flip the sign.
      return Pair.of(
          SqlStdOperatorTable.reverse(operator),
          ImmutableList.of(operand1, operand0));
    }
    return original;
  }

  /**
   * Compares two operands to see which one should be normalized to be in front of the other.
   *
   * <p>We can always use the #hashCode to reorder the operands, do it as a fallback to keep
   * good readability.
   *
   * @param operand0  First operand
   * @param operand1  Second operand
   *
   * @return non-negative (>=0) if {@code operand0} should be in the front,
   * negative if {@code operand1} should be in the front
   */
  private static int reorderOperands(RexNode operand0, RexNode operand1) {
    // Reorder the operands based on the SqlKind enumeration sequence,
    // smaller is in the behind, e.g. the literal is behind of input ref and AND, OR.
    int x = operand0.getKind().compareTo(operand1.getKind());
    // If the operands are same kind, use the hashcode to reorder.
    // Note: the RexInputRef's hash code is its index.
    return x != 0 ? x : operand1.hashCode() - operand0.hashCode();
  }

  /** Returns whether a call is symmetrical. **/
  private static boolean isSymmetricalCall(
      SqlOperator operator,
      RexNode operand0,
      RexNode operand1) {
    return operator.isSymmetrical()
        || SqlKind.SYMMETRICAL_SAME_ARG_TYPE.contains(operator.getKind())
            && SqlTypeUtil.equalSansNullability(operand0.getType(), operand1.getType());
  }

  /**
   * The digest of {@code RexNode} is normalized by default.
   *
   * @return true if the digest allows normalization
   */
  private static boolean allowsNormalize() {
    return CalciteSystemProperty.ENABLE_REX_DIGEST_NORMALIZE.value();
  }
}
