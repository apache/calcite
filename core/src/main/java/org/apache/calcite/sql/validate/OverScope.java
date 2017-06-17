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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * The name-resolution scope of a OVER clause. The objects visible are those in
 * the parameters found on the left side of the over clause, and objects
 * inherited from the parent scope.
 *
 * <p>This object is both a {@link SqlValidatorScope} only. In the query</p>
 *
 * <blockquote>
 * <pre>SELECT name FROM (
 *     SELECT *
 *     FROM emp OVER (
 *         ORDER BY empno
 *         RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING))
 * </pre>
 * </blockquote>
 *
 * <p>We need to use the {@link OverScope} as a {@link SqlValidatorNamespace}
 * when resolving names used in the window specification.</p>
 */
public class OverScope extends ListScope {
  //~ Instance fields --------------------------------------------------------

  private final SqlCall overCall;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a scope corresponding to a SELECT clause.
   *
   * @param parent   Parent scope, or null
   * @param overCall Call to OVER operator
   */
  OverScope(
      SqlValidatorScope parent,
      SqlCall overCall) {
    super(parent);
    this.overCall = overCall;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode getNode() {
    return overCall;
  }

  public SqlMonotonicity getMonotonicity(SqlNode expr) {
    SqlMonotonicity monotonicity = expr.getMonotonicity(this);
    if (monotonicity != SqlMonotonicity.NOT_MONOTONIC) {
      return monotonicity;
    }

    if (children.size() == 1) {
      final SqlValidatorNamespace child = children.get(0).namespace;
      final List<Pair<SqlNode, SqlMonotonicity>> monotonicExprs =
          child.getMonotonicExprs();
      for (Pair<SqlNode, SqlMonotonicity> pair : monotonicExprs) {
        if (expr.equalsDeep(pair.left, Litmus.IGNORE)) {
          return pair.right;
        }
      }
    }
    return super.getMonotonicity(expr);
  }
}

// End OverScope.java
