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

import org.apache.calcite.sql.SqlNode;

/**
 * An extension to the {@link SqlValidatorScope} interface which indicates that
 * the scope is aggregating.
 *
 * <p>A scope which is aggregating must implement this interface. Such a scope
 * will return the same set of identifiers as its parent scope, but some of
 * those identifiers may not be accessible because they are not in the GROUP BY
 * clause.
 */
public interface AggregatingScope extends SqlValidatorScope {
  //~ Methods ----------------------------------------------------------------

  /**
   * Checks whether an expression is constant within the GROUP BY clause. If
   * the expression completely matches an expression in the GROUP BY clause,
   * returns true. If the expression is constant within the group, but does
   * not exactly match, returns false. If the expression is not constant,
   * throws an exception. Examples:
   *
   * <ul>
   * <li>If we are 'f(b, c)' in 'SELECT a + f(b, c) FROM t GROUP BY a', then
   * the whole expression matches a group column. Return true.
   * <li>Just an ordinary expression in a GROUP BY query, such as 'f(SUM(a),
   * 1, b)' in 'SELECT f(SUM(a), 1, b) FROM t GROUP BY b'. Returns false.
   * <li>Illegal expression, such as 'f(5, a, b)' in 'SELECT f(a, b) FROM t
   * GROUP BY a'. Throws when it enounters the 'b' operand, because it is not
   * in the group clause.
   * </ul>
   */
  boolean checkAggregateExpr(SqlNode expr, boolean deep);
}

// End AggregatingScope.java
