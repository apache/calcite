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

/** Scope providing the objects that are available after evaluating an item
 * in a WITH clause.
 *
 * <p>For example, in</p>
 *
 * <blockquote>{@code WITH t1 AS (q1) t2 AS (q2) q3}</blockquote>
 *
 * <p>{@code t1} provides a scope that is used to validate {@code q2}
 * (and therefore {@code q2} may reference {@code t1}),
 * and {@code t2} provides a scope that is used to validate {@code q3}
 * (and therefore q3 may reference {@code t1} and {@code t2}).</p>
 */
public class LateralScope extends ListScope {
  private final SqlNode leftJoin;

  /** Creates a LateralScope. */
  LateralScope(SqlValidatorScope parent, SqlNode leftJoin) {
    super(parent);
    this.leftJoin = leftJoin;
  }

  @Override public SqlNode getNode() {
    return leftJoin;
  }


}
