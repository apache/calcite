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
import org.apache.calcite.sql.SqlSelect;

import java.util.Objects;

/**
 * The name-resolution scope of a LATERAL TABLE clause.
 *
 * <p>The objects visible are those in the parameters found on the left side of
 * the LATERAL TABLE clause, and objects inherited from the parent scope.
 */
class TableScope extends ListScope {
  //~ Instance fields --------------------------------------------------------

  private final SqlNode node;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a scope corresponding to a LATERAL TABLE clause.
   *
   * @param parent  Parent scope
   */
  TableScope(SqlValidatorScope parent, SqlNode node) {
    super(Objects.requireNonNull(parent));
    this.node = Objects.requireNonNull(node);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode getNode() {
    return node;
  }

  @Override public boolean isWithin(SqlValidatorScope scope2) {
    if (this == scope2) {
      return true;
    }
    SqlValidatorScope s = getValidator().getSelectScope((SqlSelect) node);
    return s.isWithin(scope2);
  }
}

// End TableScope.java
