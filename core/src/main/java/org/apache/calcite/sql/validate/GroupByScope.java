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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

/**
 * Represents the name-resolution context for expressions in an GROUP BY clause.
 *
 * <p>In some dialects of SQL, the GROUP BY clause can reference column aliases
 * in the SELECT clause. For example, the query</p>
 *
 * <blockquote><code>SELECT empno AS x<br>
 * FROM emp<br>
 * GROUP BY x</code></blockquote>
 *
 * <p>is valid.</p>
 */
public class GroupByScope extends DelegatingScope {
  //~ Instance fields --------------------------------------------------------

  private final SqlNodeList groupByList;
  private final SqlSelect select;

  //~ Constructors -----------------------------------------------------------

  GroupByScope(
      SqlValidatorScope parent,
      SqlNodeList groupByList,
      SqlSelect select) {
    super(parent);
    this.groupByList = groupByList;
    this.select = select;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode getNode() {
    return groupByList;
  }

  public void validateExpr(SqlNode expr) {
    SqlNode expanded = validator.expandGroupByOrHavingExpr(expr, this, select, false);

    // expression needs to be valid in parent scope too
    parent.validateExpr(expanded);
  }
}

// End GroupByScope.java
