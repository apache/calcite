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
 * Scope of a Qualify clause. Mostly a wrapper around the parent's selectScope, but has some utility
 * for checking validity of the qualify clause.
 */
public class QualifyScope extends DelegatingScope implements WindowedSelectScope {

  //~ Instance fields --------------------------------------------------------

  private final SqlNode qualifyNode;

  //~ Constructors -----------------------------------------------------------

  QualifyScope(
      SqlValidatorScope parent,
      SqlNode qualifyNode) {
    super(parent);
    this.qualifyNode = qualifyNode;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean checkWindowedAggregateExpr(SqlNode expr, boolean deep) {
    // Fully-qualify any identifiers in expr.
    if (deep) {
      expr = validator.expand(expr, this);
    }

    // Create a new checker, which will visit all the nested expressions and determine if we have
    // a window operation
    final WindowedAggChecker windowedAggChecker = new WindowedAggChecker();

    // Do the visiting
    expr.accept(windowedAggChecker);

    // return if we saw a window
    return windowedAggChecker.sawWindow();
  }

  @Override public SqlNode getNode() {
    return qualifyNode;
  }


  @Override public void validateExpr(SqlNode expr) {
    // For a qualify clause, it should be validated in the scope of the enclosing expression
    // This is not the default for a DelegatingScope, strangely.
    parent.validateExpr(expr);
  }

}
