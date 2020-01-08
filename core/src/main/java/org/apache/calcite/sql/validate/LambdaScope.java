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
import org.apache.calcite.sql.fun.SqlLambda;

import java.util.List;

class LambdaScope extends ListScope {

  private final SqlLambda lambda;

  /**
   * Creates a LambdaScope.
   */
  LambdaScope(SqlValidatorScope parent, SqlLambda lambda) {
    super(parent);
    this.lambda = lambda;
  }

  public SqlNode getNode() {
    return lambda;
  }

  @Override
  public SqlValidatorNamespace getTableNamespace(List<String> names) {
    return super.getTableNamespace(names);
  }

  @Override
  public void resolveTable(List<String> names,
      SqlNameMatcher nameMatcher, Path path, Resolved resolved) {
    super.resolveTable(names, nameMatcher, path, resolved);
  }

  @Override
  public void resolve(List<String> names, SqlNameMatcher nameMatcher,
      boolean deep, Resolved resolved) {
    super.resolve(names, nameMatcher, deep, resolved);
  }
}
