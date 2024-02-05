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
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWithItem;

import java.util.Set;

/**
 * Validates the parse tree of a SQL statement to verify that
 * fields tagged as 'filter required' using {@link SemanticTable}
 * have WHERE and HAVING clauses on the designated fields.
 *
 */
public interface AlwaysFilterValidator {

  SqlValidatorCatalogReader getCatalogReader();


  SqlNode validate(SqlNode topNode);

  void validateQueryAlwaysFilter(SqlNode node, SqlValidatorScope scope,
      Set<String> alwaysFilterFields);

  void validateCall(
      SqlCall call,
      SqlValidatorScope scope);

  void validateSelect(SqlSelect select, Set<String> alwaysFilterFields);

  void validateJoin(SqlJoin join, SqlValidatorScope scope,
      Set<String> alwaysFilterFields);

  void validateWithItemAlwaysFilter(SqlWithItem withItem,
      Set<String> alwaysFilterFields);
}
