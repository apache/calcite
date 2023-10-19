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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParserPos;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Very similar to {@link WithItemNamespace} but created only for RECURSIVE queries. */
class WithItemRecursiveNamespace extends WithItemNamespace {
  private final SqlWithItem withItem;
  private final SqlWithItemTableRef withItemTableRef;

  /**
   * Creates a Namespace for a query specified in {@code WITH RECURSIVE} clause.
   *
   * @param validator Validator
   * @param withItem A with query item specified in {@code WITH} clause
   * @param enclosingNode Enclosing node
   */
  WithItemRecursiveNamespace(SqlValidatorImpl validator,
      SqlWithItem withItem,
      @Nullable SqlNode enclosingNode) {
    super(validator, withItem, enclosingNode);
    this.withItem = withItem;
    this.withItemTableRef = new SqlWithItemTableRef(SqlParserPos.ZERO, withItem);
  }

  @Override protected SqlNode getQuery() {
    SqlNode call = this.withItem.query;
    while (call.getKind() == SqlKind.WITH) {
      call = ((SqlWith) call).body;
    }
    return ((SqlCall) call).operand(0);
  }

  @Override public @Nullable SqlNode getNode() {
    return withItemTableRef;
  }
}
