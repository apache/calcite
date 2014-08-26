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
package org.eigenbase.sql.validate;

import java.util.List;

import org.eigenbase.sql.*;
import org.eigenbase.sql.SqlWithItem;

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
class WithScope extends ListScope {
  private final SqlWithItem withItem;

  /** Creates a WithScope. */
  WithScope(SqlValidatorScope parent, SqlWithItem withItem) {
    super(parent);
    this.withItem = withItem;
  }

  public SqlNode getNode() {
    return withItem;
  }

  @Override
  public SqlValidatorNamespace getTableNamespace(List<String> names) {
    if (names.size() == 1 && names.get(0).equals(withItem.name.getSimple())) {
      return validator.getNamespace(withItem);
    }
    return super.getTableNamespace(names);
  }

  @Override
  public SqlValidatorNamespace resolve(String name,
      SqlValidatorScope[] ancestorOut,
      int[] offsetOut) {
    if (name.equals(withItem.name.getSimple())) {
      return validator.getNamespace(withItem);
    }
    return super.resolve(name, ancestorOut, offsetOut);
  }
}

// End WithScope.java
