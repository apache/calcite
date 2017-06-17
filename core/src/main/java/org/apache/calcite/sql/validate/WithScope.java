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

import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWithItem;

import java.util.List;

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

  @Override public SqlValidatorNamespace getTableNamespace(List<String> names) {
    if (names.size() == 1 && names.get(0).equals(withItem.name.getSimple())) {
      return validator.getNamespace(withItem);
    }
    return super.getTableNamespace(names);
  }

  @Override public void resolveTable(List<String> names,
      SqlNameMatcher nameMatcher, Path path, Resolved resolved) {
    if (names.size() == 1
        && names.equals(withItem.name.names)) {
      final SqlValidatorNamespace ns = validator.getNamespace(withItem);
      final Step path2 = path
          .plus(ns.getRowType(), 0, names.get(0), StructKind.FULLY_QUALIFIED);
      resolved.found(ns, false, null, path2, null);
      return;
    }
    super.resolveTable(names, nameMatcher, path, resolved);
  }

  @Override public void resolve(List<String> names, SqlNameMatcher nameMatcher,
      boolean deep, Resolved resolved) {
    if (names.size() == 1
        && names.equals(withItem.name.names)) {
      final SqlValidatorNamespace ns = validator.getNamespace(withItem);
      final Step path = Path.EMPTY.plus(ns.getRowType(), 0, names.get(0),
          StructKind.FULLY_QUALIFIED);
      resolved.found(ns, false, null, path, null);
      return;
    }
    super.resolve(names, nameMatcher, deep, resolved);
  }
}

// End WithScope.java
