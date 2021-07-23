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

import org.apache.calcite.sql.SqlUnpivot;

import static java.util.Objects.requireNonNull;

/**
 * Scope for expressions in an {@code UNPIVOT} clause.
 */
public class UnpivotScope extends ListScope {

  //~ Instance fields ---------------------------------------------
  private final SqlUnpivot unpivot;

  /** Creates an UnpivotScope. */
  public UnpivotScope(SqlValidatorScope parent, SqlUnpivot unpivot) {
    super(parent);
    this.unpivot = unpivot;
  }

  /** By analogy with
   * {@link ListScope#getChildren()}, but this
   * scope only has one namespace, and it is anonymous. */
  public SqlValidatorNamespace getChild() {
    return requireNonNull(
        validator.getNamespace(unpivot.query),
        () -> "namespace for unpivot.query " + unpivot.query);
  }

  @Override public SqlUnpivot getNode() {
    return unpivot;
  }
}
