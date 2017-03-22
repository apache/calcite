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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * An implementation of {@link SqlValidatorNamespace} that delegates all methods
 * to an underlying object.
 */
public abstract class DelegatingNamespace implements SqlValidatorNamespace {
  //~ Instance fields --------------------------------------------------------

  protected final SqlValidatorNamespace namespace;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a DelegatingNamespace.
   *
   * @param namespace Underlying namespace, to delegate to
   */
  protected DelegatingNamespace(SqlValidatorNamespace namespace) {
    this.namespace = namespace;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlValidator getValidator() {
    return namespace.getValidator();
  }

  public SqlValidatorTable getTable() {
    return namespace.getTable();
  }

  public RelDataType getRowType() {
    return namespace.getRowType();
  }

  public void setType(RelDataType type) {
    namespace.setType(type);
  }

  public RelDataType getRowTypeSansSystemColumns() {
    return namespace.getRowTypeSansSystemColumns();
  }

  public RelDataType getType() {
    return namespace.getType();
  }

  public void validate(RelDataType targetRowType) {
    namespace.validate(targetRowType);
  }

  public SqlNode getNode() {
    return namespace.getNode();
  }

  public SqlNode getEnclosingNode() {
    return namespace.getEnclosingNode();
  }

  public SqlValidatorNamespace lookupChild(
      String name) {
    return namespace.lookupChild(name);
  }

  public boolean fieldExists(String name) {
    return namespace.fieldExists(name);
  }

  public List<Pair<SqlNode, SqlMonotonicity>> getMonotonicExprs() {
    return namespace.getMonotonicExprs();
  }

  public SqlMonotonicity getMonotonicity(String columnName) {
    return namespace.getMonotonicity(columnName);
  }

  @SuppressWarnings("deprecation")
  public void makeNullable() {
  }

  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    } else {
      return namespace.unwrap(clazz);
    }
  }

  public boolean isWrapperFor(Class<?> clazz) {
    return clazz.isInstance(this)
        || namespace.isWrapperFor(clazz);
  }
}

// End DelegatingNamespace.java
