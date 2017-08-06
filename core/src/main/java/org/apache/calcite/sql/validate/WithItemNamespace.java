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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.util.Pair;

/** Very similar to {@link AliasNamespace}. */
class WithItemNamespace extends AbstractNamespace {
  private final SqlWithItem withItem;

  WithItemNamespace(SqlValidatorImpl validator, SqlWithItem withItem,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.withItem = withItem;
  }

  @Override protected RelDataType validateImpl(RelDataType targetRowType) {
    final SqlValidatorNamespace childNs =
        validator.getNamespace(withItem.query);
    final RelDataType rowType = childNs.getRowTypeSansSystemColumns();
    if (withItem.columnList == null) {
      return rowType;
    }
    final RelDataTypeFactory.Builder builder =
        validator.getTypeFactory().builder();
    for (Pair<SqlNode, RelDataTypeField> pair
        : Pair.zip(withItem.columnList, rowType.getFieldList())) {
      builder.add(((SqlIdentifier) pair.left).getSimple(),
          pair.right.getType());
    }
    return builder.build();
  }

  public SqlNode getNode() {
    return withItem;
  }

  @Override public String translate(String name) {
    if (withItem.columnList == null) {
      return name;
    }
    final RelDataType underlyingRowType =
          validator.getValidatedNodeType(withItem.query);
    int i = 0;
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (field.getName().equals(name)) {
        return underlyingRowType.getFieldList().get(i).getName();
      }
      ++i;
    }
    throw new AssertionError("unknown field '" + name
        + "' in rowtype " + underlyingRowType);
  }
}

// End WithItemNamespace.java
