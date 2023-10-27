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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Very similar to {@link WithItemNamespace} but created only for RECURSIVE queries. */
class WithItemRecursiveNameSpace extends WithItemNamespace {
  private final SqlWithItem withItem;
  private final SqlWithItemTableRef withItemTableRef;
  WithItemRecursiveNameSpace(SqlValidatorImpl validator,
      SqlWithItem withItem,
      @Nullable SqlNode enclosingNode) {
    super(validator, withItem, enclosingNode);
    this.withItem = withItem;
    this.withItemTableRef = new SqlWithItemTableRef(SqlParserPos.ZERO, withItem);
  }

  @Override public RelDataType getRowType() {
    if (rowType == null) {
      SqlBasicCall call;
      if (this.withItem.query.getKind() == SqlKind.WITH) {
        call = (SqlBasicCall) ((SqlWith) this.withItem.query).body;
      } else {
        call = (SqlBasicCall) this.withItem.query;
      }
      // As this is a recursive query we only should evaluate left child for getting the rowType.
      RelDataType leftChildType =
          validator.getNamespaceOrThrow(
              call.operand(0)).getRowType();
      SqlNodeList columnList = withItem.columnList;
      if (columnList == null || columnList.size() == 0) {
        // This should never happen but added to protect against the NullPointerException.
        return leftChildType;
      }
      final RelDataTypeFactory.Builder builder =
          validator.getTypeFactory().builder();
      Pair.forEach(SqlIdentifier.simpleNames(columnList),
          leftChildType.getFieldList(),
          (name, field) -> builder.add(name, field.getType()));
      rowType = builder.build();
    }
    return rowType;
  }

  @Override public @Nullable SqlNode getNode() {
    return withItemTableRef;
  }
}
