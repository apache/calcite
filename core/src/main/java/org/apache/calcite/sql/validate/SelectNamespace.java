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
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeUtil;

import static java.util.Objects.requireNonNull;

/**
 * Namespace offered by a sub-query.
 *
 * @see SelectScope
 * @see SetopNamespace
 */
public class SelectNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlSelect select;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SelectNamespace.
   *
   * @param validator     Validate
   * @param select        Select node
   * @param enclosingNode Enclosing node
   */
  public SelectNamespace(
      SqlValidatorImpl validator,
      SqlSelect select,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.select = select;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSelect getNode() {
    return select;
  }

  @Override public RelDataType validateImpl(RelDataType targetRowType) {
    validator.validateSelect(select, targetRowType);
    return requireNonNull(rowType, "rowType");
  }

  @Override public boolean supportsModality(SqlModality modality) {
    return validator.validateModality(select, modality, false);
  }

  @Override public SqlMonotonicity getMonotonicity(String columnName) {
    final RelDataType rowType = this.getRowTypeSansSystemColumns();
    final int field = SqlTypeUtil.findField(rowType, columnName);
    SelectScope selectScope = requireNonNull(
        validator.getRawSelectScope(select),
        () -> "rawSelectScope for " + select);
    final SqlNode selectItem = requireNonNull(
        selectScope.getExpandedSelectList(),
        () -> "expandedSelectList for selectScope of " + select).get(field);
    return validator.getSelectScope(select).getMonotonicity(selectItem);
  }
}
