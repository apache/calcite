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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Represents the name-resolution context for expressions in an ORDER BY clause.
 *
 * <p>In some dialects of SQL, the ORDER BY clause can reference column aliases
 * in the SELECT clause. For example, the query</p>
 *
 * <blockquote><code>SELECT empno AS x<br>
 * FROM emp<br>
 * ORDER BY x</code></blockquote>
 *
 * <p>is valid.</p>
 */
public class OrderByScope extends DelegatingScope {
  //~ Instance fields --------------------------------------------------------

  private final SqlNodeList orderList;
  private final SqlSelect select;

  //~ Constructors -----------------------------------------------------------

  OrderByScope(
      SqlValidatorScope parent,
      SqlNodeList orderList,
      SqlSelect select) {
    super(parent);
    this.orderList = orderList;
    this.select = select;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode getNode() {
    return orderList;
  }

  public void findAllColumnNames(List<SqlMoniker> result) {
    final SqlValidatorNamespace ns = validator.getNamespace(select);
    addColumnNames(ns, result);
  }

  public SqlQualified fullyQualify(SqlIdentifier identifier) {
    // If it's a simple identifier, look for an alias.
    if (identifier.isSimple()
        && validator.getConformance().isSortByAlias()) {
      String name = identifier.names.get(0);
      final SqlValidatorNamespace selectNs =
          validator.getNamespace(select);
      final RelDataType rowType = selectNs.getRowType();

      final SqlNameMatcher nameMatcher = validator.catalogReader.nameMatcher();
      final RelDataTypeField field = nameMatcher.field(rowType, name);
      int qualifidSelectAsCount = 0;
      String simpleOrderByName = identifier.getSimple();
      for (SqlNode selectedColumn : select.getSelectList()) {
        if (selectedColumn instanceof SqlBasicCall) {
          SqlBasicCall basicCall = (SqlBasicCall) selectedColumn;
          if (basicCall.getOperator().getKind().equals(SqlKind.AS)) {
            SqlIdentifier columnAsIdentifier =
              (SqlIdentifier) basicCall.getOperandList().get(1);
            String simpleSelect = columnAsIdentifier.getSimple();
            if (simpleOrderByName.equals(simpleSelect)) {
              qualifidSelectAsCount++;
            }
          }
        }
        if (qualifidSelectAsCount > 1) {
          throw validator.newValidationError(identifier,
              RESOURCE.columnAmbiguous(simpleOrderByName));
        }
      }
      if (field != null && !field.isDynamicStar() && qualifidSelectAsCount == 1) {
        // if identifier is resolved to a dynamic star, use super.fullyQualify() for such case.
        return SqlQualified.create(this, 1, selectNs, identifier);
      }
    }
    return super.fullyQualify(identifier);
  }

  public RelDataType resolveColumn(String name, SqlNode ctx) {
    final SqlValidatorNamespace selectNs = validator.getNamespace(select);
    final RelDataType rowType = selectNs.getRowType();
    final SqlNameMatcher nameMatcher = validator.catalogReader.nameMatcher();
    final RelDataTypeField field = nameMatcher.field(rowType, name);
    if (field != null) {
      return field.getType();
    }
    final SqlValidatorScope selectScope = validator.getSelectScope(select);
    return selectScope.resolveColumn(name, ctx);
  }

  public void validateExpr(SqlNode expr) {
    SqlNode expanded = validator.expandOrderExpr(select, expr);

    // expression needs to be valid in parent scope too
    parent.validateExpr(expanded);
  }
}

// End OrderByScope.java
