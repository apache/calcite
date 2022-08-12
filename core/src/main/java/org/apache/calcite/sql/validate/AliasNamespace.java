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
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Namespace for an <code>AS t(c1, c2, ...)</code> clause.
 *
 * <p>A namespace is necessary only if there is a column list, in order to
 * re-map column names; a <code>relation AS t</code> clause just uses the same
 * namespace as <code>relation</code>.
 */
public class AliasNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  protected final SqlCall call;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AliasNamespace.
   *
   * @param validator     Validator
   * @param call          Call to AS operator
   * @param enclosingNode Enclosing node
   */
  protected AliasNamespace(
      SqlValidatorImpl validator,
      SqlCall call,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.call = call;
    assert call.getOperator() == SqlStdOperatorTable.AS;
    assert call.operandCount() >= 2;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean supportsModality(SqlModality modality) {
    final List<SqlNode> operands = call.getOperandList();
    final SqlValidatorNamespace childNs =
        validator.getNamespaceOrThrow(operands.get(0));
    return childNs.supportsModality(modality);
  }

  @Override protected RelDataType validateImpl(RelDataType targetRowType) {
    final List<SqlNode> operands = call.getOperandList();
    final SqlValidatorNamespace childNs =
        validator.getNamespaceOrThrow(operands.get(0));
    final RelDataType rowType = childNs.getRowTypeSansSystemColumns();
    final RelDataType aliasedType;
    if (operands.size() == 2) {
      final SqlNode node = operands.get(0);
      // Alias is 'AS t' (no column list).
      // If the sub-query is UNNEST or VALUES,
      // and the sub-query has one column,
      // then the namespace's sole column is named after the alias.
      if (rowType.getFieldCount() == 1) {
        aliasedType = validator.getTypeFactory().builder()
            .kind(rowType.getStructKind())
            .add(((SqlIdentifier) operands.get(1)).getSimple(),
                rowType.getFieldList().get(0).getType())
            .build();
        // If the sub-query is UNNEST with ordinality
        // and the sub-query has two columns: data column, ordinality column
        // then the namespace's sole column is named after the alias.
      } else if (node.getKind() == SqlKind.UNNEST && rowType.getFieldCount() == 2
          && ((SqlUnnestOperator) ((SqlBasicCall) node).getOperator()).withOrdinality) {
        aliasedType = validator.getTypeFactory().builder()
            .kind(rowType.getStructKind())
            .add(((SqlIdentifier) operands.get(1)).getSimple(),
                rowType.getFieldList().get(0).getType())
            .add(rowType.getFieldList().get(1))
            .build();
      } else {
        aliasedType = rowType;
      }
    } else {
      // Alias is 'AS t (c0, ..., cN)'
      final List<SqlNode> columnNames = Util.skip(operands, 2);
      final List<String> nameList = SqlIdentifier.simpleNames(columnNames);
      final int i = Util.firstDuplicate(nameList);
      if (i >= 0) {
        final SqlIdentifier id = (SqlIdentifier) columnNames.get(i);
        throw validator.newValidationError(id,
            RESOURCE.aliasListDuplicate(id.getSimple()));
      }
      if (columnNames.size() != rowType.getFieldCount()) {
        // Position error over all column names
        final SqlNode node = operands.size() == 3
            ? operands.get(2)
            : new SqlNodeList(columnNames, SqlParserPos.sum(columnNames));
        throw validator.newValidationError(node,
            RESOURCE.aliasListDegree(rowType.getFieldCount(),
                getString(rowType), columnNames.size()));
      }
      aliasedType = validator.getTypeFactory().builder()
          .addAll(
              Util.transform(rowType.getFieldList(), f ->
                  Pair.of(nameList.get(f.getIndex()), f.getType())))
          .kind(rowType.getStructKind())
          .build();
    }

    // As per suggestion in CALCITE-4085, JavaType has its special nullability handling.
    if (rowType instanceof RelDataTypeFactoryImpl.JavaType) {
      return aliasedType;
    } else {
      return validator.getTypeFactory()
          .createTypeWithNullability(aliasedType, rowType.isNullable());
    }
  }

  private static String getString(RelDataType rowType) {
    StringBuilder buf = new StringBuilder();
    buf.append("(");
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (field.getIndex() > 0) {
        buf.append(", ");
      }
      buf.append("'");
      buf.append(field.getName());
      buf.append("'");
    }
    buf.append(")");
    return buf.toString();
  }

  @Override public @Nullable SqlNode getNode() {
    return call;
  }

  @Override public String translate(String name) {
    final RelDataType underlyingRowType =
        validator.getValidatedNodeType(call.operand(0));
    int i = 0;
    for (RelDataTypeField field : getRowType().getFieldList()) {
      if (field.getName().equals(name)) {
        return underlyingRowType.getFieldList().get(i).getName();
      }
      ++i;
    }
    throw new AssertionError("unknown field '" + name
        + "' in rowtype " + underlyingRowType);
  }
}
