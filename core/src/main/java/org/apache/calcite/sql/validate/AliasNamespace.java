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
import org.apache.calcite.rel.type.SingleColumnAliasRelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.calcite.sql.SqlUtil;
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
    final RelDataType rowType0 = childNs.getRowTypeSansSystemColumns();
    // An alias namespace renames columns positionally (or, without a column
    // list, not at all), so the child's filter requirement applies unchanged
    filterRequirement = childNs.getFilterRequirement();
    final RelDataType rowType;
    if (rowType0.isStruct()) {
      rowType = rowType0;
    } else {
      // Joins produce RelCrossType, which is not a struct. Convert to a struct
      // so that columns can be resolved via the alias.
      rowType = validator.getTypeFactory().builder()
          .kind(StructKind.FULLY_QUALIFIED)
          .addAll(
              Util.transform(rowType0.getFieldList(),
                  f -> Pair.of(f.getName(), f.getType())))
          .build();
    }
    final RelDataType aliasedType;
    if (operands.size() == 2) {
      final SqlNode node = operands.get(0);
      // 'AS t' on a single-column result, or on UNNEST WITH ORDINALITY with one
      // data column: the data column is named after the alias. A ROW element's
      // field keeps its own name for star expansion, and the alias names it as
      // well. COLLECTION_TABLE handles its column the same way.
      final boolean keepsElementFieldName = node.getKind() == SqlKind.UNNEST
          && keepsElementFieldName((SqlCall) node);
      if (rowType.getFieldCount() == 1) {
        final RelDataType dataColumnAlias =
            aliasDataColumn(rowType, (SqlIdentifier) operands.get(1));
        aliasedType = node.getKind() == SqlKind.COLLECTION_TABLE || keepsElementFieldName
            ? new SingleColumnAliasRelDataType(rowType, dataColumnAlias) : dataColumnAlias;
      } else if (node.getKind() == SqlKind.UNNEST && rowType.getFieldCount() == 2
          && ((SqlUnnestOperator) ((SqlBasicCall) node).getOperator()).withOrdinality) {
        // WITH ORDINALITY: data column, ordinality column. The data
        // column is named after the alias, same as the single-column case.
        final RelDataType dataColumnAlias =
            aliasDataColumn(rowType, (SqlIdentifier) operands.get(1));
        aliasedType = keepsElementFieldName
            ? new SingleColumnAliasRelDataType(rowType, dataColumnAlias)
            : validator.getTypeFactory().builder()
                .kind(rowType.getStructKind())
                .addAll(dataColumnAlias.getFieldList())
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

  /** Returns a single-field struct holding the first column of
   * {@code rowType}, named after the alias. */
  private RelDataType aliasDataColumn(RelDataType rowType, SqlIdentifier alias) {
    return validator.getTypeFactory().builder()
        .kind(rowType.getStructKind())
        .add(alias.getSimple(), rowType.getFieldList().get(0).getType())
        .build();
  }

  /**
   * Returns whether UNNEST expands the collection's ROW element into one
   * column per field. Such a column keeps its field name, and the alias
   * names it as well; any other data column takes the alias as its name.
   */
  private boolean keepsElementFieldName(SqlCall unnestCall) {
    final RelDataType operandType =
        SqlUnnestOperator.unwrapOperandType(
            validator.getValidatedNodeType(unnestCall.operand(0)));
    final RelDataType componentType = operandType.getComponentType();
    return componentType != null
        && SqlUnnestOperator.expandsStructIntoColumns(componentType,
            validator.config().conformance().allowAliasUnnestItems())
        && !hasGeneratedFirstFieldName(componentType);
  }

  /** Returns whether the first field of a ROW element has a generated name,
   * such as {@code EXPR$0} for {@code ROW(1)}. The alias then names the
   * column, because a generated name is not worth keeping. */
  private static boolean hasGeneratedFirstFieldName(RelDataType componentType) {
    final List<RelDataTypeField> fields = componentType.getFieldList();
    return !fields.isEmpty() && SqlUtil.isGeneratedAlias(fields.get(0).getName());
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
