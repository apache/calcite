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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ModifiableViewTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/** Namespace based on a table from the catalog. */
class TableNamespace extends AbstractNamespace {
  private final SqlValidatorTable table;
  public final ImmutableList<RelDataTypeField> extendedFields;

  /** Creates a TableNamespace. */
  private TableNamespace(SqlValidatorImpl validator, SqlValidatorTable table,
      List<RelDataTypeField> fields) {
    super(validator, null);
    this.table = Objects.requireNonNull(table);
    this.extendedFields = ImmutableList.copyOf(fields);
  }

  TableNamespace(SqlValidatorImpl validator, SqlValidatorTable table) {
    this(validator, table, ImmutableList.of());
  }

  protected RelDataType validateImpl(RelDataType targetRowType) {
    if (extendedFields.isEmpty()) {
      return table.getRowType();
    }
    final RelDataTypeFactory.Builder builder =
        validator.getTypeFactory().builder();
    builder.addAll(table.getRowType().getFieldList());
    builder.addAll(extendedFields);
    return builder.build();
  }

  public SqlNode getNode() {
    // This is the only kind of namespace not based on a node in the parse tree.
    return null;
  }

  @Override public SqlValidatorTable getTable() {
    return table;
  }

  @Override public SqlMonotonicity getMonotonicity(String columnName) {
    final SqlValidatorTable table = getTable();
    return table.getMonotonicity(columnName);
  }

  /** Creates a TableNamespace based on the same table as this one, but with
   * extended fields.
   *
   * <p>Extended fields are "hidden" or undeclared fields that may nevertheless
   * be present if you ask for them. Phoenix uses them, for instance, to access
   * rarely used fields in the underlying HBase table. */
  public TableNamespace extend(SqlNodeList extendList) {
    final List<SqlNode> identifierList = Util.quotientList(extendList.getList(), 2, 0);
    SqlValidatorUtil.checkIdentifierListForDuplicates(
        identifierList, validator.getValidationErrorFunction());
    final ImmutableList.Builder<RelDataTypeField> builder =
        ImmutableList.builder();
    builder.addAll(this.extendedFields);
    builder.addAll(
        SqlValidatorUtil.getExtendedColumns(validator,
            getTable(), extendList));
    final List<RelDataTypeField> extendedFields = builder.build();
    final Table schemaTable = table.unwrap(Table.class);
    if (table instanceof RelOptTable
        && (schemaTable instanceof ExtensibleTable
          || schemaTable instanceof ModifiableViewTable)) {
      checkExtendedColumnTypes(extendList);
      final RelOptTable relOptTable =
          ((RelOptTable) table).extend(extendedFields);
      final SqlValidatorTable validatorTable =
          relOptTable.unwrap(SqlValidatorTable.class);
      return new TableNamespace(validator, validatorTable, ImmutableList.of());
    }
    return new TableNamespace(validator, table, extendedFields);
  }

  /**
   * Gets the data-type of all columns in a table (for a view table: including
   * columns of the underlying table)
   */
  private RelDataType getBaseRowType() {
    final Table schemaTable = table.unwrap(Table.class);
    if (schemaTable instanceof ModifiableViewTable) {
      final Table underlying =
          ((ModifiableViewTable) schemaTable).unwrap(Table.class);
      assert underlying != null;
      return underlying.getRowType(validator.typeFactory);
    }
    return schemaTable.getRowType(validator.typeFactory);
  }

  /**
   * Ensures that extended columns that have the same name as a base column also
   * have the same data-type.
   */
  private void checkExtendedColumnTypes(SqlNodeList extendList) {
    final List<RelDataTypeField> extendedFields =
        SqlValidatorUtil.getExtendedColumns(validator, table, extendList);
    final List<RelDataTypeField> baseFields =
        getBaseRowType().getFieldList();
    final Map<String, Integer> nameToIndex =
        SqlValidatorUtil.mapNameToIndex(baseFields);

    for (final RelDataTypeField extendedField : extendedFields) {
      final String extFieldName = extendedField.getName();
      if (nameToIndex.containsKey(extFieldName)) {
        final Integer baseIndex = nameToIndex.get(extFieldName);
        final RelDataType baseType = baseFields.get(baseIndex).getType();
        final RelDataType extType = extendedField.getType();

        if (!extType.equals(baseType)) {
          // Get the extended column node that failed validation.
          final SqlNode extColNode =
              Iterables.find(extendList.getList(),
                  sqlNode -> sqlNode instanceof SqlIdentifier
                      && Util.last(((SqlIdentifier) sqlNode).names).equals(
                          extendedField.getName()));

          throw validator.getValidationErrorFunction().apply(extColNode,
              RESOURCE.typeNotAssignable(
                  baseFields.get(baseIndex).getName(), baseType.getFullTypeString(),
                  extendedField.getName(), extType.getFullTypeString()));
        }
      }
    }
  }
}

// End TableNamespace.java
