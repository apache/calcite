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
import org.apache.calcite.sql.SqlNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

/** Namespace based on a table from the catalog. */
class TableNamespace extends AbstractNamespace {
  private final SqlValidatorTable table;
  public final ImmutableList<RelDataTypeField> extendedFields;

  /** Creates a TableNamespace. */
  TableNamespace(SqlValidatorImpl validator, SqlValidatorTable table,
      ImmutableList<RelDataTypeField> fields) {
    super(validator, null);
    this.table = Preconditions.checkNotNull(table);
    this.extendedFields = fields;
  }

  public TableNamespace(SqlValidatorImpl validator, SqlValidatorTable table) {
    this(validator, table, ImmutableList.<RelDataTypeField>of());
  }

  protected RelDataType validateImpl(RelDataType targetRowType) {
    if (extendedFields.isEmpty()) {
      return table.getRowType();
    }
    final RelDataTypeFactory.FieldInfoBuilder builder =
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
  public TableNamespace extend(List<RelDataTypeField> extendedFields) {
    final Table schemaTable = table.unwrap(Table.class);
    if (schemaTable != null
        && table instanceof RelOptTable
        && schemaTable instanceof ExtensibleTable) {
      final SqlValidatorTable validatorTable =
          ((RelOptTable) table).extend(ImmutableList.copyOf(
              Iterables.concat(this.extendedFields, extendedFields)))
          .unwrap(SqlValidatorTable.class);
      return new TableNamespace(
          validator, validatorTable, ImmutableList.<RelDataTypeField>of());
    }
    return new TableNamespace(validator, table,
        ImmutableList.copyOf(
            Iterables.concat(this.extendedFields, extendedFields)));
  }
}

// End TableNamespace.java
