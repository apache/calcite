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
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Namespace whose contents are defined by the type of an
 * {@link org.apache.calcite.sql.SqlIdentifier identifier}.
 */
public class IdentifierNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier id;
  private final SqlValidatorScope parentScope;
  private final SqlNodeList extendList;

  /**
   * The underlying namespace. Often a {@link TableNamespace}.
   * Set on validate.
   */
  private SqlValidatorNamespace resolvedNamespace;

  /**
   * List of monotonic expressions. Set on validate.
   */
  private List<Pair<SqlNode, SqlMonotonicity>> monotonicExprs;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an IdentifierNamespace.
   *
   * @param validator     Validator
   * @param id            Identifier node (or "identifier EXTEND column-list")
   * @param extendList    Extension columns, or null
   * @param enclosingNode Enclosing node
   * @param parentScope   Parent scope which this namespace turns to in order to
   */
  IdentifierNamespace(SqlValidatorImpl validator, SqlIdentifier id,
      @Nullable SqlNodeList extendList, SqlNode enclosingNode,
      SqlValidatorScope parentScope) {
    super(validator, enclosingNode);
    this.id = id;
    this.extendList = extendList;
    this.parentScope = parentScope;
    assert parentScope != null;
  }

  IdentifierNamespace(SqlValidatorImpl validator, SqlNode node,
      SqlNode enclosingNode, SqlValidatorScope parentScope) {
    this(validator, split(node).left, split(node).right, enclosingNode,
        parentScope);
  }

  //~ Methods ----------------------------------------------------------------

  protected static Pair<SqlIdentifier, SqlNodeList> split(SqlNode node) {
    switch (node.getKind()) {
    case EXTEND:
      final SqlCall call = (SqlCall) node;
      return Pair.of((SqlIdentifier) call.getOperandList().get(0),
          (SqlNodeList) call.getOperandList().get(1));
    default:
      return Pair.of((SqlIdentifier) node, null);
    }
  }

  public RelDataType validateImpl(RelDataType targetRowType) {
    resolvedNamespace = parentScope.getTableNamespace(id.names);
    if (resolvedNamespace == null) {
      throw validator.newValidationError(id,
          RESOURCE.tableNameNotFound(id.toString()));
    }

    if (resolvedNamespace instanceof TableNamespace) {
      SqlValidatorTable table = resolvedNamespace.getTable();
      if (validator.shouldExpandIdentifiers()) {
        // TODO:  expand qualifiers for column references also
        List<String> qualifiedNames = table.getQualifiedName();
        if (qualifiedNames != null) {
          // Assign positions to the components of the fully-qualified
          // identifier, as best we can. We assume that qualification
          // adds names to the front, e.g. FOO.BAR becomes BAZ.FOO.BAR.
          List<SqlParserPos> poses =
              new ArrayList<SqlParserPos>(
                  Collections.nCopies(
                      qualifiedNames.size(), id.getParserPosition()));
          int offset = qualifiedNames.size() - id.names.size();

          // Test offset in case catalog supports fewer qualifiers than catalog
          // reader.
          if (offset >= 0) {
            for (int i = 0; i < id.names.size(); i++) {
              poses.set(i + offset, id.getComponentParserPosition(i));
            }
          }
          id.setNames(qualifiedNames, poses);
        }
      }
    }

    RelDataType rowType = resolvedNamespace.getRowType();

    if (extendList != null) {
      final List<RelDataTypeField> fields = Lists.newArrayList();
      final Iterator<SqlNode> extendIterator = extendList.iterator();
      while (extendIterator.hasNext()) {
        SqlIdentifier id = (SqlIdentifier) extendIterator.next();
        SqlDataTypeSpec type = (SqlDataTypeSpec) extendIterator.next();
        fields.add(
            new RelDataTypeFieldImpl(id.getSimple(), fields.size(),
                type.deriveType(validator)));
      }

      if (!(resolvedNamespace instanceof TableNamespace)) {
        throw new RuntimeException("cannot convert");
      }
      resolvedNamespace = ((TableNamespace) resolvedNamespace).extend(fields);
      rowType = resolvedNamespace.getRowType();
    }

    // Build a list of monotonic expressions.
    final ImmutableList.Builder<Pair<SqlNode, SqlMonotonicity>> builder =
        ImmutableList.builder();
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (RelDataTypeField field : fields) {
      final String fieldName = field.getName();
      final SqlMonotonicity monotonicity =
          resolvedNamespace.getMonotonicity(fieldName);
      if (monotonicity != SqlMonotonicity.NOT_MONOTONIC) {
        builder.add(
            Pair.of((SqlNode) new SqlIdentifier(fieldName, SqlParserPos.ZERO),
                monotonicity));
      }
    }
    monotonicExprs = builder.build();

    // Validation successful.
    return rowType;
  }

  public SqlIdentifier getId() {
    return id;
  }

  public SqlNode getNode() {
    return id;
  }

  @Override public SqlValidatorNamespace resolve() {
    assert resolvedNamespace != null : "must call validate first";
    return resolvedNamespace.resolve();
  }

  @Override public String translate(String name) {
    return resolvedNamespace.translate(name);
  }

  @Override public SqlValidatorTable getTable() {
    return resolve().getTable();
  }

  public List<Pair<SqlNode, SqlMonotonicity>> getMonotonicExprs() {
    return monotonicExprs;
  }

  @Override public SqlMonotonicity getMonotonicity(String columnName) {
    final SqlValidatorTable table = getTable();
    return table.getMonotonicity(columnName);
  }

  @Override public boolean supportsModality(SqlModality modality) {
    final SqlValidatorTable table = getTable();
    if (table == null) {
      return modality == SqlModality.RELATION;
    }
    return table.supportsModality(modality);
  }
}

// End IdentifierNamespace.java
