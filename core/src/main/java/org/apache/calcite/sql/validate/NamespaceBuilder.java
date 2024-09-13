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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLambda;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnpivot;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;

import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * A builder to allow for customization of namespaces down stream.
 */
public class NamespaceBuilder {

  private final SqlValidatorImpl sqlValidatorImpl;

  public NamespaceBuilder(@UnknownInitialization SqlValidatorImpl sqlValidatorImpl) {
    @SuppressWarnings("argument.type.incompatible")
    SqlValidatorImpl sqlValidatorCast = sqlValidatorImpl;
    this.sqlValidatorImpl = sqlValidatorCast;
  }

  /**
   * Creates a namespace for a <code>SELECT</code> node. Derived class may
   * override this factory method.
   *
   * @param select        Select node
   * @param enclosingNode Enclosing node
   * @return Select namespace
   */
  public SelectNamespace createSelectNamespace(
      SqlSelect select,
      SqlNode enclosingNode) {
    return new SelectNamespace(sqlValidatorImpl, select, enclosingNode);
  }

  /**
   * Creates a namespace for a set operation (<code>UNION</code>, <code>
   * INTERSECT</code>, or <code>EXCEPT</code>). Derived class may override
   * this factory method.
   *
   * @param call          Call to set operation
   * @param enclosingNode Enclosing node
   * @return Set operation namespace
   */
  public SetopNamespace createSetopNamespace(
      SqlCall call,
      SqlNode enclosingNode) {
    return new SetopNamespace(sqlValidatorImpl, call, enclosingNode);
  }


  public MatchRecognizeNamespace createMatchRecognizeNameSpace(
      SqlMatchRecognize call,
      SqlNode enclosingNode) {
    return new MatchRecognizeNamespace(sqlValidatorImpl, call, enclosingNode);
  }

  public UnpivotNamespace createUnpivotNameSpace(
      SqlUnpivot call,
      SqlNode enclosingNode) {
    return new UnpivotNamespace(sqlValidatorImpl, call, enclosingNode);
  }


  public PivotNamespace createPivotNameSpace(
      SqlPivot call,
      SqlNode enclosingNode) {
    return new PivotNamespace(sqlValidatorImpl, call, enclosingNode);
  }

  public DmlNamespace createDeleteNamespace(SqlDelete delete,
      SqlNode enclosingNode, SqlValidatorScope parentScope) {
    return new DeleteNamespace(sqlValidatorImpl, delete, enclosingNode, parentScope);
  }

  public DmlNamespace createInsertNamespace(SqlInsert insert,
      SqlNode enclosingNode, SqlValidatorScope parentScope) {
    return new InsertNamespace(sqlValidatorImpl, insert, enclosingNode, parentScope);
  }

  public DmlNamespace createMergeNamespace(SqlMerge merge, SqlNode enclosing,
      SqlValidatorScope parentScope) {
    return new MergeNamespace(sqlValidatorImpl, merge, enclosing, parentScope);
  }

  public DmlNamespace createUpdate(SqlUpdate sqlUpdate, SqlNode enclosing,
      SqlValidatorScope parentScope) {
    return new UpdateNamespace(sqlValidatorImpl, sqlUpdate, enclosing, parentScope);
  }

  public TableConstructorNamespace createTableConstructorNamespace(
      SqlCall call,
      SqlValidatorScope parentScope,
      SqlNode enclosingNode) {
    return new TableConstructorNamespace(sqlValidatorImpl, call, parentScope, enclosingNode);
  }

  public LambdaNamespace createLambdaNamespace(SqlLambda call, SqlNode node) {
    return new LambdaNamespace(sqlValidatorImpl, call, node);
  }

  public UnnestNamespace createUnnestNamespace(
      SqlCall call,
      SqlValidatorScope parentScope,
      SqlNode enclosingNode) {
    return new UnnestNamespace(sqlValidatorImpl, call, parentScope, enclosingNode);
  }

  public ProcedureNamespace createProcedureNamespace(
      SqlValidatorScope parentScope,
      SqlCall call,
      SqlNode enclosingNode) {
    return new ProcedureNamespace(sqlValidatorImpl, parentScope, call, enclosingNode);
  }

  public WithNamespace createWithNamespace(SqlWith with, SqlNode enclosingNode) {
    return new WithNamespace(sqlValidatorImpl, with, enclosingNode);
  }

  public SqlValidatorNamespace createWithItemNamespace(
      SqlWithItem withItem,
      SqlNode enclosingNode) {
    return new WithItemNamespace(sqlValidatorImpl, withItem, enclosingNode);
  }

  public SqlValidatorNamespace createAliasNamespace(SqlCall call, SqlNode enclosingNode) {
    return new AliasNamespace(sqlValidatorImpl, call, enclosingNode);
  }

  JoinNamespace createJoinNamespace(SqlJoin join) {
    return new JoinNamespace(sqlValidatorImpl, join);
  }

  public IdentifierNamespace createIdentifierNamespace(
      SqlIdentifier id,
      SqlNodeList extendList,
      SqlNode enclosingNode,
      SqlValidatorScope parentScope) {
    return new IdentifierNamespace(sqlValidatorImpl, id, extendList, enclosingNode, parentScope);
  }

  /**
   * Interface for creating new {@link NamespaceBuilder}.
   */
  public interface Factory {
    NamespaceBuilder create(@UnknownInitialization SqlValidatorImpl sqlValidatorImpl);
  }


  /**
   * Common base class for DML statement namespaces.
   */
  public abstract static class DmlNamespace extends IdentifierNamespace {
    protected DmlNamespace(SqlValidatorImpl validator, SqlNode id,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, id, enclosingNode, parentScope);
    }
  }

  /**
   * Namespace for an INSERT statement.
   */
  private static class InsertNamespace extends DmlNamespace {
    private final SqlInsert node;

    InsertNamespace(SqlValidatorImpl validator, SqlInsert node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = requireNonNull(node, "node");
    }

    @Override public @Nullable SqlNode getNode() {
      return node;
    }
  }

  /**
   * Namespace for an UPDATE statement.
   */
  private static class UpdateNamespace extends DmlNamespace {
    private final SqlUpdate node;

    UpdateNamespace(SqlValidatorImpl validator, SqlUpdate node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = requireNonNull(node, "node");
    }

    @Override public @Nullable SqlNode getNode() {
      return node;
    }
  }

  /**
   * Namespace for a DELETE statement.
   */
  private static class DeleteNamespace extends DmlNamespace {
    private final SqlDelete node;

    DeleteNamespace(SqlValidatorImpl validator, SqlDelete node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = requireNonNull(node, "node");
    }

    @Override public @Nullable SqlNode getNode() {
      return node;
    }
  }

  /**
   * Namespace for a MERGE statement.
   */
  private static class MergeNamespace extends DmlNamespace {
    private final SqlMerge node;

    MergeNamespace(SqlValidatorImpl validator, SqlMerge node,
        SqlNode enclosingNode, SqlValidatorScope parentScope) {
      super(validator, node.getTargetTable(), enclosingNode, parentScope);
      this.node = requireNonNull(node, "node");
    }

    @Override public @Nullable SqlNode getNode() {
      return node;
    }
  }
}
