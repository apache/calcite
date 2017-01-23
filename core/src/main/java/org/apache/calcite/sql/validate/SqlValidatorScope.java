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
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.util.Pair;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Name-resolution scope. Represents any position in a parse tree than an
 * expression can be, or anything in the parse tree which has columns.
 *
 * <p>When validating an expression, say "foo"."bar", you first use the
 * {@link #resolve} method of the scope where the expression is defined to
 * locate "foo". If successful, this returns a
 * {@link SqlValidatorNamespace namespace} describing the type of the resulting
 * object.
 */
public interface SqlValidatorScope {
  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the validator which created this scope.
   */
  SqlValidator getValidator();

  /**
   * Returns the root node of this scope. Never null.
   */
  SqlNode getNode();

  /**
   * Looks up a node with a given name. Returns null if none is found.
   *
   * @param names       Name of node to find, maybe partially or fully qualified
   * @param nameMatcher Name matcher
   * @param deep        Whether to look more than one level deep
   * @param resolved    Callback wherein to write the match(es) we find
   */
  void resolve(List<String> names, SqlNameMatcher nameMatcher, boolean deep,
      Resolved resolved);

  /** @deprecated Use
   * {@link #findQualifyingTableNames(String, SqlNode, SqlNameMatcher)} */
  @Deprecated // to be removed before 2.0
  Pair<String, SqlValidatorNamespace> findQualifyingTableName(String columnName,
      SqlNode ctx);

  /**
   * Finds all table aliases which are implicitly qualifying an unqualified
   * column name.
   *
   * <p>This method is only implemented in scopes (such as
   * {@link org.apache.calcite.sql.validate.SelectScope}) which can be the
   * context for name-resolution. In scopes such as
   * {@link org.apache.calcite.sql.validate.IdentifierNamespace}, it throws
   * {@link UnsupportedOperationException}.
   *
   * @param columnName Column name
   * @param ctx        Validation context, to appear in any error thrown
   * @param nameMatcher Name matcher
   *
   * @return Map of applicable table alias and namespaces, never null, empty
   * if no aliases found
   */
  Map<String, ScopeChild> findQualifyingTableNames(String columnName,
      SqlNode ctx, SqlNameMatcher nameMatcher);

  /**
   * Collects the {@link SqlMoniker}s of all possible columns in this scope.
   *
   * @param result an array list of strings to add the result to
   */
  void findAllColumnNames(List<SqlMoniker> result);

  /**
   * Collects the {@link SqlMoniker}s of all table aliases (uses of tables in
   * query FROM clauses) available in this scope.
   *
   * @param result a list of monikers to add the result to
   */
  void findAliases(Collection<SqlMoniker> result);

  /**
   * Converts an identifier into a fully-qualified identifier. For example,
   * the "empno" in "select empno from emp natural join dept" becomes
   * "emp.empno".
   *
   * @return A qualified identifier, never null
   */
  SqlQualified fullyQualify(SqlIdentifier identifier);

  /**
   * Registers a relation in this scope.
   *
   * @param ns    Namespace representing the result-columns of the relation
   * @param alias Alias with which to reference the relation, must not be null
   * @param nullable Whether this is a null-generating side of a join
   */
  void addChild(SqlValidatorNamespace ns, String alias, boolean nullable);

  /**
   * Finds a window with a given name. Returns null if not found.
   */
  SqlWindow lookupWindow(String name);

  /**
   * Returns whether an expression is monotonic in this scope. For example, if
   * the scope has previously been sorted by columns X, Y, then X is monotonic
   * in this scope, but Y is not.
   */
  SqlMonotonicity getMonotonicity(SqlNode expr);

  /**
   * Returns the expressions by which the rows in this scope are sorted. If
   * the rows are unsorted, returns null.
   */
  SqlNodeList getOrderList();

  /**
   * Resolves a single identifier to a column, and returns the datatype of
   * that column.
   *
   * <p>If it cannot find the column, returns null. If the column is
   * ambiguous, throws an error with context <code>ctx</code>.
   *
   * @param name Name of column
   * @param ctx  Context for exception
   * @return Type of column, if found and unambiguous; null if not found
   */
  RelDataType resolveColumn(String name, SqlNode ctx);

  /**
   * Returns the scope within which operands to a call are to be validated.
   * Usually it is this scope, but when the call is to an aggregate function
   * and this is an aggregating scope, it will be a a different scope.
   *
   * @param call Call
   * @return Scope within which to validate arguments to call.
   */
  SqlValidatorScope getOperandScope(SqlCall call);

  /**
   * Performs any scope-specific validation of an expression. For example, an
   * aggregating scope requires that expressions are valid aggregations. The
   * expression has already been validated.
   */
  void validateExpr(SqlNode expr);

  /** @deprecated Use
   * {@link #resolveTable(List, SqlNameMatcher, Path, Resolved)}. */
  @Deprecated // to be removed before 2.0
  SqlValidatorNamespace getTableNamespace(List<String> names);

  /**
   * Looks up a table in this scope from its name. If found, calls
   * {@link Resolved#resolve(List, SqlNameMatcher, boolean, Resolved)}.
   * {@link TableNamespace} that wraps it. If the "table" is defined in a
   * {@code WITH} clause it may be a query, not a table after all.
   *
   * <p>The name matcher is not null, and one typically uses
   * {@link SqlValidatorCatalogReader#nameMatcher()}.
   *
   * @param names Name of table, may be qualified or fully-qualified
   * @param nameMatcher Name matcher
   * @param path List of names that we have traversed through so far
   */
  void resolveTable(List<String> names, SqlNameMatcher nameMatcher, Path path,
      Resolved resolved);

  /** Converts the type of an expression to nullable, if the context
   * warrants it. */
  RelDataType nullifyType(SqlNode node, RelDataType type);

  /** Callback from {@link SqlValidatorScope#resolve}. */
  interface Resolved {
    void found(SqlValidatorNamespace namespace, boolean nullable,
        SqlValidatorScope scope, Path path, List<String> remainingNames);
    int count();
  }

  /** A sequence of steps by which an identifier was resolved. Immutable. */
  abstract class Path {
    /** The empty path. */
    @SuppressWarnings("StaticInitializerReferencesSubClass")
    public static final EmptyPath EMPTY = new EmptyPath();

    /** Creates a path that consists of this path plus one additional step. */
    public Step plus(RelDataType rowType, int i, String name, StructKind kind) {
      return new Step(this, rowType, i, name, kind);
    }

    /** Number of steps in this path. */
    public int stepCount() {
      return 0;
    }

    /** Returns the steps in this path. */
    public List<Step> steps() {
      ImmutableList.Builder<Step> paths = new ImmutableList.Builder<>();
      build(paths);
      return paths.build();
    }

    /** Returns a list ["step1", "step2"]. */
    List<String> stepNames() {
      return Lists.transform(steps(),
          new Function<Step, String>() {
            public String apply(Step input) {
              return input.name;
            }
          });
    }

    protected void build(ImmutableList.Builder<Step> paths) {
    }
  }

  /** A path that has no steps. */
  class EmptyPath extends Path {
  }

  /** A step in resolving an identifier. */
  class Step extends Path {
    final Path parent;
    final RelDataType rowType;
    public final int i;
    public final String name;
    final StructKind kind;

    Step(Path parent, RelDataType rowType, int i, String name,
        StructKind kind) {
      this.parent = Preconditions.checkNotNull(parent);
      this.rowType = rowType; // may be null
      this.i = i;
      this.name = name;
      this.kind = Preconditions.checkNotNull(kind);
    }

    @Override public int stepCount() {
      return 1 + parent.stepCount();
    }

    protected void build(ImmutableList.Builder<Step> paths) {
      parent.build(paths);
      paths.add(this);
    }
  }

  /** Default implementation of
   * {@link org.apache.calcite.sql.validate.SqlValidatorScope.Resolved}. */
  class ResolvedImpl implements Resolved {
    final List<Resolve> resolves = new ArrayList<>();

    public void found(SqlValidatorNamespace namespace, boolean nullable,
        SqlValidatorScope scope, Path path, List<String> remainingNames) {
      resolves.add(
          new Resolve(namespace, nullable, scope, path, remainingNames));
    }

    public int count() {
      return resolves.size();
    }

    public Resolve only() {
      return Iterables.getOnlyElement(resolves);
    }

    /** Resets all state. */
    public void clear() {
      resolves.clear();
    }

  }

  /** A match found when looking up a name. */
  class Resolve {
    public final SqlValidatorNamespace namespace;
    private final boolean nullable;
    public final SqlValidatorScope scope; // may be null
    public final Path path;
    /** Names not matched; empty if it was a full match. */
    final List<String> remainingNames;

    Resolve(SqlValidatorNamespace namespace, boolean nullable,
        SqlValidatorScope scope, Path path, List<String> remainingNames) {
      this.namespace = Preconditions.checkNotNull(namespace);
      this.nullable = nullable;
      this.scope = scope;
      this.path = Preconditions.checkNotNull(path);
      this.remainingNames = remainingNames == null ? ImmutableList.<String>of()
          : ImmutableList.copyOf(remainingNames);
    }

    /** The row type of the found namespace, nullable if the lookup has
     * looked into outer joins. */
    public RelDataType rowType() {
      return namespace.getValidator().getTypeFactory()
          .createTypeWithNullability(namespace.getRowType(), nullable);
    }
  }
}

// End SqlValidatorScope.java
