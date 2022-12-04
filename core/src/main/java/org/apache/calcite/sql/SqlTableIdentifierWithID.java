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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlTableIdentifierWithIDQualified;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A <code>SqlTableIdentifierWithID</code> is an identifier, possibly compound.
 * This differs from SqlIdentifier because it only refers to tables that will have
 * an extra column not found in their type, __bodo_row_id. We use this identifier
 * to uniquely identify particular tables of interest, for example the target table
 * in MergeInto.
 */
public class SqlTableIdentifierWithID extends SqlNode {
  /**
   * Array of the components of this compound identifier.
   **
   */
  public ImmutableList<String> names;

  /**
   * This identifier's collation (if any).
   */
  final @Nullable SqlCollation collation;

  /**
   * A list of the positions of the components of compound identifiers.
   */
  protected @Nullable ImmutableList<SqlParserPos> componentPositions;

  /** Create a SqlTableIdentifierWithID from identifier components. This is used in case
   * we make a copy. This is mostly copied from SQLIdentifier.
   */
  public SqlTableIdentifierWithID(
      List<String> names,
      @Nullable SqlCollation collation,
      SqlParserPos pos,
      @Nullable List<SqlParserPos> componentPositions) {
    super(pos);
    this.names = ImmutableList.copyOf(names);
    this.collation = collation;
    this.componentPositions = componentPositions == null ? null
        : ImmutableList.copyOf(componentPositions);
    for (String name : names) {
      assert name != null;
      // We don't support wildcards
      assert !name.equals("");
    }
  }

  /**
   * Create the identifier from an existing SqlIdentifier.
   * @param id Original SQL identifier for a table.
   */
  public SqlTableIdentifierWithID(SqlIdentifier id) {
    super(id.pos);
    this.names = ImmutableList.copyOf(id.names);
    this.collation = id.collation;
    this.componentPositions = componentPositions == null ? null
        : ImmutableList.copyOf(componentPositions);
    for (String name : names) {
      assert name != null;
      // We don't support wildcards
      assert !name.equals("");
    }

  }

  @Override public SqlKind getKind() {
    return SqlKind.TABLE_IDENTIFIER_WITH_ID;
  }


  /**
   * Returns whether this is a simple identifier. "FOO" is simple
   * and "FOO.BAR" is not.
   */
  public boolean isSimple() {
    return names.size() == 1;
  }

  public ImmutableList<String> getNames() {
    return names;
  }

  public String getSimple() {
    assert names.size() == 1;
    return names.get(0);
  }


  /**
   * Modifies the components of this SqlTableIdentifierWithID and their positions.
   *
   * @param names Names of components
   * @param poses Positions of components
   */
  public void setNames(List<String> names, @Nullable List<SqlParserPos> poses) {
    this.names = ImmutableList.copyOf(names);
    this.componentPositions = poses == null ? null
        : ImmutableList.copyOf(poses);
  }


  /**
   * Copies names and components from another identifier. Does not modify the
   * cross-component parser position.
   *
   * @param other identifier from which to copy
   */
  public void assignNamesFrom(SqlTableIdentifierWithID other) {
    setNames(other.names, other.componentPositions);
  }

  /** Returns a SqlTableIdentifierWithID that is the same as this except one modified name.
   * Does not modify this identifier. */
  public SqlTableIdentifierWithID setName(int i, String name) {
    if (!names.get(i).equals(name)) {
      String[] nameArray = names.toArray(new String[0]);
      nameArray[i] = name;
      return new SqlTableIdentifierWithID(ImmutableList.copyOf(nameArray), collation, pos,
          componentPositions);
    } else {
      return this;
    }
  }

  /** Returns a SqlTableIdentifierWithID that is the same as this except with a component
   * added at a given position. Does not modify this identifier. */
  public SqlTableIdentifierWithID add(int i, String name, SqlParserPos pos) {
    final List<String> names2 = new ArrayList<>(names);
    names2.add(i, name);
    final List<SqlParserPos> pos2;
    if (componentPositions == null) {
      pos2 = null;
    } else {
      pos2 = new ArrayList<>(componentPositions);
      pos2.add(i, pos);
    }
    return new SqlTableIdentifierWithID(names2, collation, pos, pos2);
  }


  public SqlIdentifier convertToSQLIdentifier() {
    return new SqlIdentifier(names, collation, pos, componentPositions);
  }


  @Override public SqlNode clone(SqlParserPos pos) {
    return new SqlTableIdentifierWithID(names, collation, pos, componentPositions);
  }

  @Override public SqlNode deepCopy(@Nullable SqlParserPos pos) {
    if (pos == null) {
      pos = this.pos;
    }
    return this.clone(pos);
  }

  @Override public String toString() {
    return getString(names);
  }

  /** Converts a list of strings to a qualified identifier. */
  public static String getString(List<String> names) {
    return Util.sepList(names, ".");
  }

  /**
   * Returns the position of the <code>i</code>th component of a compound
   * identifier, or the position of the whole identifier if that information
   * is not present.
   *
   * @param i Ordinal of component.
   * @return Position of i'th component
   */
  public SqlParserPos getComponentParserPosition(int i) {
    assert (i >= 0) && (i < names.size());
    return (componentPositions == null) ? getParserPosition()
        : componentPositions.get(i);
  }

  /**
   * Creates an identifier which contains only the <code>ordinal</code>th
   * component of this compound identifier. It will have the correct
   * {@link SqlParserPos}, provided that detailed position information is
   * available.
   */
  public SqlTableIdentifierWithID getComponent(int ordinal) {
    return getComponent(ordinal, ordinal + 1);
  }

  public SqlTableIdentifierWithID getComponent(int from, int to) {
    final SqlParserPos pos;
    final ImmutableList<SqlParserPos> pos2;
    if (componentPositions == null) {
      pos2 = null;
      pos = this.pos;
    } else {
      pos2 = componentPositions.subList(from, to);
      pos = SqlParserPos.sum(pos2);
    }
    return new SqlTableIdentifierWithID(names.subList(from, to), collation, pos, pos2);
  }

  /** Creates an identifier that consists of all but the last {@code n}
   * name segments of this one. */
  public SqlTableIdentifierWithID skipLast(int n) {
    return getComponent(0, names.size() - n);
  }

  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    SqlUtil.unparseSqlTableIdentifierWithIDSyntax(writer, this);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateTableIdentifierWithID(this, scope);
  }

  @Override public void validateExpr(SqlValidator validator, SqlValidatorScope scope) {
    try {
      validator.requireNonCall(this);
    } catch (ValidationException e) {
      throw new RuntimeException(e);
    }
    validator.validateTableIdentifierWithID(this, scope);
  }

  @Override public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlTableIdentifierWithID)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlTableIdentifierWithID that = (SqlTableIdentifierWithID) node;
    if (this.names.size() != that.names.size()) {
      return litmus.fail("{} != {}", this, node);
    }
    for (int i = 0; i < names.size(); i++) {
      if (!this.names.get(i).equals(that.names.get(i))) {
        return litmus.fail("{} != {}", this, node);
      }
    }
    return litmus.succeed();
  }

  @Override public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  @Pure
  public @Nullable SqlCollation getCollation() {
    return collation;
  }

  /**
   * Returns whether the {@code i}th component of a compound identifier is
   * quoted.
   *
   * @param i Ordinal of component
   * @return Whether i'th component is quoted
   */
  public boolean isComponentQuoted(int i) {
    return componentPositions != null
        && componentPositions.get(i).isQuoted();
  }

  @Override public SqlMonotonicity getMonotonicity(@Nullable SqlValidatorScope scope) {
    Objects.requireNonNull(scope, "scope");
    final SqlValidator validator = scope.getValidator();
    try {
      validator.requireNonCall(this);
    } catch (ValidationException e) {
      throw new RuntimeException(e);
    }
    final SqlTableIdentifierWithIDQualified qualified = scope.fullyQualify(this);
    assert qualified.namespace != null : "namespace must not be null in " + qualified;
    final SqlTableIdentifierWithID fqId = qualified.identifier;
    return qualified.namespace.resolve().getMonotonicity(Util.last(fqId.names));
  }

}
