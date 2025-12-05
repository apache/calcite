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

import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * A <code>SqlIdentifier</code> is an identifier, possibly compound.
 */
public class SqlIdentifier extends SqlNode {
  /** An identifier for star, "*".
   *
   * @see SqlNodeList#SINGLETON_STAR */
  public static final SqlIdentifier STAR = star(SqlParserPos.ZERO);

  //~ Instance fields --------------------------------------------------------

  /**
   * Array of the components of this compound identifier.
   *
   * <p>The empty string represents the wildcard "*",
   * to distinguish it from a real "*" (presumably specified using quotes).
   *
   * <p>It's convenient to have this member public, and it's convenient to
   * have this member not-final, but it's a shame it's public and not-final.
   * If you assign to this member, please use
   * {@link #setNames(java.util.List, java.util.List)}.
   * And yes, we'd like to make identifiers immutable one day.
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

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a compound identifier, for example <code>foo.bar</code>.
   *
   * @param names Parts of the identifier, length &ge; 1
   */
  public SqlIdentifier(
      List<String> names,
      @Nullable SqlCollation collation,
      SqlParserPos pos,
      @Nullable List<SqlParserPos> componentPositions) {
    super(pos);
    this.names = ImmutableList.copyOf(names);
    this.collation = collation;
    this.componentPositions =
        componentPositions == null ? null
            : ImmutableList.copyOf(componentPositions);
  }

  public SqlIdentifier(List<String> names, SqlParserPos pos) {
    this(names, null, pos, null);
  }

  /**
   * Creates a simple identifier, for example <code>foo</code>, with a
   * collation.
   */
  public SqlIdentifier(
      String name,
      @Nullable SqlCollation collation,
      SqlParserPos pos) {
    this(ImmutableList.of(name), collation, pos, null);
  }

  /**
   * Creates a simple identifier, for example <code>foo</code>.
   */
  public SqlIdentifier(
      String name,
      SqlParserPos pos) {
    this(ImmutableList.of(name), null, pos, null);
  }

  /** Creates an identifier that is a singleton wildcard star. */
  public static SqlIdentifier star(SqlParserPos pos) {
    return star(ImmutableList.of(""), pos, ImmutableList.of(pos));
  }

  /** Creates an identifier that ends in a wildcard star. */
  public static SqlIdentifier star(List<String> names, SqlParserPos pos,
      List<SqlParserPos> componentPositions) {
    return new SqlIdentifier(
        Util.transform(names, s -> s.equals("*") ? "" : s), null, pos,
        componentPositions);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.IDENTIFIER;
  }

  @Override public SqlNode clone(SqlParserPos pos) {
    return new SqlIdentifier(names, collation, pos, componentPositions);
  }

  @Override public String toString() {
    return getString(names);
  }

  /** Converts a list of strings to a qualified identifier. */
  public static String getString(List<String> names) {
    return Util.sepList(toStar(names), ".");
  }

  /** Converts empty strings in a list of names to stars. */
  public static List<String> toStar(List<String> names) {
    return Util.transform(names, s -> s.equals("") ? "*" : s);
  }

  /**
   * Modifies the components of this identifier and their positions.
   *
   * @param names Names of components
   * @param poses Positions of components
   */
  public void setNames(List<String> names, @Nullable List<SqlParserPos> poses) {
    this.names = ImmutableList.copyOf(names);
    this.componentPositions =
        poses == null ? null
            : ImmutableList.copyOf(poses);
  }

  /** Returns an identifier that is the same as this except one modified name.
   * Does not modify this identifier. */
  public SqlIdentifier setName(int i, String name) {
    if (!names.get(i).equals(name)) {
      String[] nameArray = names.toArray(new String[0]);
      nameArray[i] = name;
      return new SqlIdentifier(ImmutableList.copyOf(nameArray), collation, pos,
          componentPositions);
    } else {
      return this;
    }
  }

  /** Returns an identifier that is the same as this except with a component
   * added at a given position. Does not modify this identifier. */
  public SqlIdentifier add(int i, String name, SqlParserPos pos) {
    final List<String> names2 = new ArrayList<>(names);
    names2.add(i, name);
    final List<SqlParserPos> pos2;
    if (componentPositions == null) {
      pos2 = null;
    } else {
      pos2 = new ArrayList<>(componentPositions);
      pos2.add(i, pos);
    }
    return new SqlIdentifier(names2, collation, pos, pos2);
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
   * Copies names and components from another identifier. Does not modify the
   * cross-component parser position.
   *
   * @param other identifier from which to copy
   */
  public void assignNamesFrom(SqlIdentifier other) {
    setNames(other.names, other.componentPositions);
  }

  /**
   * Creates an identifier which contains only the <code>ordinal</code>th
   * component of this compound identifier. It will have the correct
   * {@link SqlParserPos}, provided that detailed position information is
   * available.
   */
  public SqlIdentifier getComponent(int ordinal) {
    return getComponent(ordinal, ordinal + 1);
  }

  public SqlIdentifier getComponent(int from, int to) {
    final SqlParserPos pos;
    final ImmutableList<SqlParserPos> pos2;
    if (componentPositions == null) {
      pos2 = null;
      pos = this.pos;
    } else {
      pos2 = componentPositions.subList(from, to);
      pos = SqlParserPos.sum(pos2);
    }
    return new SqlIdentifier(names.subList(from, to), collation, pos, pos2);
  }

  /**
   * Creates an identifier that consists of this identifier plus a name segment.
   * Does not modify this identifier.
   */
  public SqlIdentifier plus(String name, SqlParserPos pos) {
    final ImmutableList<String> names =
        ImmutableList.<String>builder().addAll(this.names).add(name).build();
    final ImmutableList<SqlParserPos> componentPositions;
    final SqlParserPos pos2;
    ImmutableList<SqlParserPos> thisComponentPositions = this.componentPositions;
    if (thisComponentPositions != null) {
      final ImmutableList.Builder<SqlParserPos> builder =
          ImmutableList.builder();
      componentPositions =
          builder.addAll(thisComponentPositions).add(pos).build();
      pos2 = SqlParserPos.sum(builder.add(this.pos).build());
    } else {
      componentPositions = null;
      pos2 = pos;
    }
    return new SqlIdentifier(names, collation, pos2, componentPositions);
  }

  /**
   * Creates an identifier that consists of this identifier plus a wildcard star.
   * Does not modify this identifier.
   */
  public SqlIdentifier plusStar() {
    final SqlIdentifier id = this.plus("*", SqlParserPos.ZERO);
    return new SqlIdentifier(
        id.names.stream().map(s -> s.equals("*") ? "" : s)
            .collect(toImmutableList()),
        null, id.pos, id.componentPositions);
  }

  /** Creates an identifier that consists of all but the last {@code n}
   * name segments of this one. */
  public SqlIdentifier skipLast(int n) {
    return getComponent(0, names.size() - n);
  }

  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    SqlUtil.unparseSqlIdentifierSyntax(writer, this, false);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateIdentifier(this, scope);
  }

  @Override public void validateExpr(SqlValidator validator, SqlValidatorScope scope) {
    // First check for builtin functions which don't have parentheses,
    // like "LOCALTIME".
    final SqlCall call = validator.makeNullaryCall(this);
    if (call != null) {
      validator.validateCall(call, scope);
      return;
    }

    validator.validateIdentifier(this, scope);
  }

  @Override public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlIdentifier)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlIdentifier that = (SqlIdentifier) node;
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

  public String getSimple() {
    assert names.size() == 1;
    return names.get(0);
  }

  /** Returns the simple names in a list of identifiers.
   * Assumes that the list consists of are not-null, simple identifiers. */
  public static List<String> simpleNames(List<? extends SqlNode> list) {
    return Util.transform(list, n -> ((SqlIdentifier) n).getSimple());
  }

  /** Returns the simple names in a iterable of identifiers.
   * Assumes that the iterable consists of not-null, simple identifiers. */
  public static Iterable<String> simpleNames(Iterable<? extends SqlNode> list) {
    return Util.transform(list, n -> ((SqlIdentifier) n).getSimple());
  }

  /**
   * Returns whether this identifier is a star, such as "*" or "foo.bar.*".
   */
  public boolean isStar() {
    return Util.last(names).equals("");
  }

  /**
   * Returns whether this is a simple identifier. "FOO" is simple; "*",
   * "FOO.*" and "FOO.BAR" are not.
   */
  public boolean isSimple() {
    return names.size() == 1 && !isStar();
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

  @Override public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
    // for "star" column, whether it's static or dynamic return not_monotonic directly.
    if (Util.last(names).isEmpty()
        || DynamicRecordType.isDynamicStarColName(Util.last(names))) {
      return SqlMonotonicity.NOT_MONOTONIC;
    }

    // First check for builtin functions which don't have parentheses,
    // like "LOCALTIME".
    final SqlValidator validator = scope.getValidator();
    final SqlCall call = validator.makeNullaryCall(this);
    if (call != null) {
      return call.getMonotonicity(scope);
    }
    final SqlQualified qualified = scope.fullyQualify(this);
    if (qualified.namespace == null) {
      throw new IllegalArgumentException("namespace must not be null in "
          + qualified);
    }
    final SqlIdentifier fqId = qualified.identifier;
    return qualified.namespace.resolve().getMonotonicity(Util.last(fqId.names));
  }
}
