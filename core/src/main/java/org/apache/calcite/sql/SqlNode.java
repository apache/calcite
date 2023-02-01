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

import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;

/**
 * A <code>SqlNode</code> is a SQL parse tree.
 *
 * <p>It may be a
 * {@link SqlCall call}, {@link SqlLiteral literal},
 * {@link SqlIdentifier identifier}, and so forth.
 */
public abstract class SqlNode implements Cloneable {
  //~ Static fields/initializers ---------------------------------------------

  public static final @Nullable SqlNode[] EMPTY_ARRAY = new SqlNode[0];

  //~ Instance fields --------------------------------------------------------

  protected final SqlParserPos pos;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a node.
   *
   * @param pos Parser position, must not be null.
   */
  SqlNode(SqlParserPos pos) {
    this.pos = Objects.requireNonNull(pos, "pos");
  }

  //~ Methods ----------------------------------------------------------------

  // CHECKSTYLE: IGNORE 1
  /** @deprecated Please use {@link #clone(SqlNode)}; this method brings
   * along too much baggage from early versions of Java */
  @Deprecated
  @SuppressWarnings({"MethodDoesntCallSuperMethod", "AmbiguousMethodReference"})
  @Override public Object clone() {
    return clone(getParserPosition());
  }

  /** Creates a copy of a SqlNode. */
  @SuppressWarnings("AmbiguousMethodReference")
  public static <E extends SqlNode> E clone(E e) {
    //noinspection unchecked
    return (E) e.clone(e.pos);
  }

  /**
   * Clones a SqlNode with a different position.
   */
  public abstract SqlNode clone(SqlParserPos pos);

  /**
   * Returns the type of node this is, or
   * {@link org.apache.calcite.sql.SqlKind#OTHER} if it's nothing special.
   *
   * @return a {@link SqlKind} value, never null
   * @see #isA
   */
  public SqlKind getKind() {
    return SqlKind.OTHER;
  }

  /**
   * Returns whether this node is a member of an aggregate category.
   *
   * <p>For example, {@code node.isA(SqlKind.QUERY)} returns {@code true}
   * if the node is a SELECT, INSERT, UPDATE etc.
   *
   * <p>This method is shorthand: {@code node.isA(category)} is always
   * equivalent to {@code node.getKind().belongsTo(category)}.
   *
   * @param category Category
   * @return Whether this node belongs to the given category.
   */
  public final boolean isA(Set<SqlKind> category) {
    return getKind().belongsTo(category);
  }

  @Deprecated // to be removed before 2.0
  public static SqlNode[] cloneArray(SqlNode[] nodes) {
    SqlNode[] clones = nodes.clone();
    for (int i = 0; i < clones.length; i++) {
      SqlNode node = clones[i];
      if (node != null) {
        clones[i] = SqlNode.clone(node);
      }
    }
    return clones;
  }

  @Override public String toString() {
    return toSqlString(c -> c.withDialect(AnsiSqlDialect.DEFAULT)
        .withAlwaysUseParentheses(false)
        .withSelectListItemsOnSeparateLines(false)
        .withUpdateSetListNewline(false)
        .withIndentation(0)).getSql();
  }

  /**
   * Returns the SQL text of the tree of which this <code>SqlNode</code> is
   * the root.
   *
   * <p>Typical return values are:
   *
   * <ul>
   * <li>'It''s a bird!'
   * <li>NULL
   * <li>12.3
   * <li>DATE '1969-04-29'
   * </ul>
   *
   * @param transform   Transform that sets desired writer configuration
   */
  public SqlString toSqlString(UnaryOperator<SqlWriterConfig> transform) {
    final SqlWriterConfig config = transform.apply(SqlPrettyWriter.config());
    SqlPrettyWriter writer = new SqlPrettyWriter(config);
    unparse(writer, 0, 0);
    return writer.toSqlString();
  }

  /**
   * Returns the SQL text of the tree of which this <code>SqlNode</code> is
   * the root.
   *
   * <p>Typical return values are:
   *
   * <ul>
   * <li>'It''s a bird!'
   * <li>NULL
   * <li>12.3
   * <li>DATE '1969-04-29'
   * </ul>
   *
   * @param dialect     Dialect (null for ANSI SQL)
   * @param forceParens Whether to wrap all expressions in parentheses;
   *                    useful for parse test, but false by default
   */
  public SqlString toSqlString(@Nullable SqlDialect dialect, boolean forceParens) {
    return toSqlString(c ->
        c.withDialect(Util.first(dialect, AnsiSqlDialect.DEFAULT))
            .withAlwaysUseParentheses(forceParens)
            .withSelectListItemsOnSeparateLines(false)
            .withUpdateSetListNewline(false)
            .withIndentation(0));
  }

  public SqlString toSqlString(@Nullable SqlDialect dialect) {
    return toSqlString(dialect, false);
  }

  /**
   * Writes a SQL representation of this node to a writer.
   *
   * <p>The <code>leftPrec</code> and <code>rightPrec</code> parameters give
   * us enough context to decide whether we need to enclose the expression in
   * parentheses. For example, we need parentheses around "2 + 3" if preceded
   * by "5 *". This is because the precedence of the "*" operator is greater
   * than the precedence of the "+" operator.
   *
   * <p>The algorithm handles left- and right-associative operators by giving
   * them slightly different left- and right-precedence.
   *
   * <p>If {@link SqlWriter#isAlwaysUseParentheses()} is true, we use
   * parentheses even when they are not required by the precedence rules.
   *
   * <p>For the details of this algorithm, see {@link SqlCall#unparse}.
   *
   * @param writer    Target writer
   * @param leftPrec  The precedence of the {@link SqlNode} immediately
   *                  preceding this node in a depth-first scan of the parse
   *                  tree
   * @param rightPrec The precedence of the {@link SqlNode} immediately
   */
  public abstract void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec);

  public void unparseWithParentheses(SqlWriter writer, int leftPrec,
      int rightPrec, boolean parentheses) {
    if (parentheses) {
      final SqlWriter.Frame frame = writer.startList("(", ")");
      unparse(writer, 0, 0);
      writer.endList(frame);
    } else {
      unparse(writer, leftPrec, rightPrec);
    }
  }

  public SqlParserPos getParserPosition() {
    return pos;
  }

  /**
   * Validates this node.
   *
   * <p>The typical implementation of this method will make a callback to the
   * validator appropriate to the node type and context. The validator has
   * methods such as {@link SqlValidator#validateLiteral} for these purposes.
   *
   * @param scope Validator
   */
  public abstract void validate(
      SqlValidator validator,
      SqlValidatorScope scope);

  /**
   * Lists all the valid alternatives for this node if the parse position of
   * the node matches that of pos. Only implemented now for SqlCall and
   * SqlOperator.
   *
   * @param validator Validator
   * @param scope     Validation scope
   * @param pos       SqlParserPos indicating the cursor position at which
   *                  completion hints are requested for
   * @param hintList  list of valid options
   */
  public void findValidOptions(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    // no valid options
  }

  /**
   * Validates this node in an expression context.
   *
   * <p>Usually, this method does much the same as {@link #validate}, but a
   * {@link SqlIdentifier} can occur in expression and non-expression
   * contexts.
   */
  public void validateExpr(
      SqlValidator validator,
      SqlValidatorScope scope) {
    validate(validator, scope);
    Util.discard(validator.deriveType(scope, this));
  }

  /**
   * Accepts a generic visitor.
   *
   * <p>Implementations of this method in subtypes simply call the appropriate
   * <code>visit</code> method on the
   * {@link org.apache.calcite.sql.util.SqlVisitor visitor object}.
   *
   * <p>The type parameter <code>R</code> must be consistent with the type
   * parameter of the visitor.
   */
  public abstract <R> R accept(SqlVisitor<R> visitor);

  /**
   * Returns whether this node is structurally equivalent to another node.
   * Some examples:
   *
   * <ul>
   * <li>1 + 2 is structurally equivalent to 1 + 2</li>
   * <li>1 + 2 + 3 is structurally equivalent to (1 + 2) + 3, but not to 1 +
   * (2 + 3), because the '+' operator is left-associative</li>
   * </ul>
   */
  public abstract boolean equalsDeep(@Nullable SqlNode node, Litmus litmus);

  @Deprecated // to be removed before 2.0
  public final boolean equalsDeep(@Nullable SqlNode node, boolean fail) {
    return equalsDeep(node, fail ? Litmus.THROW : Litmus.IGNORE);
  }

  /**
   * Returns whether two nodes are equal (using
   * {@link #equalsDeep(SqlNode, Litmus)}) or are both null.
   *
   * @param node1 First expression
   * @param node2 Second expression
   * @param litmus What to do if an error is detected (expressions are
   *              not equal)
   */
  public static boolean equalDeep(
      @Nullable SqlNode node1,
      @Nullable SqlNode node2,
      Litmus litmus) {
    if (node1 == null) {
      return node2 == null;
    } else if (node2 == null) {
      return false;
    } else {
      return node1.equalsDeep(node2, litmus);
    }
  }

  /**
   * Returns whether expression is always ascending, descending or constant.
   * This property is useful because it allows to safely aggregate infinite
   * streams of values.
   *
   * <p>The default implementation returns
   * {@link SqlMonotonicity#NOT_MONOTONIC}.
   *
   * @param scope Scope
   */
  public SqlMonotonicity getMonotonicity(@Nullable SqlValidatorScope scope) {
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  /** Returns whether two lists of operands are equal, comparing using
   * {@link SqlNode#equalsDeep(SqlNode, Litmus)}. */
  public static boolean equalDeep(List<? extends @Nullable SqlNode> operands0,
      List<? extends @Nullable SqlNode> operands1, Litmus litmus) {
    if (operands0.size() != operands1.size()) {
      return litmus.fail(null);
    }
    for (int i = 0; i < operands0.size(); i++) {
      if (!SqlNode.equalDeep(operands0.get(i), operands1.get(i), litmus)) {
        return litmus.fail(null);
      }
    }
    return litmus.succeed();
  }

  /**
   * Returns a {@code Collector} that accumulates the input elements into a
   * {@link SqlNodeList}, with zero position.
   *
   * @param <T> Type of the input elements
   *
   * @return a {@code Collector} that collects all the input elements into a
   * {@link SqlNodeList}, in encounter order
   */
  public static <T extends SqlNode> Collector<T, ArrayList<@Nullable SqlNode>, SqlNodeList>
      toList() {
    return toList(SqlParserPos.ZERO);
  }

  /**
   * Returns a {@code Collector} that accumulates the input elements into a
   * {@link SqlNodeList}.
   *
   * @param <T> Type of the input elements
   *
   * @return a {@code Collector} that collects all the input elements into a
   * {@link SqlNodeList}, in encounter order
   */
  public static <T extends @Nullable SqlNode> Collector<T,
      ArrayList<@Nullable SqlNode>, SqlNodeList> toList(SqlParserPos pos) {
    //noinspection RedundantTypeArguments
    return Collector.<T, ArrayList<@Nullable SqlNode>, SqlNodeList>of(
        ArrayList::new, ArrayList::add, Util::combine,
        (ArrayList<@Nullable SqlNode> list) -> SqlNodeList.of(pos, list));
  }
}
