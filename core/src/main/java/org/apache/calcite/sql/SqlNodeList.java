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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.function.Consumer;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * A <code>SqlNodeList</code> is a list of {@link SqlNode}s. It is also a
 * {@link SqlNode}, so may appear in a parse tree.
 *
 * @see SqlNode#toList()
 */
public class SqlNodeList extends SqlNode implements List<SqlNode>, RandomAccess {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * An immutable, empty SqlNodeList.
   */
  public static final SqlNodeList EMPTY =
      new SqlNodeList(ImmutableList.of(), SqlParserPos.ZERO);

  /**
   * A SqlNodeList that has a single element that is an empty list.
   */
  public static final SqlNodeList SINGLETON_EMPTY =
      new SqlNodeList(ImmutableList.of(EMPTY), SqlParserPos.ZERO);

  /**
   * A SqlNodeList that has a single element that is a star identifier.
   */
  public static final SqlNodeList SINGLETON_STAR =
      new SqlNodeList(ImmutableList.of(SqlIdentifier.STAR), SqlParserPos.ZERO);

  //~ Instance fields --------------------------------------------------------

  // Sometimes null values are present in the list, however, it is assumed that callers would
  // perform all the required null-checks.
  private final List<@Nullable SqlNode> list;

  //~ Constructors -----------------------------------------------------------

  /** Creates a SqlNodeList with a given backing list.
   *
   * <p>Because SqlNodeList implements {@link RandomAccess}, the backing list
   * should allow O(1) access to elements. */
  private SqlNodeList(SqlParserPos pos, List<@Nullable SqlNode> list) {
    super(pos);
    this.list = Objects.requireNonNull(list, "list");
  }

  /**
   * Creates a SqlNodeList that is initially empty.
   */
  public SqlNodeList(SqlParserPos pos) {
    this(pos, new ArrayList<>());
  }

  /**
   * Creates a <code>SqlNodeList</code> containing the nodes in <code>
   * list</code>. The list is copied, but the nodes in it are not.
   */
  public SqlNodeList(
      Collection<? extends @Nullable SqlNode> collection,
      SqlParserPos pos) {
    this(pos, new ArrayList<@Nullable SqlNode>(collection));
  }

  /**
   * Creates a SqlNodeList with a given backing list.
   * Does not copy the list.
   */
  public static SqlNodeList of(SqlParserPos pos, List<@Nullable SqlNode> list) {
    return new SqlNodeList(pos, list);
  }

  //~ Methods ----------------------------------------------------------------

  // List, Collection and Iterable methods


  @Override public int hashCode() {
    return list.hashCode();
  }

  @Override public boolean equals(@Nullable Object o) {
    return this == o
        || o instanceof SqlNodeList && list.equals(((SqlNodeList) o).list)
        || o instanceof List && list.equals(o);
  }

  @Override public boolean isEmpty() {
    return list.isEmpty();
  }

  @Override public int size() {
    return list.size();
  }

  @Override public void forEach(Consumer<? super SqlNode> action) {
    //noinspection RedundantCast
    ((List<SqlNode>) list).forEach(action);
  }

  @SuppressWarnings("return.type.incompatible")
  @Override public Iterator</*Nullable*/ SqlNode> iterator() {
    return list.iterator();
  }

  @SuppressWarnings("return.type.incompatible")
  @Override public ListIterator</*Nullable*/ SqlNode> listIterator() {
    return list.listIterator();
  }

  @SuppressWarnings("return.type.incompatible")
  @Override public ListIterator</*Nullable*/ SqlNode> listIterator(int index) {
    return list.listIterator(index);
  }

  @SuppressWarnings("return.type.incompatible")
  @Override public List</*Nullable*/ SqlNode> subList(int fromIndex, int toIndex) {
    return list.subList(fromIndex, toIndex);
  }

  @SuppressWarnings("return.type.incompatible")
  @Override public /*Nullable*/ SqlNode get(int n) {
    return list.get(n);
  }

  @Override public SqlNode set(int n, @Nullable SqlNode node) {
    return castNonNull(list.set(n, node));
  }

  @Override public boolean contains(@Nullable Object o) {
    return list.contains(o);
  }

  @Override public boolean containsAll(Collection<?> c) {
    return list.containsAll(c);
  }

  @Override public int indexOf(@Nullable Object o) {
    return list.indexOf(o);
  }

  @Override public int lastIndexOf(@Nullable Object o) {
    return list.lastIndexOf(o);
  }

  @SuppressWarnings("return.type.incompatible")
  @Override public Object[] toArray() {
    // Per JDK specification, must return an Object[] not SqlNode[]; see e.g.
    // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6260652
    return list.toArray();
  }

  @SuppressWarnings("return.type.incompatible")
  @Override public <T> @Nullable T[] toArray(T @Nullable [] a) {
    return list.toArray(a);
  }

  @Override public boolean add(@Nullable SqlNode node) {
    return list.add(node);
  }

  @Override public void add(int index, @Nullable SqlNode element) {
    list.add(index, element);
  }

  @Override public boolean addAll(Collection<? extends @Nullable SqlNode> c) {
    return list.addAll(c);
  }

  @Override public boolean addAll(int index, Collection<? extends @Nullable SqlNode> c) {
    return list.addAll(index, c);
  }

  @Override public void clear() {
    list.clear();
  }

  @Override public boolean remove(@Nullable Object o) {
    return list.remove(o);
  }

  @Override public SqlNode remove(int index) {
    return castNonNull(list.remove(index));
  }

  @Override public boolean removeAll(Collection<?> c) {
    return list.removeAll(c);
  }

  @Override public boolean retainAll(Collection<?> c) {
    return list.retainAll(c);
  }

  // SqlNodeList-specific methods

  public List<@Nullable SqlNode> getList() {
    return list;
  }

  @Override public SqlNodeList clone(SqlParserPos pos) {
    return new SqlNodeList(list, pos);
  }

  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.FrameTypeEnum frameType =
        (leftPrec > 0 || rightPrec > 0)
            ? SqlWriter.FrameTypeEnum.PARENTHESES
            : SqlWriter.FrameTypeEnum.SIMPLE;
    writer.list(frameType, SqlWriter.COMMA, this);
  }

  @Deprecated // to be removed before 2.0
  void commaList(SqlWriter writer) {
    unparse(writer, 0, 0);
  }

  @Deprecated // to be removed before 2.0
  void andOrList(SqlWriter writer, SqlBinaryOperator sepOp) {
    writer.list(SqlWriter.FrameTypeEnum.WHERE_LIST, sepOp, this);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    for (SqlNode child : list) {
      if (child == null) {
        continue;
      }
      child.validate(validator, scope);
    }
  }

  @Override public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlNodeList)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlNodeList that = (SqlNodeList) node;
    return SqlNode.equalDeep(list, that.list,
        litmus.withMessageArgs("{} != {}", this, node));
  }

  public static boolean isEmptyList(final SqlNode node) {
    return node instanceof SqlNodeList
        && ((SqlNodeList) node).isEmpty();
  }

  public static SqlNodeList of(SqlNode node1) {
    final List<@Nullable SqlNode> list = new ArrayList<>(1);
    list.add(node1);
    return new SqlNodeList(SqlParserPos.ZERO, list);
  }

  public static SqlNodeList of(SqlNode node1, SqlNode node2) {
    final List<@Nullable SqlNode> list = new ArrayList<>(2);
    list.add(node1);
    list.add(node2);
    return new SqlNodeList(SqlParserPos.ZERO, list);
  }

  public static SqlNodeList of(SqlNode node1, SqlNode node2, @Nullable SqlNode... nodes) {
    final List<@Nullable SqlNode> list = new ArrayList<>(nodes.length + 2);
    list.add(node1);
    list.add(node2);
    Collections.addAll(list, nodes);
    return new SqlNodeList(SqlParserPos.ZERO, list);
  }

  @Override public void validateExpr(SqlValidator validator, SqlValidatorScope scope) {
    // While a SqlNodeList is not always a valid expression, this
    // implementation makes that assumption. It just validates the members
    // of the list.
    //
    // One example where this is valid is the IN operator. The expression
    //
    //    empno IN (10, 20)
    //
    // results in a call with operands
    //
    //    {  SqlIdentifier({"empno"}),
    //       SqlNodeList(SqlLiteral(10), SqlLiteral(20))  }

    for (SqlNode node : list) {
      if (node == null) {
        continue;
      }
      node.validateExpr(validator, scope);
    }
  }
}
