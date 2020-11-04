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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.RandomAccess;
import javax.annotation.Nonnull;

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

  private final List<SqlNode> list;

  //~ Constructors -----------------------------------------------------------

  /** Creates a SqlNodeList with a given backing list.
   *
   * <p>Because SqlNodeList implements {@link RandomAccess}, the backing list
   * should allow O(1) access to elements. */
  private SqlNodeList(SqlParserPos pos, List<SqlNode> list) {
    super(pos);
    this.list = Objects.requireNonNull(list);
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
      Collection<? extends SqlNode> collection,
      SqlParserPos pos) {
    this(pos, new ArrayList<>(collection));
  }

  /**
   * Creates a SqlNodeList with a given backing list.
   * Does not copy the list.
   */
  public static SqlNodeList of(SqlParserPos pos, List<SqlNode> list) {
    return new SqlNodeList(pos, list);
  }

  //~ Methods ----------------------------------------------------------------

  // List, Collection and Iterable methods


  @Override public int hashCode() {
    return list.hashCode();
  }

  @Override public boolean equals(Object o) {
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

  @Override public @Nonnull Iterator<SqlNode> iterator() {
    return list.iterator();
  }

  @Override public ListIterator<SqlNode> listIterator() {
    return list.listIterator();
  }

  @Override public ListIterator<SqlNode> listIterator(int index) {
    return list.listIterator(index);
  }

  @Override public List<SqlNode> subList(int fromIndex, int toIndex) {
    return list.subList(fromIndex, toIndex);
  }

  @Override public SqlNode get(int n) {
    return list.get(n);
  }

  @Override public SqlNode set(int n, SqlNode node) {
    return list.set(n, node);
  }

  @Override public boolean contains(Object o) {
    return list.contains(o);
  }

  @Override public boolean containsAll(Collection<?> c) {
    return list.containsAll(c);
  }

  @Override public int indexOf(Object o) {
    return list.indexOf(o);
  }

  @Override public int lastIndexOf(Object o) {
    return list.lastIndexOf(o);
  }

  @Override public @Nonnull Object[] toArray() {
    // Per JDK specification, must return an Object[] not SqlNode[]; see e.g.
    // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6260652
    return list.toArray();
  }

  @Override public @Nonnull <T> T[] toArray(T[] a) {
    return list.toArray(a);
  }

  @Override public boolean add(SqlNode node) {
    return list.add(node);
  }

  @Override public void add(int index, SqlNode element) {
    list.add(index, element);
  }

  @Override public boolean addAll(Collection<? extends SqlNode> c) {
    return list.addAll(c);
  }

  @Override public boolean addAll(int index, Collection<? extends SqlNode> c) {
    return list.addAll(index, c);
  }

  @Override public void clear() {
    list.clear();
  }

  @Override public boolean remove(Object o) {
    return list.remove(o);
  }

  @Override public SqlNode remove(int index) {
    return list.remove(index);
  }

  @Override public boolean removeAll(Collection<?> c) {
    return list.removeAll(c);
  }

  @Override public boolean retainAll(Collection<?> c) {
    return list.retainAll(c);
  }

  // SqlNodeList-specific methods

  public List<SqlNode> getList() {
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
      child.validate(validator, scope);
    }
  }

  @Override public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override public boolean equalsDeep(SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlNodeList)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlNodeList that = (SqlNodeList) node;
    if (this.size() != that.size()) {
      return litmus.fail("{} != {}", this, node);
    }
    for (int i = 0; i < list.size(); i++) {
      SqlNode thisChild = list.get(i);
      final SqlNode thatChild = that.list.get(i);
      if (!thisChild.equalsDeep(thatChild, litmus)) {
        return litmus.fail(null);
      }
    }
    return litmus.succeed();
  }

  public static boolean isEmptyList(final SqlNode node) {
    return node instanceof SqlNodeList
        && ((SqlNodeList) node).isEmpty();
  }

  public static SqlNodeList of(SqlNode node1) {
    final List<SqlNode> list = new ArrayList<>(1);
    list.add(node1);
    return new SqlNodeList(SqlParserPos.ZERO, list);
  }

  public static SqlNodeList of(SqlNode node1, SqlNode node2) {
    final List<SqlNode> list = new ArrayList<>(2);
    list.add(node1);
    list.add(node2);
    return new SqlNodeList(SqlParserPos.ZERO, list);
  }

  public static SqlNodeList of(SqlNode node1, SqlNode node2, SqlNode... nodes) {
    final List<SqlNode> list = new ArrayList<>(nodes.length + 2);
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
      node.validateExpr(validator, scope);
    }
  }
}
