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
import java.util.Iterator;
import java.util.List;

/**
 * A <code>SqlNodeList</code> is a list of {@link SqlNode}s. It is also a
 * {@link SqlNode}, so may appear in a parse tree.
 */
public class SqlNodeList extends SqlNode implements Iterable<SqlNode> {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * An immutable, empty SqlNodeList.
   */
  public static final SqlNodeList EMPTY =
      new SqlNodeList(SqlParserPos.ZERO) {
        public void add(SqlNode node) {
          throw new UnsupportedOperationException();
        }
      };

  /**
   * A SqlNodeList that has a single element that is an empty list.
   */
  public static final SqlNodeList SINGLETON_EMPTY =
      new SqlNodeList(ImmutableList.of(EMPTY), SqlParserPos.ZERO);

  /**
   * A SqlNodeList that has a single element that is a star identifier.
   */
  public static final SqlNodeList SINGLETON_STAR =
      new SqlNodeList(ImmutableList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
          SqlParserPos.ZERO);

  //~ Instance fields --------------------------------------------------------

  private final List<SqlNode> list;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an empty <code>SqlNodeList</code>.
   */
  public SqlNodeList(SqlParserPos pos) {
    super(pos);
    list = new ArrayList<>();
  }

  /**
   * Creates a <code>SqlNodeList</code> containing the nodes in <code>
   * list</code>. The list is copied, but the nodes in it are not.
   */
  public SqlNodeList(
      Collection<? extends SqlNode> collection,
      SqlParserPos pos) {
    super(pos);
    list = new ArrayList<>(collection);
  }

  //~ Methods ----------------------------------------------------------------

  // implement Iterable<SqlNode>
  public Iterator<SqlNode> iterator() {
    return list.iterator();
  }

  public List<SqlNode> getList() {
    return list;
  }

  public void add(SqlNode node) {
    list.add(node);
  }

  public SqlNodeList clone(SqlParserPos pos) {
    return new SqlNodeList(list, pos);
  }

  public SqlNode get(int n) {
    return list.get(n);
  }

  public SqlNode set(int n, SqlNode node) {
    return list.set(n, node);
  }

  public int size() {
    return list.size();
  }

  public void unparse(
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

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    for (SqlNode child : list) {
      child.validate(validator, scope);
    }
  }

  public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public boolean equalsDeep(SqlNode node, Litmus litmus) {
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

  public SqlNode[] toArray() {
    return list.toArray(new SqlNode[0]);
  }

  public static boolean isEmptyList(final SqlNode node) {
    if (node instanceof SqlNodeList) {
      if (0 == ((SqlNodeList) node).size()) {
        return true;
      }
    }
    return false;
  }

  public static SqlNodeList of(SqlNode node1) {
    SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);
    list.add(node1);
    return list;
  }

  public static SqlNodeList of(SqlNode node1, SqlNode node2) {
    SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);
    list.add(node1);
    list.add(node2);
    return list;
  }

  public static SqlNodeList of(SqlNode node1, SqlNode node2, SqlNode... nodes) {
    SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);
    list.add(node1);
    list.add(node2);
    for (SqlNode node : nodes) {
      list.add(node);
    }
    return list;
  }

  public void validateExpr(SqlValidator validator, SqlValidatorScope scope) {
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
