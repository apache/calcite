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

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

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
    final SqlWriter.Frame frame =
        ((leftPrec > 0) || (rightPrec > 0)) ? writer.startList("(", ")")
            : writer.startList("", "");
    commaList(writer);
    writer.endList(frame);
  }

  void commaList(SqlWriter writer) {
    // The precedence of the comma operator if low but not zero. For
    // instance, this ensures parentheses in
    //    select x, (select * from foo order by z), y from t
    for (SqlNode node : list) {
      writer.sep(",");
      node.unparse(writer, 2, 3);
    }
  }

  void andOrList(SqlWriter writer, SqlKind sepKind) {
    SqlBinaryOperator sepOp =
        sepKind == SqlKind.AND
            ? SqlStdOperatorTable.AND
            : SqlStdOperatorTable.OR;
    for (int i = 0; i < list.size(); i++) {
      SqlNode node = list.get(i);
      writer.sep(sepKind.name(), false);

      // The precedence pulling on the LHS of a node is the
      // right-precedence of the separator operator, except at the start
      // of the list; similarly for the RHS of a node. If the operator
      // has left precedence 4 and right precedence 5, the precedences
      // in a 3-node list will look as follows:
      //   0 <- node1 -> 4  5 <- node2 -> 4  5 <- node3 -> 0
      int lprec = (i == 0) ? 0 : sepOp.getRightPrec();
      int rprec = (i == (list.size() - 1)) ? 0 : sepOp.getLeftPrec();
      node.unparse(writer, lprec, rprec);
    }
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

// End SqlNodeList.java
