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
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * An operator describing a query. (Not a query itself.)
 *
 * <p>Operands are:</p>
 *
 * <ul>
 * <li>0: distinct ({@link SqlLiteral})</li>
 * <li>1: selectClause ({@link SqlNodeList})</li>
 * <li>2: fromClause ({@link SqlCall} to "join" operator)</li>
 * <li>3: whereClause ({@link SqlNode})</li>
 * <li>4: havingClause ({@link SqlNode})</li>
 * <li>5: groupClause ({@link SqlNode})</li>
 * <li>6: windowClause ({@link SqlNodeList})</li>
 * <li>7: orderClause ({@link SqlNode})</li>
 * </ul>
 */
public class SqlSelectOperator extends SqlOperator {
  public static final SqlSelectOperator INSTANCE =
      new SqlSelectOperator();

  //~ Constructors -----------------------------------------------------------

  private SqlSelectOperator() {
    super("SELECT", SqlKind.SELECT, 2, true, ReturnTypes.SCOPE, null, null);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  @Override public SqlCall createCall(
      @Nullable SqlLiteral functionQualifier,
      SqlParserPos pos,
      @Nullable SqlNode... operands) {
    assert functionQualifier == null;
    return new SqlSelect(pos,
        (SqlNodeList) operands[0],
        requireNonNull((SqlNodeList) operands[1], "selectList"),
        operands[2],
        operands[3],
        (SqlNodeList) operands[4],
        operands[5],
        (SqlNodeList) operands[6],
        (SqlNodeList) operands[7],
        operands[8],
        operands[9],
        (SqlNodeList) operands[10]);
  }

  /**
   * Creates a call to the <code>SELECT</code> operator.
   *
   * @deprecated Use {@link #createCall(SqlLiteral, SqlParserPos, SqlNode...)}.
   */
  @Deprecated // to be removed before 2.0
  public SqlSelect createCall(
      SqlNodeList keywordList,
      SqlNodeList selectList,
      SqlNode fromClause,
      SqlNode whereClause,
      SqlNodeList groupBy,
      SqlNode having,
      SqlNodeList windowDecls,
      SqlNodeList orderBy,
      SqlNode offset,
      SqlNode fetch,
      SqlNodeList hints,
      SqlParserPos pos) {
    return new SqlSelect(
        pos,
        keywordList,
        selectList,
        fromClause,
        whereClause,
        groupBy,
        having,
        windowDecls,
        orderBy,
        offset,
        fetch,
        hints);
  }

  @Override public <R> void acceptCall(
      SqlVisitor<R> visitor,
      SqlCall call,
      boolean onlyExpressions,
      SqlBasicVisitor.ArgHandler<R> argHandler) {
    if (!onlyExpressions) {
      // None of the arguments to the SELECT operator are expressions.
      super.acceptCall(visitor, call, onlyExpressions, argHandler);
    }
  }

  @SuppressWarnings("deprecation")
  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    SqlSelect select = (SqlSelect) call;
    final SqlWriter.Frame selectFrame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.sep("SELECT");

    if (select.hasHints()) {
      writer.sep("/*+");
      castNonNull(select.hints).unparse(writer, 0, 0);
      writer.print("*/");
      writer.newlineAndIndent();
    }

    for (int i = 0; i < select.keywordList.size(); i++) {
      final SqlNode keyword = select.keywordList.get(i);
      keyword.unparse(writer, 0, 0);
    }
    writer.topN(select.fetch, select.offset);
    final SqlNodeList selectClause = select.selectList;
    writer.list(SqlWriter.FrameTypeEnum.SELECT_LIST, SqlWriter.COMMA,
        selectClause);

    if (select.from != null) {
      // Calcite SQL requires FROM but MySQL does not.
      writer.sep("FROM");

      // for FROM clause, use precedence just below join operator to make
      // sure that an un-joined nested select will be properly
      // parenthesized
      final SqlWriter.Frame fromFrame =
          writer.startList(SqlWriter.FrameTypeEnum.FROM_LIST);
      select.from.unparse(
          writer,
          SqlJoin.COMMA_OPERATOR.getLeftPrec() - 1,
          SqlJoin.COMMA_OPERATOR.getRightPrec() - 1);
      writer.endList(fromFrame);
    }

    SqlNode where = select.where;
    if (where != null) {
      writer.sep("WHERE");

      if (!writer.isAlwaysUseParentheses()) {
        SqlNode node = where;

        // decide whether to split on ORs or ANDs
        SqlBinaryOperator whereSep = SqlStdOperatorTable.AND;
        if ((node instanceof SqlCall)
            && node.getKind() == SqlKind.OR) {
          whereSep = SqlStdOperatorTable.OR;
        }

        // unroll whereClause
        final List<SqlNode> list = new ArrayList<>(0);
        while (node.getKind() == whereSep.kind) {
          assert node instanceof SqlCall;
          final SqlCall call1 = (SqlCall) node;
          list.add(0, call1.operand(1));
          node = call1.operand(0);
        }
        list.add(0, node);

        // unparse in a WHERE_LIST frame
        writer.list(SqlWriter.FrameTypeEnum.WHERE_LIST, whereSep,
            new SqlNodeList(list, where.getParserPosition()));
      } else {
        where.unparse(writer, 0, 0);
      }
    }
    if (select.groupBy != null) {
      SqlNodeList groupBy =
          select.groupBy.size() == 0 ? SqlNodeList.SINGLETON_EMPTY
              : select.groupBy;
      // if the DISTINCT keyword of GROUP BY is present it can be the only item
      if (groupBy.size() == 1 && groupBy.get(0) != null
          && groupBy.get(0).getKind() == SqlKind.GROUP_BY_DISTINCT) {
        writer.sep("GROUP BY DISTINCT");
        List<SqlNode> operandList = ((SqlCall) groupBy.get(0)).getOperandList();
        groupBy = new SqlNodeList(operandList, groupBy.getParserPosition());
      } else {
        writer.sep("GROUP BY");
      }
      writer.list(SqlWriter.FrameTypeEnum.GROUP_BY_LIST, SqlWriter.COMMA,
          groupBy);
    }
    if (select.having != null) {
      writer.sep("HAVING");
      select.having.unparse(writer, 0, 0);
    }
    if (select.windowDecls.size() > 0) {
      writer.sep("WINDOW");
      writer.list(SqlWriter.FrameTypeEnum.WINDOW_DECL_LIST, SqlWriter.COMMA,
          select.windowDecls);
    }
    if (select.orderBy != null && select.orderBy.size() > 0) {
      writer.sep("ORDER BY");
      writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
          select.orderBy);
    }
    writer.fetchOffset(select.fetch, select.offset);
    writer.endList(selectFrame);
  }

  @Override public boolean argumentMustBeScalar(int ordinal) {
    return ordinal == SqlSelect.WHERE_OPERAND;
  }
}
