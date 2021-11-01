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
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Parse tree node that represents a PIVOT applied to a table reference
 * (or sub-query).
 *
 * <p>Syntax:
 * <blockquote>{@code
 * SELECT *
 * FROM query PIVOT (agg, ... FOR axis, ... IN (in, ...)) AS alias}
 * </blockquote>
 */
public class SqlPivot extends SqlCall {

  public SqlNode query;
  public final SqlNodeList aggList;
  public final SqlNodeList axisList;
  public final SqlNodeList inList;

  static final Operator OPERATOR = new Operator(SqlKind.PIVOT);

  //~ Constructors -----------------------------------------------------------

  public SqlPivot(SqlParserPos pos, SqlNode query, SqlNodeList aggList,
      SqlNodeList axisList, SqlNodeList inList) {
    super(pos);
    this.query = Objects.requireNonNull(query, "query");
    this.aggList = Objects.requireNonNull(aggList, "aggList");
    this.axisList = Objects.requireNonNull(axisList, "axisList");
    this.inList = Objects.requireNonNull(inList, "inList");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(query, aggList, axisList, inList);
  }

  @SuppressWarnings("nullness")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    // Only 'query' is mutable. (It is required for validation.)
    switch (i) {
    case 0:
      query = operand;
      break;
    default:
      super.setOperand(i, operand);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    query.unparse(writer, leftPrec, 0);
    writer.keyword("PIVOT");
    final SqlWriter.Frame frame = writer.startList("(", ")");
    aggList.unparse(writer, 0, 0);
    writer.sep("FOR");
    // force parentheses if there is more than one axis
    final int leftPrec1 = axisList.size() > 1 ? 1 : 0;
    axisList.unparse(writer, leftPrec1, 0);
    writer.sep("IN");
    writer.list(SqlWriter.FrameTypeEnum.PARENTHESES, SqlWriter.COMMA,
        stripList(inList));
    writer.endList(frame);
  }

  static SqlNodeList stripList(SqlNodeList list) {
    return list.stream().map(SqlPivot::strip)
        .collect(SqlNode.toList(list.pos));
  }

  /** Converts a single-element SqlNodeList to its constituent node.
   * For example, "(1)" becomes "1";
   * "(2) as a" becomes "2 as a";
   * "(3, 4)" remains "(3, 4)";
   * "(5, 6) as b" remains "(5, 6) as b". */
  private static SqlNode strip(SqlNode e) {
    switch (e.getKind()) {
    case AS:
      final SqlCall call = (SqlCall) e;
      final List<SqlNode> operands = call.getOperandList();
      return SqlStdOperatorTable.AS.createCall(e.pos,
          strip(operands.get(0)), operands.get(1));
    default:
      if (e instanceof SqlNodeList && ((SqlNodeList) e).size() == 1) {
        return ((SqlNodeList) e).get(0);
      }
      return e;
    }
  }

  /** Returns the aggregate list as (alias, call) pairs.
   * If there is no 'AS', alias is null. */
  public void forEachAgg(BiConsumer<@Nullable String, SqlNode> consumer) {
    for (SqlNode agg : aggList) {
      final SqlNode call = SqlUtil.stripAs(agg);
      final @Nullable String alias = SqlValidatorUtil.alias(agg);
      consumer.accept(alias, call);
    }
  }

  /** Returns the value list as (alias, node list) pairs. */
  public void forEachNameValues(BiConsumer<String, SqlNodeList> consumer) {
    for (SqlNode node : inList) {
      String alias;
      if (node.getKind() == SqlKind.AS) {
        final List<SqlNode> operands = ((SqlCall) node).getOperandList();
        alias = ((SqlIdentifier) operands.get(1)).getSimple();
        node = operands.get(0);
      } else {
        alias = pivotAlias(node);
      }
      consumer.accept(alias, toNodes(node));
    }
  }

  static String pivotAlias(SqlNode node) {
    if (node instanceof SqlNodeList) {
      return ((SqlNodeList) node).stream()
          .map(SqlPivot::pivotAlias).collect(Collectors.joining("_"));
    }
    return node.toString();
  }

  /** Converts a SqlNodeList to a list, and other nodes to a singleton list. */
  static SqlNodeList toNodes(SqlNode node) {
    if (node instanceof SqlNodeList) {
      return (SqlNodeList) node;
    } else {
      return new SqlNodeList(ImmutableList.of(node), node.getParserPosition());
    }
  }

  /** Returns the set of columns that are referenced as an argument to an
   * aggregate function or in a column in the {@code FOR} clause. All columns
   * that are not used will become "GROUP BY" columns. */
  public Set<String> usedColumnNames() {
    final Set<String> columnNames = new HashSet<>();
    final SqlVisitor<Void> nameCollector = new SqlBasicVisitor<Void>() {
      @Override public Void visit(SqlIdentifier id) {
        columnNames.add(Util.last(id.names));
        return super.visit(id);
      }
    };
    for (SqlNode agg : aggList) {
      final SqlCall call = (SqlCall) SqlUtil.stripAs(agg);
      call.accept(nameCollector);
    }
    for (SqlNode axis : axisList) {
      axis.accept(nameCollector);
    }
    return columnNames;
  }

  /** Pivot operator. */
  static class Operator extends SqlSpecialOperator {
    Operator(SqlKind kind) {
      super(kind.name(), kind);
    }
  }
}
