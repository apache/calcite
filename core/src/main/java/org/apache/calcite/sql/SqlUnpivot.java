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
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Parse tree node that represents UNPIVOT applied to a table reference
 * (or sub-query).
 *
 * <p>Syntax:
 * <blockquote><pre>{@code
 * SELECT *
 * FROM query
 * UNPIVOT [ { INCLUDE | EXCLUDE } NULLS ] (
 *   columns FOR columns IN ( columns [ AS values ], ...))
 *
 * where:
 *
 * columns: column
 *        | '(' column, ... ')'
 * values:  value
 *        | '(' value, ... ')'
 * }</pre></blockquote>
 */
public class SqlUnpivot extends SqlCall {

  public SqlNode query;
  public final boolean includeNulls;
  public final SqlNodeList measureList;
  public final SqlNodeList axisList;
  public final SqlNodeList inList;

  static final Operator OPERATOR = new Operator(SqlKind.UNPIVOT);

  //~ Constructors -----------------------------------------------------------

  public SqlUnpivot(SqlParserPos pos, SqlNode query, boolean includeNulls,
      SqlNodeList measureList, SqlNodeList axisList, SqlNodeList inList) {
    super(pos);
    this.query = requireNonNull(query, "query");
    this.includeNulls = includeNulls;
    this.measureList = requireNonNull(measureList, "measureList");
    this.axisList = requireNonNull(axisList, "axisList");
    this.inList = requireNonNull(inList, "inList");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(query, measureList, axisList, inList);
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
    writer.keyword("UNPIVOT");
    writer.keyword(includeNulls ? "INCLUDE NULLS" : "EXCLUDE NULLS");
    final SqlWriter.Frame frame = writer.startList("(", ")");
    // force parentheses if there is more than one foo
    final int leftPrec1 = measureList.size() > 1 ? 1 : 0;
    measureList.unparse(writer, leftPrec1, 0);
    writer.sep("FOR");
    // force parentheses if there is more than one axis
    final int leftPrec2 = axisList.size() > 1 ? 1 : 0;
    axisList.unparse(writer, leftPrec2, 0);
    writer.sep("IN");
    writer.list(SqlWriter.FrameTypeEnum.PARENTHESES, SqlWriter.COMMA,
        SqlPivot.stripList(inList));
    writer.endList(frame);
  }

  /** Returns the measure list as SqlIdentifiers. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void forEachMeasure(Consumer<SqlIdentifier> consumer) {
    ((List<SqlIdentifier>) (List) measureList).forEach(consumer);
  }

  /** Returns contents of the IN clause {@code (nodeList, valueList)} pairs.
   * {@code valueList} is null if the entry has no {@code AS} clause. */
  public void forEachNameValues(
      BiConsumer<SqlNodeList, @Nullable SqlNodeList> consumer) {
    for (SqlNode node : inList) {
      switch (node.getKind()) {
      case AS:
        final SqlCall call = (SqlCall) node;
        assert call.getOperandList().size() == 2;
        final SqlNodeList nodeList = call.operand(0);
        final SqlNodeList valueList = call.operand(1);
        consumer.accept(nodeList, valueList);
        break;
      default:
        final SqlNodeList nodeList2 = (SqlNodeList) node;
        consumer.accept(nodeList2, null);
      }
    }
  }

  /** Returns the set of columns that are referenced in the {@code FOR}
   * clause. All columns that are not used will be part of the returned row. */
  public Set<String> usedColumnNames() {
    final Set<String> columnNames = new HashSet<>();
    final SqlVisitor<Void> nameCollector = new SqlBasicVisitor<Void>() {
      @Override public Void visit(SqlIdentifier id) {
        columnNames.add(Util.last(id.names));
        return super.visit(id);
      }
    };
    forEachNameValues((aliasList, valueList) ->
        aliasList.accept(nameCollector));
    return columnNames;
  }

  /** Computes an alias. In the query fragment
   * <blockquote>
   *   {@code UNPIVOT ... FOR ... IN ((c1, c2) AS 'c1_c2', (c3, c4))}
   * </blockquote>
   * note that {@code (c3, c4)} has no {@code AS}. The computed alias is
   * 'C3_C4'. */
  public static String aliasValue(SqlNodeList aliasList) {
    final StringBuilder b = new StringBuilder();
    aliasList.forEach(alias -> {
      if (b.length() > 0) {
        b.append('_');
      }
      b.append(Util.last(((SqlIdentifier) alias).names));
    });
    return b.toString();
  }

  /** Unpivot operator. */
  static class Operator extends SqlSpecialOperator {
    Operator(SqlKind kind) {
      super(kind.name(), kind);
    }
  }
}
