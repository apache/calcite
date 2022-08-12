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

import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlSelect</code> is a node of a parse tree which represents a select
 * statement. It warrants its own node type just because we have a lot of
 * methods to put somewhere.
 */
public class SqlSelect extends SqlCall {
  //~ Static fields/initializers ---------------------------------------------

  // constants representing operand positions
  public static final int FROM_OPERAND = 2;
  public static final int WHERE_OPERAND = 3;
  public static final int HAVING_OPERAND = 5;

  SqlNodeList keywordList;
  SqlNodeList selectList;
  @Nullable SqlNode from;
  @Nullable SqlNode where;
  @Nullable SqlNodeList groupBy;
  @Nullable SqlNode having;
  SqlNodeList windowDecls;
  @Nullable SqlNodeList orderBy;
  @Nullable SqlNode offset;
  @Nullable SqlNode fetch;
  @Nullable SqlNodeList hints;

  //~ Constructors -----------------------------------------------------------

  public SqlSelect(SqlParserPos pos,
      @Nullable SqlNodeList keywordList,
      SqlNodeList selectList,
      @Nullable SqlNode from,
      @Nullable SqlNode where,
      @Nullable SqlNodeList groupBy,
      @Nullable SqlNode having,
      @Nullable SqlNodeList windowDecls,
      @Nullable SqlNodeList orderBy,
      @Nullable SqlNode offset,
      @Nullable SqlNode fetch,
      @Nullable SqlNodeList hints) {
    super(pos);
    this.keywordList = requireNonNull(keywordList != null
        ? keywordList : new SqlNodeList(pos));
    this.selectList = requireNonNull(selectList, "selectList");
    this.from = from;
    this.where = where;
    this.groupBy = groupBy;
    this.having = having;
    this.windowDecls = requireNonNull(windowDecls != null
        ? windowDecls : new SqlNodeList(pos));
    this.orderBy = orderBy;
    this.offset = offset;
    this.fetch = fetch;
    this.hints = hints;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return SqlSelectOperator.INSTANCE;
  }

  @Override public SqlKind getKind() {
    return SqlKind.SELECT;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(keywordList, selectList, from, where,
        groupBy, having, windowDecls, orderBy, offset, fetch, hints);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      keywordList = requireNonNull((SqlNodeList) operand);
      break;
    case 1:
      selectList = requireNonNull((SqlNodeList) operand);
      break;
    case 2:
      from = operand;
      break;
    case 3:
      where = operand;
      break;
    case 4:
      groupBy = (SqlNodeList) operand;
      break;
    case 5:
      having = operand;
      break;
    case 6:
      windowDecls = requireNonNull((SqlNodeList) operand);
      break;
    case 7:
      orderBy = (SqlNodeList) operand;
      break;
    case 8:
      offset = operand;
      break;
    case 9:
      fetch = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  public final boolean isDistinct() {
    return getModifierNode(SqlSelectKeyword.DISTINCT) != null;
  }

  public final @Nullable SqlNode getModifierNode(SqlSelectKeyword modifier) {
    for (SqlNode keyword : keywordList) {
      SqlSelectKeyword keyword2 =
          ((SqlLiteral) keyword).symbolValue(SqlSelectKeyword.class);
      if (keyword2 == modifier) {
        return keyword;
      }
    }
    return null;
  }

  @Pure
  public final @Nullable SqlNode getFrom() {
    return from;
  }

  public void setFrom(@Nullable SqlNode from) {
    this.from = from;
  }

  @Pure
  public final @Nullable SqlNodeList getGroup() {
    return groupBy;
  }

  public void setGroupBy(@Nullable SqlNodeList groupBy) {
    this.groupBy = groupBy;
  }

  @Pure
  public final @Nullable SqlNode getHaving() {
    return having;
  }

  public void setHaving(@Nullable SqlNode having) {
    this.having = having;
  }

  @Pure
  public final SqlNodeList getSelectList() {
    return selectList;
  }

  public void setSelectList(SqlNodeList selectList) {
    this.selectList = selectList;
  }

  @Pure
  public final @Nullable SqlNode getWhere() {
    return where;
  }

  public void setWhere(@Nullable SqlNode whereClause) {
    this.where = whereClause;
  }

  public final SqlNodeList getWindowList() {
    return windowDecls;
  }

  @Pure
  public final @Nullable SqlNodeList getOrderList() {
    return orderBy;
  }

  public void setOrderBy(@Nullable SqlNodeList orderBy) {
    this.orderBy = orderBy;
  }

  @Pure
  public final @Nullable SqlNode getOffset() {
    return offset;
  }

  public void setOffset(@Nullable SqlNode offset) {
    this.offset = offset;
  }

  @Pure
  public final @Nullable SqlNode getFetch() {
    return fetch;
  }

  public void setFetch(@Nullable SqlNode fetch) {
    this.fetch = fetch;
  }

  public void setHints(@Nullable SqlNodeList hints) {
    this.hints = hints;
  }

  @Pure
  public @Nullable SqlNodeList getHints() {
    return this.hints;
  }

  @EnsuresNonNullIf(expression = "hints", result = true)
  public boolean hasHints() {
    // The hints may be passed as null explicitly.
    return this.hints != null && this.hints.size() > 0;
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateQuery(this, scope, validator.getUnknownType());
  }

  // Override SqlCall, to introduce a sub-query frame.
  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (!writer.inQuery()
        || getFetch() != null
            && (leftPrec > SqlInternalOperators.FETCH.getLeftPrec()
                || rightPrec > SqlInternalOperators.FETCH.getLeftPrec())
        || getOffset() != null
            && (leftPrec > SqlInternalOperators.OFFSET.getLeftPrec()
                || rightPrec > SqlInternalOperators.OFFSET.getLeftPrec())
        || getOrderList() != null
            && (leftPrec > SqlOrderBy.OPERATOR.getLeftPrec()
                || rightPrec > SqlOrderBy.OPERATOR.getRightPrec())) {
      // If this SELECT is the topmost item in a sub-query, introduce a new
      // frame. (The topmost item in the sub-query might be a UNION or
      // ORDER. In this case, we don't need a wrapper frame.)
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.SUB_QUERY, "(", ")");
      writer.getDialect().unparseCall(writer, this, 0, 0);
      writer.endList(frame);
    } else {
      writer.getDialect().unparseCall(writer, this, leftPrec, rightPrec);
    }
  }

  public boolean hasOrderBy() {
    return orderBy != null && orderBy.size() != 0;
  }

  public boolean hasWhere() {
    return where != null;
  }

  public boolean isKeywordPresent(SqlSelectKeyword targetKeyWord) {
    return getModifierNode(targetKeyWord) != null;
  }
}
