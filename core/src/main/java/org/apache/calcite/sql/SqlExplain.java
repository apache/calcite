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
package org.eigenbase.sql;

import java.util.List;

import org.eigenbase.sql.parser.*;
import org.eigenbase.util.ImmutableNullableList;

/**
 * A <code>SqlExplain</code> is a node of a parse tree which represents an
 * EXPLAIN PLAN statement.
 */
public class SqlExplain extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("EXPLAIN", SqlKind.EXPLAIN) {
        @Override
        public SqlCall createCall(SqlLiteral functionQualifier,
            SqlParserPos pos, SqlNode... operands) {
          return new SqlExplain(pos, operands[0], (SqlLiteral) operands[1],
              (SqlLiteral) operands[2], (SqlLiteral) operands[3], 0);
        }
      };

  //~ Enums ------------------------------------------------------------------

  /**
   * The level of abstraction with which to display the plan.
   */
  public static enum Depth implements SqlLiteral.SqlSymbol {
    TYPE, LOGICAL, PHYSICAL;

    /**
     * Creates a parse-tree node representing an occurrence of this symbol
     * at a particular position in the parsed text.
     */
    public SqlLiteral symbol(SqlParserPos pos) {
      return SqlLiteral.createSymbol(this, pos);
    }
  }

  //~ Instance fields --------------------------------------------------------

  SqlNode explicandum;
  SqlLiteral detailLevel;
  SqlLiteral depth;
  SqlLiteral asXml;
  private final int dynamicParameterCount;

  //~ Constructors -----------------------------------------------------------

  public SqlExplain(SqlParserPos pos,
      SqlNode explicandum,
      SqlLiteral detailLevel,
      SqlLiteral depth,
      SqlLiteral asXml,
      int dynamicParameterCount) {
    super(pos);
    this.explicandum = explicandum;
    this.detailLevel = detailLevel;
    this.depth = depth;
    this.asXml = asXml;
    this.dynamicParameterCount = dynamicParameterCount;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.EXPLAIN;
  }

  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(explicandum, detailLevel, depth, asXml);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      explicandum = operand;
      break;
    case 1:
      detailLevel = (SqlLiteral) operand;
      break;
    case 2:
      depth = (SqlLiteral) operand;
      break;
    case 3:
      asXml = (SqlLiteral) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /**
   * @return the underlying SQL statement to be explained
   */
  public SqlNode getExplicandum() {
    return explicandum;
  }

  /**
   * @return detail level to be generated
   */
  public SqlExplainLevel getDetailLevel() {
    return detailLevel.symbolValue();
  }

  /**
   * Returns the level of abstraction at which this plan should be displayed.
   */
  public Depth getDepth() {
    return depth.symbolValue();
  }

  /**
   * @return the number of dynamic parameters in the statement
   */
  public int getDynamicParamCount() {
    return dynamicParameterCount;
  }

  /**
   * @return whether physical plan implementation should be returned
   */
  public boolean withImplementation() {
    return getDepth() == Depth.PHYSICAL;
  }

  /**
   * @return whether type should be returned
   */
  public boolean withType() {
    return getDepth() == Depth.TYPE;
  }

  /**
   * Returns whether result is to be in XML format.
   */
  public boolean isXml() {
    return asXml.booleanValue();
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("EXPLAIN PLAN");
    switch (getDetailLevel()) {
    case NO_ATTRIBUTES:
      writer.keyword("EXCLUDING ATTRIBUTES");
      break;
    case EXPPLAN_ATTRIBUTES:
      writer.keyword("INCLUDING ATTRIBUTES");
      break;
    case ALL_ATTRIBUTES:
      writer.keyword("INCLUDING ALL ATTRIBUTES");
      break;
    }
    switch (getDepth()) {
    case TYPE:
      writer.keyword("WITH TYPE");
      break;
    case LOGICAL:
      writer.keyword("WITHOUT IMPLEMENTATION");
      break;
    case PHYSICAL:
      writer.keyword("WITH IMPLEMENTATION");
      break;
    default:
      throw new UnsupportedOperationException();
    }
    if (isXml()) {
      writer.keyword("AS XML");
    }
    writer.keyword("FOR");
    writer.newlineAndIndent();
    explicandum.unparse(
        writer, getOperator().getLeftPrec(), getOperator().getRightPrec());
  }
}

// End SqlExplain.java
