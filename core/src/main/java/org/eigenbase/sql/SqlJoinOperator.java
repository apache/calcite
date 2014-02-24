/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql;

import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;

/**
 * <code>SqlJoinOperator</code> describes the syntax of the SQL <code>
 * JOIN</code> operator. Since there is only one such operator, this class is
 * almost certainly a singleton.
 */
public class SqlJoinOperator extends SqlOperator {
  //~ Static fields/initializers ---------------------------------------------

  private static final SqlWriter.FrameType FRAME_TYPE =
      SqlWriter.FrameTypeEnum.create("USING");

  //~ Enums ------------------------------------------------------------------

  /**
   * Enumerates the types of condition in a join expression.
   */
  public enum ConditionType implements SqlLiteral.SqlSymbol {
    /**
     * Join clause has no condition, for example "FROM EMP, DEPT"
     */
    NONE,

    /**
     * Join clause has an ON condition, for example "FROM EMP JOIN DEPT ON
     * EMP.DEPTNO = DEPT.DEPTNO"
     */
    ON,

    /**
     * Join clause has a USING condition, for example "FROM EMP JOIN DEPT
     * USING (DEPTNO)"
     */
    USING;

    /**
     * Creates a parse-tree node representing an occurrence of this join
     * type at a particular position in the parsed text.
     */
    public SqlLiteral symbol(SqlParserPos pos) {
      return SqlLiteral.createSymbol(this, pos);
    }
  }

  /**
   * Enumerates the types of join.
   */
  public enum JoinType implements SqlLiteral.SqlSymbol {
    /**
     * Inner join.
     */
    INNER,

    /**
     * Full outer join.
     */
    FULL,

    /**
     * Cross join (also known as Cartesian product).
     */
    CROSS,

    /**
     * Left outer join.
     */
    LEFT,

    /**
     * Right outer join.
     */
    RIGHT,

    /**
     * Comma join: the good old-fashioned SQL <code>FROM</code> clause,
     * where table expressions are specified with commas between them, and
     * join conditions are specified in the <code>WHERE</code> clause.
     */
    COMMA;

    /**
     * Creates a parse-tree node representing an occurrence of this
     * condition type keyword at a particular position in the parsed
     * text.
     */
    public SqlLiteral symbol(SqlParserPos pos) {
      return SqlLiteral.createSymbol(this, pos);
    }
  }

  //~ Constructors -----------------------------------------------------------

  public SqlJoinOperator() {
    super("JOIN", SqlKind.JOIN, 16, true, null, null, null);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  public SqlCall createCall(
      SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNode... operands) {
    assert functionQualifier == null;
    return new SqlJoin(pos, operands[0], (SqlLiteral) operands[1],
        (SqlLiteral) operands[2], operands[3], (SqlLiteral) operands[4],
        operands[5]);
  }

  public SqlCall createCall(
      SqlNode left,
      SqlLiteral isNatural,
      SqlLiteral joinType,
      SqlNode right,
      SqlLiteral conditionType,
      SqlNode condition,
      SqlParserPos pos) {
    return createCall(
        pos,
        left,
        isNatural,
        joinType,
        right,
        conditionType,
        condition);
  }

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlJoin join = (SqlJoin) call;

    join.left.unparse(
        writer,
        leftPrec,
        getLeftPrec());
    String natural = "";
    if (join.isNatural()) {
      natural = "NATURAL ";
    }
    switch (join.getJoinType()) {
    case COMMA:
      writer.sep(",", true);
      break;
    case CROSS:
      writer.sep(natural + "CROSS JOIN");
      break;
    case FULL:
      writer.sep(natural + "FULL JOIN");
      break;
    case INNER:
      writer.sep(natural + "INNER JOIN");
      break;
    case LEFT:
      writer.sep(natural + "LEFT JOIN");
      break;
    case RIGHT:
      writer.sep(natural + "RIGHT JOIN");
      break;
    default:
      throw Util.unexpected(join.getJoinType());
    }
    join.right.unparse(writer, getRightPrec(), rightPrec);
    if (join.condition != null) {
      switch (join.getConditionType()) {
      case USING:
        // No need for an extra pair of parens -- the condition is a
        // list. The result is something like "USING (deptno, gender)".
        writer.keyword("USING");
        assert join.condition instanceof SqlNodeList;
        final SqlWriter.Frame frame =
            writer.startList(FRAME_TYPE, "(", ")");
        join.condition.unparse(writer, 0, 0);
        writer.endList(frame);
        break;

      case ON:
        writer.keyword("ON");
        join.condition.unparse(writer, leftPrec, rightPrec);
        break;

      default:
        throw Util.unexpected(join.getConditionType());
      }
    }
  }
}

// End SqlJoinOperator.java
