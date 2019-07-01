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

import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Locale;

/**
 * Enumeration representing different join types used in correlation
 * relations.
 *
 * @deprecated Use {@link JoinRelType} instead.
 */
@Deprecated // To be removed before 2.0
public enum SemiJoinType {
  /**
   * Inner join.
   */
  INNER,

  /**
   * Left-outer join.
   */
  LEFT,

  /**
   * Semi-join.
   *
   * <p>For example, {@code EMP semi-join DEPT} finds all {@code EMP} records
   * that have a corresponding {@code DEPT} record:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  SEMI,

  /**
   * Anti-join.
   *
   * <p>For example, {@code EMP anti-join DEPT} finds all {@code EMP} records
   * that do not have a corresponding {@code DEPT} record:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE NOT EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  ANTI;

  /** Lower-case name. */
  public final String lowerName = name().toLowerCase(Locale.ROOT);

  /**
   * Creates a parse-tree node representing an occurrence of this
   * condition type keyword at a particular position in the parsed
   * text.
   */
  public SqlLiteral symbol(SqlParserPos pos) {
    return SqlLiteral.createSymbol(this, pos);
  }

  public static SemiJoinType of(JoinRelType joinType) {
    switch (joinType) {
    case INNER:
      return INNER;
    case LEFT:
      return LEFT;
    }
    throw new IllegalArgumentException(
        "Unsupported join type for semi-join " + joinType);
  }

  public JoinRelType toJoinType() {
    switch (this) {
    case INNER:
      return JoinRelType.INNER;
    case LEFT:
      return JoinRelType.LEFT;
    }
    throw new IllegalStateException(
        "Unable to convert " + this + " to JoinRelType");
  }

  public JoinType toLinq4j() {
    switch (this) {
    case INNER:
      return JoinType.INNER;
    case LEFT:
      return JoinType.LEFT;
    case SEMI:
      return JoinType.SEMI;
    case ANTI:
      return JoinType.ANTI;
    }
    throw new IllegalStateException(
        "Unable to convert " + this + " to JoinRelType");
  }

  public boolean returnsJustFirstInput() {
    switch (this) {
    case INNER:
    case LEFT:
      return false;
    case SEMI:
    case ANTI:
      return true;
    }
    throw new IllegalStateException(
        "Unable to convert " + this + " to JoinRelType");
  }
}

// End SemiJoinType.java
