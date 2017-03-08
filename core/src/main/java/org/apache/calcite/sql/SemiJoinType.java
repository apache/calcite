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

import org.apache.calcite.linq4j.CorrelateJoinType;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Locale;

/**
 * Enumeration representing different join types used in correlation
 * relations.
 */
public enum SemiJoinType {
  /**
   * Inner join
   */
  INNER,

  /**
   * Left-outer join
   */
  LEFT,

  /**
   * Semi-join
   * <p>Similar to from A ... where a in (select b from B ...)</p>
   */
  SEMI,

  /**
   * Anti-join
   * <p>Similar to from A ... where a NOT in (select b from B ...)</p>
   * <p>Note: if B.b is nullable and B has nulls, no rows must be returned</p>
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

  public CorrelateJoinType toLinq4j() {
    switch (this) {
    case INNER:
      return CorrelateJoinType.INNER;
    case LEFT:
      return CorrelateJoinType.LEFT;
    case SEMI:
      return CorrelateJoinType.SEMI;
    case ANTI:
      return CorrelateJoinType.ANTI;
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
