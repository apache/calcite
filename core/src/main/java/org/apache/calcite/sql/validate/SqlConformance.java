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
package org.apache.calcite.sql.validate;

/**
 * Enumeration of valid SQL compatibility modes.
 *
 * <p>For most purposes, one of the built-in compatibility modes in enum
 * {@link SqlConformanceEnum} will suffice.
 *
 * <p>If you wish to implement this interface to build your own conformance,
 * we strongly recommend that you extend {@link SqlAbstractConformance},
 * or use a {@link SqlDelegatingConformance},
 * so that you won't be broken by future changes.
 *
 * @see SqlConformanceEnum
 * @see SqlAbstractConformance
 * @see SqlDelegatingConformance
 */
public interface SqlConformance {
  /** Short-cut for {@link SqlConformanceEnum#DEFAULT}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum DEFAULT = SqlConformanceEnum.DEFAULT;
  /** Short-cut for {@link SqlConformanceEnum#STRICT_92}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum STRICT_92 = SqlConformanceEnum.STRICT_92;
  /** Short-cut for {@link SqlConformanceEnum#STRICT_99}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum STRICT_99 = SqlConformanceEnum.STRICT_99;
  /** Short-cut for {@link SqlConformanceEnum#PRAGMATIC_99}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum PRAGMATIC_99 = SqlConformanceEnum.PRAGMATIC_99;
  /** Short-cut for {@link SqlConformanceEnum#ORACLE_10}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum ORACLE_10 = SqlConformanceEnum.ORACLE_10;
  /** Short-cut for {@link SqlConformanceEnum#STRICT_2003}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum STRICT_2003 = SqlConformanceEnum.STRICT_2003;
  /** Short-cut for {@link SqlConformanceEnum#PRAGMATIC_2003}. */
  @SuppressWarnings("unused")
  @Deprecated // to be removed before 2.0
  SqlConformanceEnum PRAGMATIC_2003 = SqlConformanceEnum.PRAGMATIC_2003;

  /**
   * Whether 'order by 2' is interpreted to mean 'sort by the 2nd column in
   * the select list'.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#DEFAULT},
   * {@link SqlConformanceEnum#ORACLE_10},
   * {@link SqlConformanceEnum#STRICT_92},
   * {@link SqlConformanceEnum#PRAGMATIC_99},
   * {@link SqlConformanceEnum#PRAGMATIC_2003};
   * {@link SqlConformanceEnum#SQL_SERVER_2008};
   * false otherwise.
   */
  boolean isSortByOrdinal();

  /**
   * Whether 'order by x' is interpreted to mean 'sort by the select list item
   * whose alias is x' even if there is a column called x.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#DEFAULT},
   * {@link SqlConformanceEnum#ORACLE_10},
   * {@link SqlConformanceEnum#STRICT_92};
   * {@link SqlConformanceEnum#SQL_SERVER_2008};
   * false otherwise.
   */
  boolean isSortByAlias();

  /**
   * Whether "empno" is invalid in "select empno as x from emp order by empno"
   * because the alias "x" obscures it.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#STRICT_92};
   * false otherwise.
   */
  boolean isSortByAliasObscures();

  /**
   * Whether FROM clause is required in a SELECT statement.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#ORACLE_10},
   * {@link SqlConformanceEnum#STRICT_92},
   * {@link SqlConformanceEnum#STRICT_99},
   * {@link SqlConformanceEnum#STRICT_2003};
   * false otherwise.
   */
  boolean isFromRequired();

  /**
   * Whether the bang-equal token != is allowed as an alternative to &lt;&gt; in
   * the parser.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#ORACLE_10};
   * false otherwise.
   */
  boolean isBangEqualAllowed();

  /**
   * Whether {@code MINUS} is allowed as an alternative to {@code EXCEPT} in
   * the parser.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#ORACLE_10};
   * false otherwise.
   */
  boolean isMinusAllowed();

  /**
   * Whether {@code CROSS APPLY} and {@code OUTER APPLY} is allowed in the parser.
   * The {@code APPLY} allows to invoke a table-valued function for each row returned
   * by an outer table expression of a query.
   *
   * <p>Among the built-in conformance levels, true in
   * {@link SqlConformanceEnum#SQL_SERVER_2008};
   * false otherwise.
   */
  boolean isApplyAllowed();
}

// End SqlConformance.java
