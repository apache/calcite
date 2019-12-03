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
package org.apache.calcite.linq4j;

/**
 * Enumeration of join types.
 */
public enum JoinType {
  /**
   * Inner join.
   */
  INNER,

  /**
   * Left-outer join.
   */
  LEFT,

  /**
   * Right-outer join.
   */
  RIGHT,

  /**
   * Full-outer join.
   */
  FULL,

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
   * Anti-join (also known as Anti-semi-join).
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

  /**
   * Returns whether a join of this type may generate NULL values on the
   * right-hand side.
   */
  public boolean generatesNullsOnRight() {
    return (this == LEFT) || (this == FULL);
  }

  /**
   * Returns whether a join of this type may generate NULL values on the
   * left-hand side.
   */
  public boolean generatesNullsOnLeft() {
    return (this == RIGHT) || (this == FULL);
  }

}

// End JoinType.java
