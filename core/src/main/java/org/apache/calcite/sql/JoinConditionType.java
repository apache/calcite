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

/**
 * Enumerates the types of condition in a join expression.
 */
public enum JoinConditionType {
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

// End JoinConditionType.java
