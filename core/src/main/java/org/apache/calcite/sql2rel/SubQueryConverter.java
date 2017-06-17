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
package org.apache.calcite.sql2rel;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;

/**
 * SubQueryConverter provides the interface for classes that convert sub-queries
 * into equivalent expressions.
 */
public interface SubQueryConverter {
  //~ Methods ----------------------------------------------------------------

  /**
   * @return Whether the sub-query can be converted
   */
  boolean canConvertSubQuery();

  /**
   * Converts the sub-query to an equivalent expression.
   *
   * @param subQuery        the SqlNode tree corresponding to a sub-query
   * @param parentConverter sqlToRelConverter of the parent query
   * @param isExists        whether the sub-query is part of an EXISTS
   *                        expression
   * @param isExplain       whether the sub-query is part of an EXPLAIN PLAN
   *                        statement
   * @return the equivalent expression or null if the sub-query couldn't be
   * converted
   */
  RexNode convertSubQuery(
      SqlCall subQuery,
      SqlToRelConverter parentConverter,
      boolean isExists,
      boolean isExplain);
}

// End SubQueryConverter.java
