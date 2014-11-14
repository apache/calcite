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
package org.eigenbase.sql2rel;

import org.eigenbase.rex.*;
import org.eigenbase.sql.*;

/**
 * SubqueryConverter provides the interface for classes that convert subqueries
 * into equivalent expressions.
 */
public interface SubqueryConverter {
  //~ Methods ----------------------------------------------------------------

  /**
   * @return true if the subquery can be converted
   */
  boolean canConvertSubquery();

  /**
   * Converts the subquery to an equivalent expression.
   *
   * @param subquery        the SqlNode tree corresponding to a subquery
   * @param parentConverter sqlToRelConverter of the parent query
   * @param isExists        whether the subquery is part of an EXISTS expression
   * @param isExplain       whether the subquery is part of an EXPLAIN PLAN
   *                        statement
   * @return the equivalent expression or null if the subquery couldn't be
   * converted
   */
  RexNode convertSubquery(
      SqlCall subquery,
      SqlToRelConverter parentConverter,
      boolean isExists,
      boolean isExplain);
}

// End SubqueryConverter.java
