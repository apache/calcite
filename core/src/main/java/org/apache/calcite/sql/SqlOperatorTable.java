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

import org.apache.calcite.sql.validate.SqlNameMatcher;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * SqlOperatorTable defines a directory interface for enumerating and looking up
 * SQL operators and functions.
 */
public interface SqlOperatorTable {
  //~ Methods ----------------------------------------------------------------

  /**
   * Retrieves a list of operators with a given name and syntax. For example,
   * by passing SqlSyntax.Function, the returned list is narrowed to only
   * matching SqlFunction objects.
   *
   * @param opName   name of operator
   * @param category function category to look up, or null for any matching
   *                 operator
   * @param syntax   syntax type of operator
   * @param operatorList mutable list to which to append matches
   * @param nameMatcher Name matcher
   */
  void lookupOperatorOverloads(SqlIdentifier opName,
      @Nullable SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList,
      SqlNameMatcher nameMatcher);

  /**
   * Retrieves a list of all functions and operators in this table. Used for
   * automated testing. Depending on the table type, may or may not be mutable.
   *
   * @return list of SqlOperator objects
   */
  List<SqlOperator> getOperatorList();
}
