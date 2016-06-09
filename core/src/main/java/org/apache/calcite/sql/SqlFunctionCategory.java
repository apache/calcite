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

import org.apache.calcite.util.Util;

import static org.apache.calcite.sql.SqlFunctionCategoryProperty.FUNCTION;
import static org.apache.calcite.sql.SqlFunctionCategoryProperty.SPECIFIC;
import static org.apache.calcite.sql.SqlFunctionCategoryProperty.TABLE_FUNCTION;
import static org.apache.calcite.sql.SqlFunctionCategoryProperty.USER_DEFINED;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Enumeration of the categories of
 * SQL-invoked routines.
 */
public enum SqlFunctionCategory {
  STRING("STRING", "String function", FUNCTION),
  NUMERIC("NUMERIC", "Numeric function", FUNCTION),
  TIMEDATE("TIMEDATE", "Time and date function", FUNCTION),
  SYSTEM("SYSTEM", "System function", FUNCTION),
  USER_DEFINED_FUNCTION("UDF", "User-defined function", USER_DEFINED, FUNCTION),
  USER_DEFINED_PROCEDURE("UDP", "User-defined procedure", USER_DEFINED),
  USER_DEFINED_CONSTRUCTOR("UDC", "User-defined constructor", USER_DEFINED),
  USER_DEFINED_SPECIFIC_FUNCTION(
      "UDF_SPECIFIC", "User-defined function with SPECIFIC name", USER_DEFINED, SPECIFIC, FUNCTION),
  USER_DEFINED_TABLE_FUNCTION(
      "TABLE_UDF", "User-defined table function", USER_DEFINED, TABLE_FUNCTION),
  USER_DEFINED_TABLE_SPECIFIC_FUNCTION("TABLE_UDF_SPECIFIC",
      "User-defined table function with SPECIFIC name", USER_DEFINED, TABLE_FUNCTION, SPECIFIC);

  private final boolean userDefined;
  private final boolean tableFunction;
  private final boolean specific;
  private final boolean function;

  SqlFunctionCategory(String abbrev, String description, SqlFunctionCategoryProperty... types) {
    Util.discard(abbrev);
    Util.discard(description);
    Set<SqlFunctionCategoryProperty> typeSet = new HashSet<>(Arrays.asList(types));
    this.userDefined = typeSet.contains(USER_DEFINED);
    this.tableFunction = typeSet.contains(TABLE_FUNCTION);
    this.specific = typeSet.contains(SPECIFIC);
    this.function = typeSet.contains(FUNCTION);
  }

  public boolean isUserDefined() {
    return userDefined;
  }

  public boolean isTableFunction() {
    return tableFunction;
  }

  public boolean isFunction() {
    return function;
  }

  public boolean isSpecific() {
    return specific;
  }

  public boolean isUserDefinedNotSpecificFunction() {
    return userDefined && (function || tableFunction) && !specific;
  }

}

/**
 * Properties of a SqlFunctionCategory
 */
enum SqlFunctionCategoryProperty {
  USER_DEFINED, TABLE_FUNCTION, SPECIFIC, FUNCTION;
}

// End SqlFunctionCategory.java
