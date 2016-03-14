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

/**
 * Enumeration of the categories of
 * SQL-invoked routines.
 */
public enum SqlFunctionCategory {
  STRING("STRING", "String function"),
  NUMERIC("NUMERIC", "Numeric function"),
  TIMEDATE("TIMEDATE", "Time and date function"),
  SYSTEM("SYSTEM", "System function"),
  USER_DEFINED_FUNCTION("UDF", "User-defined function"),
  USER_DEFINED_PROCEDURE("UDP", "User-defined procedure"),
  USER_DEFINED_CONSTRUCTOR("UDC", "User-defined constructor"),
  USER_DEFINED_SPECIFIC_FUNCTION("UDF_SPECIFIC",
      "User-defined function with SPECIFIC name"),
  USER_DEFINED_TABLE_FUNCTION("TABLE_UDF", "User-defined table function"),
  USER_DEFINED_TABLE_SPECIFIC_FUNCTION("TABLE_UDF_SPECIFIC",
      "User-defined table function with SPECIFIC name");

  SqlFunctionCategory(String abbrev, String description) {
    Util.discard(abbrev);
    Util.discard(description);
  }

  public final boolean isUserdefined() {
    return isOneOf(
        USER_DEFINED_FUNCTION,
        USER_DEFINED_PROCEDURE,
        USER_DEFINED_CONSTRUCTOR,
        USER_DEFINED_SPECIFIC_FUNCTION,
        USER_DEFINED_TABLE_FUNCTION,
        USER_DEFINED_TABLE_SPECIFIC_FUNCTION);
  }

  public final boolean isTableFunction() {
    return isOneOf(
        USER_DEFINED_TABLE_FUNCTION,
        USER_DEFINED_TABLE_SPECIFIC_FUNCTION);
  }

  public final boolean isSpecific() {
    return isOneOf(
        USER_DEFINED_SPECIFIC_FUNCTION,
        USER_DEFINED_TABLE_SPECIFIC_FUNCTION);
  }

  public final boolean isUnresolvedUserDefinedFunction() {
    return isOneOf(
        USER_DEFINED_FUNCTION,
        USER_DEFINED_TABLE_FUNCTION);
  }

  public final boolean isOneOf(SqlFunctionCategory... categories) {
    for (SqlFunctionCategory category : categories) {
      if (category == this) {
        return true;
      }
    }
    return false;
  }

}

// End SqlFunctionCategory.java
