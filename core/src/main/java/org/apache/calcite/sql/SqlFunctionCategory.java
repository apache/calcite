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

import static org.apache.calcite.sql.SqlFunctionCategory.Property.FUNCTION;
import static org.apache.calcite.sql.SqlFunctionCategory.Property.SPECIFIC;
import static org.apache.calcite.sql.SqlFunctionCategory.Property.TABLE_FUNCTION;
import static org.apache.calcite.sql.SqlFunctionCategory.Property.USER_DEFINED;

import java.util.Arrays;
import java.util.EnumSet;

/**
 * Enumeration of the categories of
 * SQL-invoked routines.
 */
public enum SqlFunctionCategory {
  STRING("STRING", "String function", FUNCTION),
  NUMERIC("NUMERIC", "Numeric function", FUNCTION),
  TIMEDATE("TIMEDATE", "Time and date function", FUNCTION),
  SYSTEM("SYSTEM", "System function", FUNCTION),
  USER_DEFINED_FUNCTION("UDF", "User-defined function", USER_DEFINED,
      FUNCTION),
  USER_DEFINED_PROCEDURE("UDP", "User-defined procedure", USER_DEFINED),
  USER_DEFINED_CONSTRUCTOR("UDC", "User-defined constructor", USER_DEFINED),
  USER_DEFINED_SPECIFIC_FUNCTION("UDF_SPECIFIC",
      "User-defined function with SPECIFIC name", USER_DEFINED, SPECIFIC,
      FUNCTION),
  USER_DEFINED_TABLE_FUNCTION("TABLE_UDF", "User-defined table function",
      USER_DEFINED, TABLE_FUNCTION),
  USER_DEFINED_TABLE_SPECIFIC_FUNCTION("TABLE_UDF_SPECIFIC",
      "User-defined table function with SPECIFIC name", USER_DEFINED,
      TABLE_FUNCTION, SPECIFIC),
  MATCH_RECOGNIZE("MATCH_RECOGNIZE", "MATCH_RECOGNIZE function", TABLE_FUNCTION);

  private final EnumSet<Property> properties;

  SqlFunctionCategory(String abbrev, String description,
      Property... properties) {
    Util.discard(abbrev);
    Util.discard(description);
    this.properties = EnumSet.copyOf(Arrays.asList(properties));
  }

  public boolean isUserDefined() {
    return properties.contains(USER_DEFINED);
  }

  public boolean isTableFunction() {
    return properties.contains(TABLE_FUNCTION);
  }

  public boolean isFunction() {
    return properties.contains(FUNCTION);
  }

  public boolean isSpecific() {
    return properties.contains(SPECIFIC);
  }

  public boolean isUserDefinedNotSpecificFunction() {
    return isUserDefined()
        && (isFunction() || isTableFunction())
        && !isSpecific();
  }

  /**
   * Property of a SqlFunctionCategory.
   */
  enum Property {
    USER_DEFINED, TABLE_FUNCTION, SPECIFIC, FUNCTION
  }
}

// End SqlFunctionCategory.java
