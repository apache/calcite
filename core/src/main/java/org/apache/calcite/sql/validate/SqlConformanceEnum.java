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
 * Enumeration of built-in SQL compatibility modes.
 */
public enum SqlConformanceEnum implements SqlConformance {
  /** Calcite's default SQL behavior. */
  DEFAULT,

  /** Conformance value that allows just about everything. */
  LENIENT,

  /** Conformance value that instructs Calcite to use SQL semantics strictly
   * consistent with the SQL:92 standard. */
  STRICT_92,

  /** Conformance value that instructs Calcite to use SQL semantics strictly
   * consistent with the SQL:99 standard. */
  STRICT_99,

  /** Conformance value that instructs Calcite to use SQL semantics
   * consistent with the SQL:99 standard, but ignoring its more
   * inconvenient or controversial dicta. */
  PRAGMATIC_99,

  /** Conformance value that instructs Calcite to use SQL semantics
   * consistent with MySQL version 5.x. */
  MYSQL_5,

  /** Conformance value that instructs Calcite to use SQL semantics
   * consistent with Oracle version 10. */
  ORACLE_10,

  /** Conformance value that instructs Calcite to use SQL semantics
   * consistent with Oracle version 12.
   *
   * <p>As {@link #ORACLE_10} except for {@link #isApplyAllowed()}. */
  ORACLE_12,

  /** Conformance value that instructs Calcite to use SQL semantics strictly
   * consistent with the SQL:2003 standard. */
  STRICT_2003,

  /** Conformance value that instructs Calcite to use SQL semantics
   * consistent with the SQL:2003 standard, but ignoring its more
   * inconvenient or controversial dicta. */
  PRAGMATIC_2003,

  /** Conformance value that instructs Calcite to use SQL semantics
   * consistent with Microsoft SQL Server version 2008. */
  SQL_SERVER_2008;

  public boolean isGroupByAlias() {
    switch (this) {
    case LENIENT:
    case MYSQL_5:
      return true;
    default:
      return false;
    }
  }

  public boolean isGroupByOrdinal() {
    switch (this) {
    case LENIENT:
    case MYSQL_5:
      return true;
    default:
      return false;
    }
  }

  public boolean isHavingAlias() {
    switch (this) {
    case LENIENT:
    case MYSQL_5:
      return true;
    default:
      return false;
    }
  }

  public boolean isSortByOrdinal() {
    switch (this) {
    case DEFAULT:
    case LENIENT:
    case MYSQL_5:
    case ORACLE_10:
    case ORACLE_12:
    case STRICT_92:
    case PRAGMATIC_99:
    case PRAGMATIC_2003:
    case SQL_SERVER_2008:
      return true;
    default:
      return false;
    }
  }

  public boolean isSortByAlias() {
    switch (this) {
    case DEFAULT:
    case LENIENT:
    case MYSQL_5:
    case ORACLE_10:
    case ORACLE_12:
    case STRICT_92:
    case SQL_SERVER_2008:
      return true;
    default:
      return false;
    }
  }

  public boolean isSortByAliasObscures() {
    return this == SqlConformanceEnum.STRICT_92;
  }

  public boolean isFromRequired() {
    switch (this) {
    case ORACLE_10:
    case ORACLE_12:
    case STRICT_92:
    case STRICT_99:
    case STRICT_2003:
      return true;
    default:
      return false;
    }
  }

  public boolean isBangEqualAllowed() {
    switch (this) {
    case LENIENT:
    case MYSQL_5:
    case ORACLE_10:
    case ORACLE_12:
      return true;
    default:
      return false;
    }
  }

  @Override public boolean isMinusAllowed() {
    switch (this) {
    case LENIENT:
    case ORACLE_10:
    case ORACLE_12:
      return true;
    default:
      return false;
    }
  }

  @Override public boolean isPercentRemainderAllowed() {
    switch (this) {
    case LENIENT:
    case MYSQL_5:
      return true;
    default:
      return false;
    }
  }

  public boolean isApplyAllowed() {
    switch (this) {
    case LENIENT:
    case SQL_SERVER_2008:
    case ORACLE_12:
      return true;
    default:
      return false;
    }
  }

  public boolean isInsertSubsetColumnsAllowed() {
    switch (this) {
    case LENIENT:
    case PRAGMATIC_99:
    case PRAGMATIC_2003:
      return true;
    default:
      return false;
    }
  }

  public boolean allowNiladicParentheses() {
    switch (this) {
    case LENIENT:
    case MYSQL_5:
      return true;
    default:
      return false;
    }
  }

  public boolean allowExtend() {
    switch (this) {
    case LENIENT:
      return true;
    default:
      return false;
    }
  }

  public boolean isLimitStartCountAllowed() {
    switch (this) {
    case LENIENT:
    case MYSQL_5:
      return true;
    default:
      return false;
    }
  }
}

// End SqlConformanceEnum.java
