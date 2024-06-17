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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * A <code>SqlDialect</code> implementation for the Exasol database.
 */
public class ExasolSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT =
      SqlDialect.EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.EXASOL)
          .withIdentifierQuoteString("\"");

  public static final SqlDialect DEFAULT = new ExasolSqlDialect(DEFAULT_CONTEXT);

  /** An unquoted Exasol identifier must start with a letter and be followed
   * by zero or more letters, digits or _. */
  private static final Pattern IDENTIFIER_REGEX =
      Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

  private static final List<String> RESERVED_KEYWORDS =
      ImmutableList.of("ABS", "ACCESS", "ACOS", "ADAPTER", "ADD_DAYS",
          "ADD_HOURS", "ADD_MINUTES", "ADD_MONTHS", "ADD_SECONDS", "ADD_WEEKS",
          "ADD_YEARS", "ADMIN", "ALIGN", "ALWAYS", "ANALYZE", "ANSI",
          "APPROXIMATE_COUNT_DISTINCT", "ASCII", "ASIN", "ASSIGNMENT",
          "ASYMMETRIC", "ATAN", "ATAN2", "ATOMIC", "ATTEMPTS", "AUDIT",
          "AUTHENTICATED", "AUTO", "AVG", "BACKUP", "BERNOULLI", "BIT_AND",
          "BIT_CHECK", "BIT_LENGTH", "BIT_LROTATE", "BIT_LSHIFT", "BIT_NOT",
          "BIT_OR", "BIT_RROTATE", "BIT_RSHIFT", "BIT_SET", "BIT_TO_NUM",
          "BIT_XOR", "BREADTH", "CEIL", "CEILING", "CHANGE", "CHARACTERS",
          "CHARACTER_LENGTH", "CHR", "CLEAR", "COBOL", "COLOGNE_PHONETIC",
          "COMMENT", "COMMENTS", "COMMITTED", "CONCAT", "CONNECT", "CONVERT_TZ",
          "CORR", "COS", "COSH", "COT", "COUNT", "COVAR_POP", "COVAR_SAMP",
          "CREATED", "CROSS", "CURDATE", "DATABASE", "DATE_TRUNC",
          "DAYS_BETWEEN", "DECODE", "DEFAULTS", "DEFAULT_PRIORITY_GROUP",
          "DEGREES", "DELIMIT", "DELIMITER", "DENSE_RANK", "DEPTH",
          "DIAGNOSTICS", "DICTIONARY", "DISTRIBUTE", "DISTRIBUTION", "DIV",
          "DOWN", "DUMP", "EDIT_DISTANCE", "ENCODING", "ESTIMATE", "EVALUATE",
          "EVERY", "EXA", "EXCLUDE", "EXCLUDING", "EXP", "EXPIRE", "EXPLAIN",
          "EXPRESSION", "FAILED", "FIRST_VALUE", "FLOOR", "FLUSH", "FOREIGN",
          "FORTRAN", "FROM_POSIX_TIME", "GREATEST", "GROUPING_ID", "HANDLER",
          "HAS", "HASH", "HASH_MD5", "HASH_SHA", "HASH_SHA1", "HASH_SHA256",
          "HASH_SHA512", "HASH_TIGER", "HIERARCHY", "HOURS_BETWEEN",
          "IDENTIFIED", "IGNORE", "IMPERSONATION", "INCLUDING", "INITCAP",
          "INITIALLY", "INSTR", "INVALID", "IPROC", "ISOLATION", "IS_BOOLEAN",
          "IS_DATE", "IS_DSINTERVAL", "IS_NUMBER", "IS_TIMESTAMP",
          "IS_YMINTERVAL", "JAVA", "JAVASCRIPT", "KEEP", "KEY", "KEYS", "KILL",
          "LAG", "LANGUAGE", "LAST_VALUE", "LCASE", "LEAD", "LEAST", "LENGTH",
          "LINK", "LN", "LOCATE", "LOCK", "LOG10", "LOG2", "LOGIN", "LOGS",
          "LONG", "LOWER", "LPAD", "LTRIM", "LUA", "MANAGE", "MAX", "MAXIMAL",
          "MEDIAN", "MID", "MIN", "MINUTES_BETWEEN", "MONTHS_BETWEEN", "MUMPS",
          "NEVER", "NICE", "NORMALIZED", "NOW", "NPROC", "NULLIFZERO", "NULLS",
          "NUMTODSINTERVAL", "NUMTOYMINTERVAL", "NVL", "NVL2", "OCTETS",
          "OCTET_LENGTH", "OFFSET", "OPTIMIZE", "ORA", "OWNER", "PADDING",
          "PARTITION", "PASCAL", "PASSWORD", "PASSWORD_EXPIRY_POLICY",
          "PASSWORD_SECURITY_POLICY", "PERCENTILE_CONT", "PERCENTILE_DISC",
          "PI", "PLI", "POSIX_TIME", "POWER", "PRECISION", "PRELOAD", "PRIMARY",
          "PRIORITY", "PRIVILEGE", "PYTHON", "QUERY_CACHE", "QUERY_TIMEOUT",
          "R", "RADIANS", "RAND", "RANK", "RATIO_TO_REPORT", "RAW_SIZE_LIMIT",
          "RECOMPRESS", "RECORD", "REGEXP_INSTR", "REGEXP_REPLACE",
          "REGEXP_SUBSTR", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT",
          "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY",
          "REGR_SYY", "REJECT", "REORGANIZE", "REPEATABLE", "RESET", "REVERSE",
          "ROLE", "ROUND", "ROWID", "ROW_NUMBER", "RPAD", "RTRIM", "SCALAR",
          "SCHEMAS", "SCHEME", "SCRIPT_LANGUAGES", "SCRIPT_OUTPUT_ADDRESS",
          "SECONDS_BETWEEN", "SECURE", "SERIALIZABLE", "SHUT", "SIGN", "SIMPLE",
          "SIN", "SINH", "SIZE", "SKIP", "SOUNDEX", "SQRT", "STATISTICS",
          "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "ST_AREA", "ST_BOUNDARY",
          "ST_BUFFER", "ST_CENTROID", "ST_CONTAINS", "ST_CONVEXHULL",
          "ST_CROSSES", "ST_DIFFERENCE", "ST_DIMENSION", "ST_DISJOINT",
          "ST_DISTANCE", "ST_ENDPOINT", "ST_ENVELOPE", "ST_EQUALS",
          "ST_EXTERIORRING", "ST_FORCE2D", "ST_GEOMETRYN", "ST_GEOMETRYTYPE",
          "ST_INTERIORRINGN", "ST_INTERSECTION", "ST_INTERSECTS", "ST_ISCLOSED",
          "ST_ISEMPTY", "ST_ISRING", "ST_ISSIMPLE", "ST_LENGTH",
          "ST_NUMGEOMETRIES", "ST_NUMINTERIORRINGS", "ST_NUMPOINTS",
          "ST_OVERLAPS", "ST_POINTN", "ST_SETSRID", "ST_STARTPOINT",
          "ST_SYMDIFFERENCE", "ST_TOUCHES", "ST_TRANSFORM", "ST_UNION",
          "ST_WITHIN", "ST_X", "ST_Y", "SUBSTR", "SUM", "SYMMETRIC",
          "SYS_CONNECT_BY_PATH", "SYS_GUID", "TABLES", "TABLESAMPLE", "TAN",
          "TANH", "TASKS", "TIES", "TIMESTAMP_ARITHMETIC_BEHAVIOR", "TIME_ZONE",
          "TIME_ZONE_BEHAVIOR", "TO_CHAR", "TO_DATE", "TO_DSINTERVAL",
          "TO_NUMBER", "TO_TIMESTAMP", "TO_YMINTERVAL", "TRANSLATE", "TRUNC",
          "TYPE", "UCASE", "UNBOUNDED", "UNCOMMITTED", "UNDO", "UNICODE",
          "UNICODECHR", "UNLIMITED", "UPPER", "UTF8", "VALUE2PROC", "VARIANCE",
          "VARYING", "VAR_POP", "VAR_SAMP", "VIRTUAL", "WEEK", "WEIGHT",
          "WRITE", "YEARS_BETWEEN", "ZEROIFNULL");

  /** Creates a ExasolSqlDialect. */
  public ExasolSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public boolean supportsAggregateFunctionFilter() {
    return false;
  }

  @Override public boolean supportsTimestampPrecision() {
    return false;
  }

  @Override public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
    case AVG:
    case COUNT:
    case COVAR_POP:
    case COVAR_SAMP:
    case MAX:
    case MIN:
    case STDDEV_POP:
    case STDDEV_SAMP:
    case SUM:
    case VAR_POP:
    case VAR_SAMP:
      return true;
    default:
      return false;
    }
  }

  @Override protected boolean identifierNeedsQuote(String val) {
    return !IDENTIFIER_REGEX.matcher(val).matches()
        || RESERVED_KEYWORDS.contains(val.toUpperCase(Locale.ROOT));
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
    @Nullable SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
    int leftPrec, int rightPrec) {
    // Same as PostgreSQL implementation
    PostgresqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
  }
}
