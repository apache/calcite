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
package org.apache.calcite.sql.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.test.SqlTests;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.test.DiffTestCase;
import org.apache.calcite.tools.Hoist;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.SourceStringReader;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;

import org.hamcrest.BaseMatcher;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A <code>SqlParserTest</code> is a unit-test for
 * {@link SqlParser the SQL parser}.
 *
 * <p>To reuse this test for an extension parser, implement the
 * {@link #parserImplFactory()} method to return the extension parser
 * implementation.
 */
public class SqlParserTest {
  /**
   * List of reserved keywords.
   *
   * <p>Each keyword is followed by tokens indicating whether it is reserved in
   * the SQL:92, SQL:99, SQL:2003, SQL:2011, SQL:2014 standards and in Calcite.
   *
   * <p>The standard keywords are derived from
   * <a href="https://developer.mimer.com/wp-content/uploads/2018/05/Standard-SQL-Reserved-Words-Summary.pdf">Mimer</a>
   * and from the specification.
   *
   * <p>If a new <b>reserved</b> keyword is added to the parser, include it in
   * this list, flagged "c". If the keyword is not intended to be a reserved
   * keyword, add it to the non-reserved keyword list in the parser.
   */
  private static final List<String> RESERVED_KEYWORDS = ImmutableList.of(
      "ABS",                                               "2011", "2014", "c",
      "ABSOLUTE",                      "92", "99",
      "ACTION",                        "92", "99",
      "ADD",                           "92", "99", "2003",
      "AFTER",                               "99",
      "ALL",                           "92", "99", "2003", "2011", "2014", "c",
      "ALLOCATE",                      "92", "99", "2003", "2011", "2014", "c",
      "ALLOW",                                                             "c",
      "ALTER",                         "92", "99", "2003", "2011", "2014", "c",
      "AND",                           "92", "99", "2003", "2011", "2014", "c",
      "ANY",                           "92", "99", "2003", "2011", "2014", "c",
      "ARE",                           "92", "99", "2003", "2011", "2014", "c",
      "ARRAY",                               "99", "2003", "2011", "2014", "c",
      "ARRAY_AGG",                                         "2011",
      "ARRAY_MAX_CARDINALITY",                                     "2014", "c",
      "AS",                            "92", "99", "2003", "2011", "2014", "c",
      "ASC",                           "92", "99",
      "ASENSITIVE",                          "99", "2003", "2011", "2014", "c",
      "ASSERTION",                     "92", "99",
      "ASYMMETRIC",                          "99", "2003", "2011", "2014", "c",
      "AT",                            "92", "99", "2003", "2011", "2014", "c",
      "ATOMIC",                              "99", "2003", "2011", "2014", "c",
      "AUTHORIZATION",                 "92", "99", "2003", "2011", "2014", "c",
      "AVG",                           "92",               "2011", "2014", "c",
      "BEFORE",                              "99",
      "BEGIN",                         "92", "99", "2003", "2011", "2014", "c",
      "BEGIN_FRAME",                                               "2014", "c",
      "BEGIN_PARTITION",                                           "2014", "c",
      "BETWEEN",                       "92", "99", "2003", "2011", "2014", "c",
      "BIGINT",                                    "2003", "2011", "2014", "c",
      "BINARY",                              "99", "2003", "2011", "2014", "c",
      "BIT",                           "92", "99",                         "c",
      "BIT_LENGTH",                    "92",
      "BLOB",                                "99", "2003", "2011", "2014", "c",
      "BOOLEAN",                             "99", "2003", "2011", "2014", "c",
      "BOTH",                          "92", "99", "2003", "2011", "2014", "c",
      "BREADTH",                             "99",
      "BY",                            "92", "99", "2003", "2011", "2014", "c",
      "CALL",                          "92", "99", "2003", "2011", "2014", "c",
      "CALLED",                                    "2003", "2011", "2014", "c",
      "CARDINALITY",                                       "2011", "2014", "c",
      "CASCADE",                       "92", "99",
      "CASCADED",                      "92", "99", "2003", "2011", "2014", "c",
      "CASE",                          "92", "99", "2003", "2011", "2014", "c",
      "CAST",                          "92", "99", "2003", "2011", "2014", "c",
      "CATALOG",                       "92", "99",
      "CEIL",                                              "2011", "2014", "c",
      "CEILING",                                           "2011", "2014", "c",
      "CHAR",                          "92", "99", "2003", "2011", "2014", "c",
      "CHARACTER",                     "92", "99", "2003", "2011", "2014", "c",
      "CHARACTER_LENGTH",              "92",               "2011", "2014", "c",
      "CHAR_LENGTH",                   "92",               "2011", "2014", "c",
      "CHECK",                         "92", "99", "2003", "2011", "2014", "c",
      "CLASSIFIER",                                                "2014", "c",
      "CLOB",                                "99", "2003", "2011", "2014", "c",
      "CLOSE",                         "92", "99", "2003", "2011", "2014", "c",
      "COALESCE",                      "92",               "2011", "2014", "c",
      "COLLATE",                       "92", "99", "2003", "2011", "2014", "c",
      "COLLATION",                     "92", "99",
      "COLLECT",                                           "2011", "2014", "c",
      "COLUMN",                        "92", "99", "2003", "2011", "2014", "c",
      "COMMIT",                        "92", "99", "2003", "2011", "2014", "c",
      "CONDITION",                     "92", "99", "2003", "2011", "2014", "c",
      "CONNECT",                       "92", "99", "2003", "2011", "2014", "c",
      "CONNECTION",                    "92", "99",
      "CONSTRAINT",                    "92", "99", "2003", "2011", "2014", "c",
      "CONSTRAINTS",                   "92", "99",
      "CONSTRUCTOR",                         "99",
      "CONTAINS",                      "92",               "2011", "2014", "c",
      "CONTINUE",                      "92", "99", "2003",
      "CONVERT",                       "92",               "2011", "2014", "c",
      "CORR",                                              "2011", "2014", "c",
      "CORRESPONDING",                 "92", "99", "2003", "2011", "2014", "c",
      "COUNT",                         "92",               "2011", "2014", "c",
      "COVAR_POP",                                         "2011", "2014", "c",
      "COVAR_SAMP",                                        "2011", "2014", "c",
      "CREATE",                        "92", "99", "2003", "2011", "2014", "c",
      "CROSS",                         "92", "99", "2003", "2011", "2014", "c",
      "CUBE",                                "99", "2003", "2011", "2014", "c",
      "CUME_DIST",                                         "2011", "2014", "c",
      "CURRENT",                       "92", "99", "2003", "2011", "2014", "c",
      "CURRENT_CATALOG",                                   "2011", "2014", "c",
      "CURRENT_DATE",                  "92", "99", "2003", "2011", "2014", "c",
      "CURRENT_DEFAULT_TRANSFORM_GROUP",     "99", "2003", "2011", "2014", "c",
      "CURRENT_PATH",                  "92", "99", "2003", "2011", "2014", "c",
      "CURRENT_ROLE",                        "99", "2003", "2011", "2014", "c",
      "CURRENT_ROW",                                               "2014", "c",
      "CURRENT_SCHEMA",                                    "2011", "2014", "c",
      "CURRENT_TIME",                  "92", "99", "2003", "2011", "2014", "c",
      "CURRENT_TIMESTAMP",             "92", "99", "2003", "2011", "2014", "c",
      "CURRENT_TRANSFORM_GROUP_FOR_TYPE",    "99", "2003", "2011", "2014", "c",
      "CURRENT_USER",                  "92", "99", "2003", "2011", "2014", "c",
      "CURSOR",                        "92", "99", "2003", "2011", "2014", "c",
      "CYCLE",                               "99", "2003", "2011", "2014", "c",
      "DATA",                                "99",
      "DATE",                          "92", "99", "2003", "2011", "2014", "c",
      "DAY",                           "92", "99", "2003", "2011", "2014", "c",
      "DAYS",                                              "2011",
      "DEALLOCATE",                    "92", "99", "2003", "2011", "2014", "c",
      "DEC",                           "92", "99", "2003", "2011", "2014", "c",
      "DECIMAL",                       "92", "99", "2003", "2011", "2014", "c",
      "DECLARE",                       "92", "99", "2003", "2011", "2014", "c",
      "DEFAULT",                       "92", "99", "2003", "2011", "2014", "c",
      "DEFERRABLE",                    "92", "99",
      "DEFERRED",                      "92", "99",
      "DEFINE",                                                    "2014", "c",
      "DELETE",                        "92", "99", "2003", "2011", "2014", "c",
      "DENSE_RANK",                                        "2011", "2014", "c",
      "DEPTH",                               "99",
      "DEREF",                               "99", "2003", "2011", "2014", "c",
      "DESC",                          "92", "99",
      "DESCRIBE",                      "92", "99", "2003", "2011", "2014", "c",
      "DESCRIPTOR",                    "92", "99",
      "DETERMINISTIC",                 "92", "99", "2003", "2011", "2014", "c",
      "DIAGNOSTICS",                   "92", "99",
      "DISALLOW",                                                          "c",
      "DISCONNECT",                    "92", "99", "2003", "2011", "2014", "c",
      "DISTINCT",                      "92", "99", "2003", "2011", "2014", "c",
      "DO",                            "92", "99", "2003",
      "DOMAIN",                        "92", "99",
      "DOUBLE",                        "92", "99", "2003", "2011", "2014", "c",
      "DROP",                          "92", "99", "2003", "2011", "2014", "c",
      "DYNAMIC",                             "99", "2003", "2011", "2014", "c",
      "EACH",                                "99", "2003", "2011", "2014", "c",
      "ELEMENT",                                   "2003", "2011", "2014", "c",
      "ELSE",                          "92", "99", "2003", "2011", "2014", "c",
      "ELSEIF",                        "92", "99", "2003",
      "EMPTY",                                                     "2014", "c",
      "END",                           "92", "99", "2003", "2011", "2014", "c",
      "END-EXEC",                                          "2011", "2014", "c",
      "END_FRAME",                                                 "2014", "c",
      "END_PARTITION",                                             "2014", "c",
      "EQUALS",                              "99",                 "2014", "c",
      "ESCAPE",                        "92", "99", "2003", "2011", "2014", "c",
      "EVERY",                                             "2011", "2014", "c",
      "EXCEPT",                        "92", "99", "2003", "2011", "2014", "c",
      "EXCEPTION",                     "92", "99",
      "EXEC",                          "92", "99", "2003", "2011", "2014", "c",
      "EXECUTE",                       "92", "99", "2003", "2011", "2014", "c",
      "EXISTS",                        "92", "99", "2003", "2011", "2014", "c",
      "EXIT",                          "92", "99", "2003",
      "EXP",                                               "2011", "2014", "c",
      "EXPLAIN",                                                           "c",
      "EXTEND",                                                            "c",
      "EXTERNAL",                      "92", "99", "2003", "2011", "2014", "c",
      "EXTRACT",                       "92",               "2011", "2014", "c",
      "FALSE",                         "92", "99", "2003", "2011", "2014", "c",
      "FETCH",                         "92", "99", "2003", "2011", "2014", "c",
      "FILTER",                              "99", "2003", "2011", "2014", "c",
      "FIRST",                         "92", "99",
      "FIRST_VALUE",                                       "2011", "2014", "c",
      "FLOAT",                         "92", "99", "2003", "2011", "2014", "c",
      "FLOOR",                                             "2011", "2014", "c",
      "FOR",                           "92", "99", "2003", "2011", "2014", "c",
      "FOREIGN",                       "92", "99", "2003", "2011", "2014", "c",
      "FOREVER",                                           "2011",
      "FOUND",                         "92", "99",
      "FRAME_ROW",                                                 "2014", "c",
      "FREE",                                "99", "2003", "2011", "2014", "c",
      "FROM",                          "92", "99", "2003", "2011", "2014", "c",
      "FULL",                          "92", "99", "2003", "2011", "2014", "c",
      "FUNCTION",                      "92", "99", "2003", "2011", "2014", "c",
      "FUSION",                                            "2011", "2014", "c",
      "GENERAL",                             "99",
      "GET",                           "92", "99", "2003", "2011", "2014", "c",
      "GLOBAL",                        "92", "99", "2003", "2011", "2014", "c",
      "GO",                            "92", "99",
      "GOTO",                          "92", "99",
      "GRANT",                         "92", "99", "2003", "2011", "2014", "c",
      "GROUP",                         "92", "99", "2003", "2011", "2014", "c",
      "GROUPING",                            "99", "2003", "2011", "2014", "c",
      "GROUPS",                                                    "2014", "c",
      "HANDLER",                       "92", "99", "2003",
      "HAVING",                        "92", "99", "2003", "2011", "2014", "c",
      "HOLD",                                "99", "2003", "2011", "2014", "c",
      "HOUR",                          "92", "99", "2003", "2011", "2014", "c",
      "HOURS",                                             "2011",
      "IDENTITY",                      "92", "99", "2003", "2011", "2014", "c",
      "IF",                            "92", "99", "2003",
      "ILIKE", // PostgreSQL
      "IMMEDIATE",                     "92", "99", "2003",
      "IMMEDIATELY",
      "IMPORT",                                                            "c",
      "IN",                            "92", "99", "2003", "2011", "2014", "c",
      "INDICATOR",                     "92", "99", "2003", "2011", "2014", "c",
      "INITIAL",                                                   "2014", "c",
      "INITIALLY",                     "92", "99",
      "INNER",                         "92", "99", "2003", "2011", "2014", "c",
      "INOUT",                         "92", "99", "2003", "2011", "2014", "c",
      "INPUT",                         "92", "99", "2003",
      "INSENSITIVE",                   "92", "99", "2003", "2011", "2014", "c",
      "INSERT",                        "92", "99", "2003", "2011", "2014", "c",
      "INT",                           "92", "99", "2003", "2011", "2014", "c",
      "INTEGER",                       "92", "99", "2003", "2011", "2014", "c",
      "INTERSECT",                     "92", "99", "2003", "2011", "2014", "c",
      "INTERSECTION",                                      "2011", "2014", "c",
      "INTERVAL",                      "92", "99", "2003", "2011", "2014", "c",
      "INTO",                          "92", "99", "2003", "2011", "2014", "c",
      "IS",                            "92", "99", "2003", "2011", "2014", "c",
      "ISOLATION",                     "92", "99",
      "ITERATE",                             "99", "2003",
      "JOIN",                          "92", "99", "2003", "2011", "2014", "c",
      "JSON_ARRAY",                                                        "c",
      "JSON_ARRAYAGG",                                                     "c",
      "JSON_EXISTS",                                                       "c",
      "JSON_OBJECT",                                                       "c",
      "JSON_OBJECTAGG",                                                    "c",
      "JSON_QUERY",                                                        "c",
      "JSON_VALUE",                                                        "c",
      "KEEP",                                              "2011",
      "KEY",                           "92", "99",
      "LAG",                                               "2011", "2014", "c",
      "LANGUAGE",                      "92", "99", "2003", "2011", "2014", "c",
      "LARGE",                               "99", "2003", "2011", "2014", "c",
      "LAST",                          "92", "99",
      "LAST_VALUE",                                        "2011", "2014", "c",
      "LATERAL",                             "99", "2003", "2011", "2014", "c",
      "LEAD",                                              "2011", "2014", "c",
      "LEADING",                       "92", "99", "2003", "2011", "2014", "c",
      "LEAVE",                         "92", "99", "2003",
      "LEFT",                          "92", "99", "2003", "2011", "2014", "c",
      "LEVEL",                         "92", "99",
      "LIKE",                          "92", "99", "2003", "2011", "2014", "c",
      "LIKE_REGEX",                                        "2011", "2014", "c",
      "LIMIT",                                                             "c",
      "LN",                                                "2011", "2014", "c",
      "LOCAL",                         "92", "99", "2003", "2011", "2014", "c",
      "LOCALTIME",                           "99", "2003", "2011", "2014", "c",
      "LOCALTIMESTAMP",                      "99", "2003", "2011", "2014", "c",
      "LOCATOR",                             "99",
      "LOOP",                          "92", "99", "2003",
      "LOWER",                         "92",               "2011", "2014", "c",
      "MAP",                                 "99",
      "MATCH",                         "92", "99", "2003", "2011", "2014", "c",
      "MATCHES",                                                   "2014", "c",
      "MATCH_NUMBER",                                              "2014", "c",
      "MATCH_RECOGNIZE",                                           "2014", "c",
      "MAX",                           "92",               "2011", "2014", "c",
      "MAX_CARDINALITY",                                   "2011",
      "MEASURES",                                                          "c",
      "MEMBER",                                    "2003", "2011", "2014", "c",
      "MERGE",                                     "2003", "2011", "2014", "c",
      "METHOD",                              "99", "2003", "2011", "2014", "c",
      "MIN",                           "92",               "2011", "2014", "c",
      "MINUS",                                                             "c",
      "MINUTE",                        "92", "99", "2003", "2011", "2014", "c",
      "MINUTES",                                           "2011",
      "MOD",                                               "2011", "2014", "c",
      "MODIFIES",                            "99", "2003", "2011", "2014", "c",
      "MODULE",                        "92", "99", "2003", "2011", "2014", "c",
      "MONTH",                         "92", "99", "2003", "2011", "2014", "c",
      "MULTISET",                                  "2003", "2011", "2014", "c",
      "NAMES",                         "92", "99",
      "NATIONAL",                      "92", "99", "2003", "2011", "2014", "c",
      "NATURAL",                       "92", "99", "2003", "2011", "2014", "c",
      "NCHAR",                         "92", "99", "2003", "2011", "2014", "c",
      "NCLOB",                               "99", "2003", "2011", "2014", "c",
      "NEW",                                 "99", "2003", "2011", "2014", "c",
      "NEXT",                          "92", "99",                         "c",
      "NO",                            "92", "99", "2003", "2011", "2014", "c",
      "NONE",                                "99", "2003", "2011", "2014", "c",
      "NORMALIZE",                                         "2011", "2014", "c",
      "NOT",                           "92", "99", "2003", "2011", "2014", "c",
      "NTH_VALUE",                                         "2011", "2014", "c",
      "NTILE",                                             "2011", "2014", "c",
      "NULL",                          "92", "99", "2003", "2011", "2014", "c",
      "NULLIF",                        "92",               "2011", "2014", "c",
      "NUMERIC",                       "92", "99", "2003", "2011", "2014", "c",
      "OBJECT",                              "99",
      "OCCURRENCES_REGEX",                                 "2011", "2014", "c",
      "OCTET_LENGTH",                  "92",               "2011", "2014", "c",
      "OF",                            "92", "99", "2003", "2011", "2014", "c",
      "OFFSET",                                            "2011", "2014", "c",
      "OLD",                                 "99", "2003", "2011", "2014", "c",
      "OMIT",                                                      "2014", "c",
      "ON",                            "92", "99", "2003", "2011", "2014", "c",
      "ONE",                                                       "2014", "c",
      "ONLY",                          "92", "99", "2003", "2011", "2014", "c",
      "OPEN",                          "92", "99", "2003", "2011", "2014", "c",
      "OPTION",                        "92", "99",
      "OR",                            "92", "99", "2003", "2011", "2014", "c",
      "ORDER",                         "92", "99", "2003", "2011", "2014", "c",
      "ORDINALITY",                          "99",
      "OUT",                           "92", "99", "2003", "2011", "2014", "c",
      "OUTER",                         "92", "99", "2003", "2011", "2014", "c",
      "OUTPUT",                        "92", "99", "2003",
      "OVER",                                "99", "2003", "2011", "2014", "c",
      "OVERLAPS",                      "92", "99", "2003", "2011", "2014", "c",
      "OVERLAY",                                           "2011", "2014", "c",
      "PAD",                           "92", "99",
      "PARAMETER",                     "92", "99", "2003", "2011", "2014", "c",
      "PARTIAL",                       "92", "99",
      "PARTITION",                           "99", "2003", "2011", "2014", "c",
      "PATH",                          "92", "99",
      "PATTERN",                                                   "2014", "c",
      "PER",                                                       "2014", "c",
      "PERCENT",                                                   "2014", "c",
      "PERCENTILE_CONT",                                   "2011", "2014", "c",
      "PERCENTILE_DISC",                                   "2011", "2014", "c",
      "PERCENT_RANK",                                      "2011", "2014", "c",
      "PERIOD",                                                    "2014", "c",
      "PERMUTE",                                                           "c",
      "PORTION",                                                   "2014", "c",
      "POSITION",                      "92",               "2011", "2014", "c",
      "POSITION_REGEX",                                    "2011", "2014", "c",
      "POWER",                                             "2011", "2014", "c",
      "PRECEDES",                                                  "2014", "c",
      "PRECISION",                     "92", "99", "2003", "2011", "2014", "c",
      "PREPARE",                       "92", "99", "2003", "2011", "2014", "c",
      "PRESERVE",                      "92", "99",
      "PREV",                                                              "c",
      "PRIMARY",                       "92", "99", "2003", "2011", "2014", "c",
      "PRIOR",                         "92", "99",
      "PRIVILEGES",                    "92", "99",
      "PROCEDURE",                     "92", "99", "2003", "2011", "2014", "c",
      "PUBLIC",                        "92", "99",
      "RANGE",                               "99", "2003", "2011", "2014", "c",
      "RANK",                                              "2011", "2014", "c",
      "READ",                          "92", "99",
      "READS",                               "99", "2003", "2011", "2014", "c",
      "REAL",                          "92", "99", "2003", "2011", "2014", "c",
      "RECURSIVE",                           "99", "2003", "2011", "2014", "c",
      "REF",                                 "99", "2003", "2011", "2014", "c",
      "REFERENCES",                    "92", "99", "2003", "2011", "2014", "c",
      "REFERENCING",                         "99", "2003", "2011", "2014", "c",
      "REGR_AVGX",                                         "2011", "2014", "c",
      "REGR_AVGY",                                         "2011", "2014", "c",
      "REGR_COUNT",                                        "2011", "2014", "c",
      "REGR_INTERCEPT",                                    "2011", "2014", "c",
      "REGR_R2",                                           "2011", "2014", "c",
      "REGR_SLOPE",                                        "2011", "2014", "c",
      "REGR_SXX",                                          "2011", "2014", "c",
      "REGR_SXY",                                          "2011", "2014", "c",
      "REGR_SYY",                                          "2011", "2014", "c",
      "RELATIVE",                      "92", "99",
      "RELEASE",                             "99", "2003", "2011", "2014", "c",
      "REPEAT",                        "92", "99", "2003",
      "RESET",                                                             "c",
      "RESIGNAL",                      "92", "99", "2003",
      "RESTRICT",                      "92", "99",
      "RESULT",                              "99", "2003", "2011", "2014", "c",
      "RETURN",                        "92", "99", "2003", "2011", "2014", "c",
      "RETURNS",                       "92", "99", "2003", "2011", "2014", "c",
      "REVOKE",                        "92", "99", "2003", "2011", "2014", "c",
      "RIGHT",                         "92", "99", "2003", "2011", "2014", "c",
      "RLIKE", // Hive and Spark
      "ROLE",                                "99",
      "ROLLBACK",                      "92", "99", "2003", "2011", "2014", "c",
      "ROLLUP",                              "99", "2003", "2011", "2014", "c",
      "ROUTINE",                       "92", "99",
      "ROW",                                 "99", "2003", "2011", "2014", "c",
      "ROWS",                          "92", "99", "2003", "2011", "2014", "c",
      "ROW_NUMBER",                                        "2011", "2014", "c",
      "RUNNING",                                                   "2014", "c",
      "SAVEPOINT",                           "99", "2003", "2011", "2014", "c",
      "SCHEMA",                        "92", "99",
      "SCOPE",                               "99", "2003", "2011", "2014", "c",
      "SCROLL",                        "92", "99", "2003", "2011", "2014", "c",
      "SEARCH",                              "99", "2003", "2011", "2014", "c",
      "SECOND",                        "92", "99", "2003", "2011", "2014", "c",
      "SECONDS",                                           "2011",
      "SECTION",                       "92", "99",
      "SEEK",                                                      "2014", "c",
      "SELECT",                        "92", "99", "2003", "2011", "2014", "c",
      "SENSITIVE",                           "99", "2003", "2011", "2014", "c",
      "SESSION",                       "92", "99",
      "SESSION_USER",                  "92", "99", "2003", "2011", "2014", "c",
      "SET",                           "92", "99", "2003", "2011", "2014", "c",
      "SETS",                                "99",
      "SHOW",                                                      "2014", "c",
      "SIGNAL",                        "92", "99", "2003",
      "SIMILAR",                             "99", "2003", "2011", "2014", "c",
      "SIZE",                          "92", "99",
      "SKIP",                                                      "2014", "c",
      "SMALLINT",                      "92", "99", "2003", "2011", "2014", "c",
      "SOME",                          "92", "99", "2003", "2011", "2014", "c",
      "SPACE",                         "92", "99",
      "SPECIFIC",                      "92", "99", "2003", "2011", "2014", "c",
      "SPECIFICTYPE",                        "99", "2003", "2011", "2014", "c",
      "SQL",                           "92", "99", "2003", "2011", "2014", "c",
      "SQLCODE",                       "92",
      "SQLERROR",                      "92",
      "SQLEXCEPTION",                  "92", "99", "2003", "2011", "2014", "c",
      "SQLSTATE",                      "92", "99", "2003", "2011", "2014", "c",
      "SQLWARNING",                    "92", "99", "2003", "2011", "2014", "c",
      "SQRT",                                              "2011", "2014", "c",
      "START",                               "99", "2003", "2011", "2014", "c",
      "STATE",                               "99",
      "STATIC",                              "99", "2003", "2011", "2014", "c",
      "STDDEV_POP",                                        "2011", "2014", "c",
      "STDDEV_SAMP",                                       "2011", "2014", "c",
      "STREAM",                                                            "c",
      "SUBMULTISET",                               "2003", "2011", "2014", "c",
      "SUBSET",                                                    "2014", "c",
      "SUBSTRING",                     "92",               "2011", "2014", "c",
      "SUBSTRING_REGEX",                                   "2011", "2014", "c",
      "SUCCEEDS",                                                  "2014", "c",
      "SUM",                           "92",               "2011", "2014", "c",
      "SYMMETRIC",                           "99", "2003", "2011", "2014", "c",
      "SYSTEM",                              "99", "2003", "2011", "2014", "c",
      "SYSTEM_TIME",                                               "2014", "c",
      "SYSTEM_USER",                   "92", "99", "2003", "2011", "2014", "c",
      "TABLE",                         "92", "99", "2003", "2011", "2014", "c",
      "TABLESAMPLE",                               "2003", "2011", "2014", "c",
      "TEMPORARY",                     "92", "99",
      "THEN",                          "92", "99", "2003", "2011", "2014", "c",
      "TIME",                          "92", "99", "2003", "2011", "2014", "c",
      "TIMESTAMP",                     "92", "99", "2003", "2011", "2014", "c",
      "TIMEZONE_HOUR",                 "92", "99", "2003", "2011", "2014", "c",
      "TIMEZONE_MINUTE",               "92", "99", "2003", "2011", "2014", "c",
      "TINYINT",                                                           "c",
      "TO",                            "92", "99", "2003", "2011", "2014", "c",
      "TRAILING",                      "92", "99", "2003", "2011", "2014", "c",
      "TRANSACTION",                   "92", "99",
      "TRANSLATE",                     "92",               "2011", "2014", "c",
      "TRANSLATE_REGEX",                                   "2011", "2014", "c",
      "TRANSLATION",                   "92", "99", "2003", "2011", "2014", "c",
      "TREAT",                               "99", "2003", "2011", "2014", "c",
      "TRIGGER",                             "99", "2003", "2011", "2014", "c",
      "TRIM",                          "92",               "2011", "2014", "c",
      "TRIM_ARRAY",                                        "2011", "2014", "c",
      "TRUE",                          "92", "99", "2003", "2011", "2014", "c",
      "TRUNCATE",                                          "2011", "2014", "c",
      "UESCAPE",                                           "2011", "2014", "c",
      "UNDER",                               "99",
      "UNDO",                          "92", "99", "2003",
      "UNION",                         "92", "99", "2003", "2011", "2014", "c",
      "UNIQUE",                        "92", "99", "2003", "2011", "2014", "c",
      "UNKNOWN",                       "92", "99", "2003", "2011", "2014", "c",
      "UNNEST",                              "99", "2003", "2011", "2014", "c",
      "UNTIL",                         "92", "99", "2003",
      "UPDATE",                        "92", "99", "2003", "2011", "2014", "c",
      "UPPER",                         "92",               "2011", "2014", "c",
      "UPSERT",                                                            "c",
      "USAGE",                         "92", "99",
      "USER",                          "92", "99", "2003", "2011", "2014", "c",
      "USING",                         "92", "99", "2003", "2011", "2014", "c",
      "VALUE",                         "92", "99", "2003", "2011", "2014", "c",
      "VALUES",                        "92", "99", "2003", "2011", "2014", "c",
      "VALUE_OF",                                                  "2014", "c",
      "VARBINARY",                                         "2011", "2014", "c",
      "VARCHAR",                       "92", "99", "2003", "2011", "2014", "c",
      "VARYING",                       "92", "99", "2003", "2011", "2014", "c",
      "VAR_POP",                                           "2011", "2014", "c",
      "VAR_SAMP",                                          "2011", "2014", "c",
      "VERSION",                                           "2011",
      "VERSIONING",                                        "2011", "2014", "c",
      "VERSIONS",                                          "2011",
      "VIEW",                          "92", "99",
      "WHEN",                          "92", "99", "2003", "2011", "2014", "c",
      "WHENEVER",                      "92", "99", "2003", "2011", "2014", "c",
      "WHERE",                         "92", "99", "2003", "2011", "2014", "c",
      "WHILE",                         "92", "99", "2003",
      "WIDTH_BUCKET",                                      "2011", "2014", "c",
      "WINDOW",                              "99", "2003", "2011", "2014", "c",
      "WITH",                          "92", "99", "2003", "2011", "2014", "c",
      "WITHIN",                              "99", "2003", "2011", "2014", "c",
      "WITHOUT",                             "99", "2003", "2011", "2014", "c",
      "WORK",                          "92", "99",
      "WRITE",                         "92", "99",
      "YEAR",                          "92", "99", "2003", "2011", "2014", "c",
      "YEARS",                                             "2011",
      "ZONE",                          "92", "99");

  private static final String ANY = "(?s).*";

  private static final ThreadLocal<boolean[]> LINUXIFY =
      ThreadLocal.withInitial(() -> new boolean[] {true});

  private static final SqlWriterConfig SQL_WRITER_CONFIG =
      SqlPrettyWriter.config()
          .withAlwaysUseParentheses(true)
          .withUpdateSetListNewline(false)
          .withFromFolding(SqlWriterConfig.LineFolding.TALL)
          .withIndentation(0);

  private static final SqlDialect BIG_QUERY =
      SqlDialect.DatabaseProduct.BIG_QUERY.getDialect();
  private static final SqlDialect CALCITE =
      SqlDialect.DatabaseProduct.CALCITE.getDialect();
  private static final SqlDialect MSSQL =
      SqlDialect.DatabaseProduct.MSSQL.getDialect();
  private static final SqlDialect MYSQL =
      SqlDialect.DatabaseProduct.MYSQL.getDialect();
  private static final SqlDialect ORACLE =
      SqlDialect.DatabaseProduct.ORACLE.getDialect();
  private static final SqlDialect POSTGRESQL =
      SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();
  private static final SqlDialect REDSHIFT =
      SqlDialect.DatabaseProduct.REDSHIFT.getDialect();

  Quoting quoting = Quoting.DOUBLE_QUOTE;
  Casing unquotedCasing = Casing.TO_UPPER;
  Casing quotedCasing = Casing.UNCHANGED;
  SqlConformance conformance = SqlConformanceEnum.DEFAULT;

  protected Tester getTester() {
    return new TesterImpl();
  }

  protected Sql sql(String sql) {
    return new Sql(StringAndPos.of(sql), false, null, parser -> { });
  }

  protected Sql expr(String sql) {
    return new Sql(StringAndPos.of(sql), true, null, parser -> { });
  }

  /** Creates an instance of helper class {@link SqlList} to test parsing a
   * list of statements. */
  protected SqlList sqlList(String sql) {
    return new SqlList(sql);
  }

  /**
   * Implementors of custom parsing logic who want to reuse this test should
   * override this method with the factory for their extension parser.
   */
  protected SqlParserImplFactory parserImplFactory() {
    return SqlParserImpl.FACTORY;
  }

  public SqlParser getSqlParser(String sql) {
    return getSqlParser(new SourceStringReader(sql), UnaryOperator.identity());
  }

  protected SqlParser getSqlParser(Reader source,
      UnaryOperator<SqlParser.Config> transform) {
    final SqlParser.Config configBuilder =
        SqlParser.config()
            .withParserFactory(parserImplFactory())
            .withQuoting(quoting)
            .withUnquotedCasing(unquotedCasing)
            .withQuotedCasing(quotedCasing)
            .withConformance(conformance);
    final SqlParser.Config config = transform.apply(configBuilder);
    return SqlParser.create(source, config);
  }

  private static UnaryOperator<SqlParser.Config> getTransform(
      SqlDialect dialect) {
    return dialect == null ? UnaryOperator.identity()
        : dialect::configureParser;
  }

  /** Returns a {@link Matcher} that succeeds if the given {@link SqlNode} is a
   * DDL statement. */
  public static Matcher<SqlNode> isDdl() {
    return new BaseMatcher<SqlNode>() {
      public boolean matches(Object item) {
        return item instanceof SqlNode
            && SqlKind.DDL.contains(((SqlNode) item).getKind());
      }

      public void describeTo(Description description) {
        description.appendText("isDdl");
      }
    };
  }

  /** Returns a {@link Matcher} that succeeds if the given {@link SqlNode} is a
   * VALUES that contains a ROW that contains an identifier whose {@code i}th
   * element is quoted. */
  private static Matcher<SqlNode> isQuoted(final int i,
      final boolean quoted) {
    return new CustomTypeSafeMatcher<SqlNode>("quoting") {
      protected boolean matchesSafely(SqlNode item) {
        final SqlCall valuesCall = (SqlCall) item;
        final SqlCall rowCall = valuesCall.operand(0);
        final SqlIdentifier id = rowCall.operand(0);
        return id.isComponentQuoted(i) == quoted;
      }
    };
  }

  protected SortedSet<String> getReservedKeywords() {
    return keywords("c");
  }

  /** Returns whether a word is reserved in this parser. This method can be
   * used to disable tests that behave differently with different collections
   * of reserved words. */
  protected boolean isReserved(String word) {
    SqlAbstractParserImpl.Metadata metadata = getSqlParser("").getMetadata();
    return metadata.isReservedWord(word.toUpperCase(Locale.ROOT));
  }

  protected static SortedSet<String> keywords(String dialect) {
    final ImmutableSortedSet.Builder<String> builder =
        ImmutableSortedSet.naturalOrder();
    String r = null;
    for (String w : RESERVED_KEYWORDS) {
      switch (w) {
      case "92":
      case "99":
      case "2003":
      case "2011":
      case "2014":
      case "c":
        assert r != null;
        if (dialect == null || dialect.equals(w)) {
          builder.add(r);
        }
        break;
      default:
        assert r == null || r.compareTo(w) < 0 : "table should be sorted: " + w;
        r = w;
      }
    }
    return builder.build();
  }

  /**
   * Tests that when there is an error, non-reserved keywords such as "A",
   * "ABSOLUTE" (which naturally arise whenever a production uses
   * "&lt;IDENTIFIER&gt;") are removed, but reserved words such as "AND"
   * remain.
   */
  @Test void testExceptionCleanup() {
    sql("select 0.5e1^.1^ from sales.emps")
        .fails("(?s).*Encountered \".1\" at line 1, column 13.\n"
            + "Was expecting one of:\n"
            + "    <EOF> \n"
            + "    \"AS\" \\.\\.\\.\n"
            + "    \"EXCEPT\" \\.\\.\\.\n"
            + ".*");
  }

  @Test void testInvalidToken() {
    // Causes problems to the test infrastructure because the token mgr
    // throws a java.lang.Error. The usual case is that the parser throws
    // an exception.
    sql("values (a^#^b)")
        .fails("Lexical error at line 1, column 10\\.  Encountered: \"#\" \\(35\\), after : \"\"");
  }

  // TODO: should fail in parser
  @Test void testStarAsFails() {
    sql("select * as x from emp")
        .ok("SELECT * AS `X`\n"
            + "FROM `EMP`");
  }

  @Test void testFromStarFails() {
    sql("select * from sales^.^*")
        .fails("(?s)Encountered \"\\. \\*\" at .*");
    sql("select emp.empno AS x from sales^.^*")
        .fails("(?s)Encountered \"\\. \\*\" at .*");
    sql("select * from emp^.^*")
        .fails("(?s)Encountered \"\\. \\*\" at .*");
    sql("select emp.empno AS x from emp^.^*")
        .fails("(?s)Encountered \"\\. \\*\" at .*");
    sql("select emp.empno AS x from ^*^")
        .fails("(?s)Encountered \"\\*\" at .*");
  }

  @Test void testHyphenatedTableName() {
    sql("select * from bigquery^-^foo-bar.baz")
        .fails("(?s)Encountered \"-\" at .*")
        .withDialect(BIG_QUERY)
        .ok("SELECT *\n"
            + "FROM `bigquery-foo-bar`.baz");

    // Like BigQuery, MySQL allows back-ticks.
    sql("select `baz`.`buzz` from foo.`baz`")
        .withDialect(BIG_QUERY)
        .ok("SELECT baz.buzz\n"
            + "FROM foo.baz")
        .withDialect(MYSQL)
        .ok("SELECT `baz`.`buzz`\n"
            + "FROM `foo`.`baz`");

    // Unlike BigQuery, MySQL does not allow hyphenated identifiers.
    sql("select `baz`.`buzz` from foo^-^bar.`baz`")
        .withDialect(BIG_QUERY)
        .ok("SELECT baz.buzz\n"
            + "FROM `foo-bar`.baz")
        .withDialect(MYSQL)
        .fails("(?s)Encountered \"-\" at .*");

    // No hyphenated identifiers as table aliases.
    sql("select * from foo.baz as hyphenated^-^alias-not-allowed")
        .withDialect(BIG_QUERY)
        .fails("(?s)Encountered \"-\" at .*");

    sql("select * from foo.baz as `hyphenated-alias-allowed-if-quoted`")
        .withDialect(BIG_QUERY)
        .ok("SELECT *\n"
            + "FROM foo.baz AS `hyphenated-alias-allowed-if-quoted`");

    // No hyphenated identifiers as column names.
    sql("select * from foo-bar.baz cross join (select alpha-omega from t) as t")
        .withDialect(BIG_QUERY)
        .ok("SELECT *\n"
            + "FROM `foo-bar`.baz\n"
            + "CROSS JOIN (SELECT (alpha - omega)\n"
            + "FROM t) AS t");

    sql("select * from bigquery-foo-bar.baz as hyphenated^-^alias-not-allowed")
        .withDialect(BIG_QUERY)
        .fails("(?s)Encountered \"-\" at .*");

    sql("insert into bigquery^-^public-data.foo values (1)")
        .fails("Non-query expression encountered in illegal context")
        .withDialect(BIG_QUERY)
        .ok("INSERT INTO `bigquery-public-data`.foo\n"
            + "VALUES (1)");

    sql("update bigquery^-^public-data.foo set a = b")
        .fails("(?s)Encountered \"-\" at .*")
        .withDialect(BIG_QUERY)
        .ok("UPDATE `bigquery-public-data`.foo SET a = b");

    sql("delete from bigquery^-^public-data.foo where a = 5")
        .fails("(?s)Encountered \"-\" at .*")
        .withDialect(BIG_QUERY)
        .ok("DELETE FROM `bigquery-public-data`.foo\n"
            + "WHERE (a = 5)");

    final String mergeSql = "merge into bigquery^-^public-data.emps e\n"
        + "using (\n"
        + "  select *\n"
        + "  from bigquery-public-data.tempemps\n"
        + "  where deptno is null) t\n"
        + "on e.empno = t.empno\n"
        + "when matched then\n"
        + "  update set name = t.name, deptno = t.deptno,\n"
        + "    salary = t.salary * .1\n"
        + "when not matched then\n"
        + "    insert (name, dept, salary)\n"
        + "    values(t.name, 10, t.salary * .15)";
    final String mergeExpected = "MERGE INTO `bigquery-public-data`.emps AS e\n"
        + "USING (SELECT *\n"
        + "FROM `bigquery-public-data`.tempemps\n"
        + "WHERE (deptno IS NULL)) AS t\n"
        + "ON (e.empno = t.empno)\n"
        + "WHEN MATCHED THEN"
        + " UPDATE SET name = t.name, deptno = t.deptno,"
        + " salary = (t.salary * 0.1)\n"
        + "WHEN NOT MATCHED THEN"
        + " INSERT (name, dept, salary)"
        + " (VALUES (t.name, 10, (t.salary * 0.15)))";
    sql(mergeSql)
        .fails("(?s)Encountered \"-\" at .*")
        .withDialect(BIG_QUERY)
        .ok(mergeExpected);

    // Hyphenated identifiers may not contain spaces, even in BigQuery.
    sql("select * from bigquery ^-^ foo - bar as t where x < y")
        .fails("(?s)Encountered \"-\" at .*")
        .withDialect(BIG_QUERY)
        .fails("(?s)Encountered \"-\" at .*");
  }

  @Test void testHyphenatedColumnName() {
    // While BigQuery allows hyphenated table names, no dialect allows
    // hyphenated column names; they are parsed as arithmetic minus.
    final String expected = "SELECT (`FOO` - `BAR`)\n"
        + "FROM `EMP`";
    final String expectedBigQuery = "SELECT (foo - bar)\n"
        + "FROM emp";
    sql("select foo-bar from emp")
        .ok(expected)
        .withDialect(BIG_QUERY)
        .ok(expectedBigQuery);
  }

  @Test void testDerivedColumnList() {
    sql("select * from emp as e (empno, gender) where true")
        .ok("SELECT *\n"
            + "FROM `EMP` AS `E` (`EMPNO`, `GENDER`)\n"
            + "WHERE TRUE");
  }

  @Test void testDerivedColumnListInJoin() {
    final String sql = "select * from emp as e (empno, gender)\n"
        + " join dept as d (deptno, dname) on emp.deptno = dept.deptno";
    final String expected = "SELECT *\n"
        + "FROM `EMP` AS `E` (`EMPNO`, `GENDER`)\n"
        + "INNER JOIN `DEPT` AS `D` (`DEPTNO`, `DNAME`) ON (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)";
    sql(sql).ok(expected);
  }

  /** Test case that does not reproduce but is related to
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2637">[CALCITE-2637]
   * Prefix '-' operator failed between BETWEEN and AND</a>. */
  @Test void testBetweenAnd() {
    final String sql = "select * from emp\n"
        + "where deptno between - DEPTNO + 1 and 5";
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`DEPTNO` BETWEEN ASYMMETRIC ((- `DEPTNO`) + 1) AND 5)";
    sql(sql).ok(expected);
  }

  @Test void testBetweenAnd2() {
    final String sql = "select * from emp\n"
        + "where deptno between - DEPTNO + 1 and - empno - 3";
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`DEPTNO` BETWEEN ASYMMETRIC ((- `DEPTNO`) + 1)"
        + " AND ((- `EMPNO`) - 3))";
    sql(sql).ok(expected);
  }

  @Disabled
  @Test void testDerivedColumnListNoAs() {
    sql("select * from emp e (empno, gender) where true").ok("foo");
  }

  // jdbc syntax
  @Disabled
  @Test void testEmbeddedCall() {
    expr("{call foo(?, ?)}")
        .ok("foo");
  }

  @Disabled
  @Test void testEmbeddedFunction() {
    expr("{? = call bar (?, ?)}")
        .ok("foo");
  }

  @Test void testColumnAliasWithAs() {
    sql("select 1 as foo from emp")
        .ok("SELECT 1 AS `FOO`\n"
            + "FROM `EMP`");
  }

  @Test void testColumnAliasWithoutAs() {
    sql("select 1 foo from emp")
        .ok("SELECT 1 AS `FOO`\n"
            + "FROM `EMP`");
  }

  @Test void testEmbeddedDate() {
    expr("{d '1998-10-22'}")
        .ok("DATE '1998-10-22'");
  }

  @Test void testEmbeddedTime() {
    expr("{t '16:22:34'}")
        .ok("TIME '16:22:34'");
  }

  @Test void testEmbeddedTimestamp() {
    expr("{ts '1998-10-22 16:22:34'}")
        .ok("TIMESTAMP '1998-10-22 16:22:34'");
  }

  @Test void testNot() {
    sql("select not true, not false, not null, not unknown from t")
        .ok("SELECT (NOT TRUE), (NOT FALSE), (NOT NULL), (NOT UNKNOWN)\n"
            + "FROM `T`");
  }

  @Test void testBooleanPrecedenceAndAssociativity() {
    sql("select * from t where true and false")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (TRUE AND FALSE)");

    sql("select * from t where null or unknown and unknown")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (NULL OR (UNKNOWN AND UNKNOWN))");

    sql("select * from t where true and (true or true) or false")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((TRUE AND (TRUE OR TRUE)) OR FALSE)");

    sql("select * from t where 1 and true")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (1 AND TRUE)");
  }

  @Test void testLessThanAssociativity() {
    expr("NOT a = b")
        .ok("(NOT (`A` = `B`))");

    // comparison operators are left-associative
    expr("x < y < z")
        .ok("((`X` < `Y`) < `Z`)");
    expr("x < y <= z = a")
        .ok("(((`X` < `Y`) <= `Z`) = `A`)");
    expr("a = x < y <= z = a")
        .ok("((((`A` = `X`) < `Y`) <= `Z`) = `A`)");

    // IS NULL has lower precedence than comparison
    expr("a = x is null")
        .ok("((`A` = `X`) IS NULL)");
    expr("a = x is not null")
        .ok("((`A` = `X`) IS NOT NULL)");

    // BETWEEN, IN, LIKE have higher precedence than comparison
    expr("a = x between y = b and z = c")
        .ok("((`A` = (`X` BETWEEN ASYMMETRIC (`Y` = `B`) AND `Z`)) = `C`)");
    expr("a = x like y = b")
        .ok("((`A` = (`X` LIKE `Y`)) = `B`)");
    expr("a = x not like y = b")
        .ok("((`A` = (`X` NOT LIKE `Y`)) = `B`)");
    expr("a = x similar to y = b")
        .ok("((`A` = (`X` SIMILAR TO `Y`)) = `B`)");
    expr("a = x not similar to y = b")
        .ok("((`A` = (`X` NOT SIMILAR TO `Y`)) = `B`)");
    expr("a = x not in (y, z) = b")
        .ok("((`A` = (`X` NOT IN (`Y`, `Z`))) = `B`)");

    // LIKE has higher precedence than IS NULL
    expr("a like b is null")
        .ok("((`A` LIKE `B`) IS NULL)");
    expr("a not like b is not null")
        .ok("((`A` NOT LIKE `B`) IS NOT NULL)");

    // = has higher precedence than NOT
    expr("NOT a = b")
        .ok("(NOT (`A` = `B`))");
    expr("NOT a = NOT b")
        .ok("(NOT (`A` = (NOT `B`)))");

    // IS NULL has higher precedence than NOT
    expr("NOT a IS NULL")
        .ok("(NOT (`A` IS NULL))");
    expr("NOT a = b IS NOT NULL")
        .ok("(NOT ((`A` = `B`) IS NOT NULL))");

    // NOT has higher precedence than AND, which  has higher precedence than OR
    expr("NOT a AND NOT b")
        .ok("((NOT `A`) AND (NOT `B`))");
    expr("NOT a OR NOT b")
        .ok("((NOT `A`) OR (NOT `B`))");
    expr("NOT a = b AND NOT c = d OR NOT e = f")
        .ok("(((NOT (`A` = `B`)) AND (NOT (`C` = `D`))) OR (NOT (`E` = `F`)))");
    expr("NOT a = b OR NOT c = d AND NOT e = f")
        .ok("((NOT (`A` = `B`)) OR ((NOT (`C` = `D`)) AND (NOT (`E` = `F`))))");
    expr("NOT NOT a = b OR NOT NOT c = d")
        .ok("((NOT (NOT (`A` = `B`))) OR (NOT (NOT (`C` = `D`))))");
  }

  @Test void testIsBooleans() {
    String[] inOuts = {"NULL", "TRUE", "FALSE", "UNKNOWN"};

    for (String inOut : inOuts) {
      sql("select * from t where nOt fAlSe Is " + inOut)
          .ok("SELECT *\n"
              + "FROM `T`\n"
              + "WHERE (NOT (FALSE IS " + inOut + "))");

      sql("select * from t where c1=1.1 IS NOT " + inOut)
          .ok("SELECT *\n"
              + "FROM `T`\n"
              + "WHERE ((`C1` = 1.1) IS NOT " + inOut + ")");
    }
  }

  @Test void testIsBooleanPrecedenceAndAssociativity() {
    sql("select * from t where x is unknown is not unknown")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`X` IS UNKNOWN) IS NOT UNKNOWN)");

    sql("select 1 from t where not true is unknown")
        .ok("SELECT 1\n"
            + "FROM `T`\n"
            + "WHERE (NOT (TRUE IS UNKNOWN))");

    sql("select * from t where x is unknown is not unknown is false is not false"
        + " is true is not true is null is not null")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((((((((`X` IS UNKNOWN) IS NOT UNKNOWN) IS FALSE) IS NOT FALSE) IS TRUE) IS NOT TRUE) IS NULL) IS NOT NULL)");

    // combine IS postfix operators with infix (AND) and prefix (NOT) ops
    final String sql = "select * from t "
        + "where x is unknown is false "
        + "and x is unknown is true "
        + "or not y is unknown is not null";
    final String expected = "SELECT *\n"
        + "FROM `T`\n"
        + "WHERE ((((`X` IS UNKNOWN) IS FALSE)"
        + " AND ((`X` IS UNKNOWN) IS TRUE))"
        + " OR (NOT ((`Y` IS UNKNOWN) IS NOT NULL)))";
    sql(sql).ok(expected);
  }

  @Test void testEqualNotEqual() {
    expr("'abc'=123")
        .ok("('abc' = 123)");
    expr("'abc'<>123")
        .ok("('abc' <> 123)");
    expr("'abc'<>123='def'<>456")
        .ok("((('abc' <> 123) = 'def') <> 456)");
    expr("'abc'<>123=('def'<>456)")
        .ok("(('abc' <> 123) = ('def' <> 456))");
  }

  @Test void testBangEqualIsBad() {
    // Quoth www.ocelot.ca:
    //   "Other relators besides '=' are what you'd expect if
    //   you've used any programming language: > and >= and < and <=. The
    //   only potential point of confusion is that the operator for 'not
    //   equals' is <> as in BASIC. There are many texts which will tell
    //   you that != is SQL's not-equals operator; those texts are false;
    //   it's one of those unstampoutable urban myths."
    // Therefore, we only support != with certain SQL conformance levels.
    expr("'abc'^!=^123")
        .fails("Bang equal '!=' is not allowed under the current SQL conformance level");
  }

  @Test void testBetween() {
    sql("select * from t where price between 1 and 2")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` BETWEEN ASYMMETRIC 1 AND 2)");

    sql("select * from t where price between symmetric 1 and 2")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` BETWEEN SYMMETRIC 1 AND 2)");

    sql("select * from t where price not between symmetric 1 and 2")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` NOT BETWEEN SYMMETRIC 1 AND 2)");

    sql("select * from t where price between ASYMMETRIC 1 and 2+2*2")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` BETWEEN ASYMMETRIC 1 AND (2 + (2 * 2)))");

    final String sql0 = "select * from t\n"
        + " where price > 5\n"
        + " and price not between 1 + 2 and 3 * 4 AnD price is null";
    final String expected0 = "SELECT *\n"
        + "FROM `T`\n"
        + "WHERE (((`PRICE` > 5) "
        + "AND (`PRICE` NOT BETWEEN ASYMMETRIC (1 + 2) AND (3 * 4))) "
        + "AND (`PRICE` IS NULL))";
    sql(sql0).ok(expected0);

    final String sql1 = "select * from t\n"
        + "where price > 5\n"
        + "and price between 1 + 2 and 3 * 4 + price is null";
    final String expected1 = "SELECT *\n"
        + "FROM `T`\n"
        + "WHERE ((`PRICE` > 5) "
        + "AND ((`PRICE` BETWEEN ASYMMETRIC (1 + 2) AND ((3 * 4) + `PRICE`)) "
        + "IS NULL))";
    sql(sql1).ok(expected1);

    final String sql2 = "select * from t\n"
        + "where price > 5\n"
        + "and price between 1 + 2 and 3 * 4 or price is null";
    final String expected2 = "SELECT *\n"
        + "FROM `T`\n"
        + "WHERE (((`PRICE` > 5) "
        + "AND (`PRICE` BETWEEN ASYMMETRIC (1 + 2) AND (3 * 4))) "
        + "OR (`PRICE` IS NULL))";
    sql(sql2).ok(expected2);

    final String sql3 = "values a between c and d and e and f between g and h";
    final String expected3 = "VALUES ("
        + "ROW((((`A` BETWEEN ASYMMETRIC `C` AND `D`) AND `E`)"
        + " AND (`F` BETWEEN ASYMMETRIC `G` AND `H`))))";
    sql(sql3).ok(expected3);

    sql("values a between b or c^")
        .fails(".*BETWEEN operator has no terminating AND");

    sql("values a ^between^")
        .fails("(?s).*Encountered \"between <EOF>\" at line 1, column 10.*");

    sql("values a between symmetric 1^")
        .fails(".*BETWEEN operator has no terminating AND");

    // precedence of BETWEEN is higher than AND and OR, but lower than '+'
    sql("values a between b and c + 2 or d and e")
        .ok("VALUES (ROW(((`A` BETWEEN ASYMMETRIC `B` AND (`C` + 2)) OR (`D` AND `E`))))");

    // '=' has slightly lower precedence than BETWEEN; both are left-assoc
    sql("values x = a between b and c = d = e")
        .ok("VALUES (ROW((((`X` = (`A` BETWEEN ASYMMETRIC `B` AND `C`)) = `D`) = `E`)))");

    // AND doesn't match BETWEEN if it's between parentheses!
    sql("values a between b or (c and d) or e and f")
        .ok("VALUES (ROW((`A` BETWEEN ASYMMETRIC ((`B` OR (`C` AND `D`)) OR `E`) AND `F`)))");
  }

  @Test void testOperateOnColumn() {
    sql("select c1*1,c2  + 2,c3/3,c4-4,c5*c4  from t")
        .ok("SELECT (`C1` * 1), (`C2` + 2), (`C3` / 3), (`C4` - 4), (`C5` * `C4`)\n"
            + "FROM `T`");
  }

  @Test void testRow() {
    sql("select t.r.\"EXPR$1\", t.r.\"EXPR$0\" from (select (1,2) r from sales.depts) t")
        .ok("SELECT `T`.`R`.`EXPR$1`, `T`.`R`.`EXPR$0`\n"
            + "FROM (SELECT (ROW(1, 2)) AS `R`\n"
            + "FROM `SALES`.`DEPTS`) AS `T`");

    sql("select t.r.\"EXPR$1\".\"EXPR$2\" "
        + "from (select ((1,2),(3,4,5)) r from sales.depts) t")
        .ok("SELECT `T`.`R`.`EXPR$1`.`EXPR$2`\n"
            + "FROM (SELECT (ROW((ROW(1, 2)), (ROW(3, 4, 5)))) AS `R`\n"
            + "FROM `SALES`.`DEPTS`) AS `T`");

    sql("select t.r.\"EXPR$1\".\"EXPR$2\" "
        + "from (select ((1,2),(3,4,5,6)) r from sales.depts) t")
        .ok("SELECT `T`.`R`.`EXPR$1`.`EXPR$2`\n"
            + "FROM (SELECT (ROW((ROW(1, 2)), (ROW(3, 4, 5, 6)))) AS `R`\n"
            + "FROM `SALES`.`DEPTS`) AS `T`");

    // Conformance DEFAULT and LENIENT support explicit row value constructor
    conformance = SqlConformanceEnum.DEFAULT;
    final String selectRow = "select ^row(t1a, t2a)^ from t1";
    final String expected = "SELECT (ROW(`T1A`, `T2A`))\n"
        + "FROM `T1`";
    sql(selectRow).ok(expected);
    conformance = SqlConformanceEnum.LENIENT;
    sql(selectRow).ok(expected);

    final String pattern = "ROW expression encountered in illegal context";
    conformance = SqlConformanceEnum.MYSQL_5;
    sql(selectRow).fails(pattern);
    conformance = SqlConformanceEnum.ORACLE_12;
    sql(selectRow).fails(pattern);
    conformance = SqlConformanceEnum.STRICT_2003;
    sql(selectRow).fails(pattern);
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    sql(selectRow).fails(pattern);

    final String whereRow = "select 1 from t2 where ^row (x, y)^ < row (a, b)";
    final String whereExpected = "SELECT 1\n"
        + "FROM `T2`\n"
        + "WHERE ((ROW(`X`, `Y`)) < (ROW(`A`, `B`)))";
    conformance = SqlConformanceEnum.DEFAULT;
    sql(whereRow).ok(whereExpected);
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    sql(whereRow).fails(pattern);

    final String whereRow2 = "select 1 from t2 where ^(x, y)^ < (a, b)";
    conformance = SqlConformanceEnum.DEFAULT;
    sql(whereRow2).ok(whereExpected);

    // After this point, SqlUnparserTest has problems.
    // We generate ROW in a dialect that does not allow ROW in all contexts.
    // So bail out.
    assumeFalse(isUnparserTest());
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    sql(whereRow2).ok(whereExpected);
  }

  @Test void testRowValueExpression() {
    final String expected0 = "INSERT INTO \"EMPS\"\n"
            + "VALUES (ROW(1, 'Fred')),\n"
            + "(ROW(2, 'Eric'))";
    String sql = "insert into emps values (1,'Fred'),(2, 'Eric')";
    sql(sql)
        .withDialect(CALCITE)
        .ok(expected0);

    final String expected1 = "INSERT INTO `emps`\n"
            + "VALUES (1, 'Fred'),\n"
            + "(2, 'Eric')";
    sql(sql)
        .withDialect(MYSQL)
        .ok(expected1);

    final String expected2 = "INSERT INTO \"EMPS\"\n"
            + "VALUES (1, 'Fred'),\n"
            + "(2, 'Eric')";
    sql(sql)
        .withDialect(ORACLE)
        .ok(expected2);

    final String expected3 = "INSERT INTO [EMPS]\n"
            + "VALUES (1, 'Fred'),\n"
            + "(2, 'Eric')";
    sql(sql)
        .withDialect(MSSQL)
        .ok(expected3);

    conformance = SqlConformanceEnum.DEFAULT;
    expr("ROW(EMP.EMPNO, EMP.ENAME)").ok("(ROW(`EMP`.`EMPNO`, `EMP`.`ENAME`))");
    expr("ROW(EMP.EMPNO + 1, EMP.ENAME)").ok("(ROW((`EMP`.`EMPNO` + 1), `EMP`.`ENAME`))");
    expr("ROW((select deptno from dept where dept.deptno = emp.deptno), EMP.ENAME)")
        .ok("(ROW((SELECT `DEPTNO`\n"
            + "FROM `DEPT`\n"
            + "WHERE (`DEPT`.`DEPTNO` = `EMP`.`DEPTNO`)), `EMP`.`ENAME`))");
  }

  /** Whether this is a sub-class that tests un-parsing as well as parsing. */
  protected boolean isUnparserTest() {
    return false;
  }

  @Test void testRowWithDot() {
    sql("select (1,2).a from c.t")
        .ok("SELECT ((ROW(1, 2)).`A`)\nFROM `C`.`T`");
    sql("select row(1,2).a from c.t")
        .ok("SELECT ((ROW(1, 2)).`A`)\nFROM `C`.`T`");
    sql("select tbl.foo(0).col.bar from tbl")
        .ok("SELECT ((`TBL`.`FOO`(0).`COL`).`BAR`)\nFROM `TBL`");
  }

  @Test void testPeriod() {
    // We don't have a PERIOD constructor currently;
    // ROW constructor is sufficient for now.
    expr("period (date '1969-01-05', interval '2-3' year to month)")
        .ok("(ROW(DATE '1969-01-05', INTERVAL '2-3' YEAR TO MONTH))");
  }

  @Test void testOverlaps() {
    final String[] ops = {
        "overlaps", "equals", "precedes", "succeeds",
        "immediately precedes", "immediately succeeds"
    };
    final String[] periods = {"period ", ""};
    for (String period : periods) {
      for (String op : ops) {
        checkPeriodPredicate(new Checker(op, period));
      }
    }
  }

  void checkPeriodPredicate(Checker checker) {
    checker.checkExp("$p(x,xx) $op $p(y,yy)",
        "(PERIOD (`X`, `XX`) $op PERIOD (`Y`, `YY`))");

    checker.checkExp(
        "$p(x,xx) $op $p(y,yy) or false",
        "((PERIOD (`X`, `XX`) $op PERIOD (`Y`, `YY`)) OR FALSE)");

    checker.checkExp(
        "true and not $p(x,xx) $op $p(y,yy) or false",
        "((TRUE AND (NOT (PERIOD (`X`, `XX`) $op PERIOD (`Y`, `YY`)))) OR FALSE)");

    if (checker.period.isEmpty()) {
      checker.checkExp("$p(x,xx,xxx) $op $p(y,yy) or false",
          "((PERIOD (`X`, `XX`) $op PERIOD (`Y`, `YY`)) OR FALSE)");
    } else {
      // 3-argument rows are valid in the parser, rejected by the validator
      checker.checkExpFails("$p(x,xx^,^xxx) $op $p(y,yy) or false",
          "(?s).*Encountered \",\" at .*");
    }
  }

  /** Parses a list of statements (that contains only one statement). */
  @Test void testStmtListWithSelect() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    sqlList("select * from emp, dept").ok(expected);
  }

  @Test void testStmtListWithSelectAndSemicolon() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    sqlList("select * from emp, dept;").ok(expected);
  }

  @Test void testStmtListWithTwoSelect() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    sqlList("select * from emp, dept ; select * from emp, dept")
        .ok(expected, expected);
  }

  @Test void testStmtListWithTwoSelectSemicolon() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    sqlList("select * from emp, dept ; select * from emp, dept;")
        .ok(expected, expected);
  }

  @Test void testStmtListWithSelectDelete() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    final String expected1 = "DELETE FROM `EMP`";
    sqlList("select * from emp, dept; delete from emp")
        .ok(expected, expected1);
  }

  @Test void testStmtListWithSelectDeleteUpdate() {
    final String sql = "select * from emp, dept; "
        + "delete from emp; "
        + "update emps set empno = empno + 1";
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    final String expected1 = "DELETE FROM `EMP`";
    final String expected2 = "UPDATE `EMPS` SET `EMPNO` = (`EMPNO` + 1)";
    sqlList(sql).ok(expected, expected1, expected2);
  }

  @Test void testStmtListWithSemiColonInComment() {
    final String sql = ""
        + "select * from emp, dept; // comment with semicolon ; values 1\n"
        + "values 2";
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    final String expected1 = "VALUES (ROW(2))";
    sqlList(sql).ok(expected, expected1);
  }

  @Test void testStmtListWithSemiColonInWhere() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`NAME` LIKE 'toto;')";
    final String expected1 = "DELETE FROM `EMP`";
    sqlList("select * from emp where name like 'toto;'; delete from emp")
        .ok(expected, expected1);
  }

  @Test void testStmtListWithInsertSelectInsert() {
    final String sql = "insert into dept (name, deptno) values ('a', 123); "
        + "select * from emp where name like 'toto;'; "
        + "insert into dept (name, deptno) values ('b', 123);";
    final String expected = "INSERT INTO `DEPT` (`NAME`, `DEPTNO`)\n"
        + "VALUES (ROW('a', 123))";
    final String expected1 = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`NAME` LIKE 'toto;')";
    final String expected2 = "INSERT INTO `DEPT` (`NAME`, `DEPTNO`)\n"
        + "VALUES (ROW('b', 123))";
    sqlList(sql).ok(expected, expected1, expected2);
  }

  /** Should fail since the first statement lacks semicolon. */
  @Test void testStmtListWithoutSemiColon1() {
    sqlList("select * from emp where name like 'toto' "
        + "^delete^ from emp")
        .fails("(?s).*Encountered \"delete\" at .*");
  }

  /** Should fail since the third statement lacks semicolon. */
  @Test void testStmtListWithoutSemiColon2() {
    sqlList("select * from emp where name like 'toto'; "
        + "delete from emp; "
        + "insert into dept (name, deptno) values ('a', 123) "
        + "^select^ * from dept")
        .fails("(?s).*Encountered \"select\" at .*");
  }

  @Test void testIsDistinctFrom() {
    sql("select x is distinct from y from t")
        .ok("SELECT (`X` IS DISTINCT FROM `Y`)\n"
            + "FROM `T`");

    sql("select * from t where x is distinct from y")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` IS DISTINCT FROM `Y`)");

    sql("select * from t where x is distinct from (4,5,6)")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` IS DISTINCT FROM (ROW(4, 5, 6)))");

    sql("select * from t where x is distinct from row (4,5,6)")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` IS DISTINCT FROM (ROW(4, 5, 6)))");

    sql("select * from t where true is distinct from true")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (TRUE IS DISTINCT FROM TRUE)");

    sql("select * from t where true is distinct from true is true")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((TRUE IS DISTINCT FROM TRUE) IS TRUE)");
  }

  @Test void testIsNotDistinct() {
    sql("select x is not distinct from y from t")
        .ok("SELECT (`X` IS NOT DISTINCT FROM `Y`)\n"
            + "FROM `T`");

    sql("select * from t where true is not distinct from true")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (TRUE IS NOT DISTINCT FROM TRUE)");
  }

  @Test void testFloor() {
    expr("floor(1.5)")
        .ok("FLOOR(1.5)");
    expr("floor(x)")
        .ok("FLOOR(`X`)");

    expr("floor(x to second)")
        .ok("FLOOR(`X` TO SECOND)");
    expr("floor(x to epoch)")
        .ok("FLOOR(`X` TO EPOCH)");
    expr("floor(x to minute)")
        .ok("FLOOR(`X` TO MINUTE)");
    expr("floor(x to hour)")
        .ok("FLOOR(`X` TO HOUR)");
    expr("floor(x to day)")
        .ok("FLOOR(`X` TO DAY)");
    expr("floor(x to dow)")
        .ok("FLOOR(`X` TO DOW)");
    expr("floor(x to doy)")
        .ok("FLOOR(`X` TO DOY)");
    expr("floor(x to week)")
        .ok("FLOOR(`X` TO WEEK)");
    expr("floor(x to month)")
        .ok("FLOOR(`X` TO MONTH)");
    expr("floor(x to quarter)")
        .ok("FLOOR(`X` TO QUARTER)");
    expr("floor(x to year)")
        .ok("FLOOR(`X` TO YEAR)");
    expr("floor(x to decade)")
        .ok("FLOOR(`X` TO DECADE)");
    expr("floor(x to century)")
        .ok("FLOOR(`X` TO CENTURY)");
    expr("floor(x to millennium)")
        .ok("FLOOR(`X` TO MILLENNIUM)");

    expr("floor(x + interval '1:20' minute to second)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND))");
    expr("floor(x + interval '1:20' minute to second to second)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO SECOND)");
    expr("floor(x + interval '1:20' minute to second to epoch)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO EPOCH)");
    expr("floor(x + interval '1:20' hour to minute)")
        .ok("FLOOR((`X` + INTERVAL '1:20' HOUR TO MINUTE))");
    expr("floor(x + interval '1:20' hour to minute to minute)")
        .ok("FLOOR((`X` + INTERVAL '1:20' HOUR TO MINUTE) TO MINUTE)");
    expr("floor(x + interval '1:20' minute to second to hour)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO HOUR)");
    expr("floor(x + interval '1:20' minute to second to day)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DAY)");
    expr("floor(x + interval '1:20' minute to second to dow)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DOW)");
    expr("floor(x + interval '1:20' minute to second to doy)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DOY)");
    expr("floor(x + interval '1:20' minute to second to week)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO WEEK)");
    expr("floor(x + interval '1:20' minute to second to month)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO MONTH)");
    expr("floor(x + interval '1:20' minute to second to quarter)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO QUARTER)");
    expr("floor(x + interval '1:20' minute to second to year)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO YEAR)");
    expr("floor(x + interval '1:20' minute to second to decade)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DECADE)");
    expr("floor(x + interval '1:20' minute to second to century)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO CENTURY)");
    expr("floor(x + interval '1:20' minute to second to millennium)")
        .ok("FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO MILLENNIUM)");
  }

  @Test void testCeil() {
    expr("ceil(3453.2)")
        .ok("CEIL(3453.2)");
    expr("ceil(x)")
        .ok("CEIL(`X`)");
    expr("ceil(x to second)")
        .ok("CEIL(`X` TO SECOND)");
    expr("ceil(x to epoch)")
        .ok("CEIL(`X` TO EPOCH)");
    expr("ceil(x to minute)")
        .ok("CEIL(`X` TO MINUTE)");
    expr("ceil(x to hour)")
        .ok("CEIL(`X` TO HOUR)");
    expr("ceil(x to day)")
        .ok("CEIL(`X` TO DAY)");
    expr("ceil(x to dow)")
        .ok("CEIL(`X` TO DOW)");
    expr("ceil(x to doy)")
        .ok("CEIL(`X` TO DOY)");
    expr("ceil(x to week)")
        .ok("CEIL(`X` TO WEEK)");
    expr("ceil(x to month)")
        .ok("CEIL(`X` TO MONTH)");
    expr("ceil(x to quarter)")
        .ok("CEIL(`X` TO QUARTER)");
    expr("ceil(x to year)")
        .ok("CEIL(`X` TO YEAR)");
    expr("ceil(x to decade)")
        .ok("CEIL(`X` TO DECADE)");
    expr("ceil(x to century)")
        .ok("CEIL(`X` TO CENTURY)");
    expr("ceil(x to millennium)")
        .ok("CEIL(`X` TO MILLENNIUM)");

    expr("ceil(x + interval '1:20' minute to second)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND))");
    expr("ceil(x + interval '1:20' minute to second to second)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO SECOND)");
    expr("ceil(x + interval '1:20' minute to second to epoch)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO EPOCH)");
    expr("ceil(x + interval '1:20' hour to minute)")
        .ok("CEIL((`X` + INTERVAL '1:20' HOUR TO MINUTE))");
    expr("ceil(x + interval '1:20' hour to minute to minute)")
        .ok("CEIL((`X` + INTERVAL '1:20' HOUR TO MINUTE) TO MINUTE)");
    expr("ceil(x + interval '1:20' minute to second to hour)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO HOUR)");
    expr("ceil(x + interval '1:20' minute to second to day)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DAY)");
    expr("ceil(x + interval '1:20' minute to second to dow)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DOW)");
    expr("ceil(x + interval '1:20' minute to second to doy)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DOY)");
    expr("ceil(x + interval '1:20' minute to second to week)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO WEEK)");
    expr("ceil(x + interval '1:20' minute to second to month)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO MONTH)");
    expr("ceil(x + interval '1:20' minute to second to quarter)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO QUARTER)");
    expr("ceil(x + interval '1:20' minute to second to year)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO YEAR)");
    expr("ceil(x + interval '1:20' minute to second to decade)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DECADE)");
    expr("ceil(x + interval '1:20' minute to second to century)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO CENTURY)");
    expr("ceil(x + interval '1:20' minute to second to millennium)")
        .ok("CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO MILLENNIUM)");
  }

  @Test void testCast() {
    expr("cast(x as boolean)")
        .ok("CAST(`X` AS BOOLEAN)");
    expr("cast(x as integer)")
        .ok("CAST(`X` AS INTEGER)");
    expr("cast(x as varchar(1))")
        .ok("CAST(`X` AS VARCHAR(1))");
    expr("cast(x as date)")
        .ok("CAST(`X` AS DATE)");
    expr("cast(x as time)")
        .ok("CAST(`X` AS TIME)");
    expr("cast(x as time without time zone)")
        .ok("CAST(`X` AS TIME)");
    expr("cast(x as time with local time zone)")
        .ok("CAST(`X` AS TIME WITH LOCAL TIME ZONE)");
    expr("cast(x as timestamp without time zone)")
        .ok("CAST(`X` AS TIMESTAMP)");
    expr("cast(x as timestamp with local time zone)")
        .ok("CAST(`X` AS TIMESTAMP WITH LOCAL TIME ZONE)");
    expr("cast(x as time(0))")
        .ok("CAST(`X` AS TIME(0))");
    expr("cast(x as time(0) without time zone)")
        .ok("CAST(`X` AS TIME(0))");
    expr("cast(x as time(0) with local time zone)")
        .ok("CAST(`X` AS TIME(0) WITH LOCAL TIME ZONE)");
    expr("cast(x as timestamp(0))")
        .ok("CAST(`X` AS TIMESTAMP(0))");
    expr("cast(x as timestamp(0) without time zone)")
        .ok("CAST(`X` AS TIMESTAMP(0))");
    expr("cast(x as timestamp(0) with local time zone)")
        .ok("CAST(`X` AS TIMESTAMP(0) WITH LOCAL TIME ZONE)");
    expr("cast(x as timestamp)")
        .ok("CAST(`X` AS TIMESTAMP)");
    expr("cast(x as decimal(1,1))")
        .ok("CAST(`X` AS DECIMAL(1, 1))");
    expr("cast(x as char(1))")
        .ok("CAST(`X` AS CHAR(1))");
    expr("cast(x as binary(1))")
        .ok("CAST(`X` AS BINARY(1))");
    expr("cast(x as varbinary(1))")
        .ok("CAST(`X` AS VARBINARY(1))");
    expr("cast(x as tinyint)")
        .ok("CAST(`X` AS TINYINT)");
    expr("cast(x as smallint)")
        .ok("CAST(`X` AS SMALLINT)");
    expr("cast(x as bigint)")
        .ok("CAST(`X` AS BIGINT)");
    expr("cast(x as real)")
        .ok("CAST(`X` AS REAL)");
    expr("cast(x as double)")
        .ok("CAST(`X` AS DOUBLE)");
    expr("cast(x as decimal)")
        .ok("CAST(`X` AS DECIMAL)");
    expr("cast(x as decimal(0))")
        .ok("CAST(`X` AS DECIMAL(0))");
    expr("cast(x as decimal(1,2))")
        .ok("CAST(`X` AS DECIMAL(1, 2))");

    expr("cast('foo' as bar)")
        .ok("CAST('foo' AS `BAR`)");
  }

  @Test void testCastFails() {
    expr("cast(x as time with ^time^ zone)")
        .fails("(?s).*Encountered \"time\" at .*");
    expr("cast(x as time(0) with ^time^ zone)")
        .fails("(?s).*Encountered \"time\" at .*");
    expr("cast(x as timestamp with ^time^ zone)")
        .fails("(?s).*Encountered \"time\" at .*");
    expr("cast(x as timestamp(0) with ^time^ zone)")
        .fails("(?s).*Encountered \"time\" at .*");
    expr("cast(x as varchar(10) ^with^ local time zone)")
        .fails("(?s).*Encountered \"with\" at line 1, column 23.\n.*");
    expr("cast(x as varchar(10) ^without^ time zone)")
        .fails("(?s).*Encountered \"without\" at line 1, column 23.\n.*");
  }

  @Test void testLikeAndSimilar() {
    sql("select * from t where x like '%abc%'")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` LIKE '%abc%')");

    sql("select * from t where x+1 not siMilaR to '%abc%' ESCAPE 'e'")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`X` + 1) NOT SIMILAR TO '%abc%' ESCAPE 'e')");

    // LIKE has higher precedence than AND
    sql("select * from t where price > 5 and x+2*2 like y*3+2 escape (select*from t)")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`PRICE` > 5) AND ((`X` + (2 * 2)) LIKE ((`Y` * 3) + 2) ESCAPE (SELECT *\n"
            + "FROM `T`)))");

    sql("values a and b like c")
        .ok("VALUES (ROW((`A` AND (`B` LIKE `C`))))");

    // LIKE has higher precedence than AND
    sql("values a and b like c escape d and e")
        .ok("VALUES (ROW(((`A` AND (`B` LIKE `C` ESCAPE `D`)) AND `E`)))");

    // LIKE has same precedence as '='; LIKE is right-assoc, '=' is left
    sql("values a = b like c = d")
        .ok("VALUES (ROW(((`A` = (`B` LIKE `C`)) = `D`)))");

    // Nested LIKE
    sql("values a like b like c escape d")
        .ok("VALUES (ROW((`A` LIKE (`B` LIKE `C` ESCAPE `D`))))");
    sql("values a like b like c escape d and false")
        .ok("VALUES (ROW(((`A` LIKE (`B` LIKE `C` ESCAPE `D`)) AND FALSE)))");
    sql("values a like b like c like d escape e escape f")
        .ok("VALUES (ROW((`A` LIKE (`B` LIKE (`C` LIKE `D` ESCAPE `E`) ESCAPE `F`))))");

    // Mixed LIKE and SIMILAR TO
    sql("values a similar to b like c similar to d escape e escape f")
        .ok("VALUES (ROW((`A` SIMILAR TO (`B` LIKE (`C` SIMILAR TO `D` ESCAPE `E`) ESCAPE `F`))))");

    if (isReserved("ESCAPE")) {
      sql("select * from t where ^escape^ 'e'")
          .fails("(?s).*Encountered \"escape\" at .*");
    }

    // LIKE with +
    sql("values a like b + c escape d")
        .ok("VALUES (ROW((`A` LIKE (`B` + `C`) ESCAPE `D`)))");

    // LIKE with ||
    sql("values a like b || c escape d")
        .ok("VALUES (ROW((`A` LIKE (`B` || `C`) ESCAPE `D`)))");

    // ESCAPE with no expression
    if (isReserved("ESCAPE")) {
      sql("values a ^like^ escape d")
          .fails("(?s).*Encountered \"like escape\" at .*");
    }

    // ESCAPE with no expression
    if (isReserved("ESCAPE")) {
      sql("values a like b || c ^escape^ and false")
          .fails("(?s).*Encountered \"escape and\" at line 1, column 22.*");
    }

    // basic SIMILAR TO
    sql("select * from t where x similar to '%abc%'")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` SIMILAR TO '%abc%')");

    sql("select * from t where x+1 not siMilaR to '%abc%' ESCAPE 'e'")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`X` + 1) NOT SIMILAR TO '%abc%' ESCAPE 'e')");

    // SIMILAR TO has higher precedence than AND
    sql("select * from t where price > 5 and x+2*2 SIMILAR TO y*3+2 escape (select*from t)")
        .ok("SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`PRICE` > 5) AND ((`X` + (2 * 2)) SIMILAR TO ((`Y` * 3) + 2) ESCAPE (SELECT *\n"
            + "FROM `T`)))");

    // Mixed LIKE and SIMILAR TO
    sql("values a similar to b like c similar to d escape e escape f")
        .ok("VALUES (ROW((`A` SIMILAR TO (`B` LIKE (`C` SIMILAR TO `D` ESCAPE `E`) ESCAPE `F`))))");

    // SIMILAR TO with sub-query
    sql("values a similar to (select * from t where a like b escape c) escape d")
        .ok("VALUES (ROW((`A` SIMILAR TO (SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`A` LIKE `B` ESCAPE `C`)) ESCAPE `D`)))");
  }

  @Test void testIlike() {
    // The ILIKE operator is only valid when the PostgreSQL function library is
    // enabled ('fun=postgresql'). But the parser can always parse it.
    final String expected = "SELECT *\n"
        + "FROM `T`\n"
        + "WHERE (`X` NOT ILIKE '%abc%')";
    final String sql = "select * from t where x not ilike '%abc%'";
    sql(sql).ok(expected);

    final String sql1 = "select * from t where x ilike '%abc%'";
    final String expected1 = "SELECT *\n"
        + "FROM `T`\n"
        + "WHERE (`X` ILIKE '%abc%')";
    sql(sql1).ok(expected1);
  }

  @Test void testRlike() {
    // The RLIKE operator is valid when the HIVE or SPARK function library is
    // enabled ('fun=spark' or 'fun=hive'). But the parser can always parse it.
    final String expected = "SELECT `COLA`\n"
        + "FROM `T`\n"
        + "WHERE (MAX(`EMAIL`) RLIKE '.+@.+\\\\..+')";
    final String sql = "select cola from t where max(email) rlike '.+@.+\\\\..+'";
    sql(sql).ok(expected);

    final String expected1 = "SELECT `COLA`\n"
        + "FROM `T`\n"
        + "WHERE (MAX(`EMAIL`) NOT RLIKE '.+@.+\\\\..+')";
    final String sql1 = "select cola from t where max(email) not rlike '.+@.+\\\\..+'";
    sql(sql1).ok(expected1);
  }

  @Test void testArithmeticOperators() {
    expr("1-2+3*4/5/6-7")
        .ok("(((1 - 2) + (((3 * 4) / 5) / 6)) - 7)");
    expr("power(2,3)")
        .ok("POWER(2, 3)");
    expr("aBs(-2.3e-2)")
        .ok("ABS(-2.3E-2)");
    expr("MOD(5             ,\t\f\r\n2)")
        .ok("MOD(5, 2)");
    expr("ln(5.43  )")
        .ok("LN(5.43)");
    expr("log10(- -.2  )")
        .ok("LOG10(0.2)");
  }

  @Test void testExists() {
    sql("select * from dept where exists (select 1 from emp where emp.deptno = dept.deptno)")
        .ok("SELECT *\n"
            + "FROM `DEPT`\n"
            + "WHERE (EXISTS (SELECT 1\n"
            + "FROM `EMP`\n"
            + "WHERE (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)))");
  }

  @Test void testExistsInWhere() {
    sql("select * from emp where 1 = 2 and exists (select 1 from dept) and 3 = 4")
        .ok("SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (((1 = 2) AND (EXISTS (SELECT 1\n"
            + "FROM `DEPT`))) AND (3 = 4))");
  }

  @Test void testFromWithAs() {
    sql("select 1 from emp as e where 1")
        .ok("SELECT 1\n"
            + "FROM `EMP` AS `E`\n"
            + "WHERE 1");
  }

  @Test void testConcat() {
    expr("'a' || 'b'").ok("('a' || 'b')");
  }

  @Test void testReverseSolidus() {
    expr("'\\'").ok("'\\'");
  }

  @Test void testSubstring() {
    expr("substring('a'\nFROM \t  1)")
        .ok("SUBSTRING('a' FROM 1)");
    expr("substring('a' FROM 1 FOR 3)")
        .ok("SUBSTRING('a' FROM 1 FOR 3)");
    expr("substring('a' FROM 'reg' FOR '\\')")
        .ok("SUBSTRING('a' FROM 'reg' FOR '\\')");

    expr("substring('a', 'reg', '\\')")
        .ok("SUBSTRING('a' FROM 'reg' FOR '\\')");
    expr("substring('a', 1, 2)")
        .ok("SUBSTRING('a' FROM 1 FOR 2)");
    expr("substring('a' , 1)")
        .ok("SUBSTRING('a' FROM 1)");
  }

  @Test void testFunction() {
    sql("select substring('Eggs and ham', 1, 3 + 2) || ' benedict' from emp")
        .ok("SELECT (SUBSTRING('Eggs and ham' FROM 1 FOR (3 + 2)) || ' benedict')\n"
            + "FROM `EMP`");
    expr("log10(1)\r\n"
        + "+power(2, mod(\r\n"
        + "3\n"
        + "\t\t\f\n"
        + ",ln(4))*log10(5)-6*log10(7/abs(8)+9))*power(10,11)")
        .ok("(LOG10(1) + (POWER(2, ((MOD(3, LN(4)) * LOG10(5))"
            + " - (6 * LOG10(((7 / ABS(8)) + 9))))) * POWER(10, 11)))");
  }

  @Test void testFunctionWithDistinct() {
    expr("count(DISTINCT 1)").ok("COUNT(DISTINCT 1)");
    expr("count(ALL 1)").ok("COUNT(ALL 1)");
    expr("count(1)").ok("COUNT(1)");
    sql("select count(1), count(distinct 2) from emp")
        .ok("SELECT COUNT(1), COUNT(DISTINCT 2)\n"
            + "FROM `EMP`");
  }

  @Test void testFunctionCallWithDot() {
    expr("foo(a,b).c")
        .ok("(`FOO`(`A`, `B`).`C`)");
  }

  @Test void testFunctionInFunction() {
    expr("ln(power(2,2))")
        .ok("LN(POWER(2, 2))");
  }

  @Test void testFunctionNamedArgument() {
    expr("foo(x => 1)")
        .ok("`FOO`(`X` => 1)");
    expr("foo(x => 1, \"y\" => 'a', z => x <= y)")
        .ok("`FOO`(`X` => 1, `y` => 'a', `Z` => (`X` <= `Y`))");
    expr("foo(x.y ^=>^ 1)")
        .fails("(?s).*Encountered \"=>\" at .*");
    expr("foo(a => 1, x.y ^=>^ 2, c => 3)")
        .fails("(?s).*Encountered \"=>\" at .*");
  }

  @Test void testFunctionDefaultArgument() {
    sql("foo(1, DEFAULT, default, 'default', \"default\", 3)").expression()
        .ok("`FOO`(1, DEFAULT, DEFAULT, 'default', `default`, 3)");
    sql("foo(DEFAULT)").expression()
        .ok("`FOO`(DEFAULT)");
    sql("foo(x => 1, DEFAULT)").expression()
        .ok("`FOO`(`X` => 1, DEFAULT)");
    sql("foo(y => DEFAULT, x => 1)").expression()
        .ok("`FOO`(`Y` => DEFAULT, `X` => 1)");
    sql("foo(x => 1, y => DEFAULT)").expression()
        .ok("`FOO`(`X` => 1, `Y` => DEFAULT)");
    sql("select sum(DISTINCT DEFAULT) from t group by x")
        .ok("SELECT SUM(DISTINCT DEFAULT)\n"
            + "FROM `T`\n"
            + "GROUP BY `X`");
    expr("foo(x ^+^ DEFAULT)")
        .fails("(?s).*Encountered \"\\+ DEFAULT\" at .*");
    expr("foo(0, x ^+^ DEFAULT + y)")
        .fails("(?s).*Encountered \"\\+ DEFAULT\" at .*");
    expr("foo(0, DEFAULT ^+^ y)")
        .fails("(?s).*Encountered \"\\+\" at .*");
  }

  @Test void testDefault() {
    sql("select ^DEFAULT^ from emp")
        .fails("(?s)Incorrect syntax near the keyword 'DEFAULT' at .*");
    sql("select cast(empno ^+^ DEFAULT as double) from emp")
        .fails("(?s)Encountered \"\\+ DEFAULT\" at .*");
    sql("select empno ^+^ DEFAULT + deptno from emp")
        .fails("(?s)Encountered \"\\+ DEFAULT\" at .*");
    sql("select power(0, DEFAULT ^+^ empno) from emp")
        .fails("(?s)Encountered \"\\+\" at .*");
    sql("select * from emp join dept on ^DEFAULT^")
        .fails("(?s)Incorrect syntax near the keyword 'DEFAULT' at .*");
    sql("select * from emp where empno ^>^ DEFAULT or deptno < 10")
        .fails("(?s)Encountered \"> DEFAULT\" at .*");
    sql("select * from emp order by ^DEFAULT^ desc")
        .fails("(?s)Incorrect syntax near the keyword 'DEFAULT' at .*");
    final String expected = "INSERT INTO `DEPT` (`NAME`, `DEPTNO`)\n"
        + "VALUES (ROW('a', DEFAULT))";
    sql("insert into dept (name, deptno) values ('a', DEFAULT)")
        .ok(expected);
    sql("insert into dept (name, deptno) values ('a', 1 ^+^ DEFAULT)")
        .fails("(?s)Encountered \"\\+ DEFAULT\" at .*");
    sql("insert into dept (name, deptno) select 'a', ^DEFAULT^ from (values 0)")
        .fails("(?s)Incorrect syntax near the keyword 'DEFAULT' at .*");
  }

  @Test void testAggregateFilter() {
    final String sql = "select\n"
        + " sum(sal) filter (where gender = 'F') as femaleSal,\n"
        + " sum(sal) filter (where true) allSal,\n"
        + " count(distinct deptno) filter (where (deptno < 40))\n"
        + "from emp";
    final String expected = "SELECT"
        + " SUM(`SAL`) FILTER (WHERE (`GENDER` = 'F')) AS `FEMALESAL`,"
        + " SUM(`SAL`) FILTER (WHERE TRUE) AS `ALLSAL`,"
        + " COUNT(DISTINCT `DEPTNO`) FILTER (WHERE (`DEPTNO` < 40))\n"
        + "FROM `EMP`";
    sql(sql).ok(expected);
  }

  @Test void testGroup() {
    sql("select deptno, min(foo) as x from emp group by deptno, gender")
        .ok("SELECT `DEPTNO`, MIN(`FOO`) AS `X`\n"
            + "FROM `EMP`\n"
            + "GROUP BY `DEPTNO`, `GENDER`");
  }

  @Test void testGroupEmpty() {
    sql("select count(*) from emp group by ()")
        .ok("SELECT COUNT(*)\n"
            + "FROM `EMP`\n"
            + "GROUP BY ()");

    sql("select count(*) from emp group by () having 1 = 2 order by 3")
        .ok("SELECT COUNT(*)\n"
            + "FROM `EMP`\n"
            + "GROUP BY ()\n"
            + "HAVING (1 = 2)\n"
            + "ORDER BY 3");

    // Used to be invalid, valid now that we support grouping sets.
    sql("select 1 from emp group by (), x")
        .ok("SELECT 1\n"
            + "FROM `EMP`\n"
            + "GROUP BY (), `X`");

    // Used to be invalid, valid now that we support grouping sets.
    sql("select 1 from emp group by x, ()")
        .ok("SELECT 1\n"
            + "FROM `EMP`\n"
            + "GROUP BY `X`, ()");

    // parentheses do not an empty GROUP BY make
    sql("select 1 from emp group by (empno + deptno)")
        .ok("SELECT 1\n"
            + "FROM `EMP`\n"
            + "GROUP BY (`EMPNO` + `DEPTNO`)");
  }

  @Test void testHavingAfterGroup() {
    final String sql = "select deptno from emp group by deptno, emp\n"
        + "having count(*) > 5 and 1 = 2 order by 5, 2";
    final String expected = "SELECT `DEPTNO`\n"
        + "FROM `EMP`\n"
        + "GROUP BY `DEPTNO`, `EMP`\n"
        + "HAVING ((COUNT(*) > 5) AND (1 = 2))\n"
        + "ORDER BY 5, 2";
    sql(sql).ok(expected);
  }

  @Test void testHavingBeforeGroupFails() {
    final String sql = "select deptno from emp\n"
        + "having count(*) > 5 and deptno < 4 ^group^ by deptno, emp";
    sql(sql).fails("(?s).*Encountered \"group\" at .*");
  }

  @Test void testHavingNoGroup() {
    sql("select deptno from emp having count(*) > 5")
        .ok("SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "HAVING (COUNT(*) > 5)");
  }

  @Test void testGroupingSets() {
    sql("select deptno from emp\n"
        + "group by grouping sets (deptno, (deptno, gender), ())")
        .ok("SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "GROUP BY GROUPING SETS(`DEPTNO`, (`DEPTNO`, `GENDER`), ())");

    sql("select deptno from emp\n"
        + "group by grouping sets ((deptno, gender), (deptno), (), gender)")
        .ok("SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "GROUP BY GROUPING SETS((`DEPTNO`, `GENDER`), `DEPTNO`, (), `GENDER`)");

    // Grouping sets must have parentheses
    sql("select deptno from emp\n"
        + "group by grouping sets ^deptno^, (deptno, gender), ()")
        .fails("(?s).*Encountered \"deptno\" at line 2, column 24.\n"
            + "Was expecting:\n"
            + "    \"\\(\" .*");

    // Nested grouping sets, cube, rollup, grouping sets all OK
    sql("select deptno from emp\n"
        + "group by grouping sets (deptno, grouping sets (e, d), (),\n"
        + "  cube (x, y), rollup(p, q))\n"
        + "order by a")
        .ok("SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "GROUP BY GROUPING SETS(`DEPTNO`, GROUPING SETS(`E`, `D`), (), CUBE(`X`, `Y`), ROLLUP(`P`, `Q`))\n"
            + "ORDER BY `A`");

    sql("select deptno from emp\n"
        + "group by grouping sets (())")
        .ok("SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "GROUP BY GROUPING SETS(())");
  }

  @Test void testGroupByCube() {
    final String sql = "select deptno from emp\n"
        + "group by cube ((a, b), (c, d))";
    final String expected = "SELECT `DEPTNO`\n"
        + "FROM `EMP`\n"
        + "GROUP BY CUBE((`A`, `B`), (`C`, `D`))";
    sql(sql).ok(expected);
  }

  @Test void testGroupByCube2() {
    final String sql = "select deptno from emp\n"
        + "group by cube ((a, b), (c, d)) order by a";
    final String expected = "SELECT `DEPTNO`\n"
        + "FROM `EMP`\n"
        + "GROUP BY CUBE((`A`, `B`), (`C`, `D`))\n"
        + "ORDER BY `A`";
    sql(sql).ok(expected);

    final String sql2 = "select deptno from emp\n"
        + "group by cube (^)";
    sql(sql2).fails("(?s)Encountered \"\\)\" at .*");
  }

  @Test void testGroupByRollup() {
    final String sql = "select deptno from emp\n"
        + "group by rollup (deptno, deptno + 1, gender)";
    final String expected = "SELECT `DEPTNO`\n"
        + "FROM `EMP`\n"
        + "GROUP BY ROLLUP(`DEPTNO`, (`DEPTNO` + 1), `GENDER`)";
    sql(sql).ok(expected);

    // Nested rollup not ok
    final String sql1 = "select deptno from emp\n"
        + "group by rollup (deptno^, rollup(e, d))";
    sql(sql1).fails("(?s)Encountered \", rollup\" at .*");
  }

  @Test void testGrouping() {
    final String sql = "select deptno, grouping(deptno) from emp\n"
        + "group by grouping sets (deptno, (deptno, gender), ())";
    final String expected = "SELECT `DEPTNO`, GROUPING(`DEPTNO`)\n"
        + "FROM `EMP`\n"
        + "GROUP BY GROUPING SETS(`DEPTNO`, (`DEPTNO`, `GENDER`), ())";
    sql(sql).ok(expected);
  }

  @Test void testWith() {
    final String sql = "with femaleEmps as (select * from emps where gender = 'F')"
        + "select deptno from femaleEmps";
    final String expected = "WITH `FEMALEEMPS` AS (SELECT *\n"
        + "FROM `EMPS`\n"
        + "WHERE (`GENDER` = 'F')) (SELECT `DEPTNO`\n"
        + "FROM `FEMALEEMPS`)";
    sql(sql).ok(expected);
  }

  @Test void testWith2() {
    final String sql = "with femaleEmps as (select * from emps where gender = 'F'),\n"
        + "marriedFemaleEmps(x, y) as (select * from femaleEmps where maritaStatus = 'M')\n"
        + "select deptno from femaleEmps";
    final String expected = "WITH `FEMALEEMPS` AS (SELECT *\n"
        + "FROM `EMPS`\n"
        + "WHERE (`GENDER` = 'F')), `MARRIEDFEMALEEMPS` (`X`, `Y`) AS (SELECT *\n"
        + "FROM `FEMALEEMPS`\n"
        + "WHERE (`MARITASTATUS` = 'M')) (SELECT `DEPTNO`\n"
        + "FROM `FEMALEEMPS`)";
    sql(sql).ok(expected);
  }

  @Test void testWithFails() {
    final String sql = "with femaleEmps as ^select^ *\n"
        + "from emps where gender = 'F'\n"
        + "select deptno from femaleEmps";
    sql(sql).fails("(?s)Encountered \"select\" at .*");
  }

  @Test void testWithValues() {
    final String sql = "with v(i,c) as (values (1, 'a'), (2, 'bb'))\n"
        + "select c, i from v";
    final String expected = "WITH `V` (`I`, `C`) AS (VALUES (ROW(1, 'a')),\n"
        + "(ROW(2, 'bb'))) (SELECT `C`, `I`\n"
        + "FROM `V`)";
    sql(sql).ok(expected);
  }

  @Test void testWithNestedFails() {
    // SQL standard does not allow WITH to contain WITH
    final String sql = "with emp2 as (select * from emp)\n"
        + "^with^ dept2 as (select * from dept)\n"
        + "select 1 as uno from emp, dept";
    sql(sql).fails("(?s)Encountered \"with\" at .*");
  }

  @Test void testWithNestedInSubQuery() {
    // SQL standard does not allow sub-query to contain WITH but we do
    final String sql = "with emp2 as (select * from emp)\n"
        + "(\n"
        + "  with dept2 as (select * from dept)\n"
        + "  select 1 as uno from empDept)";
    final String expected = "WITH `EMP2` AS (SELECT *\n"
        + "FROM `EMP`) (WITH `DEPT2` AS (SELECT *\n"
        + "FROM `DEPT`) (SELECT 1 AS `UNO`\n"
        + "FROM `EMPDEPT`))";
    sql(sql).ok(expected);
  }

  @Test void testWithUnion() {
    // Per the standard WITH ... SELECT ... UNION is valid even without parens.
    final String sql = "with emp2 as (select * from emp)\n"
        + "select * from emp2\n"
        + "union\n"
        + "select * from emp2\n";
    final String expected = "WITH `EMP2` AS (SELECT *\n"
        + "FROM `EMP`) (SELECT *\n"
        + "FROM `EMP2`\n"
        + "UNION\n"
        + "SELECT *\n"
        + "FROM `EMP2`)";
    sql(sql).ok(expected);
  }

  @Test void testIdentifier() {
    expr("ab").ok("`AB`");
    expr("     \"a  \"\" b!c\"").ok("`a  \" b!c`");
    expr("     ^`^a  \" b!c`")
        .fails("(?s).*Encountered.*");
    expr("\"x`y`z\"").ok("`x``y``z`");
    expr("^`^x`y`z`")
        .fails("(?s).*Encountered.*");

    expr("myMap[field] + myArray[1 + 2]")
        .ok("(`MYMAP`[`FIELD`] + `MYARRAY`[(1 + 2)])");

    sql("VALUES a").node(isQuoted(0, false));
    sql("VALUES \"a\"").node(isQuoted(0, true));
    sql("VALUES \"a\".\"b\"").node(isQuoted(1, true));
    sql("VALUES \"a\".b").node(isQuoted(1, false));
  }

  @Test void testBackTickIdentifier() {
    quoting = Quoting.BACK_TICK;
    expr("ab").ok("`AB`");
    expr("     `a  \" b!c`").ok("`a  \" b!c`");
    expr("     ^\"^a  \"\" b!c\"")
        .fails("(?s).*Encountered.*");

    expr("^\"^x`y`z\"")
        .fails("(?s).*Encountered.*");
    expr("`x``y``z`").ok("`x``y``z`");

    expr("myMap[field] + myArray[1 + 2]")
        .ok("(`MYMAP`[`FIELD`] + `MYARRAY`[(1 + 2)])");

    sql("VALUES a").node(isQuoted(0, false));
    sql("VALUES `a`").node(isQuoted(0, true));
  }

  @Test void testBracketIdentifier() {
    quoting = Quoting.BRACKET;
    expr("ab").ok("`AB`");
    expr("     [a  \" b!c]").ok("`a  \" b!c`");
    expr("     ^`^a  \" b!c`")
        .fails("(?s).*Encountered.*");
    expr("     ^\"^a  \"\" b!c\"")
        .fails("(?s).*Encountered.*");

    expr("[x`y`z]").ok("`x``y``z`");
    expr("^\"^x`y`z\"")
        .fails("(?s).*Encountered.*");
    expr("^`^x``y``z`")
        .fails("(?s).*Encountered.*");

    expr("[anything [even brackets]] is].[ok]")
        .ok("`anything [even brackets] is`.`ok`");

    // What would be a call to the 'item' function in DOUBLE_QUOTE and BACK_TICK
    // is a table alias.
    sql("select * from myMap[field], myArray[1 + 2]")
        .ok("SELECT *\n"
            + "FROM `MYMAP` AS `field`,\n"
            + "`MYARRAY` AS `1 + 2`");
    sql("select * from myMap [field], myArray [1 + 2]")
        .ok("SELECT *\n"
            + "FROM `MYMAP` AS `field`,\n"
            + "`MYARRAY` AS `1 + 2`");

    sql("VALUES a").node(isQuoted(0, false));
    sql("VALUES [a]").node(isQuoted(0, true));
  }

  @Test void testBackTickQuery() {
    quoting = Quoting.BACK_TICK;
    sql("select `x`.`b baz` from `emp` as `x` where `x`.deptno in (10, 20)")
        .ok("SELECT `x`.`b baz`\n"
            + "FROM `emp` AS `x`\n"
            + "WHERE (`x`.`DEPTNO` IN (10, 20))");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4080">[CALCITE-4080]
   * Allow character literals as column aliases, if
   * SqlConformance.allowCharLiteralAlias()</a>. */
  @Test void testSingleQuotedAlias() {
    final String expectingAlias = "Expecting alias, found character literal";

    final String sql1 = "select 1 as ^'a b'^ from t";
    conformance = SqlConformanceEnum.DEFAULT;
    sql(sql1).fails(expectingAlias);
    conformance = SqlConformanceEnum.MYSQL_5;
    final String sql1b = "SELECT 1 AS `a b`\n"
        + "FROM `T`";
    sql(sql1).ok(sql1b);
    conformance = SqlConformanceEnum.BIG_QUERY;
    sql(sql1).ok(sql1b);
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    sql(sql1).ok(sql1b);

    // valid on MSSQL (alias contains a single quote)
    final String sql2 = "with t as (select 1 as ^'x''y'^)\n"
        + "select [x'y] from t as [u]";
    conformance = SqlConformanceEnum.DEFAULT;
    quoting = Quoting.BRACKET;
    sql(sql2).fails(expectingAlias);
    conformance = SqlConformanceEnum.MYSQL_5;
    final String sql2b = "WITH `T` AS (SELECT 1 AS `x'y`) (SELECT `x'y`\n"
        + "FROM `T` AS `u`)";
    sql(sql2).ok(sql2b);
    conformance = SqlConformanceEnum.BIG_QUERY;
    sql(sql2).ok(sql2b);
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    sql(sql2).ok(sql2b);

    // also valid on MSSQL
    final String sql3 = "with [t] as (select 1 as [x]) select [x] from [t]";
    final String sql3b = "WITH `t` AS (SELECT 1 AS `x`) (SELECT `x`\n"
        + "FROM `t`)";
    conformance = SqlConformanceEnum.DEFAULT;
    quoting = Quoting.BRACKET;
    sql(sql3).ok(sql3b);
    conformance = SqlConformanceEnum.MYSQL_5;
    sql(sql3).ok(sql3b);
    conformance = SqlConformanceEnum.BIG_QUERY;
    sql(sql3).ok(sql3b);
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    sql(sql3).ok(sql3b);

    // char literal as table alias is invalid on MSSQL (and others)
    final String sql4 = "with t as (select 1 as x) select x from t as ^'u'^";
    final String sql4b = "(?s)Encountered \"\\\\'u\\\\'\" at .*";
    conformance = SqlConformanceEnum.DEFAULT;
    sql(sql4).fails(sql4b);
    conformance = SqlConformanceEnum.MYSQL_5;
    sql(sql4).fails(sql4b);
    conformance = SqlConformanceEnum.BIG_QUERY;
    sql(sql4).fails(sql4b);
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    sql(sql4).fails(sql4b);

    // char literal as table alias (without AS) is invalid on MSSQL (and others)
    final String sql5 = "with t as (select 1 as x) select x from t ^'u'^";
    final String sql5b = "(?s)Encountered \"\\\\'u\\\\'\" at .*";
    conformance = SqlConformanceEnum.DEFAULT;
    sql(sql5).fails(sql5b);
    conformance = SqlConformanceEnum.MYSQL_5;
    sql(sql5).fails(sql5b);
    conformance = SqlConformanceEnum.BIG_QUERY;
    sql(sql5).fails(sql5b);
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    sql(sql5).fails(sql5b);
  }

  @Test void testInList() {
    sql("select * from emp where deptno in (10, 20) and gender = 'F'")
        .ok("SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE ((`DEPTNO` IN (10, 20)) AND (`GENDER` = 'F'))");
  }

  @Test void testInListEmptyFails() {
    sql("select * from emp where deptno in (^)^ and gender = 'F'")
        .fails("(?s).*Encountered \"\\)\" at line 1, column 36\\..*");
  }

  @Test void testInQuery() {
    sql("select * from emp where deptno in (select deptno from dept)")
        .ok("SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (`DEPTNO` IN (SELECT `DEPTNO`\n"
            + "FROM `DEPT`))");
  }

  @Test void testSomeEveryAndIntersectionAggQuery() {
    sql("select some(deptno = 10), every(deptno > 0), intersection(multiset[1,2]) from dept")
        .ok("SELECT SOME((`DEPTNO` = 10)), EVERY((`DEPTNO` > 0)), INTERSECTION((MULTISET[1, 2]))\n"
            + "FROM `DEPT`");
  }

  /**
   * Tricky for the parser - looks like "IN (scalar, scalar)" but isn't.
   */
  @Test void testInQueryWithComma() {
    sql("select * from emp where deptno in (select deptno from dept group by 1, 2)")
        .ok("SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (`DEPTNO` IN (SELECT `DEPTNO`\n"
            + "FROM `DEPT`\n"
            + "GROUP BY 1, 2))");
  }

  @Test void testInSetop() {
    sql("select * from emp where deptno in (\n"
        + "(select deptno from dept union select * from dept)"
        + "except\n"
        + "select * from dept) and false")
        .ok("SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE ((`DEPTNO` IN ((SELECT `DEPTNO`\n"
            + "FROM `DEPT`\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `DEPT`)\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM `DEPT`)) AND FALSE)");
  }

  @Test void testSome() {
    final String sql = "select * from emp\n"
        + "where sal > some (select comm from emp)";
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`SAL` > SOME (SELECT `COMM`\n"
        + "FROM `EMP`))";
    sql(sql).ok(expected);

    // ANY is a synonym for SOME
    final String sql2 = "select * from emp\n"
        + "where sal > any (select comm from emp)";
    sql(sql2).ok(expected);

    final String sql3 = "select * from emp\n"
        + "where name like (select ^some^ name from emp)";
    sql(sql3).fails("(?s).*Encountered \"some name\" at .*");

    final String sql4 = "select * from emp\n"
        + "where name like some (select name from emp)";
    final String expected4 = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`NAME` LIKE SOME((SELECT `NAME`\n"
        +  "FROM `EMP`)))";
    sql(sql4).ok(expected4);

    final String sql5 = "select * from emp where empno = any (10,20)";
    final String expected5 = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`EMPNO` = SOME (10, 20))";
    sql(sql5).ok(expected5);
  }

  @Test void testAll() {
    final String sql = "select * from emp\n"
        + "where sal <= all (select comm from emp) or sal > 10";
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE ((`SAL` <= ALL (SELECT `COMM`\n"
        + "FROM `EMP`)) OR (`SAL` > 10))";
    sql(sql).ok(expected);
  }

  @Test void testAllList() {
    final String sql = "select * from emp\n"
        + "where sal <= all (12, 20, 30)";
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`SAL` <= ALL (12, 20, 30))";
    sql(sql).ok(expected);
  }

  @Test void testUnion() {
    sql("select * from a union select * from a")
        .ok("(SELECT *\n"
            + "FROM `A`\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `A`)");
    sql("select * from a union all select * from a")
        .ok("(SELECT *\n"
            + "FROM `A`\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `A`)");
    sql("select * from a union distinct select * from a")
        .ok("(SELECT *\n"
            + "FROM `A`\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `A`)");
  }

  @Test void testUnionOrder() {
    sql("select a, b from t "
        + "union all "
        + "select x, y from u "
        + "order by 1 asc, 2 desc")
        .ok("(SELECT `A`, `B`\n"
            + "FROM `T`\n"
            + "UNION ALL\n"
            + "SELECT `X`, `Y`\n"
            + "FROM `U`)\n"
            + "ORDER BY 1, 2 DESC");
  }

  @Test void testOrderUnion() {
    // ORDER BY inside UNION not allowed
    sql("select a from t order by a\n"
        + "^union^ all\n"
        + "select b from t order by b")
        .fails("(?s).*Encountered \"union\" at .*");
  }

  @Test void testLimitUnion() {
    // LIMIT inside UNION not allowed
    sql("select a from t limit 10\n"
        + "^union^ all\n"
        + "select b from t order by b")
        .fails("(?s).*Encountered \"union\" at .*");
  }

  @Test void testUnionOfNonQueryFails() {
    sql("select 1 from emp union ^2^ + 5")
        .fails("Non-query expression encountered in illegal context");
  }

  /**
   * In modern SQL, a query can occur almost everywhere that an expression
   * can. This test tests the few exceptions.
   */
  @Test void testQueryInIllegalContext() {
    sql("select 0, multiset[^(^select * from emp), 2] from dept")
        .fails("Query expression encountered in illegal context");
    sql("select 0, multiset[1, ^(^select * from emp), 2, 3] from dept")
        .fails("Query expression encountered in illegal context");
  }

  @Test void testExcept() {
    sql("select * from a except select * from a")
        .ok("(SELECT *\n"
            + "FROM `A`\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM `A`)");
    sql("select * from a except all select * from a")
        .ok("(SELECT *\n"
            + "FROM `A`\n"
            + "EXCEPT ALL\n"
            + "SELECT *\n"
            + "FROM `A`)");
    sql("select * from a except distinct select * from a")
        .ok("(SELECT *\n"
            + "FROM `A`\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM `A`)");
  }

  /** Tests MINUS, which is equivalent to EXCEPT but only supported in some
   * conformance levels (e.g. ORACLE). */
  @Test void testSetMinus() {
    final String pattern =
        "MINUS is not allowed under the current SQL conformance level";
    final String sql = "select col1 from table1 ^MINUS^ select col1 from table2";
    sql(sql).fails(pattern);

    conformance = SqlConformanceEnum.ORACLE_10;
    final String expected = "(SELECT `COL1`\n"
        + "FROM `TABLE1`\n"
        + "EXCEPT\n"
        + "SELECT `COL1`\n"
        + "FROM `TABLE2`)";
    sql(sql).ok(expected);

    final String sql2 =
        "select col1 from table1 MINUS ALL select col1 from table2";
    final String expected2 = "(SELECT `COL1`\n"
        + "FROM `TABLE1`\n"
        + "EXCEPT ALL\n"
        + "SELECT `COL1`\n"
        + "FROM `TABLE2`)";
    sql(sql2).ok(expected2);
  }

  /** MINUS is a <b>reserved</b> keyword in Calcite in all conformances, even
   * in the default conformance, where it is not allowed as an alternative to
   * EXCEPT. (It is reserved in Oracle but not in any version of the SQL
   * standard.) */
  @Test void testMinusIsReserved() {
    sql("select ^minus^ from t")
        .fails("(?s).*Encountered \"minus\" at .*");
    sql("select ^minus^ select")
        .fails("(?s).*Encountered \"minus\" at .*");
    sql("select * from t as ^minus^ where x < y")
        .fails("(?s).*Encountered \"minus\" at .*");
  }

  @Test void testIntersect() {
    sql("select * from a intersect select * from a")
        .ok("(SELECT *\n"
            + "FROM `A`\n"
            + "INTERSECT\n"
            + "SELECT *\n"
            + "FROM `A`)");
    sql("select * from a intersect all select * from a")
        .ok("(SELECT *\n"
            + "FROM `A`\n"
            + "INTERSECT ALL\n"
            + "SELECT *\n"
            + "FROM `A`)");
    sql("select * from a intersect distinct select * from a")
        .ok("(SELECT *\n"
            + "FROM `A`\n"
            + "INTERSECT\n"
            + "SELECT *\n"
            + "FROM `A`)");
  }

  @Test void testJoinCross() {
    sql("select * from a as a2 cross join b")
        .ok("SELECT *\n"
            + "FROM `A` AS `A2`\n"
            + "CROSS JOIN `B`");
  }

  @Test void testJoinOn() {
    sql("select * from a left join b on 1 = 1 and 2 = 2 where 3 = 3")
        .ok("SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN `B` ON ((1 = 1) AND (2 = 2))\n"
            + "WHERE (3 = 3)");
  }

  @Test void testJoinOnParentheses() {
    if (!Bug.TODO_FIXED) {
      return;
    }
    sql("select * from a\n"
        + " left join (b join c as c1 on 1 = 1) on 2 = 2\n"
        + "where 3 = 3")
        .ok("SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN (`B` INNER JOIN `C` AS `C1` ON (1 = 1)) ON (2 = 2)\n"
            + "WHERE (3 = 3)");
  }

  /**
   * Same as {@link #testJoinOnParentheses()} but fancy aliases.
   */
  @Test void testJoinOnParenthesesPlus() {
    if (!Bug.TODO_FIXED) {
      return;
    }
    sql("select * from a\n"
        + " left join (b as b1 (x, y) join (select * from c) c1 on 1 = 1) on 2 = 2\n"
        + "where 3 = 3")
        .ok("SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN (`B` AS `B1` (`X`, `Y`) INNER JOIN (SELECT *\n"
            + "FROM `C`) AS `C1` ON (1 = 1)) ON (2 = 2)\n"
            + "WHERE (3 = 3)");
  }

  @Test void testExplicitTableInJoin() {
    sql("select * from a left join (table b) on 2 = 2 where 3 = 3")
        .ok("SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN (TABLE `B`) ON (2 = 2)\n"
            + "WHERE (3 = 3)");
  }

  @Test void testSubQueryInJoin() {
    if (!Bug.TODO_FIXED) {
      return;
    }
    sql("select * from (select * from a cross join b) as ab\n"
        + " left join ((table c) join d on 2 = 2) on 3 = 3\n"
        + " where 4 = 4")
        .ok("SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `A`\n"
            + "CROSS JOIN `B`) AS `AB`\n"
            + "LEFT JOIN ((TABLE `C`) INNER JOIN `D` ON (2 = 2)) ON (3 = 3)\n"
            + "WHERE (4 = 4)");
  }

  @Test void testOuterJoinNoiseWord() {
    sql("select * from a left outer join b on 1 = 1 and 2 = 2 where 3 = 3")
        .ok("SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN `B` ON ((1 = 1) AND (2 = 2))\n"
            + "WHERE (3 = 3)");
  }

  @Test void testJoinQuery() {
    sql("select * from a join (select * from b) as b2 on true")
        .ok("SELECT *\n"
            + "FROM `A`\n"
            + "INNER JOIN (SELECT *\n"
            + "FROM `B`) AS `B2` ON TRUE");
  }

  @Test void testFullInnerJoinFails() {
    // cannot have more than one of INNER, FULL, LEFT, RIGHT, CROSS
    sql("select * from a ^full^ inner join b")
        .fails("(?s).*Encountered \"full inner\" at line 1, column 17.*");
  }

  @Test void testFullOuterJoin() {
    // OUTER is an optional extra to LEFT, RIGHT, or FULL
    sql("select * from a full outer join b")
        .ok("SELECT *\n"
            + "FROM `A`\n"
            + "FULL JOIN `B`");
  }

  @Test void testInnerOuterJoinFails() {
    sql("select * from a ^inner^ outer join b")
        .fails("(?s).*Encountered \"inner outer\" at line 1, column 17.*");
  }

  @Disabled
  @Test void testJoinAssociativity() {
    // joins are left-associative
    // 1. no parens needed
    sql("select * from (a natural left join b) left join c on b.c1 = c.c1")
        .ok("SELECT *\n"
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)\n");

    // 2. parens needed
    sql("select * from a natural left join (b left join c on b.c1 = c.c1)")
        .ok("SELECT *\n"
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)\n");

    // 3. same as 1
    sql("select * from a natural left join b left join c on b.c1 = c.c1")
        .ok("SELECT *\n"
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)\n");
  }

  // Note: "select * from a natural cross join b" is actually illegal SQL
  // ("cross" is the only join type which cannot be modified with the
  // "natural") but the parser allows it; we and catch it at validate time
  @Test void testNaturalCrossJoin() {
    sql("select * from a natural cross join b")
        .ok("SELECT *\n"
            + "FROM `A`\n"
            + "NATURAL CROSS JOIN `B`");
  }

  @Test void testJoinUsing() {
    sql("select * from a join b using (x)")
        .ok("SELECT *\n"
            + "FROM `A`\n"
            + "INNER JOIN `B` USING (`X`)");
    sql("select * from a join b using (^)^ where c = d")
        .fails("(?s).*Encountered \"[)]\" at line 1, column 31.*");
  }

  /** Tests CROSS APPLY, which is equivalent to CROSS JOIN and LEFT JOIN but
   * only supported in some conformance levels (e.g. SQL Server). */
  @Test void testApply() {
    final String pattern =
        "APPLY operator is not allowed under the current SQL conformance level";
    final String sql = "select * from dept\n"
        + "cross apply table(ramp(deptno)) as t(a^)^";
    sql(sql).fails(pattern);

    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    final String expected = "SELECT *\n"
        + "FROM `DEPT`\n"
        + "CROSS JOIN LATERAL TABLE(`RAMP`(`DEPTNO`)) AS `T` (`A`)";
    sql(sql).ok(expected);

    // Supported in Oracle 12 but not Oracle 10
    conformance = SqlConformanceEnum.ORACLE_10;
    sql(sql).fails(pattern);

    conformance = SqlConformanceEnum.ORACLE_12;
    sql(sql).ok(expected);
  }

  /** Tests OUTER APPLY. */
  @Test void testOuterApply() {
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    final String sql = "select * from dept outer apply table(ramp(deptno))";
    final String expected = "SELECT *\n"
        + "FROM `DEPT`\n"
        + "LEFT JOIN LATERAL TABLE(`RAMP`(`DEPTNO`)) ON TRUE";
    sql(sql).ok(expected);
  }

  @Test void testOuterApplySubQuery() {
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    final String sql = "select * from dept\n"
        + "outer apply (select * from emp where emp.deptno = dept.deptno)";
    final String expected = "SELECT *\n"
        + "FROM `DEPT`\n"
        + "LEFT JOIN LATERAL (SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)) ON TRUE";
    sql(sql).ok(expected);
  }

  @Test void testOuterApplyValues() {
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    final String sql = "select * from dept\n"
        + "outer apply (select * from emp where emp.deptno = dept.deptno)";
    final String expected = "SELECT *\n"
        + "FROM `DEPT`\n"
        + "LEFT JOIN LATERAL (SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)) ON TRUE";
    sql(sql).ok(expected);
  }

  /** Even in SQL Server conformance mode, we do not yet support
   * 'function(args)' as an abbreviation for 'table(function(args)'. */
  @Test void testOuterApplyFunctionFails() {
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    final String sql = "select * from dept outer apply ramp(deptno^)^)";
    sql(sql).fails("(?s).*Encountered \"\\)\" at .*");
  }

  @Test void testCrossOuterApply() {
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    final String sql = "select * from dept\n"
        + "cross apply table(ramp(deptno)) as t(a)\n"
        + "outer apply table(ramp2(a))";
    final String expected = "SELECT *\n"
        + "FROM `DEPT`\n"
        + "CROSS JOIN LATERAL TABLE(`RAMP`(`DEPTNO`)) AS `T` (`A`)\n"
        + "LEFT JOIN LATERAL TABLE(`RAMP2`(`A`)) ON TRUE";
    sql(sql).ok(expected);
  }

  @Test void testTableSample() {
    final String sql0 = "select * from ("
        + "  select * "
        + "  from emp "
        + "  join dept on emp.deptno = dept.deptno"
        + "  where gender = 'F'"
        + "  order by sal) tablesample substitute('medium')";
    final String expected0 = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM `EMP`\n"
        + "INNER JOIN `DEPT` ON (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)\n"
        + "WHERE (`GENDER` = 'F')\n"
        + "ORDER BY `SAL`) TABLESAMPLE SUBSTITUTE('MEDIUM')";
    sql(sql0).ok(expected0);

    final String sql1 = "select * "
        + "from emp as x tablesample substitute('medium') "
        + "join dept tablesample substitute('lar' /* split */ 'ge') on x.deptno = dept.deptno";
    final String expected1 = "SELECT *\n"
        + "FROM `EMP` AS `X` TABLESAMPLE SUBSTITUTE('MEDIUM')\n"
        + "INNER JOIN `DEPT` TABLESAMPLE SUBSTITUTE('LARGE') ON (`X`.`DEPTNO` = `DEPT`.`DEPTNO`)";
    sql(sql1).ok(expected1);

    final String sql2 = "select * "
        + "from emp as x tablesample bernoulli(50)";
    final String expected2 = "SELECT *\n"
        + "FROM `EMP` AS `X` TABLESAMPLE BERNOULLI(50.0)";
    sql(sql2).ok(expected2);

    final String sql3 = "select * "
        + "from emp as x "
        + "tablesample bernoulli(50) REPEATABLE(10) ";
    final String expected3 = "SELECT *\n"
        + "FROM `EMP` AS `X` TABLESAMPLE BERNOULLI(50.0) REPEATABLE(10)";
    sql(sql3).ok(expected3);

    // test repeatable with invalid int literal.
    sql("select * "
        + "from emp as x "
        + "tablesample bernoulli(50) REPEATABLE(^100000000000000000000^) ")
        .fails("Literal '100000000000000000000' "
            + "can not be parsed to type 'java\\.lang\\.Integer'");

    // test repeatable with invalid negative int literal.
    sql("select * "
        + "from emp as x "
        + "tablesample bernoulli(50) REPEATABLE(-^100000000000000000000^) ")
        .fails("Literal '100000000000000000000' "
            + "can not be parsed to type 'java\\.lang\\.Integer'");
  }

  @Test void testLiteral() {
    expr("'foo'").same();
    expr("100").same();
    sql("select 1 as uno, 'x' as x, null as n from emp")
        .ok("SELECT 1 AS `UNO`, 'x' AS `X`, NULL AS `N`\n"
            + "FROM `EMP`");

    // Even though it looks like a date, it's just a string.
    expr("'2004-06-01'")
        .ok("'2004-06-01'");
    expr("-.25")
        .ok("-0.25");
    expr("TIMESTAMP '2004-06-01 15:55:55'").same();
    expr("TIMESTAMP '2004-06-01 15:55:55.900'").same();
    expr("TIMESTAMP '2004-06-01 15:55:55.1234'")
        .ok("TIMESTAMP '2004-06-01 15:55:55.1234'");
    expr("TIMESTAMP '2004-06-01 15:55:55.1236'")
        .ok("TIMESTAMP '2004-06-01 15:55:55.1236'");
    expr("TIMESTAMP '2004-06-01 15:55:55.9999'")
        .ok("TIMESTAMP '2004-06-01 15:55:55.9999'");
    expr("NULL").same();
  }

  @Test void testContinuedLiteral() {
    expr("'abba'\n'abba'")
        .ok("'abba'\n'abba'");
    expr("'abba'\n'0001'")
        .ok("'abba'\n'0001'");
    expr("N'yabba'\n'dabba'\n'doo'")
        .ok("_ISO-8859-1'yabba'\n'dabba'\n'doo'");
    expr("_iso-8859-1'yabba'\n'dabba'\n'don''t'")
        .ok("_ISO-8859-1'yabba'\n'dabba'\n'don''t'");

    expr("x'01aa'\n'03ff'")
        .ok("X'01AA'\n'03FF'");

    // a bad hexstring
    sql("x'01aa'\n^'vvvv'^")
        .fails("Binary literal string must contain only characters '0' - '9', 'A' - 'F'");
  }

  /** Tests that ambiguity between extended string literals and character string
   * aliases is always resolved in favor of extended string literals. */
  @Test void testContinuedLiteralAlias() {
    final String expectingAlias = "Expecting alias, found character literal";

    // Not ambiguous, because of 'as'.
    final String sql0 = "select 1 an_alias,\n"
        + "  x'01'\n"
        + "  'ab' as x\n"
        + "from t";
    final String sql0b = "SELECT 1 AS `AN_ALIAS`, X'01'\n"
        + "'AB' AS `X`\n"
        + "FROM `T`";
    conformance = SqlConformanceEnum.DEFAULT;
    sql(sql0).ok(sql0b);
    conformance = SqlConformanceEnum.MYSQL_5;
    sql(sql0).ok(sql0b);
    conformance = SqlConformanceEnum.BIG_QUERY;
    sql(sql0).ok(sql0b);

    // Is 'ab' an alias or is it part of the x'01' 'ab' continued binary string
    // literal? It's ambiguous, but we prefer the latter.
    final String sql1 = "select 1 ^'an alias'^,\n"
        + "  x'01'\n"
        + "  'ab'\n"
        + "from t";
    final String sql1b = "SELECT 1 AS `an alias`, X'01'\n"
        + "'AB'\n"
        + "FROM `T`";
    conformance = SqlConformanceEnum.DEFAULT;
    sql(sql1).fails(expectingAlias);
    conformance = SqlConformanceEnum.MYSQL_5;
    sql(sql1).ok(sql1b);
    conformance = SqlConformanceEnum.BIG_QUERY;
    sql(sql1).ok(sql1b);

    // Parser prefers continued character and binary string literals over
    // character string aliases, regardless of whether the dialect allows
    // character string aliases.
    final String sql2 = "select 'continued'\n"
        + "  'char literal, not alias',\n"
        + "  x'01'\n"
        + "  'ab'\n"
        + "from t";
    final String sql2b = "SELECT 'continued'\n"
        + "'char literal, not alias', X'01'\n"
        + "'AB'\n"
        + "FROM `T`";
    conformance = SqlConformanceEnum.DEFAULT;
    sql(sql2).ok(sql2b);
    conformance = SqlConformanceEnum.MYSQL_5;
    sql(sql2).ok(sql2b);
    conformance = SqlConformanceEnum.BIG_QUERY;
    sql(sql2).ok(sql2b);
  }

  @Test void testMixedFrom() {
    // REVIEW: Is this syntax even valid?
    sql("select * from a join b using (x), c join d using (y)")
        .ok("SELECT *\n"
            + "FROM `A`\n"
            + "INNER JOIN `B` USING (`X`),\n"
            + "`C`\n"
            + "INNER JOIN `D` USING (`Y`)");
  }

  @Test void testMixedStar() {
    sql("select emp.*, 1 as foo from emp, dept")
        .ok("SELECT `EMP`.*, 1 AS `FOO`\n"
            + "FROM `EMP`,\n"
            + "`DEPT`");
  }

  @Test void testSchemaTableStar() {
    sql("select schem.emp.*, emp.empno * dept.deptno\n"
        + "from schem.emp, dept")
        .ok("SELECT `SCHEM`.`EMP`.*, (`EMP`.`EMPNO` * `DEPT`.`DEPTNO`)\n"
            + "FROM `SCHEM`.`EMP`,\n"
            + "`DEPT`");
  }

  @Test void testCatalogSchemaTableStar() {
    sql("select cat.schem.emp.* from cat.schem.emp")
        .ok("SELECT `CAT`.`SCHEM`.`EMP`.*\n"
            + "FROM `CAT`.`SCHEM`.`EMP`");
  }

  @Test void testAliasedStar() {
    // OK in parser; validator will give error
    sql("select emp.* as foo from emp")
        .ok("SELECT `EMP`.* AS `FOO`\n"
            + "FROM `EMP`");
  }

  @Test void testNotExists() {
    sql("select * from dept where not not exists (select * from emp) and true")
        .ok("SELECT *\n"
            + "FROM `DEPT`\n"
            + "WHERE ((NOT (NOT (EXISTS (SELECT *\n"
            + "FROM `EMP`)))) AND TRUE)");
  }

  @Test void testOrder() {
    sql("select * from emp order by empno, gender desc, deptno asc, empno asc, name desc")
        .ok("SELECT *\n"
            + "FROM `EMP`\n"
            + "ORDER BY `EMPNO`, `GENDER` DESC, `DEPTNO`, `EMPNO`, `NAME` DESC");
  }

  @Test void testOrderNullsFirst() {
    final String sql = "select * from emp\n"
        + "order by gender desc nulls last,\n"
        + " deptno asc nulls first,\n"
        + " empno nulls last";
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "ORDER BY `GENDER` DESC NULLS LAST, `DEPTNO` NULLS FIRST,"
        + " `EMPNO` NULLS LAST";
    sql(sql).ok(expected);
  }

  @Test void testOrderInternal() {
    sql("(select * from emp order by empno) union select * from emp")
        .ok("((SELECT *\n"
            + "FROM `EMP`\n"
            + "ORDER BY `EMPNO`)\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `EMP`)");

    sql("select * from (select * from t order by x, y) where a = b")
        .ok("SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `T`\n"
            + "ORDER BY `X`, `Y`)\n"
            + "WHERE (`A` = `B`)");
  }

  @Test void testOrderIllegalInExpression() {
    sql("select (select 1 from foo order by x,y) from t where a = b")
        .ok("SELECT (SELECT 1\n"
            + "FROM `FOO`\n"
            + "ORDER BY `X`, `Y`)\n"
            + "FROM `T`\n"
            + "WHERE (`A` = `B`)");
    sql("select (1 ^order^ by x, y) from t where a = b")
        .fails("ORDER BY unexpected");
  }

  @Test void testOrderOffsetFetch() {
    sql("select a from foo order by b, c offset 1 row fetch first 2 row only")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 2 ROWS ONLY");
    // as above, but ROWS rather than ROW
    sql("select a from foo order by b, c offset 1 rows fetch first 2 rows only")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 2 ROWS ONLY");
    // as above, but NEXT (means same as FIRST)
    sql("select a from foo order by b, c offset 1 rows fetch next 3 rows only")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 3 ROWS ONLY");
    // as above, but omit the ROWS noise word after OFFSET. This is not
    // compatible with SQL:2008 but allows the Postgres syntax
    // "LIMIT ... OFFSET".
    sql("select a from foo order by b, c offset 1 fetch next 3 rows only")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 3 ROWS ONLY");
    // as above, omit OFFSET
    sql("select a from foo order by b, c fetch next 3 rows only")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "FETCH NEXT 3 ROWS ONLY");
    // FETCH, no ORDER BY or OFFSET
    sql("select a from foo fetch next 4 rows only")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "FETCH NEXT 4 ROWS ONLY");
    // OFFSET, no ORDER BY or FETCH
    sql("select a from foo offset 1 row")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "OFFSET 1 ROWS");
    // OFFSET and FETCH, no ORDER BY
    sql("select a from foo offset 1 row fetch next 3 rows only")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 3 ROWS ONLY");
    // OFFSET and FETCH, with dynamic parameters
    sql("select a from foo offset ? row fetch next ? rows only")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "OFFSET ? ROWS\n"
            + "FETCH NEXT ? ROWS ONLY");
    // missing ROWS after FETCH
    sql("select a from foo offset 1 fetch next 3 ^only^")
        .fails("(?s).*Encountered \"only\" at .*");
    // FETCH before OFFSET is illegal
    sql("select a from foo fetch next 3 rows only ^offset^ 1")
        .fails("(?s).*Encountered \"offset\" at .*");
  }

  /**
   * "LIMIT ... OFFSET ..." is the postgres equivalent of SQL:2008
   * "OFFSET ... FETCH". It all maps down to a parse tree that looks like
   * SQL:2008.
   */
  @Test void testLimit() {
    sql("select a from foo order by b, c limit 2 offset 1")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 2 ROWS ONLY");
    sql("select a from foo order by b, c limit 2")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "FETCH NEXT 2 ROWS ONLY");
    sql("select a from foo order by b, c offset 1")
        .ok("SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4463">[CALCITE-4463]
   * JDBC adapter for Spark generates incorrect ORDER BY syntax</a>.
   *
   * <p>Similar to {@link #testLimit}, but parses and unparses in the Spark
   * dialect, which uses LIMIT and OFFSET rather than OFFSET and FETCH. */
  @Test void testLimitSpark() {
    final String sql1 = "select a from foo order by b, c limit 2 offset 1";
    final String expected1 = "SELECT A\n"
        + "FROM FOO\n"
        + "ORDER BY B, C\n"
        + "LIMIT 2\n"
        + "OFFSET 1";
    sql(sql1).withDialect(SparkSqlDialect.DEFAULT).ok(expected1);

    final String sql2 = "select a from foo order by b, c limit 2";
    final String expected2 = "SELECT A\n"
        + "FROM FOO\n"
        + "ORDER BY B, C\n"
        + "LIMIT 2";
    sql(sql2).withDialect(SparkSqlDialect.DEFAULT).ok(expected2);

    final String sql3 = "select a from foo order by b, c offset 1";
    final String expected3 = "SELECT A\n"
        + "FROM FOO\n"
        + "ORDER BY B, C\n"
        + "OFFSET 1";
    sql(sql3).withDialect(SparkSqlDialect.DEFAULT).ok(expected3);

    final String sql4 = "select a from foo offset 10";
    final String expected4 = "SELECT A\n"
        + "FROM FOO\n"
        + "OFFSET 10";
    sql(sql4).withDialect(SparkSqlDialect.DEFAULT).ok(expected4);

    final String sql5 = "select a from foo\n"
        + "union\n"
        + "select b from baz\n"
        + "limit 3";
    final String expected5 = "(SELECT A\n"
        + "FROM FOO\n"
        + "UNION\n"
        + "SELECT B\n"
        + "FROM BAZ)\n"
        + "LIMIT 3";
    sql(sql5).withDialect(SparkSqlDialect.DEFAULT).ok(expected5);
  }

  /** Test case that does not reproduce but is related to
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1238">[CALCITE-1238]
   * Unparsing LIMIT without ORDER BY after validation</a>. */
  @Test void testLimitWithoutOrder() {
    final String expected = "SELECT `A`\n"
        + "FROM `FOO`\n"
        + "FETCH NEXT 2 ROWS ONLY";
    sql("select a from foo limit 2")
        .ok(expected);
  }

  @Test void testLimitOffsetWithoutOrder() {
    final String expected = "SELECT `A`\n"
        + "FROM `FOO`\n"
        + "OFFSET 1 ROWS\n"
        + "FETCH NEXT 2 ROWS ONLY";
    sql("select a from foo limit 2 offset 1")
        .ok(expected);
  }

  @Test void testLimitStartCount() {
    conformance = SqlConformanceEnum.DEFAULT;
    final String error = "'LIMIT start, count' is not allowed under the "
        + "current SQL conformance level";
    sql("select a from foo limit 1,^2^")
        .fails(error);

    // "limit all" is equivalent to no limit
    final String expected0 = "SELECT `A`\n"
        + "FROM `FOO`";
    sql("select a from foo limit all")
        .ok(expected0);

    final String expected1 = "SELECT `A`\n"
        + "FROM `FOO`\n"
        + "ORDER BY `X`";
    sql("select a from foo order by x limit all")
        .ok(expected1);

    conformance = SqlConformanceEnum.LENIENT;
    final String expected2 = "SELECT `A`\n"
        + "FROM `FOO`\n"
        + "OFFSET 2 ROWS\n"
        + "FETCH NEXT 3 ROWS ONLY";
    sql("select a from foo limit 2,3")
        .ok(expected2);

    // "offset 4" overrides the earlier "2"
    final String expected3 = "SELECT `A`\n"
        + "FROM `FOO`\n"
        + "OFFSET 4 ROWS\n"
        + "FETCH NEXT 3 ROWS ONLY";
    sql("select a from foo limit 2,3 offset 4")
        .ok(expected3);

    // "fetch next 4" overrides the earlier "limit 3"
    final String expected4 = "SELECT `A`\n"
        + "FROM `FOO`\n"
        + "OFFSET 2 ROWS\n"
        + "FETCH NEXT 4 ROWS ONLY";
    sql("select a from foo limit 2,3 fetch next 4 rows only")
        .ok(expected4);

    // "limit start, all" is not valid
    sql("select a from foo limit 2, ^all^")
        .fails("(?s).*Encountered \"all\" at line 1.*");
  }

  @Test void testSqlInlineComment() {
    sql("select 1 from t --this is a comment\n")
        .ok("SELECT 1\n"
            + "FROM `T`");
    sql("select 1 from t--\n")
        .ok("SELECT 1\n"
            + "FROM `T`");
    sql("select 1 from t--this is a comment\n"
        + "where a>b-- this is comment\n")
        .ok("SELECT 1\n"
            + "FROM `T`\n"
            + "WHERE (`A` > `B`)");
    sql("select 1 from t\n--select")
        .ok("SELECT 1\n"
            + "FROM `T`");
  }

  @Test void testMultilineComment() {
    // on single line
    sql("select 1 /* , 2 */, 3 from t")
        .ok("SELECT 1, 3\n"
            + "FROM `T`");

    // on several lines
    sql("select /* 1,\n"
        + " 2,\n"
        + " */ 3 from t")
        .ok("SELECT 3\n"
            + "FROM `T`");

    // stuff inside comment
    sql("values ( /** 1, 2 + ** */ 3)")
        .ok("VALUES (ROW(3))");

    // comment in string is preserved
    sql("values ('a string with /* a comment */ in it')")
        .ok("VALUES (ROW('a string with /* a comment */ in it'))");

    // SQL:2003, 5.2, syntax rule # 8 "There shall be no <separator>
    // separating the <minus sign>s of a <simple comment introducer>".

    sql("values (- -1\n"
        + ")")
        .ok("VALUES (ROW(1))");

    sql("values (--1+\n"
        + "2)")
        .ok("VALUES (ROW(2))");

    // end of multiline comment without start
    if (Bug.FRG73_FIXED) {
      sql("values (1 */ 2)")
          .fails("xx");
    }

    // SQL:2003, 5.2, syntax rule #10 "Within a <bracket comment context>,
    // any <solidus> immediately followed by an <asterisk> without any
    // intervening <separator> shall be considered to be the <bracketed
    // comment introducer> for a <separator> that is a <bracketed
    // comment>".

    // comment inside a comment
    // Spec is unclear what should happen, but currently it crashes the
    // parser, and that's bad
    if (Bug.FRG73_FIXED) {
      sql("values (1 + /* comment /* inner comment */ */ 2)").ok("xx");
    }

    // single-line comment inside multiline comment is illegal
    //
    // SQL-2003, 5.2: "Note 63 - Conforming programs should not place
    // <simple comment> within a <bracketed comment> because if such a
    // <simple comment> contains the sequence of characters "*/" without
    // a preceding "/*" in the same <simple comment>, it will prematurely
    // terminate the containing <bracketed comment>.
    if (Bug.FRG73_FIXED) {
      final String sql = "values /* multiline contains -- singline */\n"
          + " (1)";
      sql(sql).fails("xxx");
    }

    // non-terminated multi-line comment inside single-line comment
    if (Bug.FRG73_FIXED) {
      // Test should fail, and it does, but it should give "*/" as the
      // erroneous token.
      final String sql = "values ( -- rest of line /* a comment\n"
          + " 1, ^*/^ 2)";
      sql(sql).fails("Encountered \"/\\*\" at");
    }

    sql("values (1 + /* comment -- rest of line\n"
        + " rest of comment */ 2)")
        .ok("VALUES (ROW((1 + 2)))");

    // multiline comment inside single-line comment
    sql("values -- rest of line /* a comment */\n"
        + "(1)")
        .ok("VALUES (ROW(1))");

    // non-terminated multiline comment inside single-line comment
    sql("values -- rest of line /* a comment\n"
        + "(1)")
        .ok("VALUES (ROW(1))");

    // even if comment abuts the tokens at either end, it becomes a space
    sql("values ('abc'/* a comment*/'def')")
        .ok("VALUES (ROW('abc'\n'def'))");

    // comment which starts as soon as it has begun
    sql("values /**/ (1)")
        .ok("VALUES (ROW(1))");
  }

  // expressions
  @Test void testParseNumber() {
    // Exacts
    expr("1").ok("1");
    expr("+1.").ok("1");
    expr("-1").ok("-1");
    expr("- -1").ok("1");
    expr("1.0").ok("1.0");
    expr("-3.2").ok("-3.2");
    expr("1.").ok("1");
    expr(".1").ok("0.1");
    expr("2500000000").ok("2500000000");
    expr("5000000000").ok("5000000000");

    // Approximates
    expr("1e1").ok("1E1");
    expr("+1e1").ok("1E1");
    expr("1.1e1").ok("1.1E1");
    expr("1.1e+1").ok("1.1E1");
    expr("1.1e-1").ok("1.1E-1");
    expr("+1.1e-1").ok("1.1E-1");
    expr("1.E3").ok("1E3");
    expr("1.e-3").ok("1E-3");
    expr("1.e+3").ok("1E3");
    expr(".5E3").ok("5E2");
    expr("+.5e3").ok("5E2");
    expr("-.5E3").ok("-5E2");
    expr(".5e-32").ok("5E-33");

    // Mix integer/decimals/approx
    expr("3. + 2").ok("(3 + 2)");
    expr("1++2+3").ok("((1 + 2) + 3)");
    expr("1- -2").ok("(1 - -2)");
    expr("1++2.3e-4++.5e-6++.7++8").ok("((((1 + 2.3E-4) + 5E-7) + 0.7) + 8)");
    expr("1- -2.3e-4 - -.5e-6  -\n"
        + "-.7++8")
        .ok("((((1 - -2.3E-4) - -5E-7) - -0.7) + 8)");
    expr("1+-2.*-3.e-1/-4")
        .ok("(1 + ((-2 * -3E-1) / -4))");
  }

  @Test void testParseNumberFails() {
    sql("SELECT 0.5e1^.1^ from t")
        .fails("(?s).*Encountered .*\\.1.* at line 1.*");
  }

  @Test void testMinusPrefixInExpression() {
    expr("-(1+2)")
        .ok("(- (1 + 2))");
  }

  // operator precedence
  @Test void testPrecedence0() {
    expr("1 + 2 * 3 * 4 + 5")
        .ok("((1 + ((2 * 3) * 4)) + 5)");
  }

  @Test void testPrecedence1() {
    expr("1 + 2 * (3 * (4 + 5))")
        .ok("(1 + (2 * (3 * (4 + 5))))");
  }

  @Test void testPrecedence2() {
    expr("- - 1").ok("1"); // special case for unary minus
  }

  @Test void testPrecedence2b() {
    expr("not not 1").ok("(NOT (NOT 1))"); // two prefixes
  }

  @Test void testPrecedence3() {
    expr("- 1 is null").ok("(-1 IS NULL)"); // prefix vs. postfix
  }

  @Test void testPrecedence4() {
    expr("1 - -2").ok("(1 - -2)"); // infix, prefix '-'
  }

  @Test void testPrecedence5() {
    expr("1++2").ok("(1 + 2)"); // infix, prefix '+'
    expr("1+ +2").ok("(1 + 2)"); // infix, prefix '+'
  }

  @Test void testPrecedenceSetOps() {
    final String sql = "select * from a union "
        + "select * from b intersect "
        + "select * from c intersect "
        + "select * from d except "
        + "select * from e except "
        + "select * from f union "
        + "select * from g";
    final String expected = "((((SELECT *\n"
        + "FROM `A`\n"
        + "UNION\n"
        + "((SELECT *\n"
        + "FROM `B`\n"
        + "INTERSECT\n"
        + "SELECT *\n"
        + "FROM `C`)\n"
        + "INTERSECT\n"
        + "SELECT *\n"
        + "FROM `D`))\n"
        + "EXCEPT\n"
        + "SELECT *\n"
        + "FROM `E`)\n"
        + "EXCEPT\n"
        + "SELECT *\n"
        + "FROM `F`)\n"
        + "UNION\n"
        + "SELECT *\n"
        + "FROM `G`)";
    sql(sql).ok(expected);
  }

  @Test void testQueryInFrom() {
    // one query with 'as', the other without
    sql("select * from (select * from emp) as e join (select * from dept) d")
        .ok("SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `EMP`) AS `E`\n"
            + "INNER JOIN (SELECT *\n"
            + "FROM `DEPT`) AS `D`");
  }

  @Test void testQuotesInString() {
    expr("'a''b'")
        .ok("'a''b'");
    expr("'''x'")
        .ok("'''x'");
    expr("''")
        .ok("''");
    expr("'Quoted strings aren''t \"hard\"'")
        .ok("'Quoted strings aren''t \"hard\"'");
  }

  @Test void testScalarQueryInWhere() {
    sql("select * from emp where 3 = (select count(*) from dept where dept.deptno = emp.deptno)")
        .ok("SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (3 = (SELECT COUNT(*)\n"
            + "FROM `DEPT`\n"
            + "WHERE (`DEPT`.`DEPTNO` = `EMP`.`DEPTNO`)))");
  }

  @Test void testScalarQueryInSelect() {
    sql("select x, (select count(*) from dept where dept.deptno = emp.deptno) from emp")
        .ok("SELECT `X`, (SELECT COUNT(*)\n"
            + "FROM `DEPT`\n"
            + "WHERE (`DEPT`.`DEPTNO` = `EMP`.`DEPTNO`))\n"
            + "FROM `EMP`");
  }

  @Test void testSelectList() {
    sql("select * from emp, dept")
        .ok("SELECT *\n"
            + "FROM `EMP`,\n"
            + "`DEPT`");
  }

  @Test void testSelectWithoutFrom() {
    sql("select 2+2")
        .ok("SELECT (2 + 2)");
  }

  @Test void testSelectWithoutFrom2() {
    sql("select 2+2 as x, 'a' as y")
        .ok("SELECT (2 + 2) AS `X`, 'a' AS `Y`");
  }

  @Test void testSelectDistinctWithoutFrom() {
    sql("select distinct 2+2 as x, 'a' as y")
        .ok("SELECT DISTINCT (2 + 2) AS `X`, 'a' AS `Y`");
  }

  @Test void testSelectWithoutFromWhereFails() {
    sql("select 2+2 as x ^where^ 1 > 2")
        .fails("(?s).*Encountered \"where\" at line .*");
  }

  @Test void testSelectWithoutFromGroupByFails() {
    sql("select 2+2 as x ^group^ by 1, 2")
        .fails("(?s).*Encountered \"group\" at line .*");
  }

  @Test void testSelectWithoutFromHavingFails() {
    sql("select 2+2 as x ^having^ 1 > 2")
        .fails("(?s).*Encountered \"having\" at line .*");
  }

  @Test void testSelectList3() {
    sql("select 1, emp.*, 2 from emp")
        .ok("SELECT 1, `EMP`.*, 2\n"
            + "FROM `EMP`");
  }

  @Test void testSelectList4() {
    sql("select ^from^ emp")
        .fails("(?s).*Encountered \"from\" at line .*");
  }

  @Test void testStar() {
    sql("select * from emp")
        .ok("SELECT *\n"
            + "FROM `EMP`");
  }

  @Test void testCompoundStar() {
    final String sql = "select sales.emp.address.zipcode,\n"
        + " sales.emp.address.*\n"
        + "from sales.emp";
    final String expected = "SELECT `SALES`.`EMP`.`ADDRESS`.`ZIPCODE`,"
        + " `SALES`.`EMP`.`ADDRESS`.*\n"
        + "FROM `SALES`.`EMP`";
    sql(sql).ok(expected);
  }

  @Test void testSelectDistinct() {
    sql("select distinct foo from bar")
        .ok("SELECT DISTINCT `FOO`\n"
            + "FROM `BAR`");
  }

  @Test void testSelectAll() {
    // "unique" is the default -- so drop the keyword
    sql("select * from (select all foo from bar) as xyz")
        .ok("SELECT *\n"
            + "FROM (SELECT ALL `FOO`\n"
            + "FROM `BAR`) AS `XYZ`");
  }

  @Test void testSelectStream() {
    sql("select stream foo from bar")
        .ok("SELECT STREAM `FOO`\n"
            + "FROM `BAR`");
  }

  @Test void testSelectStreamDistinct() {
    sql("select stream distinct foo from bar")
        .ok("SELECT STREAM DISTINCT `FOO`\n"
            + "FROM `BAR`");
  }

  @Test void testWhere() {
    sql("select * from emp where empno > 5 and gender = 'F'")
        .ok("SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE ((`EMPNO` > 5) AND (`GENDER` = 'F'))");
  }

  @Test void testNestedSelect() {
    sql("select * from (select * from emp)")
        .ok("SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `EMP`)");
  }

  @Test void testValues() {
    sql("values(1,'two')")
        .ok("VALUES (ROW(1, 'two'))");
  }

  @Test void testValuesExplicitRow() {
    sql("values row(1,'two')")
        .ok("VALUES (ROW(1, 'two'))");
  }

  @Test void testFromValues() {
    sql("select * from (values(1,'two'), 3, (4, 'five'))")
        .ok("SELECT *\n"
            + "FROM (VALUES (ROW(1, 'two')),\n"
            + "(ROW(3)),\n"
            + "(ROW(4, 'five')))");
  }

  @Test void testFromValuesWithoutParens() {
    sql("select 1 from ^values^('x')")
        .fails("(?s)Encountered \"values\" at line 1, column 15\\.\n"
            + "Was expecting one of:\n"
            + "    \"LATERAL\" \\.\\.\\.\n"
            + "    \"TABLE\" \\.\\.\\.\n"
            + "    \"UNNEST\" \\.\\.\\.\n"
            + "    <IDENTIFIER> \\.\\.\\.\n"
            + "    <HYPHENATED_IDENTIFIER> \\.\\.\\.\n"
            + "    <QUOTED_IDENTIFIER> \\.\\.\\.\n"
            + "    <BACK_QUOTED_IDENTIFIER> \\.\\.\\.\n"
            + "    <BRACKET_QUOTED_IDENTIFIER> \\.\\.\\.\n"
            + "    <UNICODE_QUOTED_IDENTIFIER> \\.\\.\\.\n"
            + "    \"\\(\" \\.\\.\\.\n.*");
  }

  @Test void testEmptyValues() {
    sql("select * from (values(^)^)")
        .fails("(?s).*Encountered \"\\)\" at .*");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-493">[CALCITE-493]
   * Add EXTEND clause, for defining columns and their types at query/DML
   * time</a>. */
  @Test void testTableExtend() {
    sql("select * from emp extend (x int, y varchar(10) not null)")
        .ok("SELECT *\n"
            + "FROM `EMP` EXTEND (`X` INTEGER, `Y` VARCHAR(10))");
    sql("select * from emp extend (x int, y varchar(10) not null) where true")
        .ok("SELECT *\n"
            + "FROM `EMP` EXTEND (`X` INTEGER, `Y` VARCHAR(10))\n"
            + "WHERE TRUE");
    // with table alias
    sql("select * from emp extend (x int, y varchar(10) not null) as t")
        .ok("SELECT *\n"
            + "FROM `EMP` EXTEND (`X` INTEGER, `Y` VARCHAR(10)) AS `T`");
    // as previous, without AS
    sql("select * from emp extend (x int, y varchar(10) not null) t")
        .ok("SELECT *\n"
            + "FROM `EMP` EXTEND (`X` INTEGER, `Y` VARCHAR(10)) AS `T`");
    // with table alias and column alias list
    sql("select * from emp extend (x int, y varchar(10) not null) as t(a, b)")
        .ok("SELECT *\n"
            + "FROM `EMP` EXTEND (`X` INTEGER, `Y` VARCHAR(10)) AS `T` (`A`, `B`)");
    // as previous, without AS
    sql("select * from emp extend (x int, y varchar(10) not null) t(a, b)")
        .ok("SELECT *\n"
            + "FROM `EMP` EXTEND (`X` INTEGER, `Y` VARCHAR(10)) AS `T` (`A`, `B`)");
    // omit EXTEND
    sql("select * from emp (x int, y varchar(10) not null) t(a, b)")
        .ok("SELECT *\n"
            + "FROM `EMP` EXTEND (`X` INTEGER, `Y` VARCHAR(10)) AS `T` (`A`, `B`)");
    sql("select * from emp (x int, y varchar(10) not null) where x = y")
        .ok("SELECT *\n"
            + "FROM `EMP` EXTEND (`X` INTEGER, `Y` VARCHAR(10))\n"
            + "WHERE (`X` = `Y`)");
  }

  @Test void testExplicitTable() {
    sql("table emp")
        .ok("(TABLE `EMP`)");

    sql("table ^123^")
        .fails("(?s)Encountered \"123\" at line 1, column 7\\.\n.*");
  }

  @Test void testExplicitTableOrdered() {
    sql("table emp order by name")
        .ok("(TABLE `EMP`)\n"
            + "ORDER BY `NAME`");
  }

  @Test void testSelectFromExplicitTable() {
    sql("select * from (table emp)")
        .ok("SELECT *\n"
            + "FROM (TABLE `EMP`)");
  }

  @Test void testSelectFromBareExplicitTableFails() {
    sql("select * from table ^emp^")
        .fails("(?s).*Encountered \"emp\" at .*");

    sql("select * from (table ^(^select empno from emp))")
        .fails("(?s)Encountered \"\\(\".*");
  }

  @Test void testCollectionTable() {
    sql("select * from table(ramp(3, 4))")
        .ok("SELECT *\n"
            + "FROM TABLE(`RAMP`(3, 4))");
  }

  @Test void testDescriptor() {
    sql("select * from table(ramp(descriptor(column_name)))")
        .ok("SELECT *\n"
            + "FROM TABLE(`RAMP`(DESCRIPTOR(`COLUMN_NAME`)))");
    sql("select * from table(ramp(descriptor(\"COLUMN_NAME\")))")
        .ok("SELECT *\n"
            + "FROM TABLE(`RAMP`(DESCRIPTOR(`COLUMN_NAME`)))");
    sql("select * from table(ramp(descriptor(column_name1, column_name2, column_name3)))")
        .ok("SELECT *\n"
            + "FROM TABLE(`RAMP`(DESCRIPTOR(`COLUMN_NAME1`, `COLUMN_NAME2`, `COLUMN_NAME3`)))");
  }

  @Test void testCollectionTableWithCursorParam() {
    sql("select * from table(dedup(cursor(select * from emps),'name'))")
        .ok("SELECT *\n"
            + "FROM TABLE(`DEDUP`((CURSOR ((SELECT *\n"
            + "FROM `EMPS`))), 'name'))");
  }

  @Test void testCollectionTableWithColumnListParam() {
    sql("select * from table(dedup(cursor(select * from emps),"
        + "row(empno, name)))")
        .ok("SELECT *\n"
            + "FROM TABLE(`DEDUP`((CURSOR ((SELECT *\n"
            + "FROM `EMPS`))), (ROW(`EMPNO`, `NAME`))))");
  }

  @Test void testLateral() {
    // Bad: LATERAL table
    sql("select * from lateral ^emp^")
        .fails("(?s)Encountered \"emp\" at .*");
    sql("select * from lateral table ^emp^ as e")
        .fails("(?s)Encountered \"emp\" at .*");

    // Bad: LATERAL TABLE schema.table
    sql("select * from lateral table ^scott^.emp")
        .fails("(?s)Encountered \"scott\" at .*");
    final String expected = "SELECT *\n"
        + "FROM LATERAL TABLE(`RAMP`(1))";

    // Good: LATERAL TABLE function(arg, arg)
    sql("select * from lateral table(ramp(1))")
        .ok(expected);
    sql("select * from lateral table(ramp(1)) as t")
        .ok(expected + " AS `T`");
    sql("select * from lateral table(ramp(1)) as t(x)")
        .ok(expected + " AS `T` (`X`)");
    // Bad: Parentheses make it look like a sub-query
    sql("select * from lateral (table^(^ramp(1)))")
        .fails("(?s)Encountered \"\\(\" at .*");

    // Good: LATERAL (subQuery)
    final String expected2 = "SELECT *\n"
        + "FROM LATERAL (SELECT *\n"
        + "FROM `EMP`)";
    sql("select * from lateral (select * from emp)")
        .ok(expected2);
    sql("select * from lateral (select * from emp) as t")
        .ok(expected2 + " AS `T`");
    sql("select * from lateral (select * from emp) as t(x)")
        .ok(expected2 + " AS `T` (`X`)");
  }

  @Test void testTemporalTable() {
    final String sql0 = "select stream * from orders, products\n"
        + "for system_time as of TIMESTAMP '2011-01-02 00:00:00'";
    final String expected0 = "SELECT STREAM *\n"
        + "FROM `ORDERS`,\n"
        + "`PRODUCTS` FOR SYSTEM_TIME AS OF TIMESTAMP '2011-01-02 00:00:00'";
    sql(sql0).ok(expected0);

    // Can not use explicit LATERAL keyword.
    final String sql1 = "select stream * from orders, LATERAL ^products_temporal^\n"
        + "for system_time as of TIMESTAMP '2011-01-02 00:00:00'";
    final String error = "(?s)Encountered \"products_temporal\" at line .*";
    sql(sql1).fails(error);

    // Inner join with a specific timestamp
    final String sql2 = "select stream * from orders join products_temporal\n"
        + "for system_time as of timestamp '2011-01-02 00:00:00'\n"
        + "on orders.productid = products_temporal.productid";
    final String expected2 = "SELECT STREAM *\n"
        + "FROM `ORDERS`\n"
        + "INNER JOIN `PRODUCTS_TEMPORAL` "
        + "FOR SYSTEM_TIME AS OF TIMESTAMP '2011-01-02 00:00:00' "
        + "ON (`ORDERS`.`PRODUCTID` = `PRODUCTS_TEMPORAL`.`PRODUCTID`)";
    sql(sql2).ok(expected2);

    // Left join with a timestamp field
    final String sql3 = "select stream * from orders left join products_temporal\n"
        + "for system_time as of orders.rowtime "
        + "on orders.productid = products_temporal.productid";
    final String expected3 = "SELECT STREAM *\n"
        + "FROM `ORDERS`\n"
        + "LEFT JOIN `PRODUCTS_TEMPORAL` "
        + "FOR SYSTEM_TIME AS OF `ORDERS`.`ROWTIME` "
        + "ON (`ORDERS`.`PRODUCTID` = `PRODUCTS_TEMPORAL`.`PRODUCTID`)";
    sql(sql3).ok(expected3);

    // Left join with a timestamp expression
    final String sql4 = "select stream * from orders left join products_temporal\n"
        + "for system_time as of orders.rowtime - INTERVAL '3' DAY "
        + "on orders.productid = products_temporal.productid";
    final String expected4 = "SELECT STREAM *\n"
        + "FROM `ORDERS`\n"
        + "LEFT JOIN `PRODUCTS_TEMPORAL` "
        + "FOR SYSTEM_TIME AS OF (`ORDERS`.`ROWTIME` - INTERVAL '3' DAY) "
        + "ON (`ORDERS`.`PRODUCTID` = `PRODUCTS_TEMPORAL`.`PRODUCTID`)";
    sql(sql4).ok(expected4);
  }

  @Test void testCollectionTableWithLateral() {
    final String sql = "select * from dept, lateral table(ramp(dept.deptno))";
    final String expected = "SELECT *\n"
        + "FROM `DEPT`,\n"
        + "LATERAL TABLE(`RAMP`(`DEPT`.`DEPTNO`))";
    sql(sql).ok(expected);
  }

  @Test void testCollectionTableWithLateral2() {
    final String sql = "select * from dept as d\n"
        + "cross join lateral table(ramp(dept.deptno)) as r";
    final String expected = "SELECT *\n"
        + "FROM `DEPT` AS `D`\n"
        + "CROSS JOIN LATERAL TABLE(`RAMP`(`DEPT`.`DEPTNO`)) AS `R`";
    sql(sql).ok(expected);
  }

  @Test void testCollectionTableWithLateral3() {
    // LATERAL before first table in FROM clause doesn't achieve anything, but
    // it's valid.
    final String sql = "select * from lateral table(ramp(dept.deptno)), dept";
    final String expected = "SELECT *\n"
        + "FROM LATERAL TABLE(`RAMP`(`DEPT`.`DEPTNO`)),\n"
        + "`DEPT`";
    sql(sql).ok(expected);
  }

  @Test void testIllegalCursors() {
    sql("select ^cursor^(select * from emps) from emps")
        .fails("CURSOR expression encountered in illegal context");
    sql("call list(^cursor^(select * from emps))")
        .fails("CURSOR expression encountered in illegal context");
    sql("select f(^cursor^(select * from emps)) from emps")
        .fails("CURSOR expression encountered in illegal context");
  }

  @Test void testExplain() {
    final String sql = "explain plan for select * from emps";
    final String expected = "EXPLAIN PLAN"
        + " INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
        + "SELECT *\n"
        + "FROM `EMPS`";
    sql(sql).ok(expected);
  }

  @Test void testExplainAsXml() {
    final String sql = "explain plan as xml for select * from emps";
    final String expected = "EXPLAIN PLAN"
        + " INCLUDING ATTRIBUTES WITH IMPLEMENTATION AS XML FOR\n"
        + "SELECT *\n"
        + "FROM `EMPS`";
    sql(sql).ok(expected);
  }

  @Test void testExplainAsDot() {
    final String sql = "explain plan as dot for select * from emps";
    final String expected = "EXPLAIN PLAN"
        + " INCLUDING ATTRIBUTES WITH IMPLEMENTATION AS DOT FOR\n"
        + "SELECT *\n"
        + "FROM `EMPS`";
    sql(sql).ok(expected);
  }

  @Test void testExplainAsJson() {
    final String sql = "explain plan as json for select * from emps";
    final String expected = "EXPLAIN PLAN"
        + " INCLUDING ATTRIBUTES WITH IMPLEMENTATION AS JSON FOR\n"
        + "SELECT *\n"
        + "FROM `EMPS`";
    sql(sql).ok(expected);
  }

  @Test void testExplainWithImpl() {
    sql("explain plan with implementation for select * from emps")
        .ok("EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
            + "SELECT *\n"
            + "FROM `EMPS`");
  }

  @Test void testExplainWithoutImpl() {
    sql("explain plan without implementation for select * from emps")
        .ok("EXPLAIN PLAN INCLUDING ATTRIBUTES WITHOUT IMPLEMENTATION FOR\n"
            + "SELECT *\n"
            + "FROM `EMPS`");
  }

  @Test void testExplainWithType() {
    sql("explain plan with type for (values (true))")
        .ok("EXPLAIN PLAN INCLUDING ATTRIBUTES WITH TYPE FOR\n"
            + "(VALUES (ROW(TRUE)))");
  }

  @Test void testExplainJsonFormat() {
    final String sql = "explain plan as json for select * from emps";
    TesterImpl tester = (TesterImpl) getTester();
    SqlExplain sqlExplain = (SqlExplain) tester.parseStmtsAndHandleEx(sql).get(0);
    assertThat(sqlExplain.isJson(), is(true));
  }

  @Test void testDescribeSchema() {
    sql("describe schema A")
        .ok("DESCRIBE SCHEMA `A`");
    // Currently DESCRIBE DATABASE, DESCRIBE CATALOG become DESCRIBE SCHEMA.
    // See [CALCITE-1221] Implement DESCRIBE DATABASE, CATALOG, STATEMENT
    sql("describe database A")
        .ok("DESCRIBE SCHEMA `A`");
    sql("describe catalog A")
        .ok("DESCRIBE SCHEMA `A`");
  }

  @Test void testDescribeTable() {
    sql("describe emps")
        .ok("DESCRIBE TABLE `EMPS`");
    sql("describe \"emps\"")
        .ok("DESCRIBE TABLE `emps`");
    sql("describe s.emps")
        .ok("DESCRIBE TABLE `S`.`EMPS`");
    sql("describe db.c.s.emps")
        .ok("DESCRIBE TABLE `DB`.`C`.`S`.`EMPS`");
    sql("describe emps col1")
        .ok("DESCRIBE TABLE `EMPS` `COL1`");

    // BigQuery allows hyphens in schema (project) names
    sql("describe foo-bar.baz")
        .withDialect(BIG_QUERY)
        .ok("DESCRIBE TABLE `foo-bar`.baz");
    sql("describe table foo-bar.baz")
        .withDialect(BIG_QUERY)
        .ok("DESCRIBE TABLE `foo-bar`.baz");

    // table keyword is OK
    sql("describe table emps col1")
        .ok("DESCRIBE TABLE `EMPS` `COL1`");
    // character literal for column name not ok
    sql("describe emps ^'col_'^")
        .fails("(?s).*Encountered \"\\\\'col_\\\\'\" at .*");
    // composite column name not ok
    sql("describe emps c1^.^c2")
        .fails("(?s).*Encountered \"\\.\" at .*");
  }

  @Test void testDescribeStatement() {
    // Currently DESCRIBE STATEMENT becomes EXPLAIN.
    // See [CALCITE-1221] Implement DESCRIBE DATABASE, CATALOG, STATEMENT
    final String expected0 = ""
        + "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
        + "SELECT *\n"
        + "FROM `EMPS`";
    sql("describe statement select * from emps").ok(expected0);
    final String expected1 = ""
        + "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
        + "(SELECT *\n"
        + "FROM `EMPS`\n"
        + "ORDER BY 2)";
    sql("describe statement select * from emps order by 2").ok(expected1);
    sql("describe select * from emps").ok(expected0);
    sql("describe (select * from emps)").ok(expected0);
    sql("describe statement (select * from emps)").ok(expected0);
    final String expected2 = ""
        + "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
        + "(SELECT `DEPTNO`\n"
        + "FROM `EMPS`\n"
        + "UNION\n"
        + "SELECT `DEPTNO`\n"
        + "FROM `DEPTS`)";
    sql("describe select deptno from emps union select deptno from depts").ok(expected2);
    final String expected3 = ""
        + "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
        + "INSERT INTO `EMPS`\n"
        + "VALUES (ROW(1, 'a'))";
    sql("describe insert into emps values (1, 'a')").ok(expected3);
    // only allow query or DML, not explain, inside describe
    sql("describe ^explain^ plan for select * from emps")
        .fails("(?s).*Encountered \"explain\" at .*");
    sql("describe statement ^explain^ plan for select * from emps")
        .fails("(?s).*Encountered \"explain\" at .*");
  }

  @Test void testSelectIsNotDdl() {
    sql("select 1 from t")
        .node(not(isDdl()));
  }

  @Test void testInsertSelect() {
    final String expected = "INSERT INTO `EMPS`\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    sql("insert into emps select * from emps")
        .ok(expected)
        .node(not(isDdl()));
  }

  @Test void testInsertUnion() {
    final String expected = "INSERT INTO `EMPS`\n"
        + "(SELECT *\n"
        + "FROM `EMPS1`\n"
        + "UNION\n"
        + "SELECT *\n"
        + "FROM `EMPS2`)";
    sql("insert into emps select * from emps1 union select * from emps2")
        .ok(expected);
  }

  @Test void testInsertValues() {
    final String expected = "INSERT INTO `EMPS`\n"
        + "VALUES (ROW(1, 'Fredkin'))";
    sql("insert into emps values (1,'Fredkin')")
        .ok(expected)
        .node(not(isDdl()));
  }

  @Test void testInsertValuesDefault() {
    final String expected = "INSERT INTO `EMPS`\n"
        + "VALUES (ROW(1, DEFAULT, 'Fredkin'))";
    sql("insert into emps values (1,DEFAULT,'Fredkin')")
        .ok(expected)
        .node(not(isDdl()));
  }

  @Test void testInsertValuesRawDefault() {
    final String expected = "INSERT INTO `EMPS`\n"
        + "VALUES (ROW(DEFAULT))";
    sql("insert into emps values ^default^")
        .fails("(?s).*Encountered \"default\" at .*");
    sql("insert into emps values (default)")
        .ok(expected)
        .node(not(isDdl()));
  }

  @Test void testInsertColumnList() {
    final String expected = "INSERT INTO `EMPS` (`X`, `Y`)\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    sql("insert into emps(x,y) select * from emps")
        .ok(expected);
  }

  @Test void testInsertCaseSensitiveColumnList() {
    final String expected = "INSERT INTO `emps` (`x`, `y`)\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    sql("insert into \"emps\"(\"x\",\"y\") select * from emps")
        .ok(expected);
  }

  @Test void testInsertExtendedColumnList() {
    String expected = "INSERT INTO `EMPS` EXTEND (`Z` BOOLEAN) (`X`, `Y`)\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    sql("insert into emps(z boolean)(x,y) select * from emps")
        .ok(expected);
    conformance = SqlConformanceEnum.LENIENT;
    expected = "INSERT INTO `EMPS` EXTEND (`Z` BOOLEAN) (`X`, `Y`, `Z`)\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    sql("insert into emps(x, y, z boolean) select * from emps")
        .ok(expected);
  }

  @Test void testUpdateExtendedColumnList() {
    final String expected = "UPDATE `EMPDEFAULTS` EXTEND (`EXTRA` BOOLEAN, `NOTE` VARCHAR)"
        + " SET `DEPTNO` = 1"
        + ", `EXTRA` = TRUE"
        + ", `EMPNO` = 20"
        + ", `ENAME` = 'Bob'"
        + ", `NOTE` = 'legion'\n"
        + "WHERE (`DEPTNO` = 10)";
    sql("update empdefaults(extra BOOLEAN, note VARCHAR)"
        + " set deptno = 1, extra = true, empno = 20, ename = 'Bob', note = 'legion'"
        + " where deptno = 10")
        .ok(expected);
  }


  @Test void testUpdateCaseSensitiveExtendedColumnList() {
    final String expected = "UPDATE `EMPDEFAULTS` EXTEND (`extra` BOOLEAN, `NOTE` VARCHAR)"
        + " SET `DEPTNO` = 1"
        + ", `extra` = TRUE"
        + ", `EMPNO` = 20"
        + ", `ENAME` = 'Bob'"
        + ", `NOTE` = 'legion'\n"
        + "WHERE (`DEPTNO` = 10)";
    sql("update empdefaults(\"extra\" BOOLEAN, note VARCHAR)"
        + " set deptno = 1, \"extra\" = true, empno = 20, ename = 'Bob', note = 'legion'"
        + " where deptno = 10")
        .ok(expected);
  }

  @Test void testInsertCaseSensitiveExtendedColumnList() {
    String expected = "INSERT INTO `emps` EXTEND (`z` BOOLEAN) (`x`, `y`)\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    sql("insert into \"emps\"(\"z\" boolean)(\"x\",\"y\") select * from emps")
        .ok(expected);
    conformance = SqlConformanceEnum.LENIENT;
    expected = "INSERT INTO `emps` EXTEND (`z` BOOLEAN) (`x`, `y`, `z`)\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    sql("insert into \"emps\"(\"x\", \"y\", \"z\" boolean) select * from emps")
        .ok(expected);
  }

  @Test void testExplainInsert() {
    final String expected = "EXPLAIN PLAN INCLUDING ATTRIBUTES"
        + " WITH IMPLEMENTATION FOR\n"
        + "INSERT INTO `EMPS1`\n"
        + "(SELECT *\n"
        + "FROM `EMPS2`)";
    sql("explain plan for insert into emps1 select * from emps2")
        .ok(expected)
        .node(not(isDdl()));
  }

  @Test void testUpsertValues() {
    final String expected = "UPSERT INTO `EMPS`\n"
        + "VALUES (ROW(1, 'Fredkin'))";
    final String sql = "upsert into emps values (1,'Fredkin')";
    if (isReserved("UPSERT")) {
      sql(sql)
          .ok(expected)
          .node(not(isDdl()));
    }
  }

  @Test void testUpsertSelect() {
    final String sql = "upsert into emps select * from emp as e";
    final String expected = "UPSERT INTO `EMPS`\n"
        + "(SELECT *\n"
        + "FROM `EMP` AS `E`)";
    if (isReserved("UPSERT")) {
      sql(sql).ok(expected);
    }
  }

  @Test void testExplainUpsert() {
    final String sql = "explain plan for upsert into emps1 values (1, 2)";
    final String expected = "EXPLAIN PLAN INCLUDING ATTRIBUTES"
        + " WITH IMPLEMENTATION FOR\n"
        + "UPSERT INTO `EMPS1`\n"
        + "VALUES (ROW(1, 2))";
    if (isReserved("UPSERT")) {
      sql(sql).ok(expected);
    }
  }

  @Test void testDelete() {
    sql("delete from emps")
        .ok("DELETE FROM `EMPS`")
        .node(not(isDdl()));
  }

  @Test void testDeleteWhere() {
    sql("delete from emps where empno=12")
        .ok("DELETE FROM `EMPS`\n"
            + "WHERE (`EMPNO` = 12)");
  }

  @Test void testUpdate() {
    sql("update emps set empno = empno + 1, sal = sal - 1 where empno=12")
        .ok("UPDATE `EMPS` SET `EMPNO` = (`EMPNO` + 1)"
            + ", `SAL` = (`SAL` - 1)\n"
            + "WHERE (`EMPNO` = 12)");
  }

  @Test void testMergeSelectSource() {
    final String sql = "merge into emps e "
        + "using (select * from tempemps where deptno is null) t "
        + "on e.empno = t.empno "
        + "when matched then update "
        + "set name = t.name, deptno = t.deptno, salary = t.salary * .1 "
        + "when not matched then insert (name, dept, salary) "
        + "values(t.name, 10, t.salary * .15)";
    final String expected = "MERGE INTO `EMPS` AS `E`\n"
        + "USING (SELECT *\n"
        + "FROM `TEMPEMPS`\n"
        + "WHERE (`DEPTNO` IS NULL)) AS `T`\n"
        + "ON (`E`.`EMPNO` = `T`.`EMPNO`)\n"
        + "WHEN MATCHED THEN UPDATE SET `NAME` = `T`.`NAME`"
        + ", `DEPTNO` = `T`.`DEPTNO`"
        + ", `SALARY` = (`T`.`SALARY` * 0.1)\n"
        + "WHEN NOT MATCHED THEN INSERT (`NAME`, `DEPT`, `SALARY`) "
        + "(VALUES (ROW(`T`.`NAME`, 10, (`T`.`SALARY` * 0.15))))";
    sql(sql).ok(expected)
        .node(not(isDdl()));
  }

  /** Same as testMergeSelectSource but set with compound identifier. */
  @Test void testMergeSelectSource2() {
    final String sql = "merge into emps e "
        + "using (select * from tempemps where deptno is null) t "
        + "on e.empno = t.empno "
        + "when matched then update "
        + "set e.name = t.name, e.deptno = t.deptno, e.salary = t.salary * .1 "
        + "when not matched then insert (name, dept, salary) "
        + "values(t.name, 10, t.salary * .15)";
    final String expected = "MERGE INTO `EMPS` AS `E`\n"
        + "USING (SELECT *\n"
        + "FROM `TEMPEMPS`\n"
        + "WHERE (`DEPTNO` IS NULL)) AS `T`\n"
        + "ON (`E`.`EMPNO` = `T`.`EMPNO`)\n"
        + "WHEN MATCHED THEN UPDATE SET `E`.`NAME` = `T`.`NAME`"
        + ", `E`.`DEPTNO` = `T`.`DEPTNO`"
        + ", `E`.`SALARY` = (`T`.`SALARY` * 0.1)\n"
        + "WHEN NOT MATCHED THEN INSERT (`NAME`, `DEPT`, `SALARY`) "
        + "(VALUES (ROW(`T`.`NAME`, 10, (`T`.`SALARY` * 0.15))))";
    sql(sql).ok(expected)
        .node(not(isDdl()));
  }

  @Test void testMergeTableRefSource() {
    final String sql = "merge into emps e "
        + "using tempemps as t "
        + "on e.empno = t.empno "
        + "when matched then update "
        + "set name = t.name, deptno = t.deptno, salary = t.salary * .1 "
        + "when not matched then insert (name, dept, salary) "
        + "values(t.name, 10, t.salary * .15)";
    final String expected = "MERGE INTO `EMPS` AS `E`\n"
        + "USING `TEMPEMPS` AS `T`\n"
        + "ON (`E`.`EMPNO` = `T`.`EMPNO`)\n"
        + "WHEN MATCHED THEN UPDATE SET `NAME` = `T`.`NAME`"
        + ", `DEPTNO` = `T`.`DEPTNO`"
        + ", `SALARY` = (`T`.`SALARY` * 0.1)\n"
        + "WHEN NOT MATCHED THEN INSERT (`NAME`, `DEPT`, `SALARY`) "
        + "(VALUES (ROW(`T`.`NAME`, 10, (`T`.`SALARY` * 0.15))))";
    sql(sql).ok(expected);
  }

  /** Same with testMergeTableRefSource but set with compound identifier. */
  @Test void testMergeTableRefSource2() {
    final String sql = "merge into emps e "
        + "using tempemps as t "
        + "on e.empno = t.empno "
        + "when matched then update "
        + "set e.name = t.name, e.deptno = t.deptno, e.salary = t.salary * .1 "
        + "when not matched then insert (name, dept, salary) "
        + "values(t.name, 10, t.salary * .15)";
    final String expected = "MERGE INTO `EMPS` AS `E`\n"
        + "USING `TEMPEMPS` AS `T`\n"
        + "ON (`E`.`EMPNO` = `T`.`EMPNO`)\n"
        + "WHEN MATCHED THEN UPDATE SET `E`.`NAME` = `T`.`NAME`"
        + ", `E`.`DEPTNO` = `T`.`DEPTNO`"
        + ", `E`.`SALARY` = (`T`.`SALARY` * 0.1)\n"
        + "WHEN NOT MATCHED THEN INSERT (`NAME`, `DEPT`, `SALARY`) "
        + "(VALUES (ROW(`T`.`NAME`, 10, (`T`.`SALARY` * 0.15))))";
    sql(sql).ok(expected);
  }

  @Test void testBitStringNotImplemented() {
    // Bit-string is longer part of the SQL standard. We do not support it.
    sql("select (B^'1011'^ || 'foobar') from (values (true))")
        .fails("(?s).*Encountered \"\\\\'1011\\\\'\" at .*");
  }

  @Test void testHexAndBinaryString() {
    expr("x''=X'2'")
        .ok("(X'' = X'2')");
    expr("x'fffff'=X''")
        .ok("(X'FFFFF' = X'')");
    expr("x'1' \t\t\f\r\n"
        + "'2'--hi this is a comment'FF'\r\r\t\f\n"
        + "'34'")
        .ok("X'1'\n'2'\n'34'");
    expr("x'1' \t\t\f\r\n"
        + "'000'--\n"
        + "'01'")
        .ok("X'1'\n'000'\n'01'");
    expr("x'1234567890abcdef'=X'fFeEdDcCbBaA'")
        .ok("(X'1234567890ABCDEF' = X'FFEEDDCCBBAA')");

    // Check the inital zeros don't get trimmed somehow
    expr("x'001'=X'000102'")
        .ok("(X'001' = X'000102')");
  }

  @Test void testHexAndBinaryStringFails() {
    sql("select ^x'FeedGoats'^ from t")
        .fails("Binary literal string must contain only characters '0' - '9', 'A' - 'F'");
    sql("select ^x'abcdefG'^ from t")
        .fails("Binary literal string must contain only characters '0' - '9', 'A' - 'F'");
    sql("select x'1' ^x'2'^ from t")
        .fails("(?s).*Encountered .x.*2.* at line 1, column 13.*");

    // valid syntax, but should fail in the validator
    sql("select x'1' '2' from t")
        .ok("SELECT X'1'\n"
            + "'2'\n"
            + "FROM `T`");
  }

  @Test void testStringLiteral() {
    expr("_latin1'hi'")
        .ok("_LATIN1'hi'");
    expr("N'is it a plane? no it''s superman!'")
        .ok("_ISO-8859-1'is it a plane? no it''s superman!'");
    expr("n'lowercase n'")
        .ok("_ISO-8859-1'lowercase n'");
    expr("'boring string'")
        .ok("'boring string'");
    expr("_iSo-8859-1'bye'")
        .ok("_ISO-8859-1'bye'");
    expr("'three'\n' blind'\n' mice'")
        .ok("'three'\n' blind'\n' mice'");
    expr("'three' -- comment\n' blind'\n' mice'")
        .ok("'three'\n' blind'\n' mice'");
    expr("N'bye' \t\r\f\f\n' bye'")
        .ok("_ISO-8859-1'bye'\n' bye'");
    expr("_iso-8859-1'bye'\n\n--\n-- this is a comment\n' bye'")
        .ok("_ISO-8859-1'bye'\n' bye'");
    expr("_utf8'hi'")
        .ok("_UTF8'hi'");

    // newline in string literal
    expr("'foo\rbar'")
        .ok("'foo\rbar'");
    expr("'foo\nbar'")
        .ok("'foo\nbar'");

    // prevent test infrastructure from converting '\r\n' to '\n'
    boolean[] linuxify = LINUXIFY.get();
    try {
      linuxify[0] = false;
      expr("'foo\r\nbar'")
          .ok("'foo\r\nbar'");
    } finally {
      linuxify[0] = true;
    }
  }

  @Test void testStringLiteralFails() {
    sql("select (N ^'space'^)")
        .fails("(?s).*Encountered .*space.* at line 1, column ...*");
    sql("select (_latin1\n^'newline'^)")
        .fails("(?s).*Encountered.*newline.* at line 2, column ...*");
    sql("select ^_unknown-charset''^ from (values(true))")
        .fails("Unknown character set 'unknown-charset'");

    // valid syntax, but should give a validator error
    sql("select (N'1' '2') from t")
        .ok("SELECT _ISO-8859-1'1'\n"
            + "'2'\n"
            + "FROM `T`");
  }

  @Test void testStringLiteralChain() {
    final String fooBar =
        "'foo'\n"
            + "'bar'";
    final String fooBarBaz =
        "'foo'\n"
            + "'bar'\n"
            + "'baz'";
    expr("   'foo'\r'bar'")
        .ok(fooBar);
    expr("   'foo'\r\n'bar'")
        .ok(fooBar);
    expr("   'foo'\r\n\r\n'bar'\n'baz'")
        .ok(fooBarBaz);
    expr("   'foo' /* a comment */ 'bar'")
        .ok(fooBar);
    expr("   'foo' -- a comment\r\n 'bar'")
        .ok(fooBar);

    // String literals not separated by comment or newline are OK in
    // parser, should fail in validator.
    expr("   'foo' 'bar'")
        .ok(fooBar);
  }

  @Test void testStringLiteralDoubleQuoted() {
    sql("select `deptno` as d, ^\"^deptno\" as d2 from emp")
        .withDialect(MYSQL)
        .fails("(?s)Encountered \"\\\\\"\" at .*")
        .withDialect(BIG_QUERY)
        .ok("SELECT deptno AS d, 'deptno' AS d2\n"
            + "FROM emp");

    // MySQL uses single-quotes as escapes; BigQuery uses backslashes
    sql("select 'Let''s call him \"Elvis\"!'")
        .withDialect(MYSQL)
        .node(isCharLiteral("Let's call him \"Elvis\"!"));

    sql("select 'Let\\'\\'s call him \"Elvis\"!'")
        .withDialect(BIG_QUERY)
        .node(isCharLiteral("Let''s call him \"Elvis\"!"));

    sql("select 'Let\\'s ^call^ him \"Elvis\"!'")
        .withDialect(MYSQL)
        .fails("(?s)Encountered \"call\" at .*")
        .withDialect(BIG_QUERY)
        .node(isCharLiteral("Let's call him \"Elvis\"!"));

    // Oracle uses double-quotes as escapes in identifiers;
    // BigQuery uses backslashes as escapes in double-quoted character literals.
    sql("select \"Let's call him \\\"Elvis^\\^\"!\"")
        .withDialect(ORACLE)
        .fails("(?s)Lexical error at line 1, column 31\\.  "
            + "Encountered: \"\\\\\\\\\" \\(92\\), after : \"\".*")
        .withDialect(BIG_QUERY)
        .node(isCharLiteral("Let's call him \"Elvis\"!"));
  }

  private static Matcher<SqlNode> isCharLiteral(String s) {
    return new CustomTypeSafeMatcher<SqlNode>(s) {
      @Override protected boolean matchesSafely(SqlNode item) {
        final SqlNodeList selectList;
        return item instanceof SqlSelect
            && (selectList = ((SqlSelect) item).getSelectList()).size() == 1
            && selectList.get(0) instanceof SqlLiteral
            && ((SqlLiteral) selectList.get(0)).getValueAs(String.class)
            .equals(s);
      }
    };
  }

  @Test public void testCaseExpression() {
    // implicit simple "ELSE NULL" case
    expr("case \t col1 when 1 then 'one' end")
        .ok("(CASE WHEN (`COL1` = 1) THEN 'one' ELSE NULL END)");

    // implicit searched "ELSE NULL" case
    expr("case when nbr is false then 'one' end")
        .ok("(CASE WHEN (`NBR` IS FALSE) THEN 'one' ELSE NULL END)");

    // multiple WHENs
    expr("case col1 when\n1.2 then 'one' when 2 then 'two' else 'three' end")
        .ok("(CASE WHEN (`COL1` = 1.2) THEN 'one' WHEN (`COL1` = 2) THEN 'two' ELSE 'three' END)");

    // sub-queries as case expression operands
    expr("case (select * from emp) when 1 then 2 end")
        .ok("(CASE WHEN ((SELECT *\n"
            + "FROM `EMP`) = 1) THEN 2 ELSE NULL END)");
    expr("case 1 when (select * from emp) then 2 end")
        .ok("(CASE WHEN (1 = (SELECT *\n"
            + "FROM `EMP`)) THEN 2 ELSE NULL END)");
    expr("case 1 when 2 then (select * from emp) end")
        .ok("(CASE WHEN (1 = 2) THEN (SELECT *\n"
            + "FROM `EMP`) ELSE NULL END)");
    expr("case 1 when 2 then 3 else (select * from emp) end")
        .ok("(CASE WHEN (1 = 2) THEN 3 ELSE (SELECT *\n"
            + "FROM `EMP`) END)");
    expr("case x when 2, 4 then 3 else 4 end")
        .ok("(CASE WHEN (`X` IN (2, 4)) THEN 3 ELSE 4 END)");
    // comma-list must not be empty
    sql("case x when 2, 4 then 3 when ^then^ 5 else 4 end")
        .fails("(?s)Encountered \"then\" at .*");
    // commas not allowed in boolean case
    sql("case when b1, b2 ^when^ 2, 4 then 3 else 4 end")
        .fails("(?s)Encountered \"when\" at .*");
  }

  @Test void testCaseExpressionFails() {
    // Missing 'END'
    sql("select case col1 when 1 then 'one' ^from^ t")
        .fails("(?s).*from.*");

    // Wrong 'WHEN'
    sql("select case col1 ^when1^ then 'one' end from t")
        .fails("(?s).*when1.*");
  }

  @Test void testNullIf() {
    expr("nullif(v1,v2)")
        .ok("NULLIF(`V1`, `V2`)");
    if (isReserved("NULLIF")) {
      expr("1 + ^nullif^ + 3")
          .fails("(?s)Encountered \"nullif \\+\" at line 1, column 5.*");
    }
  }

  @Test void testCoalesce() {
    expr("coalesce(v1)")
        .ok("COALESCE(`V1`)");
    expr("coalesce(v1,v2)")
        .ok("COALESCE(`V1`, `V2`)");
    expr("coalesce(v1,v2,v3)")
        .ok("COALESCE(`V1`, `V2`, `V3`)");
  }

  @Test void testLiteralCollate() {
    if (!Bug.FRG78_FIXED) {
      return;
    }

    expr("'string' collate latin1$sv_SE$mega_strength")
        .ok("'string' COLLATE ISO-8859-1$sv_SE$mega_strength");
    expr("'a long '\n'string' collate latin1$sv_SE$mega_strength")
        .ok("'a long ' 'string' COLLATE ISO-8859-1$sv_SE$mega_strength");
    expr("x collate iso-8859-6$ar_LB$1")
        .ok("`X` COLLATE ISO-8859-6$ar_LB$1");
    expr("x.y.z collate shift_jis$ja_JP$2")
        .ok("`X`.`Y`.`Z` COLLATE SHIFT_JIS$ja_JP$2");
    expr("'str1'='str2' collate latin1$sv_SE")
        .ok("('str1' = 'str2' COLLATE ISO-8859-1$sv_SE$primary)");
    expr("'str1' collate latin1$sv_SE>'str2'")
        .ok("('str1' COLLATE ISO-8859-1$sv_SE$primary > 'str2')");
    expr("'str1' collate latin1$sv_SE<='str2' collate latin1$sv_FI")
        .ok("('str1' COLLATE ISO-8859-1$sv_SE$primary <= 'str2' COLLATE ISO-8859-1$sv_FI$primary)");
  }

  @Test void testCharLength() {
    expr("char_length('string')")
        .ok("CHAR_LENGTH('string')");
    expr("character_length('string')")
        .ok("CHARACTER_LENGTH('string')");
  }

  @Test void testPosition() {
    expr("posiTion('mouse' in 'house')")
        .ok("POSITION('mouse' IN 'house')");
  }

  @Test void testReplace() {
    expr("replace('x', 'y', 'z')")
        .ok("REPLACE('x', 'y', 'z')");
  }

  @Test void testDateLiteral() {
    final String expected = "SELECT DATE '1980-01-01'\n"
        + "FROM `T`";
    sql("select date '1980-01-01' from t").ok(expected);
    final String expected1 = "SELECT TIME '00:00:00'\n"
        + "FROM `T`";
    sql("select time '00:00:00' from t").ok(expected1);
    final String expected2 = "SELECT TIMESTAMP '1980-01-01 00:00:00'\n"
        + "FROM `T`";
    sql("select timestamp '1980-01-01 00:00:00' from t").ok(expected2);
    final String expected3 = "SELECT INTERVAL '3' DAY\n"
        + "FROM `T`";
    sql("select interval '3' day from t").ok(expected3);
    final String expected4 = "SELECT INTERVAL '5:6' HOUR TO MINUTE\n"
        + "FROM `T`";
    sql("select interval '5:6' hour to minute from t").ok(expected4);
  }

  // check date/time functions.
  @Test void testTimeDate() {
    // CURRENT_TIME - returns time w/ timezone
    expr("CURRENT_TIME(3)")
        .ok("CURRENT_TIME(3)");

    // checkFails("SELECT CURRENT_TIME() FROM foo",
    //     "SELECT CURRENT_TIME() FROM `FOO`");

    expr("CURRENT_TIME")
        .ok("CURRENT_TIME");
    expr("CURRENT_TIME(x+y)")
        .ok("CURRENT_TIME((`X` + `Y`))");

    // LOCALTIME returns time w/o TZ
    expr("LOCALTIME(3)")
        .ok("LOCALTIME(3)");

    // checkFails("SELECT LOCALTIME() FROM foo",
    //     "SELECT LOCALTIME() FROM `FOO`");

    expr("LOCALTIME")
        .ok("LOCALTIME");
    expr("LOCALTIME(x+y)")
        .ok("LOCALTIME((`X` + `Y`))");

    // LOCALTIMESTAMP - returns timestamp w/o TZ
    expr("LOCALTIMESTAMP(3)")
        .ok("LOCALTIMESTAMP(3)");

    // checkFails("SELECT LOCALTIMESTAMP() FROM foo",
    //     "SELECT LOCALTIMESTAMP() FROM `FOO`");

    expr("LOCALTIMESTAMP")
        .ok("LOCALTIMESTAMP");
    expr("LOCALTIMESTAMP(x+y)")
        .ok("LOCALTIMESTAMP((`X` + `Y`))");

    // CURRENT_DATE - returns DATE
    expr("CURRENT_DATE(3)")
        .ok("CURRENT_DATE(3)");

    // checkFails("SELECT CURRENT_DATE() FROM foo",
    //     "SELECT CURRENT_DATE() FROM `FOO`");
    expr("CURRENT_DATE")
        .ok("CURRENT_DATE");

    // checkFails("SELECT CURRENT_DATE(x+y) FROM foo",
    //     "CURRENT_DATE((`X` + `Y`))");

    // CURRENT_TIMESTAMP - returns timestamp w/ TZ
    expr("CURRENT_TIMESTAMP(3)")
        .ok("CURRENT_TIMESTAMP(3)");

    // checkFails("SELECT CURRENT_TIMESTAMP() FROM foo",
    //     "SELECT CURRENT_TIMESTAMP() FROM `FOO`");

    expr("CURRENT_TIMESTAMP")
        .ok("CURRENT_TIMESTAMP");
    expr("CURRENT_TIMESTAMP(x+y)")
        .ok("CURRENT_TIMESTAMP((`X` + `Y`))");

    // Date literals
    expr("DATE '2004-12-01'")
        .ok("DATE '2004-12-01'");

    // Time literals
    expr("TIME '12:01:01'")
        .ok("TIME '12:01:01'");
    expr("TIME '12:01:01.'")
        .ok("TIME '12:01:01'");
    expr("TIME '12:01:01.000'")
        .ok("TIME '12:01:01.000'");
    expr("TIME '12:01:01.001'")
        .ok("TIME '12:01:01.001'");
    expr("TIME '12:01:01.01023456789'")
        .ok("TIME '12:01:01.01023456789'");

    // Timestamp literals
    expr("TIMESTAMP '2004-12-01 12:01:01'")
        .ok("TIMESTAMP '2004-12-01 12:01:01'");
    expr("TIMESTAMP '2004-12-01 12:01:01.1'")
        .ok("TIMESTAMP '2004-12-01 12:01:01.1'");
    expr("TIMESTAMP '2004-12-01 12:01:01.'")
        .ok("TIMESTAMP '2004-12-01 12:01:01'");
    expr("TIMESTAMP  '2004-12-01 12:01:01.010234567890'")
        .ok("TIMESTAMP '2004-12-01 12:01:01.010234567890'");
    expr("TIMESTAMP '2004-12-01 12:01:01.01023456789'").same();

    // Failures.
    sql("^DATE '12/21/99'^")
        .fails("(?s).*Illegal DATE literal.*");
    sql("^TIME '1230:33'^")
        .fails("(?s).*Illegal TIME literal.*");
    sql("^TIME '12:00:00 PM'^")
        .fails("(?s).*Illegal TIME literal.*");
    sql("^TIMESTAMP '12-21-99, 12:30:00'^")
        .fails("(?s).*Illegal TIMESTAMP literal.*");
  }

  /**
   * Tests for casting to/from date/time types.
   */
  @Test void testDateTimeCast() {
    //   checkExp("CAST(DATE '2001-12-21' AS CHARACTER VARYING)",
    // "CAST(2001-12-21)");
    expr("CAST('2001-12-21' AS DATE)")
        .ok("CAST('2001-12-21' AS DATE)");
    expr("CAST(12 AS DATE)")
        .ok("CAST(12 AS DATE)");
    sql("CAST('2000-12-21' AS DATE ^NOT^ NULL)")
        .fails("(?s).*Encountered \"NOT\" at line 1, column 27.*");
    sql("CAST('foo' as ^1^)")
        .fails("(?s).*Encountered \"1\" at line 1, column 15.*");
    expr("Cast(DATE '2004-12-21' AS VARCHAR(10))")
        .ok("CAST(DATE '2004-12-21' AS VARCHAR(10))");
  }

  @Test void testTrim() {
    expr("trim('mustache' FROM 'beard')")
        .ok("TRIM(BOTH 'mustache' FROM 'beard')");
    expr("trim('mustache')")
        .ok("TRIM(BOTH ' ' FROM 'mustache')");
    expr("trim(TRAILING FROM 'mustache')")
        .ok("TRIM(TRAILING ' ' FROM 'mustache')");
    expr("trim(bOth 'mustache' FROM 'beard')")
        .ok("TRIM(BOTH 'mustache' FROM 'beard')");
    expr("trim( lEaDing       'mustache' FROM 'beard')")
        .ok("TRIM(LEADING 'mustache' FROM 'beard')");
    expr("trim(\r\n\ttrailing\n  'mustache' FROM 'beard')")
        .ok("TRIM(TRAILING 'mustache' FROM 'beard')");
    expr("trim (coalesce(cast(null as varchar(2)))||"
        + "' '||coalesce('junk ',''))")
        .ok("TRIM(BOTH ' ' FROM ((COALESCE(CAST(NULL AS VARCHAR(2))) || "
            + "' ') || COALESCE('junk ', '')))");

    sql("trim(^from^ 'beard')")
        .fails("(?s).*'FROM' without operands preceding it is illegal.*");
  }

  @Test void testConvertAndTranslate() {
    expr("convert('abc' using conversion)")
        .ok("CONVERT('abc' USING `CONVERSION`)");
    expr("translate('abc' using lazy_translation)")
        .ok("TRANSLATE('abc' USING `LAZY_TRANSLATION`)");
  }

  @Test void testTranslate3() {
    expr("translate('aaabbbccc', 'ab', '+-')")
        .ok("TRANSLATE('aaabbbccc', 'ab', '+-')");
  }

  @Test void testOverlay() {
    expr("overlay('ABCdef' placing 'abc' from 1)")
        .ok("OVERLAY('ABCdef' PLACING 'abc' FROM 1)");
    expr("overlay('ABCdef' placing 'abc' from 1 for 3)")
        .ok("OVERLAY('ABCdef' PLACING 'abc' FROM 1 FOR 3)");
  }

  @Test void testJdbcFunctionCall() {
    expr("{fn apa(1,'1')}")
        .ok("{fn APA(1, '1') }");
    expr("{ Fn apa(log10(ln(1))+2)}")
        .ok("{fn APA((LOG10(LN(1)) + 2)) }");
    expr("{fN apa(*)}")
        .ok("{fn APA(*) }");
    expr("{   FN\t\r\n apa()}")
        .ok("{fn APA() }");
    expr("{fn insert()}")
        .ok("{fn INSERT() }");
    expr("{fn convert(foo, SQL_VARCHAR)}")
        .ok("{fn CONVERT(`FOO`, SQL_VARCHAR) }");
    expr("{fn convert(log10(100), integer)}")
        .ok("{fn CONVERT(LOG10(100), SQL_INTEGER) }");
    expr("{fn convert(1, SQL_INTERVAL_YEAR)}")
        .ok("{fn CONVERT(1, SQL_INTERVAL_YEAR) }");
    expr("{fn convert(1, SQL_INTERVAL_YEAR_TO_MONTH)}")
        .ok("{fn CONVERT(1, SQL_INTERVAL_YEAR_TO_MONTH) }");
    expr("{fn convert(1, ^sql_interval_year_to_day^)}")
        .fails("(?s)Encountered \"sql_interval_year_to_day\" at line 1, column 16\\.\n.*");
    expr("{fn convert(1, sql_interval_day)}")
        .ok("{fn CONVERT(1, SQL_INTERVAL_DAY) }");
    expr("{fn convert(1, sql_interval_day_to_minute)}")
        .ok("{fn CONVERT(1, SQL_INTERVAL_DAY_TO_MINUTE) }");
    expr("{fn convert(^)^}")
        .fails("(?s)Encountered \"\\)\" at.*");
    expr("{fn convert(\"123\", SMALLINT^(^3)}")
        .fails("(?s)Encountered \"\\(\" at.*");
    // Regular types (without SQL_) are OK for regular types, but not for
    // intervals.
    expr("{fn convert(1, INTEGER)}")
        .ok("{fn CONVERT(1, SQL_INTEGER) }");
    expr("{fn convert(1, VARCHAR)}")
        .ok("{fn CONVERT(1, SQL_VARCHAR) }");
    expr("{fn convert(1, VARCHAR^(^5))}")
        .fails("(?s)Encountered \"\\(\" at.*");
    expr("{fn convert(1, ^INTERVAL^ YEAR TO MONTH)}")
        .fails("(?s)Encountered \"INTERVAL\" at.*");
    expr("{fn convert(1, ^INTERVAL^ YEAR)}")
        .fails("(?s)Encountered \"INTERVAL\" at.*");
  }

  @Test void testWindowReference() {
    expr("sum(sal) over (w)")
        .ok("(SUM(`SAL`) OVER (`W`))");

    // Only 1 window reference allowed
    expr("sum(sal) over (w ^w1^ partition by deptno)")
        .fails("(?s)Encountered \"w1\" at.*");
  }

  @Test void testWindowInSubQuery() {
    final String sql = "select * from (\n"
        + " select sum(x) over w, sum(y) over w\n"
        + " from s\n"
        + " window w as (range interval '1' minute preceding))";
    final String expected = "SELECT *\n"
        + "FROM (SELECT (SUM(`X`) OVER `W`), (SUM(`Y`) OVER `W`)\n"
        + "FROM `S`\n"
        + "WINDOW `W` AS (RANGE INTERVAL '1' MINUTE PRECEDING))";
    sql(sql).ok(expected);
  }

  @Test void testWindowSpec() {
    // Correct syntax
    final String sql1 = "select count(z) over w as foo\n"
        + "from Bids\n"
        + "window w as (partition by y + yy, yyy\n"
        + " order by x\n"
        + " rows between 2 preceding and 2 following)";
    final String expected1 = "SELECT (COUNT(`Z`) OVER `W`) AS `FOO`\n"
        + "FROM `BIDS`\n"
        + "WINDOW `W` AS (PARTITION BY (`Y` + `YY`), `YYY` ORDER BY `X` ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)";
    sql(sql1).ok(expected1);

    final String sql2 = "select count(*) over w\n"
        + "from emp window w as (rows 2 preceding)";
    final String expected2 = "SELECT (COUNT(*) OVER `W`)\n"
        + "FROM `EMP`\n"
        + "WINDOW `W` AS (ROWS 2 PRECEDING)";
    sql(sql2).ok(expected2);

    // Chained string literals are valid syntax. They are unlikely to be
    // semantically valid, because intervals are usually numeric or
    // datetime.
    // Note: literal chain is not yet replaced with combined literal
    // since we are just parsing, and not validating the sql.
    final String sql3 = "select count(*) over w from emp window w as (\n"
        + "  rows 'foo' 'bar'\n"
        + "       'baz' preceding)";
    final String expected3 = "SELECT (COUNT(*) OVER `W`)\n"
        + "FROM `EMP`\n"
        + "WINDOW `W` AS (ROWS 'foo'\n'bar'\n'baz' PRECEDING)";
    sql(sql3).ok(expected3);

    // Partition clause out of place. Found after ORDER BY
    final String sql4 = "select count(z) over w as foo\n"
        + "from Bids\n"
        + "window w as (partition by y order by x ^partition^ by y)";
    sql(sql4)
        .fails("(?s).*Encountered \"partition\".*");

    final String sql5 = "select count(z) over w as foo\n"
        + "from Bids window w as (order by x ^partition^ by y)";
    sql(sql5)
        .fails("(?s).*Encountered \"partition\".*");

    // Cannot partition by sub-query
    sql("select sum(a) over (partition by ^(^select 1 from t), x) from t2")
        .fails("Query expression encountered in illegal context");

    // AND is required in BETWEEN clause of window frame
    final String sql7 = "select sum(x) over\n"
        + " (order by x range between unbounded preceding ^unbounded^ following)";
    sql(sql7)
        .fails("(?s).*Encountered \"unbounded\".*");

    // WINDOW keyword is not permissible.
    sql("select sum(x) over ^window^ (order by x) from bids")
        .fails("(?s).*Encountered \"window\".*");

    // ORDER BY must be before Frame spec
    sql("select sum(x) over (rows 2 preceding ^order^ by x) from emp")
        .fails("(?s).*Encountered \"order\".*");
  }

  @Test void testWindowSpecPartial() {
    // ALLOW PARTIAL is the default, and is omitted when the statement is
    // unparsed.
    sql("select sum(x) over (order by x allow partial) from bids")
        .ok("SELECT (SUM(`X`) OVER (ORDER BY `X`))\n"
            + "FROM `BIDS`");

    sql("select sum(x) over (order by x) from bids")
        .ok("SELECT (SUM(`X`) OVER (ORDER BY `X`))\n"
            + "FROM `BIDS`");

    sql("select sum(x) over (order by x disallow partial) from bids")
        .ok("SELECT (SUM(`X`) OVER (ORDER BY `X` DISALLOW PARTIAL))\n"
            + "FROM `BIDS`");

    sql("select sum(x) over (order by x) from bids")
        .ok("SELECT (SUM(`X`) OVER (ORDER BY `X`))\n"
            + "FROM `BIDS`");
  }

  @Test void testNullTreatment() {
    sql("select lead(x) respect nulls over (w) from t")
        .ok("SELECT (LEAD(`X`) RESPECT NULLS OVER (`W`))\n"
            + "FROM `T`");
    sql("select deptno, sum(sal) respect nulls from emp group by deptno")
        .ok("SELECT `DEPTNO`, SUM(`SAL`) RESPECT NULLS\n"
            + "FROM `EMP`\n"
            + "GROUP BY `DEPTNO`");
    sql("select deptno, sum(sal) ignore nulls from emp group by deptno")
        .ok("SELECT `DEPTNO`, SUM(`SAL`) IGNORE NULLS\n"
            + "FROM `EMP`\n"
            + "GROUP BY `DEPTNO`");
    final String sql = "select col1,\n"
        + " collect(col2) ignore nulls\n"
        + "   within group (order by col3)\n"
        + "   filter (where 1 = 0)\n"
        + "   over (rows 10 preceding)\n"
        + " as c\n"
        + "from t\n"
        + "order by col1 limit 10";
    final String expected = "SELECT `COL1`, (COLLECT(`COL2`) IGNORE NULLS"
        + " WITHIN GROUP (ORDER BY `COL3`)"
        + " FILTER (WHERE (1 = 0)) OVER (ROWS 10 PRECEDING)) AS `C`\n"
        + "FROM `T`\n"
        + "ORDER BY `COL1`\n"
        + "FETCH NEXT 10 ROWS ONLY";
    sql(sql).ok(expected);

    // See [CALCITE-2993] ParseException may be thrown for legal
    // SQL queries due to incorrect "LOOKAHEAD(1)" hints
    sql("select lead(x) ignore from t")
        .ok("SELECT LEAD(`X`) AS `IGNORE`\n"
            + "FROM `T`");
    sql("select lead(x) respect from t")
        .ok("SELECT LEAD(`X`) AS `RESPECT`\n"
            + "FROM `T`");
  }

  @Test void testAs() {
    // AS is optional for column aliases
    sql("select x y from t")
        .ok("SELECT `X` AS `Y`\n"
            + "FROM `T`");

    sql("select x AS y from t")
        .ok("SELECT `X` AS `Y`\n"
            + "FROM `T`");
    sql("select sum(x) y from t group by z")
        .ok("SELECT SUM(`X`) AS `Y`\n"
            + "FROM `T`\n"
            + "GROUP BY `Z`");

    // Even after OVER
    sql("select count(z) over w foo from Bids window w as (order by x)")
        .ok("SELECT (COUNT(`Z`) OVER `W`) AS `FOO`\n"
            + "FROM `BIDS`\n"
            + "WINDOW `W` AS (ORDER BY `X`)");

    // AS is optional for table correlation names
    final String expected = "SELECT `X`\n"
        + "FROM `T` AS `T1`";
    sql("select x from t as t1").ok(expected);
    sql("select x from t t1").ok(expected);

    // AS is required in WINDOW declaration
    sql("select sum(x) over w from bids window w ^(order by x)")
        .fails("(?s).*Encountered \"\\(\".*");

    // Error if OVER and AS are in wrong order
    sql("select count(*) as foo ^over^ w from Bids window w (order by x)")
        .fails("(?s).*Encountered \"over\".*");
  }

  @Test void testAsAliases() {
    sql("select x from t as t1 (a, b) where foo")
        .ok("SELECT `X`\n"
            + "FROM `T` AS `T1` (`A`, `B`)\n"
            + "WHERE `FOO`");

    sql("select x from (values (1, 2), (3, 4)) as t1 (\"a\", b) where \"a\" > b")
        .ok("SELECT `X`\n"
            + "FROM (VALUES (ROW(1, 2)),\n"
            + "(ROW(3, 4))) AS `T1` (`a`, `B`)\n"
            + "WHERE (`a` > `B`)");

    // must have at least one column
    sql("select x from (values (1, 2), (3, 4)) as t1 (^)^")
        .fails("(?s).*Encountered \"\\)\" at .*");

    // cannot have expressions
    sql("select x from t as t1 (x ^+^ y)")
        .fails("(?s).*Was expecting one of:\n"
            + "    \"\\)\" \\.\\.\\.\n"
            + "    \",\" \\.\\.\\..*");

    // cannot have compound identifiers
    sql("select x from t as t1 (x^.^y)")
        .fails("(?s).*Was expecting one of:\n"
            + "    \"\\)\" \\.\\.\\.\n"
            + "    \",\" \\.\\.\\..*");
  }

  @Test void testOver() {
    expr("sum(sal) over ()")
        .ok("(SUM(`SAL`) OVER ())");
    expr("sum(sal) over (partition by x, y)")
        .ok("(SUM(`SAL`) OVER (PARTITION BY `X`, `Y`))");
    expr("sum(sal) over (order by x desc, y asc)")
        .ok("(SUM(`SAL`) OVER (ORDER BY `X` DESC, `Y`))");
    expr("sum(sal) over (rows 5 preceding)")
        .ok("(SUM(`SAL`) OVER (ROWS 5 PRECEDING))");
    expr("sum(sal) over (range between interval '1' second preceding\n"
        + " and interval '1' second following)")
        .ok("(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '1' SECOND PRECEDING "
            + "AND INTERVAL '1' SECOND FOLLOWING))");
    expr("sum(sal) over (range between interval '1:03' hour preceding\n"
        + " and interval '2' minute following)")
        .ok("(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '1:03' HOUR PRECEDING "
            + "AND INTERVAL '2' MINUTE FOLLOWING))");
    expr("sum(sal) over (range between interval '5' day preceding\n"
        + " and current row)")
        .ok("(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '5' DAY PRECEDING "
            + "AND CURRENT ROW))");
    expr("sum(sal) over (range interval '5' day preceding)")
        .ok("(SUM(`SAL`) OVER (RANGE INTERVAL '5' DAY PRECEDING))");
    expr("sum(sal) over (range between unbounded preceding and current row)")
        .ok("(SUM(`SAL`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING "
            + "AND CURRENT ROW))");
    expr("sum(sal) over (range unbounded preceding)")
        .ok("(SUM(`SAL`) OVER (RANGE UNBOUNDED PRECEDING))");
    expr("sum(sal) over (range between current row and unbounded preceding)")
        .ok("(SUM(`SAL`) OVER (RANGE BETWEEN CURRENT ROW "
            + "AND UNBOUNDED PRECEDING))");
    expr("sum(sal) over (range between current row and unbounded following)")
        .ok("(SUM(`SAL`) OVER (RANGE BETWEEN CURRENT ROW "
            + "AND UNBOUNDED FOLLOWING))");
    expr("sum(sal) over (range between 6 preceding\n"
        + " and interval '1:03' hour preceding)")
        .ok("(SUM(`SAL`) OVER (RANGE BETWEEN 6 PRECEDING "
            + "AND INTERVAL '1:03' HOUR PRECEDING))");
    expr("sum(sal) over (range between interval '1' second following\n"
        + " and interval '5' day following)")
        .ok("(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '1' SECOND FOLLOWING "
            + "AND INTERVAL '5' DAY FOLLOWING))");
  }

  @Test void testElementFunc() {
    expr("element(a)")
        .ok("ELEMENT(`A`)");
  }

  @Test void testCardinalityFunc() {
    expr("cardinality(a)")
        .ok("CARDINALITY(`A`)");
  }

  @Test void testMemberOf() {
    expr("a member of b")
        .ok("(`A` MEMBER OF `B`)");
    expr("a member of multiset[b]")
        .ok("(`A` MEMBER OF (MULTISET[`B`]))");
  }

  @Test void testSubMultisetrOf() {
    expr("a submultiset of b")
        .ok("(`A` SUBMULTISET OF `B`)");
  }

  @Test void testIsASet() {
    expr("b is a set")
        .ok("(`B` IS A SET)");
    expr("a is a set")
        .ok("(`A` IS A SET)");
  }

  @Test void testMultiset() {
    expr("multiset[1]")
        .ok("(MULTISET[1])");
    expr("multiset[1,2.3]")
        .ok("(MULTISET[1, 2.3])");
    expr("multiset[1,    '2']")
        .ok("(MULTISET[1, '2'])");
    expr("multiset[ROW(1,2)]")
        .ok("(MULTISET[(ROW(1, 2))])");
    expr("multiset[ROW(1,2),ROW(3,4)]")
        .ok("(MULTISET[(ROW(1, 2)), (ROW(3, 4))])");

    expr("multiset(select*from T)")
        .ok("(MULTISET ((SELECT *\n"
            + "FROM `T`)))");
  }

  @Test void testMultisetUnion() {
    expr("a multiset union b")
        .ok("(`A` MULTISET UNION ALL `B`)");
    expr("a multiset union all b")
        .ok("(`A` MULTISET UNION ALL `B`)");
    expr("a multiset union distinct b")
        .ok("(`A` MULTISET UNION DISTINCT `B`)");
  }

  @Test void testMultisetExcept() {
    expr("a multiset EXCEPT b")
        .ok("(`A` MULTISET EXCEPT ALL `B`)");
    expr("a multiset EXCEPT all b")
        .ok("(`A` MULTISET EXCEPT ALL `B`)");
    expr("a multiset EXCEPT distinct b")
        .ok("(`A` MULTISET EXCEPT DISTINCT `B`)");
  }

  @Test void testMultisetIntersect() {
    expr("a multiset INTERSECT b")
        .ok("(`A` MULTISET INTERSECT ALL `B`)");
    expr("a multiset INTERSECT all b")
        .ok("(`A` MULTISET INTERSECT ALL `B`)");
    expr("a multiset INTERSECT distinct b")
        .ok("(`A` MULTISET INTERSECT DISTINCT `B`)");
  }

  @Test void testMultisetMixed() {
    expr("multiset[1] MULTISET union b")
        .ok("((MULTISET[1]) MULTISET UNION ALL `B`)");
    final String sql = "a MULTISET union b "
        + "multiset intersect c "
        + "multiset except d "
        + "multiset union e";
    final String expected = "(((`A` MULTISET UNION ALL "
        + "(`B` MULTISET INTERSECT ALL `C`)) "
        + "MULTISET EXCEPT ALL `D`) MULTISET UNION ALL `E`)";
    expr(sql).ok(expected);
  }

  @Test void testMapItem() {
    expr("a['foo']")
        .ok("`A`['foo']");
    expr("a['x' || 'y']")
        .ok("`A`[('x' || 'y')]");
    expr("a['foo'] ['bar']")
        .ok("`A`['foo']['bar']");
    expr("a['foo']['bar']")
        .ok("`A`['foo']['bar']");
  }

  @Test void testMapItemPrecedence() {
    expr("1 + a['foo'] * 3")
        .ok("(1 + (`A`['foo'] * 3))");
    expr("1 * a['foo'] + 3")
        .ok("((1 * `A`['foo']) + 3)");
    expr("a['foo']['bar']")
        .ok("`A`['foo']['bar']");
    expr("a[b['foo' || 'bar']]")
        .ok("`A`[`B`[('foo' || 'bar')]]");
  }

  @Test void testArrayElement() {
    expr("a[1]")
        .ok("`A`[1]");
    expr("a[b[1]]")
        .ok("`A`[`B`[1]]");
    expr("a[b[1 + 2] + 3]")
        .ok("`A`[(`B`[(1 + 2)] + 3)]");
  }

  @Test void testArrayElementWithDot() {
    expr("a[1+2].b.c[2].d")
        .ok("(((`A`[(1 + 2)].`B`).`C`)[2].`D`)");
    expr("a[b[1]].c.f0[d[1]]")
        .ok("((`A`[`B`[1]].`C`).`F0`)[`D`[1]]");
  }

  @Test void testArrayValueConstructor() {
    expr("array[1, 2]").ok("(ARRAY[1, 2])");
    expr("array [1, 2]").ok("(ARRAY[1, 2])"); // with space

    // parser allows empty array; validator will reject it
    expr("array[]")
        .ok("(ARRAY[])");
    expr("array[(1, 'a'), (2, 'b')]")
        .ok("(ARRAY[(ROW(1, 'a')), (ROW(2, 'b'))])");
  }

  @Test void testCastAsCollectionType() {
    // test array type.
    expr("cast(a as int array)")
        .ok("CAST(`A` AS INTEGER ARRAY)");
    expr("cast(a as varchar(5) array)")
        .ok("CAST(`A` AS VARCHAR(5) ARRAY)");
    expr("cast(a as int array array)")
        .ok("CAST(`A` AS INTEGER ARRAY ARRAY)");
    expr("cast(a as varchar(5) array array)")
        .ok("CAST(`A` AS VARCHAR(5) ARRAY ARRAY)");
    expr("cast(a as int array^<^10>)")
        .fails("(?s).*Encountered \"<\" at line 1, column 20.\n.*");
    // test multiset type.
    expr("cast(a as int multiset)")
        .ok("CAST(`A` AS INTEGER MULTISET)");
    expr("cast(a as varchar(5) multiset)")
        .ok("CAST(`A` AS VARCHAR(5) MULTISET)");
    expr("cast(a as int multiset array)")
        .ok("CAST(`A` AS INTEGER MULTISET ARRAY)");
    expr("cast(a as varchar(5) multiset array)")
        .ok("CAST(`A` AS VARCHAR(5) MULTISET ARRAY)");
    // test row type nested in collection type.
    expr("cast(a as row(f0 int array multiset, f1 varchar(5) array) array multiset)")
        .ok("CAST(`A` AS "
            + "ROW(`F0` INTEGER ARRAY MULTISET, "
            + "`F1` VARCHAR(5) ARRAY) "
            + "ARRAY MULTISET)");
    // test UDT collection type.
    expr("cast(a as MyUDT array multiset)")
        .ok("CAST(`A` AS `MYUDT` ARRAY MULTISET)");
  }

  @Test void testCastAsRowType() {
    expr("cast(a as row(f0 int, f1 varchar))")
        .ok("CAST(`A` AS ROW(`F0` INTEGER, `F1` VARCHAR))");
    expr("cast(a as row(f0 int not null, f1 varchar null))")
        .ok("CAST(`A` AS ROW(`F0` INTEGER, `F1` VARCHAR NULL))");
    // test nested row type.
    expr("cast(a as row("
        + "f0 row(ff0 int not null, ff1 varchar null) null, "
        + "f1 timestamp not null))")
        .ok("CAST(`A` AS ROW("
            + "`F0` ROW(`FF0` INTEGER, `FF1` VARCHAR NULL) NULL, "
            + "`F1` TIMESTAMP))");
    // test row type in collection data types.
    expr("cast(a as row(f0 bigint not null, f1 decimal null) array)")
        .ok("CAST(`A` AS ROW(`F0` BIGINT, `F1` DECIMAL NULL) ARRAY)");
    expr("cast(a as row(f0 varchar not null, f1 timestamp null) multiset)")
        .ok("CAST(`A` AS ROW(`F0` VARCHAR, `F1` TIMESTAMP NULL) MULTISET)");
  }

  @Test void testMapValueConstructor() {
    expr("map[1, 'x', 2, 'y']")
        .ok("(MAP[1, 'x', 2, 'y'])");
    expr("map [1, 'x', 2, 'y']")
        .ok("(MAP[1, 'x', 2, 'y'])");
    expr("map[]")
        .ok("(MAP[])");
  }

  /**
   * Runs tests for INTERVAL... YEAR that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalYearPositive() {
    // default precision
    expr("interval '1' year")
        .ok("INTERVAL '1' YEAR");
    expr("interval '99' year")
        .ok("INTERVAL '99' YEAR");

    // explicit precision equal to default
    expr("interval '1' year(2)")
        .ok("INTERVAL '1' YEAR(2)");
    expr("interval '99' year(2)")
        .ok("INTERVAL '99' YEAR(2)");

    // max precision
    expr("interval '2147483647' year(10)")
        .ok("INTERVAL '2147483647' YEAR(10)");

    // min precision
    expr("interval '0' year(1)")
        .ok("INTERVAL '0' YEAR(1)");

    // alternate precision
    expr("interval '1234' year(4)")
        .ok("INTERVAL '1234' YEAR(4)");

    // sign
    expr("interval '+1' year")
        .ok("INTERVAL '+1' YEAR");
    expr("interval '-1' year")
        .ok("INTERVAL '-1' YEAR");
    expr("interval +'1' year")
        .ok("INTERVAL '1' YEAR");
    expr("interval +'+1' year")
        .ok("INTERVAL '+1' YEAR");
    expr("interval +'-1' year")
        .ok("INTERVAL '-1' YEAR");
    expr("interval -'1' year")
        .ok("INTERVAL -'1' YEAR");
    expr("interval -'+1' year")
        .ok("INTERVAL -'+1' YEAR");
    expr("interval -'-1' year")
        .ok("INTERVAL -'-1' YEAR");
  }

  /**
   * Runs tests for INTERVAL... YEAR TO MONTH that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalYearToMonthPositive() {
    // default precision
    expr("interval '1-2' year to month")
        .ok("INTERVAL '1-2' YEAR TO MONTH");
    expr("interval '99-11' year to month")
        .ok("INTERVAL '99-11' YEAR TO MONTH");
    expr("interval '99-0' year to month")
        .ok("INTERVAL '99-0' YEAR TO MONTH");

    // explicit precision equal to default
    expr("interval '1-2' year(2) to month")
        .ok("INTERVAL '1-2' YEAR(2) TO MONTH");
    expr("interval '99-11' year(2) to month")
        .ok("INTERVAL '99-11' YEAR(2) TO MONTH");
    expr("interval '99-0' year(2) to month")
        .ok("INTERVAL '99-0' YEAR(2) TO MONTH");

    // max precision
    expr("interval '2147483647-11' year(10) to month")
        .ok("INTERVAL '2147483647-11' YEAR(10) TO MONTH");

    // min precision
    expr("interval '0-0' year(1) to month")
        .ok("INTERVAL '0-0' YEAR(1) TO MONTH");

    // alternate precision
    expr("interval '2006-2' year(4) to month")
        .ok("INTERVAL '2006-2' YEAR(4) TO MONTH");

    // sign
    expr("interval '-1-2' year to month")
        .ok("INTERVAL '-1-2' YEAR TO MONTH");
    expr("interval '+1-2' year to month")
        .ok("INTERVAL '+1-2' YEAR TO MONTH");
    expr("interval +'1-2' year to month")
        .ok("INTERVAL '1-2' YEAR TO MONTH");
    expr("interval +'-1-2' year to month")
        .ok("INTERVAL '-1-2' YEAR TO MONTH");
    expr("interval +'+1-2' year to month")
        .ok("INTERVAL '+1-2' YEAR TO MONTH");
    expr("interval -'1-2' year to month")
        .ok("INTERVAL -'1-2' YEAR TO MONTH");
    expr("interval -'-1-2' year to month")
        .ok("INTERVAL -'-1-2' YEAR TO MONTH");
    expr("interval -'+1-2' year to month")
        .ok("INTERVAL -'+1-2' YEAR TO MONTH");
  }

  /**
   * Runs tests for INTERVAL... MONTH that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalMonthPositive() {
    // default precision
    expr("interval '1' month")
        .ok("INTERVAL '1' MONTH");
    expr("interval '99' month")
        .ok("INTERVAL '99' MONTH");

    // explicit precision equal to default
    expr("interval '1' month(2)")
        .ok("INTERVAL '1' MONTH(2)");
    expr("interval '99' month(2)")
        .ok("INTERVAL '99' MONTH(2)");

    // max precision
    expr("interval '2147483647' month(10)")
        .ok("INTERVAL '2147483647' MONTH(10)");

    // min precision
    expr("interval '0' month(1)")
        .ok("INTERVAL '0' MONTH(1)");

    // alternate precision
    expr("interval '1234' month(4)")
        .ok("INTERVAL '1234' MONTH(4)");

    // sign
    expr("interval '+1' month")
        .ok("INTERVAL '+1' MONTH");
    expr("interval '-1' month")
        .ok("INTERVAL '-1' MONTH");
    expr("interval +'1' month")
        .ok("INTERVAL '1' MONTH");
    expr("interval +'+1' month")
        .ok("INTERVAL '+1' MONTH");
    expr("interval +'-1' month")
        .ok("INTERVAL '-1' MONTH");
    expr("interval -'1' month")
        .ok("INTERVAL -'1' MONTH");
    expr("interval -'+1' month")
        .ok("INTERVAL -'+1' MONTH");
    expr("interval -'-1' month")
        .ok("INTERVAL -'-1' MONTH");
  }

  /**
   * Runs tests for INTERVAL... DAY that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalDayPositive() {
    // default precision
    expr("interval '1' day")
        .ok("INTERVAL '1' DAY");
    expr("interval '99' day")
        .ok("INTERVAL '99' DAY");

    // explicit precision equal to default
    expr("interval '1' day(2)")
        .ok("INTERVAL '1' DAY(2)");
    expr("interval '99' day(2)")
        .ok("INTERVAL '99' DAY(2)");

    // max precision
    expr("interval '2147483647' day(10)")
        .ok("INTERVAL '2147483647' DAY(10)");

    // min precision
    expr("interval '0' day(1)")
        .ok("INTERVAL '0' DAY(1)");

    // alternate precision
    expr("interval '1234' day(4)")
        .ok("INTERVAL '1234' DAY(4)");

    // sign
    expr("interval '+1' day")
        .ok("INTERVAL '+1' DAY");
    expr("interval '-1' day")
        .ok("INTERVAL '-1' DAY");
    expr("interval +'1' day")
        .ok("INTERVAL '1' DAY");
    expr("interval +'+1' day")
        .ok("INTERVAL '+1' DAY");
    expr("interval +'-1' day")
        .ok("INTERVAL '-1' DAY");
    expr("interval -'1' day")
        .ok("INTERVAL -'1' DAY");
    expr("interval -'+1' day")
        .ok("INTERVAL -'+1' DAY");
    expr("interval -'-1' day")
        .ok("INTERVAL -'-1' DAY");
  }

  /**
   * Runs tests for INTERVAL... DAY TO HOUR that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalDayToHourPositive() {
    // default precision
    expr("interval '1 2' day to hour")
        .ok("INTERVAL '1 2' DAY TO HOUR");
    expr("interval '99 23' day to hour")
        .ok("INTERVAL '99 23' DAY TO HOUR");
    expr("interval '99 0' day to hour")
        .ok("INTERVAL '99 0' DAY TO HOUR");

    // explicit precision equal to default
    expr("interval '1 2' day(2) to hour")
        .ok("INTERVAL '1 2' DAY(2) TO HOUR");
    expr("interval '99 23' day(2) to hour")
        .ok("INTERVAL '99 23' DAY(2) TO HOUR");
    expr("interval '99 0' day(2) to hour")
        .ok("INTERVAL '99 0' DAY(2) TO HOUR");

    // max precision
    expr("interval '2147483647 23' day(10) to hour")
        .ok("INTERVAL '2147483647 23' DAY(10) TO HOUR");

    // min precision
    expr("interval '0 0' day(1) to hour")
        .ok("INTERVAL '0 0' DAY(1) TO HOUR");

    // alternate precision
    expr("interval '2345 2' day(4) to hour")
        .ok("INTERVAL '2345 2' DAY(4) TO HOUR");

    // sign
    expr("interval '-1 2' day to hour")
        .ok("INTERVAL '-1 2' DAY TO HOUR");
    expr("interval '+1 2' day to hour")
        .ok("INTERVAL '+1 2' DAY TO HOUR");
    expr("interval +'1 2' day to hour")
        .ok("INTERVAL '1 2' DAY TO HOUR");
    expr("interval +'-1 2' day to hour")
        .ok("INTERVAL '-1 2' DAY TO HOUR");
    expr("interval +'+1 2' day to hour")
        .ok("INTERVAL '+1 2' DAY TO HOUR");
    expr("interval -'1 2' day to hour")
        .ok("INTERVAL -'1 2' DAY TO HOUR");
    expr("interval -'-1 2' day to hour")
        .ok("INTERVAL -'-1 2' DAY TO HOUR");
    expr("interval -'+1 2' day to hour")
        .ok("INTERVAL -'+1 2' DAY TO HOUR");
  }

  /**
   * Runs tests for INTERVAL... DAY TO MINUTE that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalDayToMinutePositive() {
    // default precision
    expr("interval '1 2:3' day to minute")
        .ok("INTERVAL '1 2:3' DAY TO MINUTE");
    expr("interval '99 23:59' day to minute")
        .ok("INTERVAL '99 23:59' DAY TO MINUTE");
    expr("interval '99 0:0' day to minute")
        .ok("INTERVAL '99 0:0' DAY TO MINUTE");

    // explicit precision equal to default
    expr("interval '1 2:3' day(2) to minute")
        .ok("INTERVAL '1 2:3' DAY(2) TO MINUTE");
    expr("interval '99 23:59' day(2) to minute")
        .ok("INTERVAL '99 23:59' DAY(2) TO MINUTE");
    expr("interval '99 0:0' day(2) to minute")
        .ok("INTERVAL '99 0:0' DAY(2) TO MINUTE");

    // max precision
    expr("interval '2147483647 23:59' day(10) to minute")
        .ok("INTERVAL '2147483647 23:59' DAY(10) TO MINUTE");

    // min precision
    expr("interval '0 0:0' day(1) to minute")
        .ok("INTERVAL '0 0:0' DAY(1) TO MINUTE");

    // alternate precision
    expr("interval '2345 6:7' day(4) to minute")
        .ok("INTERVAL '2345 6:7' DAY(4) TO MINUTE");

    // sign
    expr("interval '-1 2:3' day to minute")
        .ok("INTERVAL '-1 2:3' DAY TO MINUTE");
    expr("interval '+1 2:3' day to minute")
        .ok("INTERVAL '+1 2:3' DAY TO MINUTE");
    expr("interval +'1 2:3' day to minute")
        .ok("INTERVAL '1 2:3' DAY TO MINUTE");
    expr("interval +'-1 2:3' day to minute")
        .ok("INTERVAL '-1 2:3' DAY TO MINUTE");
    expr("interval +'+1 2:3' day to minute")
        .ok("INTERVAL '+1 2:3' DAY TO MINUTE");
    expr("interval -'1 2:3' day to minute")
        .ok("INTERVAL -'1 2:3' DAY TO MINUTE");
    expr("interval -'-1 2:3' day to minute")
        .ok("INTERVAL -'-1 2:3' DAY TO MINUTE");
    expr("interval -'+1 2:3' day to minute")
        .ok("INTERVAL -'+1 2:3' DAY TO MINUTE");
  }

  /**
   * Runs tests for INTERVAL... DAY TO SECOND that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalDayToSecondPositive() {
    // default precision
    expr("interval '1 2:3:4' day to second")
        .ok("INTERVAL '1 2:3:4' DAY TO SECOND");
    expr("interval '99 23:59:59' day to second")
        .ok("INTERVAL '99 23:59:59' DAY TO SECOND");
    expr("interval '99 0:0:0' day to second")
        .ok("INTERVAL '99 0:0:0' DAY TO SECOND");
    expr("interval '99 23:59:59.999999' day to second")
        .ok("INTERVAL '99 23:59:59.999999' DAY TO SECOND");
    expr("interval '99 0:0:0.0' day to second")
        .ok("INTERVAL '99 0:0:0.0' DAY TO SECOND");

    // explicit precision equal to default
    expr("interval '1 2:3:4' day(2) to second")
        .ok("INTERVAL '1 2:3:4' DAY(2) TO SECOND");
    expr("interval '99 23:59:59' day(2) to second")
        .ok("INTERVAL '99 23:59:59' DAY(2) TO SECOND");
    expr("interval '99 0:0:0' day(2) to second")
        .ok("INTERVAL '99 0:0:0' DAY(2) TO SECOND");
    expr("interval '99 23:59:59.999999' day to second(6)")
        .ok("INTERVAL '99 23:59:59.999999' DAY TO SECOND(6)");
    expr("interval '99 0:0:0.0' day to second(6)")
        .ok("INTERVAL '99 0:0:0.0' DAY TO SECOND(6)");

    // max precision
    expr("interval '2147483647 23:59:59' day(10) to second")
        .ok("INTERVAL '2147483647 23:59:59' DAY(10) TO SECOND");
    expr("interval '2147483647 23:59:59.999999999' day(10) to second(9)")
        .ok("INTERVAL '2147483647 23:59:59.999999999' DAY(10) TO SECOND(9)");

    // min precision
    expr("interval '0 0:0:0' day(1) to second")
        .ok("INTERVAL '0 0:0:0' DAY(1) TO SECOND");
    expr("interval '0 0:0:0.0' day(1) to second(1)")
        .ok("INTERVAL '0 0:0:0.0' DAY(1) TO SECOND(1)");

    // alternate precision
    expr("interval '2345 6:7:8' day(4) to second")
        .ok("INTERVAL '2345 6:7:8' DAY(4) TO SECOND");
    expr("interval '2345 6:7:8.9012' day(4) to second(4)")
        .ok("INTERVAL '2345 6:7:8.9012' DAY(4) TO SECOND(4)");

    // sign
    expr("interval '-1 2:3:4' day to second")
        .ok("INTERVAL '-1 2:3:4' DAY TO SECOND");
    expr("interval '+1 2:3:4' day to second")
        .ok("INTERVAL '+1 2:3:4' DAY TO SECOND");
    expr("interval +'1 2:3:4' day to second")
        .ok("INTERVAL '1 2:3:4' DAY TO SECOND");
    expr("interval +'-1 2:3:4' day to second")
        .ok("INTERVAL '-1 2:3:4' DAY TO SECOND");
    expr("interval +'+1 2:3:4' day to second")
        .ok("INTERVAL '+1 2:3:4' DAY TO SECOND");
    expr("interval -'1 2:3:4' day to second")
        .ok("INTERVAL -'1 2:3:4' DAY TO SECOND");
    expr("interval -'-1 2:3:4' day to second")
        .ok("INTERVAL -'-1 2:3:4' DAY TO SECOND");
    expr("interval -'+1 2:3:4' day to second")
        .ok("INTERVAL -'+1 2:3:4' DAY TO SECOND");
  }

  /**
   * Runs tests for INTERVAL... HOUR that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalHourPositive() {
    // default precision
    expr("interval '1' hour")
        .ok("INTERVAL '1' HOUR");
    expr("interval '99' hour")
        .ok("INTERVAL '99' HOUR");

    // explicit precision equal to default
    expr("interval '1' hour(2)")
        .ok("INTERVAL '1' HOUR(2)");
    expr("interval '99' hour(2)")
        .ok("INTERVAL '99' HOUR(2)");

    // max precision
    expr("interval '2147483647' hour(10)")
        .ok("INTERVAL '2147483647' HOUR(10)");

    // min precision
    expr("interval '0' hour(1)")
        .ok("INTERVAL '0' HOUR(1)");

    // alternate precision
    expr("interval '1234' hour(4)")
        .ok("INTERVAL '1234' HOUR(4)");

    // sign
    expr("interval '+1' hour")
        .ok("INTERVAL '+1' HOUR");
    expr("interval '-1' hour")
        .ok("INTERVAL '-1' HOUR");
    expr("interval +'1' hour")
        .ok("INTERVAL '1' HOUR");
    expr("interval +'+1' hour")
        .ok("INTERVAL '+1' HOUR");
    expr("interval +'-1' hour")
        .ok("INTERVAL '-1' HOUR");
    expr("interval -'1' hour")
        .ok("INTERVAL -'1' HOUR");
    expr("interval -'+1' hour")
        .ok("INTERVAL -'+1' HOUR");
    expr("interval -'-1' hour")
        .ok("INTERVAL -'-1' HOUR");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO MINUTE that should pass both parser
   * and validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalHourToMinutePositive() {
    // default precision
    expr("interval '2:3' hour to minute")
        .ok("INTERVAL '2:3' HOUR TO MINUTE");
    expr("interval '23:59' hour to minute")
        .ok("INTERVAL '23:59' HOUR TO MINUTE");
    expr("interval '99:0' hour to minute")
        .ok("INTERVAL '99:0' HOUR TO MINUTE");

    // explicit precision equal to default
    expr("interval '2:3' hour(2) to minute")
        .ok("INTERVAL '2:3' HOUR(2) TO MINUTE");
    expr("interval '23:59' hour(2) to minute")
        .ok("INTERVAL '23:59' HOUR(2) TO MINUTE");
    expr("interval '99:0' hour(2) to minute")
        .ok("INTERVAL '99:0' HOUR(2) TO MINUTE");

    // max precision
    expr("interval '2147483647:59' hour(10) to minute")
        .ok("INTERVAL '2147483647:59' HOUR(10) TO MINUTE");

    // min precision
    expr("interval '0:0' hour(1) to minute")
        .ok("INTERVAL '0:0' HOUR(1) TO MINUTE");

    // alternate precision
    expr("interval '2345:7' hour(4) to minute")
        .ok("INTERVAL '2345:7' HOUR(4) TO MINUTE");

    // sign
    expr("interval '-1:3' hour to minute")
        .ok("INTERVAL '-1:3' HOUR TO MINUTE");
    expr("interval '+1:3' hour to minute")
        .ok("INTERVAL '+1:3' HOUR TO MINUTE");
    expr("interval +'2:3' hour to minute")
        .ok("INTERVAL '2:3' HOUR TO MINUTE");
    expr("interval +'-2:3' hour to minute")
        .ok("INTERVAL '-2:3' HOUR TO MINUTE");
    expr("interval +'+2:3' hour to minute")
        .ok("INTERVAL '+2:3' HOUR TO MINUTE");
    expr("interval -'2:3' hour to minute")
        .ok("INTERVAL -'2:3' HOUR TO MINUTE");
    expr("interval -'-2:3' hour to minute")
        .ok("INTERVAL -'-2:3' HOUR TO MINUTE");
    expr("interval -'+2:3' hour to minute")
        .ok("INTERVAL -'+2:3' HOUR TO MINUTE");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO SECOND that should pass both parser
   * and validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalHourToSecondPositive() {
    // default precision
    expr("interval '2:3:4' hour to second")
        .ok("INTERVAL '2:3:4' HOUR TO SECOND");
    expr("interval '23:59:59' hour to second")
        .ok("INTERVAL '23:59:59' HOUR TO SECOND");
    expr("interval '99:0:0' hour to second")
        .ok("INTERVAL '99:0:0' HOUR TO SECOND");
    expr("interval '23:59:59.999999' hour to second")
        .ok("INTERVAL '23:59:59.999999' HOUR TO SECOND");
    expr("interval '99:0:0.0' hour to second")
        .ok("INTERVAL '99:0:0.0' HOUR TO SECOND");

    // explicit precision equal to default
    expr("interval '2:3:4' hour(2) to second")
        .ok("INTERVAL '2:3:4' HOUR(2) TO SECOND");
    expr("interval '99:59:59' hour(2) to second")
        .ok("INTERVAL '99:59:59' HOUR(2) TO SECOND");
    expr("interval '99:0:0' hour(2) to second")
        .ok("INTERVAL '99:0:0' HOUR(2) TO SECOND");
    expr("interval '23:59:59.999999' hour to second(6)")
        .ok("INTERVAL '23:59:59.999999' HOUR TO SECOND(6)");
    expr("interval '99:0:0.0' hour to second(6)")
        .ok("INTERVAL '99:0:0.0' HOUR TO SECOND(6)");

    // max precision
    expr("interval '2147483647:59:59' hour(10) to second")
        .ok("INTERVAL '2147483647:59:59' HOUR(10) TO SECOND");
    expr("interval '2147483647:59:59.999999999' hour(10) to second(9)")
        .ok("INTERVAL '2147483647:59:59.999999999' HOUR(10) TO SECOND(9)");

    // min precision
    expr("interval '0:0:0' hour(1) to second")
        .ok("INTERVAL '0:0:0' HOUR(1) TO SECOND");
    expr("interval '0:0:0.0' hour(1) to second(1)")
        .ok("INTERVAL '0:0:0.0' HOUR(1) TO SECOND(1)");

    // alternate precision
    expr("interval '2345:7:8' hour(4) to second")
        .ok("INTERVAL '2345:7:8' HOUR(4) TO SECOND");
    expr("interval '2345:7:8.9012' hour(4) to second(4)")
        .ok("INTERVAL '2345:7:8.9012' HOUR(4) TO SECOND(4)");

    // sign
    expr("interval '-2:3:4' hour to second")
        .ok("INTERVAL '-2:3:4' HOUR TO SECOND");
    expr("interval '+2:3:4' hour to second")
        .ok("INTERVAL '+2:3:4' HOUR TO SECOND");
    expr("interval +'2:3:4' hour to second")
        .ok("INTERVAL '2:3:4' HOUR TO SECOND");
    expr("interval +'-2:3:4' hour to second")
        .ok("INTERVAL '-2:3:4' HOUR TO SECOND");
    expr("interval +'+2:3:4' hour to second")
        .ok("INTERVAL '+2:3:4' HOUR TO SECOND");
    expr("interval -'2:3:4' hour to second")
        .ok("INTERVAL -'2:3:4' HOUR TO SECOND");
    expr("interval -'-2:3:4' hour to second")
        .ok("INTERVAL -'-2:3:4' HOUR TO SECOND");
    expr("interval -'+2:3:4' hour to second")
        .ok("INTERVAL -'+2:3:4' HOUR TO SECOND");
  }

  /**
   * Runs tests for INTERVAL... MINUTE that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalMinutePositive() {
    // default precision
    expr("interval '1' minute")
        .ok("INTERVAL '1' MINUTE");
    expr("interval '99' minute")
        .ok("INTERVAL '99' MINUTE");

    // explicit precision equal to default
    expr("interval '1' minute(2)")
        .ok("INTERVAL '1' MINUTE(2)");
    expr("interval '99' minute(2)")
        .ok("INTERVAL '99' MINUTE(2)");

    // max precision
    expr("interval '2147483647' minute(10)")
        .ok("INTERVAL '2147483647' MINUTE(10)");

    // min precision
    expr("interval '0' minute(1)")
        .ok("INTERVAL '0' MINUTE(1)");

    // alternate precision
    expr("interval '1234' minute(4)")
        .ok("INTERVAL '1234' MINUTE(4)");

    // sign
    expr("interval '+1' minute")
        .ok("INTERVAL '+1' MINUTE");
    expr("interval '-1' minute")
        .ok("INTERVAL '-1' MINUTE");
    expr("interval +'1' minute")
        .ok("INTERVAL '1' MINUTE");
    expr("interval +'+1' minute")
        .ok("INTERVAL '+1' MINUTE");
    expr("interval +'+1' minute")
        .ok("INTERVAL '+1' MINUTE");
    expr("interval -'1' minute")
        .ok("INTERVAL -'1' MINUTE");
    expr("interval -'+1' minute")
        .ok("INTERVAL -'+1' MINUTE");
    expr("interval -'-1' minute")
        .ok("INTERVAL -'-1' MINUTE");
  }

  /**
   * Runs tests for INTERVAL... MINUTE TO SECOND that should pass both parser
   * and validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalMinuteToSecondPositive() {
    // default precision
    expr("interval '2:4' minute to second")
        .ok("INTERVAL '2:4' MINUTE TO SECOND");
    expr("interval '59:59' minute to second")
        .ok("INTERVAL '59:59' MINUTE TO SECOND");
    expr("interval '99:0' minute to second")
        .ok("INTERVAL '99:0' MINUTE TO SECOND");
    expr("interval '59:59.999999' minute to second")
        .ok("INTERVAL '59:59.999999' MINUTE TO SECOND");
    expr("interval '99:0.0' minute to second")
        .ok("INTERVAL '99:0.0' MINUTE TO SECOND");

    // explicit precision equal to default
    expr("interval '2:4' minute(2) to second")
        .ok("INTERVAL '2:4' MINUTE(2) TO SECOND");
    expr("interval '59:59' minute(2) to second")
        .ok("INTERVAL '59:59' MINUTE(2) TO SECOND");
    expr("interval '99:0' minute(2) to second")
        .ok("INTERVAL '99:0' MINUTE(2) TO SECOND");
    expr("interval '99:59.999999' minute to second(6)")
        .ok("INTERVAL '99:59.999999' MINUTE TO SECOND(6)");
    expr("interval '99:0.0' minute to second(6)")
        .ok("INTERVAL '99:0.0' MINUTE TO SECOND(6)");

    // max precision
    expr("interval '2147483647:59' minute(10) to second")
        .ok("INTERVAL '2147483647:59' MINUTE(10) TO SECOND");
    expr("interval '2147483647:59.999999999' minute(10) to second(9)")
        .ok("INTERVAL '2147483647:59.999999999' MINUTE(10) TO SECOND(9)");

    // min precision
    expr("interval '0:0' minute(1) to second")
        .ok("INTERVAL '0:0' MINUTE(1) TO SECOND");
    expr("interval '0:0.0' minute(1) to second(1)")
        .ok("INTERVAL '0:0.0' MINUTE(1) TO SECOND(1)");

    // alternate precision
    expr("interval '2345:8' minute(4) to second")
        .ok("INTERVAL '2345:8' MINUTE(4) TO SECOND");
    expr("interval '2345:7.8901' minute(4) to second(4)")
        .ok("INTERVAL '2345:7.8901' MINUTE(4) TO SECOND(4)");

    // sign
    expr("interval '-3:4' minute to second")
        .ok("INTERVAL '-3:4' MINUTE TO SECOND");
    expr("interval '+3:4' minute to second")
        .ok("INTERVAL '+3:4' MINUTE TO SECOND");
    expr("interval +'3:4' minute to second")
        .ok("INTERVAL '3:4' MINUTE TO SECOND");
    expr("interval +'-3:4' minute to second")
        .ok("INTERVAL '-3:4' MINUTE TO SECOND");
    expr("interval +'+3:4' minute to second")
        .ok("INTERVAL '+3:4' MINUTE TO SECOND");
    expr("interval -'3:4' minute to second")
        .ok("INTERVAL -'3:4' MINUTE TO SECOND");
    expr("interval -'-3:4' minute to second")
        .ok("INTERVAL -'-3:4' MINUTE TO SECOND");
    expr("interval -'+3:4' minute to second")
        .ok("INTERVAL -'+3:4' MINUTE TO SECOND");
  }

  /**
   * Runs tests for INTERVAL... SECOND that should pass both parser and
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXPositive() tests.
   */
  public void subTestIntervalSecondPositive() {
    // default precision
    expr("interval '1' second")
        .ok("INTERVAL '1' SECOND");
    expr("interval '99' second")
        .ok("INTERVAL '99' SECOND");

    // explicit precision equal to default
    expr("interval '1' second(2)")
        .ok("INTERVAL '1' SECOND(2)");
    expr("interval '99' second(2)")
        .ok("INTERVAL '99' SECOND(2)");
    expr("interval '1' second(2,6)")
        .ok("INTERVAL '1' SECOND(2, 6)");
    expr("interval '99' second(2,6)")
        .ok("INTERVAL '99' SECOND(2, 6)");

    // max precision
    expr("interval '2147483647' second(10)")
        .ok("INTERVAL '2147483647' SECOND(10)");
    expr("interval '2147483647.999999999' second(9,9)")
        .ok("INTERVAL '2147483647.999999999' SECOND(9, 9)");

    // min precision
    expr("interval '0' second(1)")
        .ok("INTERVAL '0' SECOND(1)");
    expr("interval '0.0' second(1,1)")
        .ok("INTERVAL '0.0' SECOND(1, 1)");

    // alternate precision
    expr("interval '1234' second(4)")
        .ok("INTERVAL '1234' SECOND(4)");
    expr("interval '1234.56789' second(4,5)")
        .ok("INTERVAL '1234.56789' SECOND(4, 5)");

    // sign
    expr("interval '+1' second")
        .ok("INTERVAL '+1' SECOND");
    expr("interval '-1' second")
        .ok("INTERVAL '-1' SECOND");
    expr("interval +'1' second")
        .ok("INTERVAL '1' SECOND");
    expr("interval +'+1' second")
        .ok("INTERVAL '+1' SECOND");
    expr("interval +'-1' second")
        .ok("INTERVAL '-1' SECOND");
    expr("interval -'1' second")
        .ok("INTERVAL -'1' SECOND");
    expr("interval -'+1' second")
        .ok("INTERVAL -'+1' SECOND");
    expr("interval -'-1' second")
        .ok("INTERVAL -'-1' SECOND");
  }

  /**
   * Runs tests for INTERVAL... YEAR that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalYearFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL '-' YEAR")
        .ok("INTERVAL '-' YEAR");
    expr("INTERVAL '1-2' YEAR")
        .ok("INTERVAL '1-2' YEAR");
    expr("INTERVAL '1.2' YEAR")
        .ok("INTERVAL '1.2' YEAR");
    expr("INTERVAL '1 2' YEAR")
        .ok("INTERVAL '1 2' YEAR");
    expr("INTERVAL '1-2' YEAR(2)")
        .ok("INTERVAL '1-2' YEAR(2)");
    expr("INTERVAL 'bogus text' YEAR")
        .ok("INTERVAL 'bogus text' YEAR");

    // negative field values
    expr("INTERVAL '--1' YEAR")
        .ok("INTERVAL '--1' YEAR");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    expr("INTERVAL '100' YEAR")
        .ok("INTERVAL '100' YEAR");
    expr("INTERVAL '100' YEAR(2)")
        .ok("INTERVAL '100' YEAR(2)");
    expr("INTERVAL '1000' YEAR(3)")
        .ok("INTERVAL '1000' YEAR(3)");
    expr("INTERVAL '-1000' YEAR(3)")
        .ok("INTERVAL '-1000' YEAR(3)");
    expr("INTERVAL '2147483648' YEAR(10)")
        .ok("INTERVAL '2147483648' YEAR(10)");
    expr("INTERVAL '-2147483648' YEAR(10)")
        .ok("INTERVAL '-2147483648' YEAR(10)");

    // precision > maximum
    expr("INTERVAL '1' YEAR(11)")
        .ok("INTERVAL '1' YEAR(11)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' YEAR(0)")
        .ok("INTERVAL '0' YEAR(0)");
  }

  /**
   * Runs tests for INTERVAL... YEAR TO MONTH that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalYearToMonthFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL '-' YEAR TO MONTH")
        .ok("INTERVAL '-' YEAR TO MONTH");
    expr("INTERVAL '1' YEAR TO MONTH")
        .ok("INTERVAL '1' YEAR TO MONTH");
    expr("INTERVAL '1:2' YEAR TO MONTH")
        .ok("INTERVAL '1:2' YEAR TO MONTH");
    expr("INTERVAL '1.2' YEAR TO MONTH")
        .ok("INTERVAL '1.2' YEAR TO MONTH");
    expr("INTERVAL '1 2' YEAR TO MONTH")
        .ok("INTERVAL '1 2' YEAR TO MONTH");
    expr("INTERVAL '1:2' YEAR(2) TO MONTH")
        .ok("INTERVAL '1:2' YEAR(2) TO MONTH");
    expr("INTERVAL 'bogus text' YEAR TO MONTH")
        .ok("INTERVAL 'bogus text' YEAR TO MONTH");

    // negative field values
    expr("INTERVAL '--1-2' YEAR TO MONTH")
        .ok("INTERVAL '--1-2' YEAR TO MONTH");
    expr("INTERVAL '1--2' YEAR TO MONTH")
        .ok("INTERVAL '1--2' YEAR TO MONTH");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    expr("INTERVAL '100-0' YEAR TO MONTH")
        .ok("INTERVAL '100-0' YEAR TO MONTH");
    expr("INTERVAL '100-0' YEAR(2) TO MONTH")
        .ok("INTERVAL '100-0' YEAR(2) TO MONTH");
    expr("INTERVAL '1000-0' YEAR(3) TO MONTH")
        .ok("INTERVAL '1000-0' YEAR(3) TO MONTH");
    expr("INTERVAL '-1000-0' YEAR(3) TO MONTH")
        .ok("INTERVAL '-1000-0' YEAR(3) TO MONTH");
    expr("INTERVAL '2147483648-0' YEAR(10) TO MONTH")
        .ok("INTERVAL '2147483648-0' YEAR(10) TO MONTH");
    expr("INTERVAL '-2147483648-0' YEAR(10) TO MONTH")
        .ok("INTERVAL '-2147483648-0' YEAR(10) TO MONTH");
    expr("INTERVAL '1-12' YEAR TO MONTH")
        .ok("INTERVAL '1-12' YEAR TO MONTH");

    // precision > maximum
    expr("INTERVAL '1-1' YEAR(11) TO MONTH")
        .ok("INTERVAL '1-1' YEAR(11) TO MONTH");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0-0' YEAR(0) TO MONTH")
        .ok("INTERVAL '0-0' YEAR(0) TO MONTH");
  }

  /**
   * Runs tests for INTERVAL... MONTH that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalMonthFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL '-' MONTH")
        .ok("INTERVAL '-' MONTH");
    expr("INTERVAL '1-2' MONTH")
        .ok("INTERVAL '1-2' MONTH");
    expr("INTERVAL '1.2' MONTH")
        .ok("INTERVAL '1.2' MONTH");
    expr("INTERVAL '1 2' MONTH")
        .ok("INTERVAL '1 2' MONTH");
    expr("INTERVAL '1-2' MONTH(2)")
        .ok("INTERVAL '1-2' MONTH(2)");
    expr("INTERVAL 'bogus text' MONTH")
        .ok("INTERVAL 'bogus text' MONTH");

    // negative field values
    expr("INTERVAL '--1' MONTH")
        .ok("INTERVAL '--1' MONTH");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    expr("INTERVAL '100' MONTH")
        .ok("INTERVAL '100' MONTH");
    expr("INTERVAL '100' MONTH(2)")
        .ok("INTERVAL '100' MONTH(2)");
    expr("INTERVAL '1000' MONTH(3)")
        .ok("INTERVAL '1000' MONTH(3)");
    expr("INTERVAL '-1000' MONTH(3)")
        .ok("INTERVAL '-1000' MONTH(3)");
    expr("INTERVAL '2147483648' MONTH(10)")
        .ok("INTERVAL '2147483648' MONTH(10)");
    expr("INTERVAL '-2147483648' MONTH(10)")
        .ok("INTERVAL '-2147483648' MONTH(10)");

    // precision > maximum
    expr("INTERVAL '1' MONTH(11)")
        .ok("INTERVAL '1' MONTH(11)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' MONTH(0)")
        .ok("INTERVAL '0' MONTH(0)");
  }

  /**
   * Runs tests for INTERVAL... DAY that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalDayFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL '-' DAY")
        .ok("INTERVAL '-' DAY");
    expr("INTERVAL '1-2' DAY")
        .ok("INTERVAL '1-2' DAY");
    expr("INTERVAL '1.2' DAY")
        .ok("INTERVAL '1.2' DAY");
    expr("INTERVAL '1 2' DAY")
        .ok("INTERVAL '1 2' DAY");
    expr("INTERVAL '1:2' DAY")
        .ok("INTERVAL '1:2' DAY");
    expr("INTERVAL '1-2' DAY(2)")
        .ok("INTERVAL '1-2' DAY(2)");
    expr("INTERVAL 'bogus text' DAY")
        .ok("INTERVAL 'bogus text' DAY");

    // negative field values
    expr("INTERVAL '--1' DAY")
        .ok("INTERVAL '--1' DAY");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    expr("INTERVAL '100' DAY")
        .ok("INTERVAL '100' DAY");
    expr("INTERVAL '100' DAY(2)")
        .ok("INTERVAL '100' DAY(2)");
    expr("INTERVAL '1000' DAY(3)")
        .ok("INTERVAL '1000' DAY(3)");
    expr("INTERVAL '-1000' DAY(3)")
        .ok("INTERVAL '-1000' DAY(3)");
    expr("INTERVAL '2147483648' DAY(10)")
        .ok("INTERVAL '2147483648' DAY(10)");
    expr("INTERVAL '-2147483648' DAY(10)")
        .ok("INTERVAL '-2147483648' DAY(10)");

    // precision > maximum
    expr("INTERVAL '1' DAY(11)")
        .ok("INTERVAL '1' DAY(11)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' DAY(0)")
        .ok("INTERVAL '0' DAY(0)");
  }

  @Test void testVisitSqlInsertWithSqlShuttle() throws Exception {
    final String sql = "insert into emps select * from emps";
    final SqlNode sqlNode = getSqlParser(sql).parseStmt();
    final SqlNode sqlNodeVisited = sqlNode.accept(new SqlShuttle() {
      @Override public SqlNode visit(SqlIdentifier identifier) {
        // Copy the identifier in order to return a new SqlInsert.
        return identifier.clone(identifier.getParserPosition());
      }
    });
    assertNotSame(sqlNodeVisited, sqlNode);
    assertThat(sqlNodeVisited.getKind(), is(SqlKind.INSERT));
  }

  @Test void testSqlInsertSqlBasicCallToString() throws Exception {
    final String sql0 = "insert into emps select * from emps";
    final SqlNode sqlNode0 = getSqlParser(sql0).parseStmt();
    final SqlNode sqlNodeVisited0 = sqlNode0.accept(new SqlShuttle() {
      @Override public SqlNode visit(SqlIdentifier identifier) {
        // Copy the identifier in order to return a new SqlInsert.
        return identifier.clone(identifier.getParserPosition());
      }
    });
    final String str0 = "INSERT INTO `EMPS`\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    assertEquals(linux(sqlNodeVisited0.toString()), str0);

    final String sql1 = "insert into emps select empno from emps";
    final SqlNode sqlNode1 = getSqlParser(sql1).parseStmt();
    final SqlNode sqlNodeVisited1 = sqlNode1.accept(new SqlShuttle() {
      @Override public SqlNode visit(SqlIdentifier identifier) {
        // Copy the identifier in order to return a new SqlInsert.
        return identifier.clone(identifier.getParserPosition());
      }
    });
    final String str1 = "INSERT INTO `EMPS`\n"
        + "(SELECT `EMPNO`\n"
        + "FROM `EMPS`)";
    assertEquals(linux(sqlNodeVisited1.toString()), str1);
  }

  @Test void testVisitSqlMatchRecognizeWithSqlShuttle() throws Exception {
    final String sql = "select *\n"
        + "from emp \n"
        + "match_recognize (\n"
        + "  pattern (strt down+ up+)\n"
        + "  define\n"
        + "    down as down.sal < PREV(down.sal),\n"
        + "    up as up.sal > PREV(up.sal)\n"
        + ") mr";
    final SqlNode sqlNode = getSqlParser(sql).parseStmt();
    final SqlNode sqlNodeVisited = sqlNode.accept(new SqlShuttle() {
      @Override public SqlNode visit(SqlIdentifier identifier) {
        // Copy the identifier in order to return a new SqlMatchRecognize.
        return identifier.clone(identifier.getParserPosition());
      }
    });
    assertNotSame(sqlNodeVisited, sqlNode);
  }

  /**
   * Runs tests for INTERVAL... DAY TO HOUR that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalDayToHourFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL '-' DAY TO HOUR")
        .ok("INTERVAL '-' DAY TO HOUR");
    expr("INTERVAL '1' DAY TO HOUR")
        .ok("INTERVAL '1' DAY TO HOUR");
    expr("INTERVAL '1:2' DAY TO HOUR")
        .ok("INTERVAL '1:2' DAY TO HOUR");
    expr("INTERVAL '1.2' DAY TO HOUR")
        .ok("INTERVAL '1.2' DAY TO HOUR");
    expr("INTERVAL '1 x' DAY TO HOUR")
        .ok("INTERVAL '1 x' DAY TO HOUR");
    expr("INTERVAL ' ' DAY TO HOUR")
        .ok("INTERVAL ' ' DAY TO HOUR");
    expr("INTERVAL '1:2' DAY(2) TO HOUR")
        .ok("INTERVAL '1:2' DAY(2) TO HOUR");
    expr("INTERVAL 'bogus text' DAY TO HOUR")
        .ok("INTERVAL 'bogus text' DAY TO HOUR");

    // negative field values
    expr("INTERVAL '--1 1' DAY TO HOUR")
        .ok("INTERVAL '--1 1' DAY TO HOUR");
    expr("INTERVAL '1 -1' DAY TO HOUR")
        .ok("INTERVAL '1 -1' DAY TO HOUR");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    expr("INTERVAL '100 0' DAY TO HOUR")
        .ok("INTERVAL '100 0' DAY TO HOUR");
    expr("INTERVAL '100 0' DAY(2) TO HOUR")
        .ok("INTERVAL '100 0' DAY(2) TO HOUR");
    expr("INTERVAL '1000 0' DAY(3) TO HOUR")
        .ok("INTERVAL '1000 0' DAY(3) TO HOUR");
    expr("INTERVAL '-1000 0' DAY(3) TO HOUR")
        .ok("INTERVAL '-1000 0' DAY(3) TO HOUR");
    expr("INTERVAL '2147483648 0' DAY(10) TO HOUR")
        .ok("INTERVAL '2147483648 0' DAY(10) TO HOUR");
    expr("INTERVAL '-2147483648 0' DAY(10) TO HOUR")
        .ok("INTERVAL '-2147483648 0' DAY(10) TO HOUR");
    expr("INTERVAL '1 24' DAY TO HOUR")
        .ok("INTERVAL '1 24' DAY TO HOUR");

    // precision > maximum
    expr("INTERVAL '1 1' DAY(11) TO HOUR")
        .ok("INTERVAL '1 1' DAY(11) TO HOUR");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0 0' DAY(0) TO HOUR")
        .ok("INTERVAL '0 0' DAY(0) TO HOUR");
  }

  /**
   * Runs tests for INTERVAL... DAY TO MINUTE that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalDayToMinuteFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL ' :' DAY TO MINUTE")
        .ok("INTERVAL ' :' DAY TO MINUTE");
    expr("INTERVAL '1' DAY TO MINUTE")
        .ok("INTERVAL '1' DAY TO MINUTE");
    expr("INTERVAL '1 2' DAY TO MINUTE")
        .ok("INTERVAL '1 2' DAY TO MINUTE");
    expr("INTERVAL '1:2' DAY TO MINUTE")
        .ok("INTERVAL '1:2' DAY TO MINUTE");
    expr("INTERVAL '1.2' DAY TO MINUTE")
        .ok("INTERVAL '1.2' DAY TO MINUTE");
    expr("INTERVAL 'x 1:1' DAY TO MINUTE")
        .ok("INTERVAL 'x 1:1' DAY TO MINUTE");
    expr("INTERVAL '1 x:1' DAY TO MINUTE")
        .ok("INTERVAL '1 x:1' DAY TO MINUTE");
    expr("INTERVAL '1 1:x' DAY TO MINUTE")
        .ok("INTERVAL '1 1:x' DAY TO MINUTE");
    expr("INTERVAL '1 1:2:3' DAY TO MINUTE")
        .ok("INTERVAL '1 1:2:3' DAY TO MINUTE");
    expr("INTERVAL '1 1:1:1.2' DAY TO MINUTE")
        .ok("INTERVAL '1 1:1:1.2' DAY TO MINUTE");
    expr("INTERVAL '1 1:2:3' DAY(2) TO MINUTE")
        .ok("INTERVAL '1 1:2:3' DAY(2) TO MINUTE");
    expr("INTERVAL '1 1' DAY(2) TO MINUTE")
        .ok("INTERVAL '1 1' DAY(2) TO MINUTE");
    expr("INTERVAL 'bogus text' DAY TO MINUTE")
        .ok("INTERVAL 'bogus text' DAY TO MINUTE");

    // negative field values
    expr("INTERVAL '--1 1:1' DAY TO MINUTE")
        .ok("INTERVAL '--1 1:1' DAY TO MINUTE");
    expr("INTERVAL '1 -1:1' DAY TO MINUTE")
        .ok("INTERVAL '1 -1:1' DAY TO MINUTE");
    expr("INTERVAL '1 1:-1' DAY TO MINUTE")
        .ok("INTERVAL '1 1:-1' DAY TO MINUTE");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    expr("INTERVAL '100 0' DAY TO MINUTE")
        .ok("INTERVAL '100 0' DAY TO MINUTE");
    expr("INTERVAL '100 0' DAY(2) TO MINUTE")
        .ok("INTERVAL '100 0' DAY(2) TO MINUTE");
    expr("INTERVAL '1000 0' DAY(3) TO MINUTE")
        .ok("INTERVAL '1000 0' DAY(3) TO MINUTE");
    expr("INTERVAL '-1000 0' DAY(3) TO MINUTE")
        .ok("INTERVAL '-1000 0' DAY(3) TO MINUTE");
    expr("INTERVAL '2147483648 0' DAY(10) TO MINUTE")
        .ok("INTERVAL '2147483648 0' DAY(10) TO MINUTE");
    expr("INTERVAL '-2147483648 0' DAY(10) TO MINUTE")
        .ok("INTERVAL '-2147483648 0' DAY(10) TO MINUTE");
    expr("INTERVAL '1 24:1' DAY TO MINUTE")
        .ok("INTERVAL '1 24:1' DAY TO MINUTE");
    expr("INTERVAL '1 1:60' DAY TO MINUTE")
        .ok("INTERVAL '1 1:60' DAY TO MINUTE");

    // precision > maximum
    expr("INTERVAL '1 1' DAY(11) TO MINUTE")
        .ok("INTERVAL '1 1' DAY(11) TO MINUTE");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0 0' DAY(0) TO MINUTE")
        .ok("INTERVAL '0 0' DAY(0) TO MINUTE");
  }

  /**
   * Runs tests for INTERVAL... DAY TO SECOND that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalDayToSecondFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL ' ::' DAY TO SECOND")
        .ok("INTERVAL ' ::' DAY TO SECOND");
    expr("INTERVAL ' ::.' DAY TO SECOND")
        .ok("INTERVAL ' ::.' DAY TO SECOND");
    expr("INTERVAL '1' DAY TO SECOND")
        .ok("INTERVAL '1' DAY TO SECOND");
    expr("INTERVAL '1 2' DAY TO SECOND")
        .ok("INTERVAL '1 2' DAY TO SECOND");
    expr("INTERVAL '1:2' DAY TO SECOND")
        .ok("INTERVAL '1:2' DAY TO SECOND");
    expr("INTERVAL '1.2' DAY TO SECOND")
        .ok("INTERVAL '1.2' DAY TO SECOND");
    expr("INTERVAL '1 1:2' DAY TO SECOND")
        .ok("INTERVAL '1 1:2' DAY TO SECOND");
    expr("INTERVAL '1 1:2:x' DAY TO SECOND")
        .ok("INTERVAL '1 1:2:x' DAY TO SECOND");
    expr("INTERVAL '1:2:3' DAY TO SECOND")
        .ok("INTERVAL '1:2:3' DAY TO SECOND");
    expr("INTERVAL '1:1:1.2' DAY TO SECOND")
        .ok("INTERVAL '1:1:1.2' DAY TO SECOND");
    expr("INTERVAL '1 1:2' DAY(2) TO SECOND")
        .ok("INTERVAL '1 1:2' DAY(2) TO SECOND");
    expr("INTERVAL '1 1' DAY(2) TO SECOND")
        .ok("INTERVAL '1 1' DAY(2) TO SECOND");
    expr("INTERVAL 'bogus text' DAY TO SECOND")
        .ok("INTERVAL 'bogus text' DAY TO SECOND");
    expr("INTERVAL '2345 6:7:8901' DAY TO SECOND(4)")
        .ok("INTERVAL '2345 6:7:8901' DAY TO SECOND(4)");

    // negative field values
    expr("INTERVAL '--1 1:1:1' DAY TO SECOND")
        .ok("INTERVAL '--1 1:1:1' DAY TO SECOND");
    expr("INTERVAL '1 -1:1:1' DAY TO SECOND")
        .ok("INTERVAL '1 -1:1:1' DAY TO SECOND");
    expr("INTERVAL '1 1:-1:1' DAY TO SECOND")
        .ok("INTERVAL '1 1:-1:1' DAY TO SECOND");
    expr("INTERVAL '1 1:1:-1' DAY TO SECOND")
        .ok("INTERVAL '1 1:1:-1' DAY TO SECOND");
    expr("INTERVAL '1 1:1:1.-1' DAY TO SECOND")
        .ok("INTERVAL '1 1:1:1.-1' DAY TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    expr("INTERVAL '100 0' DAY TO SECOND")
        .ok("INTERVAL '100 0' DAY TO SECOND");
    expr("INTERVAL '100 0' DAY(2) TO SECOND")
        .ok("INTERVAL '100 0' DAY(2) TO SECOND");
    expr("INTERVAL '1000 0' DAY(3) TO SECOND")
        .ok("INTERVAL '1000 0' DAY(3) TO SECOND");
    expr("INTERVAL '-1000 0' DAY(3) TO SECOND")
        .ok("INTERVAL '-1000 0' DAY(3) TO SECOND");
    expr("INTERVAL '2147483648 0' DAY(10) TO SECOND")
        .ok("INTERVAL '2147483648 0' DAY(10) TO SECOND");
    expr("INTERVAL '-2147483648 0' DAY(10) TO SECOND")
        .ok("INTERVAL '-2147483648 0' DAY(10) TO SECOND");
    expr("INTERVAL '1 24:1:1' DAY TO SECOND")
        .ok("INTERVAL '1 24:1:1' DAY TO SECOND");
    expr("INTERVAL '1 1:60:1' DAY TO SECOND")
        .ok("INTERVAL '1 1:60:1' DAY TO SECOND");
    expr("INTERVAL '1 1:1:60' DAY TO SECOND")
        .ok("INTERVAL '1 1:1:60' DAY TO SECOND");
    expr("INTERVAL '1 1:1:1.0000001' DAY TO SECOND")
        .ok("INTERVAL '1 1:1:1.0000001' DAY TO SECOND");
    expr("INTERVAL '1 1:1:1.0001' DAY TO SECOND(3)")
        .ok("INTERVAL '1 1:1:1.0001' DAY TO SECOND(3)");

    // precision > maximum
    expr("INTERVAL '1 1' DAY(11) TO SECOND")
        .ok("INTERVAL '1 1' DAY(11) TO SECOND");
    expr("INTERVAL '1 1' DAY TO SECOND(10)")
        .ok("INTERVAL '1 1' DAY TO SECOND(10)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0 0:0:0' DAY(0) TO SECOND")
        .ok("INTERVAL '0 0:0:0' DAY(0) TO SECOND");
    expr("INTERVAL '0 0:0:0' DAY TO SECOND(0)")
        .ok("INTERVAL '0 0:0:0' DAY TO SECOND(0)");
  }

  /**
   * Runs tests for INTERVAL... HOUR that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalHourFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL '-' HOUR")
        .ok("INTERVAL '-' HOUR");
    expr("INTERVAL '1-2' HOUR")
        .ok("INTERVAL '1-2' HOUR");
    expr("INTERVAL '1.2' HOUR")
        .ok("INTERVAL '1.2' HOUR");
    expr("INTERVAL '1 2' HOUR")
        .ok("INTERVAL '1 2' HOUR");
    expr("INTERVAL '1:2' HOUR")
        .ok("INTERVAL '1:2' HOUR");
    expr("INTERVAL '1-2' HOUR(2)")
        .ok("INTERVAL '1-2' HOUR(2)");
    expr("INTERVAL 'bogus text' HOUR")
        .ok("INTERVAL 'bogus text' HOUR");

    // negative field values
    expr("INTERVAL '--1' HOUR")
        .ok("INTERVAL '--1' HOUR");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    expr("INTERVAL '100' HOUR")
        .ok("INTERVAL '100' HOUR");
    expr("INTERVAL '100' HOUR(2)")
        .ok("INTERVAL '100' HOUR(2)");
    expr("INTERVAL '1000' HOUR(3)")
        .ok("INTERVAL '1000' HOUR(3)");
    expr("INTERVAL '-1000' HOUR(3)")
        .ok("INTERVAL '-1000' HOUR(3)");
    expr("INTERVAL '2147483648' HOUR(10)")
        .ok("INTERVAL '2147483648' HOUR(10)");
    expr("INTERVAL '-2147483648' HOUR(10)")
        .ok("INTERVAL '-2147483648' HOUR(10)");

    // negative field values
    expr("INTERVAL '--1' HOUR")
        .ok("INTERVAL '--1' HOUR");

    // precision > maximum
    expr("INTERVAL '1' HOUR(11)")
        .ok("INTERVAL '1' HOUR(11)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' HOUR(0)")
        .ok("INTERVAL '0' HOUR(0)");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO MINUTE that should pass parser but
   * fail validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalHourToMinuteFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL ':' HOUR TO MINUTE")
        .ok("INTERVAL ':' HOUR TO MINUTE");
    expr("INTERVAL '1' HOUR TO MINUTE")
        .ok("INTERVAL '1' HOUR TO MINUTE");
    expr("INTERVAL '1:x' HOUR TO MINUTE")
        .ok("INTERVAL '1:x' HOUR TO MINUTE");
    expr("INTERVAL '1.2' HOUR TO MINUTE")
        .ok("INTERVAL '1.2' HOUR TO MINUTE");
    expr("INTERVAL '1 2' HOUR TO MINUTE")
        .ok("INTERVAL '1 2' HOUR TO MINUTE");
    expr("INTERVAL '1:2:3' HOUR TO MINUTE")
        .ok("INTERVAL '1:2:3' HOUR TO MINUTE");
    expr("INTERVAL '1 2' HOUR(2) TO MINUTE")
        .ok("INTERVAL '1 2' HOUR(2) TO MINUTE");
    expr("INTERVAL 'bogus text' HOUR TO MINUTE")
        .ok("INTERVAL 'bogus text' HOUR TO MINUTE");

    // negative field values
    expr("INTERVAL '--1:1' HOUR TO MINUTE")
        .ok("INTERVAL '--1:1' HOUR TO MINUTE");
    expr("INTERVAL '1:-1' HOUR TO MINUTE")
        .ok("INTERVAL '1:-1' HOUR TO MINUTE");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    expr("INTERVAL '100:0' HOUR TO MINUTE")
        .ok("INTERVAL '100:0' HOUR TO MINUTE");
    expr("INTERVAL '100:0' HOUR(2) TO MINUTE")
        .ok("INTERVAL '100:0' HOUR(2) TO MINUTE");
    expr("INTERVAL '1000:0' HOUR(3) TO MINUTE")
        .ok("INTERVAL '1000:0' HOUR(3) TO MINUTE");
    expr("INTERVAL '-1000:0' HOUR(3) TO MINUTE")
        .ok("INTERVAL '-1000:0' HOUR(3) TO MINUTE");
    expr("INTERVAL '2147483648:0' HOUR(10) TO MINUTE")
        .ok("INTERVAL '2147483648:0' HOUR(10) TO MINUTE");
    expr("INTERVAL '-2147483648:0' HOUR(10) TO MINUTE")
        .ok("INTERVAL '-2147483648:0' HOUR(10) TO MINUTE");
    expr("INTERVAL '1:24' HOUR TO MINUTE")
        .ok("INTERVAL '1:24' HOUR TO MINUTE");

    // precision > maximum
    expr("INTERVAL '1:1' HOUR(11) TO MINUTE")
        .ok("INTERVAL '1:1' HOUR(11) TO MINUTE");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0:0' HOUR(0) TO MINUTE")
        .ok("INTERVAL '0:0' HOUR(0) TO MINUTE");
  }

  /**
   * Runs tests for INTERVAL... HOUR TO SECOND that should pass parser but
   * fail validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalHourToSecondFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL '::' HOUR TO SECOND")
        .ok("INTERVAL '::' HOUR TO SECOND");
    expr("INTERVAL '::.' HOUR TO SECOND")
        .ok("INTERVAL '::.' HOUR TO SECOND");
    expr("INTERVAL '1' HOUR TO SECOND")
        .ok("INTERVAL '1' HOUR TO SECOND");
    expr("INTERVAL '1 2' HOUR TO SECOND")
        .ok("INTERVAL '1 2' HOUR TO SECOND");
    expr("INTERVAL '1:2' HOUR TO SECOND")
        .ok("INTERVAL '1:2' HOUR TO SECOND");
    expr("INTERVAL '1.2' HOUR TO SECOND")
        .ok("INTERVAL '1.2' HOUR TO SECOND");
    expr("INTERVAL '1 1:2' HOUR TO SECOND")
        .ok("INTERVAL '1 1:2' HOUR TO SECOND");
    expr("INTERVAL '1:2:x' HOUR TO SECOND")
        .ok("INTERVAL '1:2:x' HOUR TO SECOND");
    expr("INTERVAL '1:x:3' HOUR TO SECOND")
        .ok("INTERVAL '1:x:3' HOUR TO SECOND");
    expr("INTERVAL '1:1:1.x' HOUR TO SECOND")
        .ok("INTERVAL '1:1:1.x' HOUR TO SECOND");
    expr("INTERVAL '1 1:2' HOUR(2) TO SECOND")
        .ok("INTERVAL '1 1:2' HOUR(2) TO SECOND");
    expr("INTERVAL '1 1' HOUR(2) TO SECOND")
        .ok("INTERVAL '1 1' HOUR(2) TO SECOND");
    expr("INTERVAL 'bogus text' HOUR TO SECOND")
        .ok("INTERVAL 'bogus text' HOUR TO SECOND");
    expr("INTERVAL '6:7:8901' HOUR TO SECOND(4)")
        .ok("INTERVAL '6:7:8901' HOUR TO SECOND(4)");

    // negative field values
    expr("INTERVAL '--1:1:1' HOUR TO SECOND")
        .ok("INTERVAL '--1:1:1' HOUR TO SECOND");
    expr("INTERVAL '1:-1:1' HOUR TO SECOND")
        .ok("INTERVAL '1:-1:1' HOUR TO SECOND");
    expr("INTERVAL '1:1:-1' HOUR TO SECOND")
        .ok("INTERVAL '1:1:-1' HOUR TO SECOND");
    expr("INTERVAL '1:1:1.-1' HOUR TO SECOND")
        .ok("INTERVAL '1:1:1.-1' HOUR TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    expr("INTERVAL '100:0:0' HOUR TO SECOND")
        .ok("INTERVAL '100:0:0' HOUR TO SECOND");
    expr("INTERVAL '100:0:0' HOUR(2) TO SECOND")
        .ok("INTERVAL '100:0:0' HOUR(2) TO SECOND");
    expr("INTERVAL '1000:0:0' HOUR(3) TO SECOND")
        .ok("INTERVAL '1000:0:0' HOUR(3) TO SECOND");
    expr("INTERVAL '-1000:0:0' HOUR(3) TO SECOND")
        .ok("INTERVAL '-1000:0:0' HOUR(3) TO SECOND");
    expr("INTERVAL '2147483648:0:0' HOUR(10) TO SECOND")
        .ok("INTERVAL '2147483648:0:0' HOUR(10) TO SECOND");
    expr("INTERVAL '-2147483648:0:0' HOUR(10) TO SECOND")
        .ok("INTERVAL '-2147483648:0:0' HOUR(10) TO SECOND");
    expr("INTERVAL '1:60:1' HOUR TO SECOND")
        .ok("INTERVAL '1:60:1' HOUR TO SECOND");
    expr("INTERVAL '1:1:60' HOUR TO SECOND")
        .ok("INTERVAL '1:1:60' HOUR TO SECOND");
    expr("INTERVAL '1:1:1.0000001' HOUR TO SECOND")
        .ok("INTERVAL '1:1:1.0000001' HOUR TO SECOND");
    expr("INTERVAL '1:1:1.0001' HOUR TO SECOND(3)")
        .ok("INTERVAL '1:1:1.0001' HOUR TO SECOND(3)");

    // precision > maximum
    expr("INTERVAL '1:1:1' HOUR(11) TO SECOND")
        .ok("INTERVAL '1:1:1' HOUR(11) TO SECOND");
    expr("INTERVAL '1:1:1' HOUR TO SECOND(10)")
        .ok("INTERVAL '1:1:1' HOUR TO SECOND(10)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0:0:0' HOUR(0) TO SECOND")
        .ok("INTERVAL '0:0:0' HOUR(0) TO SECOND");
    expr("INTERVAL '0:0:0' HOUR TO SECOND(0)")
        .ok("INTERVAL '0:0:0' HOUR TO SECOND(0)");
  }

  /**
   * Runs tests for INTERVAL... MINUTE that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalMinuteFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL '-' MINUTE")
        .ok("INTERVAL '-' MINUTE");
    expr("INTERVAL '1-2' MINUTE")
        .ok("INTERVAL '1-2' MINUTE");
    expr("INTERVAL '1.2' MINUTE")
        .ok("INTERVAL '1.2' MINUTE");
    expr("INTERVAL '1 2' MINUTE")
        .ok("INTERVAL '1 2' MINUTE");
    expr("INTERVAL '1:2' MINUTE")
        .ok("INTERVAL '1:2' MINUTE");
    expr("INTERVAL '1-2' MINUTE(2)")
        .ok("INTERVAL '1-2' MINUTE(2)");
    expr("INTERVAL 'bogus text' MINUTE")
        .ok("INTERVAL 'bogus text' MINUTE");

    // negative field values
    expr("INTERVAL '--1' MINUTE")
        .ok("INTERVAL '--1' MINUTE");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    expr("INTERVAL '100' MINUTE")
        .ok("INTERVAL '100' MINUTE");
    expr("INTERVAL '100' MINUTE(2)")
        .ok("INTERVAL '100' MINUTE(2)");
    expr("INTERVAL '1000' MINUTE(3)")
        .ok("INTERVAL '1000' MINUTE(3)");
    expr("INTERVAL '-1000' MINUTE(3)")
        .ok("INTERVAL '-1000' MINUTE(3)");
    expr("INTERVAL '2147483648' MINUTE(10)")
        .ok("INTERVAL '2147483648' MINUTE(10)");
    expr("INTERVAL '-2147483648' MINUTE(10)")
        .ok("INTERVAL '-2147483648' MINUTE(10)");

    // precision > maximum
    expr("INTERVAL '1' MINUTE(11)")
        .ok("INTERVAL '1' MINUTE(11)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' MINUTE(0)")
        .ok("INTERVAL '0' MINUTE(0)");
  }

  /**
   * Runs tests for INTERVAL... MINUTE TO SECOND that should pass parser but
   * fail validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalMinuteToSecondFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL ':' MINUTE TO SECOND")
        .ok("INTERVAL ':' MINUTE TO SECOND");
    expr("INTERVAL ':.' MINUTE TO SECOND")
        .ok("INTERVAL ':.' MINUTE TO SECOND");
    expr("INTERVAL '1' MINUTE TO SECOND")
        .ok("INTERVAL '1' MINUTE TO SECOND");
    expr("INTERVAL '1 2' MINUTE TO SECOND")
        .ok("INTERVAL '1 2' MINUTE TO SECOND");
    expr("INTERVAL '1.2' MINUTE TO SECOND")
        .ok("INTERVAL '1.2' MINUTE TO SECOND");
    expr("INTERVAL '1 1:2' MINUTE TO SECOND")
        .ok("INTERVAL '1 1:2' MINUTE TO SECOND");
    expr("INTERVAL '1:x' MINUTE TO SECOND")
        .ok("INTERVAL '1:x' MINUTE TO SECOND");
    expr("INTERVAL 'x:3' MINUTE TO SECOND")
        .ok("INTERVAL 'x:3' MINUTE TO SECOND");
    expr("INTERVAL '1:1.x' MINUTE TO SECOND")
        .ok("INTERVAL '1:1.x' MINUTE TO SECOND");
    expr("INTERVAL '1 1:2' MINUTE(2) TO SECOND")
        .ok("INTERVAL '1 1:2' MINUTE(2) TO SECOND");
    expr("INTERVAL '1 1' MINUTE(2) TO SECOND")
        .ok("INTERVAL '1 1' MINUTE(2) TO SECOND");
    expr("INTERVAL 'bogus text' MINUTE TO SECOND")
        .ok("INTERVAL 'bogus text' MINUTE TO SECOND");
    expr("INTERVAL '7:8901' MINUTE TO SECOND(4)")
        .ok("INTERVAL '7:8901' MINUTE TO SECOND(4)");

    // negative field values
    expr("INTERVAL '--1:1' MINUTE TO SECOND")
        .ok("INTERVAL '--1:1' MINUTE TO SECOND");
    expr("INTERVAL '1:-1' MINUTE TO SECOND")
        .ok("INTERVAL '1:-1' MINUTE TO SECOND");
    expr("INTERVAL '1:1.-1' MINUTE TO SECOND")
        .ok("INTERVAL '1:1.-1' MINUTE TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    expr("INTERVAL '100:0' MINUTE TO SECOND")
        .ok("INTERVAL '100:0' MINUTE TO SECOND");
    expr("INTERVAL '100:0' MINUTE(2) TO SECOND")
        .ok("INTERVAL '100:0' MINUTE(2) TO SECOND");
    expr("INTERVAL '1000:0' MINUTE(3) TO SECOND")
        .ok("INTERVAL '1000:0' MINUTE(3) TO SECOND");
    expr("INTERVAL '-1000:0' MINUTE(3) TO SECOND")
        .ok("INTERVAL '-1000:0' MINUTE(3) TO SECOND");
    expr("INTERVAL '2147483648:0' MINUTE(10) TO SECOND")
        .ok("INTERVAL '2147483648:0' MINUTE(10) TO SECOND");
    expr("INTERVAL '-2147483648:0' MINUTE(10) TO SECOND")
        .ok("INTERVAL '-2147483648:0' MINUTE(10) TO SECOND");
    expr("INTERVAL '1:60' MINUTE TO SECOND")
        .ok("INTERVAL '1:60' MINUTE TO SECOND");
    expr("INTERVAL '1:1.0000001' MINUTE TO SECOND")
        .ok("INTERVAL '1:1.0000001' MINUTE TO SECOND");
    expr("INTERVAL '1:1:1.0001' MINUTE TO SECOND(3)")
        .ok("INTERVAL '1:1:1.0001' MINUTE TO SECOND(3)");

    // precision > maximum
    expr("INTERVAL '1:1' MINUTE(11) TO SECOND")
        .ok("INTERVAL '1:1' MINUTE(11) TO SECOND");
    expr("INTERVAL '1:1' MINUTE TO SECOND(10)")
        .ok("INTERVAL '1:1' MINUTE TO SECOND(10)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0:0' MINUTE(0) TO SECOND")
        .ok("INTERVAL '0:0' MINUTE(0) TO SECOND");
    expr("INTERVAL '0:0' MINUTE TO SECOND(0)")
        .ok("INTERVAL '0:0' MINUTE TO SECOND(0)");
  }

  /**
   * Runs tests for INTERVAL... SECOND that should pass parser but fail
   * validator. A substantially identical set of tests exists in
   * SqlValidatorTest, and any changes here should be synchronized there.
   * Similarly, any changes to tests here should be echoed appropriately to
   * each of the other 12 subTestIntervalXXXFailsValidation() tests.
   */
  public void subTestIntervalSecondFailsValidation() {
    // Qualifier - field mismatches
    expr("INTERVAL ':' SECOND")
        .ok("INTERVAL ':' SECOND");
    expr("INTERVAL '.' SECOND")
        .ok("INTERVAL '.' SECOND");
    expr("INTERVAL '1-2' SECOND")
        .ok("INTERVAL '1-2' SECOND");
    expr("INTERVAL '1.x' SECOND")
        .ok("INTERVAL '1.x' SECOND");
    expr("INTERVAL 'x.1' SECOND")
        .ok("INTERVAL 'x.1' SECOND");
    expr("INTERVAL '1 2' SECOND")
        .ok("INTERVAL '1 2' SECOND");
    expr("INTERVAL '1:2' SECOND")
        .ok("INTERVAL '1:2' SECOND");
    expr("INTERVAL '1-2' SECOND(2)")
        .ok("INTERVAL '1-2' SECOND(2)");
    expr("INTERVAL 'bogus text' SECOND")
        .ok("INTERVAL 'bogus text' SECOND");

    // negative field values
    expr("INTERVAL '--1' SECOND")
        .ok("INTERVAL '--1' SECOND");
    expr("INTERVAL '1.-1' SECOND")
        .ok("INTERVAL '1.-1' SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    expr("INTERVAL '100' SECOND")
        .ok("INTERVAL '100' SECOND");
    expr("INTERVAL '100' SECOND(2)")
        .ok("INTERVAL '100' SECOND(2)");
    expr("INTERVAL '1000' SECOND(3)")
        .ok("INTERVAL '1000' SECOND(3)");
    expr("INTERVAL '-1000' SECOND(3)")
        .ok("INTERVAL '-1000' SECOND(3)");
    expr("INTERVAL '2147483648' SECOND(10)")
        .ok("INTERVAL '2147483648' SECOND(10)");
    expr("INTERVAL '-2147483648' SECOND(10)")
        .ok("INTERVAL '-2147483648' SECOND(10)");
    expr("INTERVAL '1.0000001' SECOND")
        .ok("INTERVAL '1.0000001' SECOND");
    expr("INTERVAL '1.0000001' SECOND(2)")
        .ok("INTERVAL '1.0000001' SECOND(2)");
    expr("INTERVAL '1.0001' SECOND(2, 3)")
        .ok("INTERVAL '1.0001' SECOND(2, 3)");
    expr("INTERVAL '1.000000001' SECOND(2, 9)")
        .ok("INTERVAL '1.000000001' SECOND(2, 9)");

    // precision > maximum
    expr("INTERVAL '1' SECOND(11)")
        .ok("INTERVAL '1' SECOND(11)");
    expr("INTERVAL '1.1' SECOND(1, 10)")
        .ok("INTERVAL '1.1' SECOND(1, 10)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    expr("INTERVAL '0' SECOND(0)")
        .ok("INTERVAL '0' SECOND(0)");
    expr("INTERVAL '0' SECOND(1, 0)")
        .ok("INTERVAL '0' SECOND(1, 0)");
  }

  /**
   * Runs tests for each of the thirteen different main types of INTERVAL
   * qualifiers (YEAR, YEAR TO MONTH, etc.) Tests in this section fall into
   * two categories:
   *
   * <ul>
   * <li>xxxPositive: tests that should pass parser and validator</li>
   * <li>xxxFailsValidation: tests that should pass parser but fail validator
   * </li>
   * </ul>
   *
   * <p>A substantially identical set of tests exists in SqlValidatorTest, and
   * any changes here should be synchronized there.
   */
  @Test void testIntervalLiterals() {
    subTestIntervalYearPositive();
    subTestIntervalYearToMonthPositive();
    subTestIntervalMonthPositive();
    subTestIntervalDayPositive();
    subTestIntervalDayToHourPositive();
    subTestIntervalDayToMinutePositive();
    subTestIntervalDayToSecondPositive();
    subTestIntervalHourPositive();
    subTestIntervalHourToMinutePositive();
    subTestIntervalHourToSecondPositive();
    subTestIntervalMinutePositive();
    subTestIntervalMinuteToSecondPositive();
    subTestIntervalSecondPositive();

    subTestIntervalYearFailsValidation();
    subTestIntervalYearToMonthFailsValidation();
    subTestIntervalMonthFailsValidation();
    subTestIntervalDayFailsValidation();
    subTestIntervalDayToHourFailsValidation();
    subTestIntervalDayToMinuteFailsValidation();
    subTestIntervalDayToSecondFailsValidation();
    subTestIntervalHourFailsValidation();
    subTestIntervalHourToMinuteFailsValidation();
    subTestIntervalHourToSecondFailsValidation();
    subTestIntervalMinuteFailsValidation();
    subTestIntervalMinuteToSecondFailsValidation();
    subTestIntervalSecondFailsValidation();
  }

  @Test void testUnparseableIntervalQualifiers() {
    // No qualifier
    expr("interval '1^'^")
        .fails("Encountered \"<EOF>\" at line 1, column 12\\.\n"
            + "Was expecting one of:\n"
            + "    \"DAY\" \\.\\.\\.\n"
            + "    \"DAYS\" \\.\\.\\.\n"
            + "    \"HOUR\" \\.\\.\\.\n"
            + "    \"HOURS\" \\.\\.\\.\n"
            + "    \"MINUTE\" \\.\\.\\.\n"
            + "    \"MINUTES\" \\.\\.\\.\n"
            + "    \"MONTH\" \\.\\.\\.\n"
            + "    \"MONTHS\" \\.\\.\\.\n"
            + "    \"SECOND\" \\.\\.\\.\n"
            + "    \"SECONDS\" \\.\\.\\.\n"
            + "    \"YEAR\" \\.\\.\\.\n"
            + "    \"YEARS\" \\.\\.\\.\n"
            + "    ");

    // illegal qualifiers, no precision in either field
    expr("interval '1' year ^to^ year")
        .fails("(?s)Encountered \"to year\" at line 1, column 19.\n"
            + "Was expecting one of:\n"
            + "    <EOF> \n"
            + "    \"\\(\" \\.\\.\\.\n"
            + "    \"\\.\" \\.\\.\\..*");
    expr("interval '1-2' year ^to^ day")
        .fails(ANY);
    expr("interval '1-2' year ^to^ hour")
        .fails(ANY);
    expr("interval '1-2' year ^to^ minute")
        .fails(ANY);
    expr("interval '1-2' year ^to^ second")
        .fails(ANY);

    expr("interval '1-2' month ^to^ year")
        .fails(ANY);
    expr("interval '1-2' month ^to^ month")
        .fails(ANY);
    expr("interval '1-2' month ^to^ day")
        .fails(ANY);
    expr("interval '1-2' month ^to^ hour")
        .fails(ANY);
    expr("interval '1-2' month ^to^ minute")
        .fails(ANY);
    expr("interval '1-2' month ^to^ second")
        .fails(ANY);

    expr("interval '1-2' day ^to^ year")
        .fails(ANY);
    expr("interval '1-2' day ^to^ month")
        .fails(ANY);
    expr("interval '1-2' day ^to^ day")
        .fails(ANY);

    expr("interval '1-2' hour ^to^ year")
        .fails(ANY);
    expr("interval '1-2' hour ^to^ month")
        .fails(ANY);
    expr("interval '1-2' hour ^to^ day")
        .fails(ANY);
    expr("interval '1-2' hour ^to^ hour")
        .fails(ANY);

    expr("interval '1-2' minute ^to^ year")
        .fails(ANY);
    expr("interval '1-2' minute ^to^ month")
        .fails(ANY);
    expr("interval '1-2' minute ^to^ day")
        .fails(ANY);
    expr("interval '1-2' minute ^to^ hour")
        .fails(ANY);
    expr("interval '1-2' minute ^to^ minute")
        .fails(ANY);

    expr("interval '1-2' second ^to^ year")
        .fails(ANY);
    expr("interval '1-2' second ^to^ month")
        .fails(ANY);
    expr("interval '1-2' second ^to^ day")
        .fails(ANY);
    expr("interval '1-2' second ^to^ hour")
        .fails(ANY);
    expr("interval '1-2' second ^to^ minute")
        .fails(ANY);
    expr("interval '1-2' second ^to^ second")
        .fails(ANY);

    // illegal qualifiers, including precision in start field
    expr("interval '1' year(3) ^to^ year")
        .fails(ANY);
    expr("interval '1-2' year(3) ^to^ day")
        .fails(ANY);
    expr("interval '1-2' year(3) ^to^ hour")
        .fails(ANY);
    expr("interval '1-2' year(3) ^to^ minute")
        .fails(ANY);
    expr("interval '1-2' year(3) ^to^ second")
        .fails(ANY);

    expr("interval '1-2' month(3) ^to^ year")
        .fails(ANY);
    expr("interval '1-2' month(3) ^to^ month")
        .fails(ANY);
    expr("interval '1-2' month(3) ^to^ day")
        .fails(ANY);
    expr("interval '1-2' month(3) ^to^ hour")
        .fails(ANY);
    expr("interval '1-2' month(3) ^to^ minute")
        .fails(ANY);
    expr("interval '1-2' month(3) ^to^ second")
        .fails(ANY);

    expr("interval '1-2' day(3) ^to^ year")
        .fails(ANY);
    expr("interval '1-2' day(3) ^to^ month")
        .fails(ANY);

    expr("interval '1-2' hour(3) ^to^ year")
        .fails(ANY);
    expr("interval '1-2' hour(3) ^to^ month")
        .fails(ANY);
    expr("interval '1-2' hour(3) ^to^ day")
        .fails(ANY);

    expr("interval '1-2' minute(3) ^to^ year")
        .fails(ANY);
    expr("interval '1-2' minute(3) ^to^ month")
        .fails(ANY);
    expr("interval '1-2' minute(3) ^to^ day")
        .fails(ANY);
    expr("interval '1-2' minute(3) ^to^ hour")
        .fails(ANY);

    expr("interval '1-2' second(3) ^to^ year")
        .fails(ANY);
    expr("interval '1-2' second(3) ^to^ month")
        .fails(ANY);
    expr("interval '1-2' second(3) ^to^ day")
        .fails(ANY);
    expr("interval '1-2' second(3) ^to^ hour")
        .fails(ANY);
    expr("interval '1-2' second(3) ^to^ minute")
        .fails(ANY);

    // illegal qualifiers, including precision in end field
    expr("interval '1' year ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' year to month^(^2)")
        .fails(ANY);
    expr("interval '1-2' year ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' year ^to^ hour(2)")
        .fails(ANY);
    expr("interval '1-2' year ^to^ minute(2)")
        .fails(ANY);
    expr("interval '1-2' year ^to^ second(2)")
        .fails(ANY);
    expr("interval '1-2' year ^to^ second(2,6)")
        .fails(ANY);

    expr("interval '1-2' month ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' month ^to^ month(2)")
        .fails(ANY);
    expr("interval '1-2' month ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' month ^to^ hour(2)")
        .fails(ANY);
    expr("interval '1-2' month ^to^ minute(2)")
        .fails(ANY);
    expr("interval '1-2' month ^to^ second(2)")
        .fails(ANY);
    expr("interval '1-2' month ^to^ second(2,6)")
        .fails(ANY);

    expr("interval '1-2' day ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' day ^to^ month(2)")
        .fails(ANY);
    expr("interval '1-2' day ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' day to hour^(^2)")
        .fails(ANY);
    expr("interval '1-2' day to minute^(^2)")
        .fails(ANY);
    expr("interval '1-2' day to second(2^,^6)")
        .fails(ANY);

    expr("interval '1-2' hour ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' hour ^to^ month(2)")
        .fails(ANY);
    expr("interval '1-2' hour ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' hour ^to^ hour(2)")
        .fails(ANY);
    expr("interval '1-2' hour to minute^(^2)")
        .fails(ANY);
    expr("interval '1-2' hour to second(2^,^6)")
        .fails(ANY);

    expr("interval '1-2' minute ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' minute ^to^ month(2)")
        .fails(ANY);
    expr("interval '1-2' minute ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' minute ^to^ hour(2)")
        .fails(ANY);
    expr("interval '1-2' minute ^to^ minute(2)")
        .fails(ANY);
    expr("interval '1-2' minute to second(2^,^6)")
        .fails(ANY);

    expr("interval '1-2' second ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' second ^to^ month(2)")
        .fails(ANY);
    expr("interval '1-2' second ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' second ^to^ hour(2)")
        .fails(ANY);
    expr("interval '1-2' second ^to^ minute(2)")
        .fails(ANY);
    expr("interval '1-2' second ^to^ second(2)")
        .fails(ANY);
    expr("interval '1-2' second ^to^ second(2,6)")
        .fails(ANY);

    // illegal qualifiers, including precision in start and end field
    expr("interval '1' year(3) ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' year(3) to month^(^2)")
        .fails(ANY);
    expr("interval '1-2' year(3) ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' year(3) ^to^ hour(2)")
        .fails(ANY);
    expr("interval '1-2' year(3) ^to^ minute(2)")
        .fails(ANY);
    expr("interval '1-2' year(3) ^to^ second(2)")
        .fails(ANY);
    expr("interval '1-2' year(3) ^to^ second(2,6)")
        .fails(ANY);

    expr("interval '1-2' month(3) ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' month(3) ^to^ month(2)")
        .fails(ANY);
    expr("interval '1-2' month(3) ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' month(3) ^to^ hour(2)")
        .fails(ANY);
    expr("interval '1-2' month(3) ^to^ minute(2)")
        .fails(ANY);
    expr("interval '1-2' month(3) ^to^ second(2)")
        .fails(ANY);
    expr("interval '1-2' month(3) ^to^ second(2,6)")
        .fails(ANY);
  }

  @Test void testUnparseableIntervalQualifiers2() {
    expr("interval '1-2' day(3) ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' day(3) ^to^ month(2)")
        .fails(ANY);
    expr("interval '1-2' day(3) ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' day(3) to hour^(^2)")
        .fails(ANY);
    expr("interval '1-2' day(3) to minute^(^2)")
        .fails(ANY);
    expr("interval '1-2' day(3) to second(2^,^6)")
        .fails(ANY);

    expr("interval '1-2' hour(3) ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' hour(3) ^to^ month(2)")
        .fails(ANY);
    expr("interval '1-2' hour(3) ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' hour(3) ^to^ hour(2)")
        .fails(ANY);
    expr("interval '1-2' hour(3) to minute^(^2)")
        .fails(ANY);
    expr("interval '1-2' hour(3) to second(2^,^6)")
        .fails(ANY);

    expr("interval '1-2' minute(3) ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' minute(3) ^to^ month(2)")
        .fails(ANY);
    expr("interval '1-2' minute(3) ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' minute(3) ^to^ hour(2)")
        .fails(ANY);
    expr("interval '1-2' minute(3) ^to^ minute(2)")
        .fails(ANY);
    expr("interval '1-2' minute(3) to second(2^,^6)")
        .fails(ANY);

    expr("interval '1-2' second(3) ^to^ year(2)")
        .fails(ANY);
    expr("interval '1-2' second(3) ^to^ month(2)")
        .fails(ANY);
    expr("interval '1-2' second(3) ^to^ day(2)")
        .fails(ANY);
    expr("interval '1-2' second(3) ^to^ hour(2)")
        .fails(ANY);
    expr("interval '1-2' second(3) ^to^ minute(2)")
        .fails(ANY);
    expr("interval '1-2' second(3) ^to^ second(2)")
        .fails(ANY);
    expr("interval '1-2' second(3) ^to^ second(2,6)")
        .fails(ANY);

    // precision of -1 (< minimum allowed)
    expr("INTERVAL '0' YEAR(^-^1)")
        .fails(ANY);
    expr("INTERVAL '0-0' YEAR(^-^1) TO MONTH")
        .fails(ANY);
    expr("INTERVAL '0' MONTH(^-^1)")
        .fails(ANY);
    expr("INTERVAL '0' DAY(^-^1)")
        .fails(ANY);
    expr("INTERVAL '0 0' DAY(^-^1) TO HOUR")
        .fails(ANY);
    expr("INTERVAL '0 0' DAY(^-^1) TO MINUTE")
        .fails(ANY);
    expr("INTERVAL '0 0:0:0' DAY(^-^1) TO SECOND")
        .fails(ANY);
    expr("INTERVAL '0 0:0:0' DAY TO SECOND(^-^1)")
        .fails(ANY);
    expr("INTERVAL '0' HOUR(^-^1)")
        .fails(ANY);
    expr("INTERVAL '0:0' HOUR(^-^1) TO MINUTE")
        .fails(ANY);
    expr("INTERVAL '0:0:0' HOUR(^-^1) TO SECOND")
        .fails(ANY);
    expr("INTERVAL '0:0:0' HOUR TO SECOND(^-^1)")
        .fails(ANY);
    expr("INTERVAL '0' MINUTE(^-^1)")
        .fails(ANY);
    expr("INTERVAL '0:0' MINUTE(^-^1) TO SECOND")
        .fails(ANY);
    expr("INTERVAL '0:0' MINUTE TO SECOND(^-^1)")
        .fails(ANY);
    expr("INTERVAL '0' SECOND(^-^1)")
        .fails(ANY);
    expr("INTERVAL '0' SECOND(1, ^-^1)")
        .fails(ANY);

    // These may actually be legal per SQL2003, as the first field is
    // "more significant" than the last, but we do not support them
    expr("interval '1' day(3) ^to^ day")
        .fails(ANY);
    expr("interval '1' hour(3) ^to^ hour")
        .fails(ANY);
    expr("interval '1' minute(3) ^to^ minute")
        .fails(ANY);
    expr("interval '1' second(3) ^to^ second")
        .fails(ANY);
    expr("interval '1' second(3,1) ^to^ second")
        .fails(ANY);
    expr("interval '1' second(2,3) ^to^ second")
        .fails(ANY);
    expr("interval '1' second(2,2) ^to^ second(3)")
        .fails(ANY);

    // Invalid units
    expr("INTERVAL '2' ^MILLENNIUM^")
        .fails(ANY);
    expr("INTERVAL '1-2' ^MILLENNIUM^ TO CENTURY")
        .fails(ANY);
    expr("INTERVAL '10' ^CENTURY^")
        .fails(ANY);
    expr("INTERVAL '10' ^DECADE^")
        .fails(ANY);
    expr("INTERVAL '4' ^QUARTER^")
        .fails(ANY);
  }

  /** Tests that plural time units are allowed when not in strict mode. */
  @Test void testIntervalPluralUnits() {
    expr("interval '2' years")
        .hasWarning(checkWarnings("YEARS"))
        .ok("INTERVAL '2' YEAR");
    expr("interval '2:1' years to months")
        .hasWarning(checkWarnings("YEARS", "MONTHS"))
        .ok("INTERVAL '2:1' YEAR TO MONTH");
    expr("interval '2' days")
        .hasWarning(checkWarnings("DAYS"))
        .ok("INTERVAL '2' DAY");
    expr("interval '2:1' days to hours")
        .hasWarning(checkWarnings("DAYS", "HOURS"))
        .ok("INTERVAL '2:1' DAY TO HOUR");
    expr("interval '2:1' day to hours")
        .hasWarning(checkWarnings("HOURS"))
        .ok("INTERVAL '2:1' DAY TO HOUR");
    expr("interval '2:1' days to hour")
        .hasWarning(checkWarnings("DAYS"))
        .ok("INTERVAL '2:1' DAY TO HOUR");
    expr("interval '1:1' minutes to seconds")
        .hasWarning(checkWarnings("MINUTES", "SECONDS"))
        .ok("INTERVAL '1:1' MINUTE TO SECOND");
  }

  private Consumer<List<? extends Throwable>> checkWarnings(
      String... tokens) {
    final List<String> messages = new ArrayList<>();
    for (String token : tokens) {
      messages.add("Warning: use of non-standard feature '" + token + "'");
    }
    return throwables -> {
      assertThat(throwables.size(), is(messages.size()));
      for (Pair<? extends Throwable, String> pair : Pair.zip(throwables, messages)) {
        assertThat(pair.left.getMessage(), containsString(pair.right));
      }
    };
  }

  @Test void testMiscIntervalQualifier() {
    expr("interval '-' day")
        .ok("INTERVAL '-' DAY");

    expr("interval '1 2:3:4.567' day to hour ^to^ second")
        .fails("(?s)Encountered \"to\" at.*");
    expr("interval '1:2' minute to second(2^,^ 2)")
        .fails("(?s)Encountered \",\" at.*");
    expr("interval '1:x' hour to minute")
        .ok("INTERVAL '1:x' HOUR TO MINUTE");
    expr("interval '1:x:2' hour to second")
        .ok("INTERVAL '1:x:2' HOUR TO SECOND");
  }

  @Test void testIntervalExpression() {
    expr("interval 0 day").ok("INTERVAL 0 DAY");
    expr("interval 0 days").ok("INTERVAL 0 DAY");
    expr("interval -10 days").ok("INTERVAL (- 10) DAY");
    expr("interval -10 days").ok("INTERVAL (- 10) DAY");
    // parser requires parentheses for expressions other than numeric
    // literal or identifier
    expr("interval 1 ^+^ x.y days")
        .fails("(?s)Encountered \"\\+\" at .*");
    expr("interval (1 + x.y) days")
        .ok("INTERVAL (1 + `X`.`Y`) DAY");
    expr("interval -x second(3)")
        .ok("INTERVAL (- `X`) SECOND(3)");
    expr("interval -x.y second(3)")
        .ok("INTERVAL (- `X`.`Y`) SECOND(3)");
    expr("interval 1 day ^to^ hour")
        .fails("(?s)Encountered \"to\" at .*");
    expr("interval '1 1' day to hour").ok("INTERVAL '1 1' DAY TO HOUR");
  }

  @Test void testIntervalOperators() {
    expr("-interval '1' day")
        .ok("(- INTERVAL '1' DAY)");
    expr("interval '1' day + interval '1' day")
        .ok("(INTERVAL '1' DAY + INTERVAL '1' DAY)");
    expr("interval '1' day - interval '1:2:3' hour to second")
        .ok("(INTERVAL '1' DAY - INTERVAL '1:2:3' HOUR TO SECOND)");

    expr("interval -'1' day")
        .ok("INTERVAL -'1' DAY");
    expr("interval '-1' day")
        .ok("INTERVAL '-1' DAY");
    expr("interval 'wael was here^'^")
        .fails("(?s)Encountered \"<EOF>\".*");

    // ok in parser, not in validator
    expr("interval 'wael was here' HOUR")
        .ok("INTERVAL 'wael was here' HOUR");
  }

  @Test void testDateMinusDate() {
    expr("(date1 - date2) HOUR")
        .ok("((`DATE1` - `DATE2`) HOUR)");
    expr("(date1 - date2) YEAR TO MONTH")
        .ok("((`DATE1` - `DATE2`) YEAR TO MONTH)");
    expr("(date1 - date2) HOUR > interval '1' HOUR")
        .ok("(((`DATE1` - `DATE2`) HOUR) > INTERVAL '1' HOUR)");
    expr("^(date1 + date2) second^")
        .fails("(?s).*Illegal expression. "
            + "Was expecting ..DATETIME - DATETIME. INTERVALQUALIFIER.*");
    expr("^(date1,date2,date2) second^")
        .fails("(?s).*Illegal expression. "
            + "Was expecting ..DATETIME - DATETIME. INTERVALQUALIFIER.*");
  }

  @Test void testExtract() {
    expr("extract(year from x)")
        .ok("EXTRACT(YEAR FROM `X`)");
    expr("extract(month from x)")
        .ok("EXTRACT(MONTH FROM `X`)");
    expr("extract(day from x)")
        .ok("EXTRACT(DAY FROM `X`)");
    expr("extract(hour from x)")
        .ok("EXTRACT(HOUR FROM `X`)");
    expr("extract(minute from x)")
        .ok("EXTRACT(MINUTE FROM `X`)");
    expr("extract(second from x)")
        .ok("EXTRACT(SECOND FROM `X`)");
    expr("extract(dow from x)")
        .ok("EXTRACT(DOW FROM `X`)");
    expr("extract(doy from x)")
        .ok("EXTRACT(DOY FROM `X`)");
    expr("extract(week from x)")
        .ok("EXTRACT(WEEK FROM `X`)");
    expr("extract(epoch from x)")
        .ok("EXTRACT(EPOCH FROM `X`)");
    expr("extract(quarter from x)")
        .ok("EXTRACT(QUARTER FROM `X`)");
    expr("extract(decade from x)")
        .ok("EXTRACT(DECADE FROM `X`)");
    expr("extract(century from x)")
        .ok("EXTRACT(CENTURY FROM `X`)");
    expr("extract(millennium from x)")
        .ok("EXTRACT(MILLENNIUM FROM `X`)");

    expr("extract(day ^to^ second from x)")
        .fails("(?s)Encountered \"to\".*");
  }

  @Test void testGeometry() {
    expr("cast(null as ^geometry^)")
        .fails("Geo-spatial extensions and the GEOMETRY data type are not enabled");
    conformance = SqlConformanceEnum.LENIENT;
    expr("cast(null as geometry)")
        .ok("CAST(NULL AS GEOMETRY)");
  }

  @Test void testIntervalArithmetics() {
    expr("TIME '23:59:59' - interval '1' hour ")
        .ok("(TIME '23:59:59' - INTERVAL '1' HOUR)");
    expr("TIMESTAMP '2000-01-01 23:59:59.1' - interval '1' hour ")
        .ok("(TIMESTAMP '2000-01-01 23:59:59.1' - INTERVAL '1' HOUR)");
    expr("DATE '2000-01-01' - interval '1' hour ")
        .ok("(DATE '2000-01-01' - INTERVAL '1' HOUR)");

    expr("TIME '23:59:59' + interval '1' hour ")
        .ok("(TIME '23:59:59' + INTERVAL '1' HOUR)");
    expr("TIMESTAMP '2000-01-01 23:59:59.1' + interval '1' hour ")
        .ok("(TIMESTAMP '2000-01-01 23:59:59.1' + INTERVAL '1' HOUR)");
    expr("DATE '2000-01-01' + interval '1' hour ")
        .ok("(DATE '2000-01-01' + INTERVAL '1' HOUR)");

    expr("interval '1' hour + TIME '23:59:59' ")
        .ok("(INTERVAL '1' HOUR + TIME '23:59:59')");

    expr("interval '1' hour * 8")
        .ok("(INTERVAL '1' HOUR * 8)");
    expr("1 * interval '1' hour")
        .ok("(1 * INTERVAL '1' HOUR)");
    expr("interval '1' hour / 8")
        .ok("(INTERVAL '1' HOUR / 8)");
  }

  @Test void testIntervalCompare() {
    expr("interval '1' hour = interval '1' second")
        .ok("(INTERVAL '1' HOUR = INTERVAL '1' SECOND)");
    expr("interval '1' hour <> interval '1' second")
        .ok("(INTERVAL '1' HOUR <> INTERVAL '1' SECOND)");
    expr("interval '1' hour < interval '1' second")
        .ok("(INTERVAL '1' HOUR < INTERVAL '1' SECOND)");
    expr("interval '1' hour <= interval '1' second")
        .ok("(INTERVAL '1' HOUR <= INTERVAL '1' SECOND)");
    expr("interval '1' hour > interval '1' second")
        .ok("(INTERVAL '1' HOUR > INTERVAL '1' SECOND)");
    expr("interval '1' hour >= interval '1' second")
        .ok("(INTERVAL '1' HOUR >= INTERVAL '1' SECOND)");
  }

  @Test void testCastToInterval() {
    expr("cast(x as interval year)")
        .ok("CAST(`X` AS INTERVAL YEAR)");
    expr("cast(x as interval month)")
        .ok("CAST(`X` AS INTERVAL MONTH)");
    expr("cast(x as interval year to month)")
        .ok("CAST(`X` AS INTERVAL YEAR TO MONTH)");
    expr("cast(x as interval day)")
        .ok("CAST(`X` AS INTERVAL DAY)");
    expr("cast(x as interval hour)")
        .ok("CAST(`X` AS INTERVAL HOUR)");
    expr("cast(x as interval minute)")
        .ok("CAST(`X` AS INTERVAL MINUTE)");
    expr("cast(x as interval second)")
        .ok("CAST(`X` AS INTERVAL SECOND)");
    expr("cast(x as interval day to hour)")
        .ok("CAST(`X` AS INTERVAL DAY TO HOUR)");
    expr("cast(x as interval day to minute)")
        .ok("CAST(`X` AS INTERVAL DAY TO MINUTE)");
    expr("cast(x as interval day to second)")
        .ok("CAST(`X` AS INTERVAL DAY TO SECOND)");
    expr("cast(x as interval hour to minute)")
        .ok("CAST(`X` AS INTERVAL HOUR TO MINUTE)");
    expr("cast(x as interval hour to second)")
        .ok("CAST(`X` AS INTERVAL HOUR TO SECOND)");
    expr("cast(x as interval minute to second)")
        .ok("CAST(`X` AS INTERVAL MINUTE TO SECOND)");
    expr("cast(interval '3-2' year to month as CHAR(5))")
        .ok("CAST(INTERVAL '3-2' YEAR TO MONTH AS CHAR(5))");
  }

  @Test void testCastToVarchar() {
    expr("cast(x as varchar(5))")
        .ok("CAST(`X` AS VARCHAR(5))");
    expr("cast(x as varchar)")
        .ok("CAST(`X` AS VARCHAR)");
    expr("cast(x as varBINARY(5))")
        .ok("CAST(`X` AS VARBINARY(5))");
    expr("cast(x as varbinary)")
        .ok("CAST(`X` AS VARBINARY)");
  }

  @Test void testTimestampAddAndDiff() {
    Map<String, List<String>> tsi = ImmutableMap.<String, List<String>>builder()
        .put("MICROSECOND",
            Arrays.asList("FRAC_SECOND", "MICROSECOND", "SQL_TSI_MICROSECOND"))
        .put("NANOSECOND", Arrays.asList("NANOSECOND", "SQL_TSI_FRAC_SECOND"))
        .put("SECOND", Arrays.asList("SECOND", "SQL_TSI_SECOND"))
        .put("MINUTE", Arrays.asList("MINUTE", "SQL_TSI_MINUTE"))
        .put("HOUR", Arrays.asList("HOUR", "SQL_TSI_HOUR"))
        .put("DAY", Arrays.asList("DAY", "SQL_TSI_DAY"))
        .put("WEEK", Arrays.asList("WEEK", "SQL_TSI_WEEK"))
        .put("MONTH", Arrays.asList("MONTH", "SQL_TSI_MONTH"))
        .put("QUARTER", Arrays.asList("QUARTER", "SQL_TSI_QUARTER"))
        .put("YEAR", Arrays.asList("YEAR", "SQL_TSI_YEAR"))
        .build();

    List<String> functions = ImmutableList.<String>builder()
        .add("timestampadd(%1$s, 12, current_timestamp)")
        .add("timestampdiff(%1$s, current_timestamp, current_timestamp)")
        .build();

    for (Map.Entry<String, List<String>> intervalGroup : tsi.entrySet()) {
      for (String function : functions) {
        for (String interval : intervalGroup.getValue()) {
          expr(String.format(Locale.ROOT, function, interval, ""))
              .ok(String.format(Locale.ROOT, function, intervalGroup.getKey(), "`")
                  .toUpperCase(Locale.ROOT));
        }
      }
    }

    expr("timestampadd(^incorrect^, 1, current_timestamp)")
        .fails("(?s).*Was expecting one of.*");
    expr("timestampdiff(^incorrect^, current_timestamp, current_timestamp)")
        .fails("(?s).*Was expecting one of.*");
  }

  @Test void testTimestampAdd() {
    final String sql = "select * from t\n"
        + "where timestampadd(sql_tsi_month, 5, hiredate) < curdate";
    final String expected = "SELECT *\n"
        + "FROM `T`\n"
        + "WHERE (TIMESTAMPADD(MONTH, 5, `HIREDATE`) < `CURDATE`)";
    sql(sql).ok(expected);
  }

  @Test void testTimestampDiff() {
    final String sql = "select * from t\n"
        + "where timestampdiff(frac_second, 5, hiredate) < curdate";
    final String expected = "SELECT *\n"
        + "FROM `T`\n"
        + "WHERE (TIMESTAMPDIFF(MICROSECOND, 5, `HIREDATE`) < `CURDATE`)";
    sql(sql).ok(expected);
  }

  @Test void testUnnest() {
    sql("select*from unnest(x)")
        .ok("SELECT *\n"
            + "FROM UNNEST(`X`)");
    sql("select*from unnest(x) AS T")
        .ok("SELECT *\n"
            + "FROM UNNEST(`X`) AS `T`");

    // UNNEST cannot be first word in query
    sql("^unnest^(x)")
        .fails("(?s)Encountered \"unnest\" at.*");

    // UNNEST with more than one argument
    final String sql = "select * from dept,\n"
        + "unnest(dept.employees, dept.managers)";
    final String expected = "SELECT *\n"
        + "FROM `DEPT`,\n"
        + "UNNEST(`DEPT`.`EMPLOYEES`, `DEPT`.`MANAGERS`)";
    sql(sql).ok(expected);

    // LATERAL UNNEST is not valid
    sql("select * from dept, lateral ^unnest^(dept.employees)")
        .fails("(?s)Encountered \"unnest\" at .*");

    // Does not generate extra parentheses around UNNEST because UNNEST is
    // a table expression.
    final String sql1 = ""
        + "SELECT\n"
        + "  item.name,\n"
        + "  relations.*\n"
        + "FROM dfs.tmp item\n"
        + "JOIN (\n"
        + "  SELECT * FROM UNNEST(item.related) i(rels)\n"
        + ") relations\n"
        + "ON TRUE";
    final String expected1 = "SELECT `ITEM`.`NAME`, `RELATIONS`.*\n"
        + "FROM `DFS`.`TMP` AS `ITEM`\n"
        + "INNER JOIN (SELECT *\n"
        + "FROM UNNEST(`ITEM`.`RELATED`) AS `I` (`RELS`)) AS `RELATIONS` ON TRUE";
    sql(sql1).ok(expected1);
  }

  @Test void testUnnestWithOrdinality() {
    sql("select * from unnest(x) with ordinality")
        .ok("SELECT *\n"
            + "FROM UNNEST(`X`) WITH ORDINALITY");
    sql("select*from unnest(x) with ordinality AS T")
        .ok("SELECT *\n"
            + "FROM UNNEST(`X`) WITH ORDINALITY AS `T`");
    sql("select*from unnest(x) with ordinality AS T(c, o)")
        .ok("SELECT *\n"
            + "FROM UNNEST(`X`) WITH ORDINALITY AS `T` (`C`, `O`)");
    sql("select*from unnest(x) as T ^with^ ordinality")
        .fails("(?s)Encountered \"with\" at .*");
  }

  @Test void testParensInFrom() {
    // UNNEST may not occur within parentheses.
    // FIXME should fail at "unnest"
    sql("select *from ^(^unnest(x))")
        .fails("(?s)Encountered \"\\( unnest\" at .*");

    // <table-name> may not occur within parentheses.
    sql("select * from (^emp^)")
        .fails("(?s)Non-query expression encountered in illegal context.*");

    // <table-name> may not occur within parentheses.
    sql("select * from (^emp^ as x)")
        .fails("(?s)Non-query expression encountered in illegal context.*");

    // <table-name> may not occur within parentheses.
    sql("select * from (^emp^) as x")
        .fails("(?s)Non-query expression encountered in illegal context.*");

    // Parentheses around JOINs are OK, and sometimes necessary.
    if (false) {
      // todo:
      sql("select * from (emp join dept using (deptno))").ok("xx");

      sql("select * from (emp join dept using (deptno)) join foo using (x)").ok("xx");
    }
  }

  @Test void testProcedureCall() {
    sql("call blubber(5)")
        .ok("CALL `BLUBBER`(5)");
    sql("call \"blubber\"(5)")
        .ok("CALL `blubber`(5)");
    sql("call whale.blubber(5)")
        .ok("CALL `WHALE`.`BLUBBER`(5)");
  }

  @Test void testNewSpecification() {
    expr("new udt()")
        .ok("(NEW `UDT`())");
    expr("new my.udt(1, 'hey')")
        .ok("(NEW `MY`.`UDT`(1, 'hey'))");
    expr("new udt() is not null")
        .ok("((NEW `UDT`()) IS NOT NULL)");
    expr("1 + new udt()")
        .ok("(1 + (NEW `UDT`()))");
  }

  @Test void testMultisetCast() {
    expr("cast(multiset[1] as double multiset)")
        .ok("CAST((MULTISET[1]) AS DOUBLE MULTISET)");
  }

  @Test void testAddCarets() {
    assertEquals(
        "values (^foo^)",
        SqlParserUtil.addCarets("values (foo)", 1, 9, 1, 12));
    assertEquals(
        "abc^def",
        SqlParserUtil.addCarets("abcdef", 1, 4, 1, 4));
    assertEquals(
        "abcdef^",
        SqlParserUtil.addCarets("abcdef", 1, 7, 1, 7));
  }

  @Test void testSnapshotForSystemTimeWithAlias() {
    sql("SELECT * FROM orders LEFT JOIN products FOR SYSTEM_TIME AS OF "
        + "orders.proctime as products ON orders.product_id = products.pro_id")
        .ok("SELECT *\n"
            + "FROM `ORDERS`\n"
            + "LEFT JOIN `PRODUCTS` FOR SYSTEM_TIME AS OF `ORDERS`.`PROCTIME` AS `PRODUCTS` ON (`ORDERS`"
            + ".`PRODUCT_ID` = `PRODUCTS`.`PRO_ID`)");
  }

  @Test protected void testMetadata() {
    SqlAbstractParserImpl.Metadata metadata = getSqlParser("").getMetadata();
    assertThat(metadata.isReservedFunctionName("ABS"), is(true));
    assertThat(metadata.isReservedFunctionName("FOO"), is(false));

    assertThat(metadata.isContextVariableName("CURRENT_USER"), is(true));
    assertThat(metadata.isContextVariableName("CURRENT_CATALOG"), is(true));
    assertThat(metadata.isContextVariableName("CURRENT_SCHEMA"), is(true));
    assertThat(metadata.isContextVariableName("ABS"), is(false));
    assertThat(metadata.isContextVariableName("FOO"), is(false));

    assertThat(metadata.isNonReservedKeyword("A"), is(true));
    assertThat(metadata.isNonReservedKeyword("KEY"), is(true));
    assertThat(metadata.isNonReservedKeyword("SELECT"), is(false));
    assertThat(metadata.isNonReservedKeyword("FOO"), is(false));
    assertThat(metadata.isNonReservedKeyword("ABS"), is(false));

    assertThat(metadata.isKeyword("ABS"), is(true));
    assertThat(metadata.isKeyword("CURRENT_USER"), is(true));
    assertThat(metadata.isKeyword("CURRENT_CATALOG"), is(true));
    assertThat(metadata.isKeyword("CURRENT_SCHEMA"), is(true));
    assertThat(metadata.isKeyword("KEY"), is(true));
    assertThat(metadata.isKeyword("SELECT"), is(true));
    assertThat(metadata.isKeyword("HAVING"), is(true));
    assertThat(metadata.isKeyword("A"), is(true));
    assertThat(metadata.isKeyword("BAR"), is(false));

    assertThat(metadata.isReservedWord("SELECT"), is(true));
    assertThat(metadata.isReservedWord("CURRENT_CATALOG"), is(true));
    assertThat(metadata.isReservedWord("CURRENT_SCHEMA"), is(true));
    assertThat(metadata.isReservedWord("KEY"), is(false));

    String jdbcKeywords = metadata.getJdbcKeywords();
    assertThat(jdbcKeywords.contains(",COLLECT,"), is(true));
    assertThat(!jdbcKeywords.contains(",SELECT,"), is(true));
  }

  /**
   * Tests that reserved keywords are not added to the parser unintentionally.
   * (Most keywords are non-reserved. The set of reserved words generally
   * only changes with a new version of the SQL standard.)
   *
   * <p>If the new keyword added is intended to be a reserved keyword, update
   * the {@link #RESERVED_KEYWORDS} list. If not, add the keyword to the
   * non-reserved keyword list in the parser.
   */
  @Test void testNoUnintendedNewReservedKeywords() {
    assumeTrue(isNotSubclass(), "don't run this test for sub-classes");
    final SqlAbstractParserImpl.Metadata metadata =
        getSqlParser("").getMetadata();

    final SortedSet<String> reservedKeywords = new TreeSet<>();
    final SortedSet<String> keywords92 = keywords("92");
    for (String s : metadata.getTokens()) {
      if (metadata.isKeyword(s) && metadata.isReservedWord(s)) {
        reservedKeywords.add(s);
      }
      // Check that the parser's list of SQL:92
      // reserved words is consistent with keywords("92").
      assertThat(s, metadata.isSql92ReservedWord(s),
          is(keywords92.contains(s)));
    }

    final String reason = "The parser has at least one new reserved keyword. "
        + "Are you sure it should be reserved? Difference:\n"
        + DiffTestCase.diffLines(ImmutableList.copyOf(getReservedKeywords()),
        ImmutableList.copyOf(reservedKeywords));
    assertThat(reason, reservedKeywords, is(getReservedKeywords()));
  }

  @Test void testTabStop() {
    sql("SELECT *\n\tFROM mytable")
        .ok("SELECT *\n"
            + "FROM `MYTABLE`");

    // make sure that the tab stops do not affect the placement of the
    // error tokens
    sql("SELECT *\tFROM mytable\t\tWHERE x ^=^ = y AND b = 1")
        .fails("(?s).*Encountered \"= =\" at line 1, column 32\\..*");
  }

  @Test void testLongIdentifiers() {
    StringBuilder ident128Builder = new StringBuilder();
    for (int i = 0; i < 128; i++) {
      ident128Builder.append((char) ('a' + (i % 26)));
    }
    String ident128 = ident128Builder.toString();
    String ident128Upper = ident128.toUpperCase(Locale.US);
    String ident129 = "x" + ident128;
    String ident129Upper = ident129.toUpperCase(Locale.US);

    sql("select * from " + ident128)
        .ok("SELECT *\n"
            + "FROM `" + ident128Upper + "`");
    sql("select * from ^" + ident129 + "^")
        .fails("Length of identifier '" + ident129Upper
            + "' must be less than or equal to 128 characters");

    sql("select " + ident128 + " from mytable")
        .ok("SELECT `" + ident128Upper + "`\n"
            + "FROM `MYTABLE`");
    sql("select ^" + ident129 + "^ from mytable")
        .fails("Length of identifier '" + ident129Upper
            + "' must be less than or equal to 128 characters");
  }

  /**
   * Tests that you can't quote the names of builtin functions.
   *
   * @see org.apache.calcite.test.SqlValidatorTest#testQuotedFunction()
   */
  @Test void testQuotedFunction() {
    expr("\"CAST\"(1 ^as^ double)")
        .fails("(?s).*Encountered \"as\" at .*");
    expr("\"POSITION\"('b' ^in^ 'alphabet')")
        .fails("(?s).*Encountered \"in \\\\'alphabet\\\\'\" at .*");
    expr("\"OVERLAY\"('a' ^PLAcing^ 'b' from 1)")
        .fails("(?s).*Encountered \"PLAcing\" at.*");
    expr("\"SUBSTRING\"('a' ^from^ 1)")
        .fails("(?s).*Encountered \"from\" at .*");
  }

  /** Tests applying a member function of a specific type as a suffix
   * function. */
  @Test void testMemberFunction() {
    sql("SELECT myColumn.func(a, b) FROM tbl")
        .ok("SELECT `MYCOLUMN`.`FUNC`(`A`, `B`)\n"
            + "FROM `TBL`");
    sql("SELECT myColumn.mySubField.func() FROM tbl")
        .ok("SELECT `MYCOLUMN`.`MYSUBFIELD`.`FUNC`()\n"
            + "FROM `TBL`");
    sql("SELECT tbl.myColumn.mySubField.func() FROM tbl")
        .ok("SELECT `TBL`.`MYCOLUMN`.`MYSUBFIELD`.`FUNC`()\n"
            + "FROM `TBL`");
    sql("SELECT tbl.foo(0).col.bar(2, 3) FROM tbl")
        .ok("SELECT ((`TBL`.`FOO`(0).`COL`).`BAR`(2, 3))\n"
            + "FROM `TBL`");
  }

  @Test void testUnicodeLiteral() {
    // Note that here we are constructing a SQL statement which directly
    // contains Unicode characters (not SQL Unicode escape sequences).  The
    // escaping here is Java-only, so by the time it gets to the SQL
    // parser, the literal already contains Unicode characters.
    String in1 =
        "values _UTF16'"
            + ConversionUtil.TEST_UNICODE_STRING + "'";
    String out1 =
        "VALUES (ROW(_UTF16'"
            + ConversionUtil.TEST_UNICODE_STRING + "'))";
    sql(in1).ok(out1);

    // Without the U& prefix, escapes are left unprocessed
    String in2 =
        "values '"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL + "'";
    String out2 =
        "VALUES (ROW('"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL + "'))";
    sql(in2).ok(out2);

    // Likewise, even with the U& prefix, if some other escape
    // character is specified, then the backslash-escape
    // sequences are not interpreted
    String in3 =
        "values U&'"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL
            + "' UESCAPE '!'";
    String out3 =
        "VALUES (ROW(_UTF16'"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL + "'))";
    sql(in3).ok(out3);
  }

  @Test void testUnicodeEscapedLiteral() {
    // Note that here we are constructing a SQL statement which
    // contains SQL-escaped Unicode characters to be handled
    // by the SQL parser.
    String in =
        "values U&'"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL + "'";
    String out =
        "VALUES (ROW(_UTF16'"
            + ConversionUtil.TEST_UNICODE_STRING + "'))";
    sql(in).ok(out);

    // Verify that we can override with an explicit escape character
    sql(in.replace("\\", "!") + "UESCAPE '!'").ok(out);
  }

  @Test void testIllegalUnicodeEscape() {
    expr("U&'abc' UESCAPE '!!'")
        .fails(".*must be exactly one character.*");
    expr("U&'abc' UESCAPE ''")
        .fails(".*must be exactly one character.*");
    expr("U&'abc' UESCAPE '0'")
        .fails(".*hex digit.*");
    expr("U&'abc' UESCAPE 'a'")
        .fails(".*hex digit.*");
    expr("U&'abc' UESCAPE 'F'")
        .fails(".*hex digit.*");
    expr("U&'abc' UESCAPE ' '")
        .fails(".*whitespace.*");
    expr("U&'abc' UESCAPE '+'")
        .fails(".*plus sign.*");
    expr("U&'abc' UESCAPE '\"'")
        .fails(".*double quote.*");
    expr("'abc' UESCAPE ^'!'^")
        .fails(".*without Unicode literal introducer.*");
    expr("^U&'\\0A'^")
        .fails(".*is not exactly four hex digits.*");
    expr("^U&'\\wxyz'^")
        .fails(".*is not exactly four hex digits.*");
  }

  @Test void testSqlOptions() throws SqlParseException {
    SqlNode node = getSqlParser("alter system set schema = true").parseStmt();
    SqlSetOption opt = (SqlSetOption) node;
    assertThat(opt.getScope(), equalTo("SYSTEM"));
    SqlPrettyWriter writer = new SqlPrettyWriter();
    assertThat(writer.format(opt.getName()), equalTo("\"SCHEMA\""));
    writer = new SqlPrettyWriter();
    assertThat(writer.format(opt.getValue()), equalTo("TRUE"));
    writer = new SqlPrettyWriter();
    assertThat(writer.format(opt),
        equalTo("ALTER SYSTEM SET \"SCHEMA\" = TRUE"));

    sql("alter system set \"a number\" = 1")
        .ok("ALTER SYSTEM SET `a number` = 1")
        .node(isDdl());
    sql("alter system set flag = false")
        .ok("ALTER SYSTEM SET `FLAG` = FALSE");
    sql("alter system set approx = -12.3450")
        .ok("ALTER SYSTEM SET `APPROX` = -12.3450");
    sql("alter system set onOff = on")
        .ok("ALTER SYSTEM SET `ONOFF` = `ON`");
    sql("alter system set onOff = off")
        .ok("ALTER SYSTEM SET `ONOFF` = `OFF`");
    sql("alter system set baz = foo")
        .ok("ALTER SYSTEM SET `BAZ` = `FOO`");


    sql("alter system set \"a\".\"number\" = 1")
        .ok("ALTER SYSTEM SET `a`.`number` = 1");
    sql("set approx = -12.3450")
        .ok("SET `APPROX` = -12.3450")
        .node(isDdl());

    node = getSqlParser("reset schema").parseStmt();
    opt = (SqlSetOption) node;
    assertThat(opt.getScope(), equalTo(null));
    writer = new SqlPrettyWriter();
    assertThat(writer.format(opt.getName()), equalTo("\"SCHEMA\""));
    assertThat(opt.getValue(), equalTo(null));
    writer = new SqlPrettyWriter();
    assertThat(writer.format(opt),
        equalTo("RESET \"SCHEMA\""));

    sql("alter system RESET flag")
        .ok("ALTER SYSTEM RESET `FLAG`");
    sql("reset onOff")
        .ok("RESET `ONOFF`")
        .node(isDdl());
    sql("reset \"this\".\"is\".\"sparta\"")
        .ok("RESET `this`.`is`.`sparta`");
    sql("alter system reset all")
        .ok("ALTER SYSTEM RESET `ALL`");
    sql("reset all")
        .ok("RESET `ALL`");

    // expressions not allowed
    sql("alter system set aString = 'abc' ^||^ 'def' ")
        .fails("(?s)Encountered \"\\|\\|\" at line 1, column 34\\..*");

    // multiple assignments not allowed
    sql("alter system set x = 1^,^ y = 2")
        .fails("(?s)Encountered \",\" at line 1, column 23\\..*");
  }

  @Test void testSequence() {
    sql("select next value for my_schema.my_seq from t")
        .ok("SELECT (NEXT VALUE FOR `MY_SCHEMA`.`MY_SEQ`)\n"
            + "FROM `T`");
    sql("select next value for my_schema.my_seq as s from t")
        .ok("SELECT (NEXT VALUE FOR `MY_SCHEMA`.`MY_SEQ`) AS `S`\n"
            + "FROM `T`");
    sql("select next value for my_seq as s from t")
        .ok("SELECT (NEXT VALUE FOR `MY_SEQ`) AS `S`\n"
            + "FROM `T`");
    sql("select 1 + next value for s + current value for s from t")
        .ok("SELECT ((1 + (NEXT VALUE FOR `S`)) + (CURRENT VALUE FOR `S`))\n"
            + "FROM `T`");
    sql("select 1 from t where next value for my_seq < 10")
        .ok("SELECT 1\n"
            + "FROM `T`\n"
            + "WHERE ((NEXT VALUE FOR `MY_SEQ`) < 10)");
    sql("select 1 from t\n"
        + "where next value for my_seq < 10 fetch next 3 rows only")
        .ok("SELECT 1\n"
            + "FROM `T`\n"
            + "WHERE ((NEXT VALUE FOR `MY_SEQ`) < 10)\n"
            + "FETCH NEXT 3 ROWS ONLY");
    sql("insert into t values next value for my_seq, current value for my_seq")
        .ok("INSERT INTO `T`\n"
            + "VALUES (ROW((NEXT VALUE FOR `MY_SEQ`))),\n"
            + "(ROW((CURRENT VALUE FOR `MY_SEQ`)))");
    sql("insert into t values (1, current value for my_seq)")
        .ok("INSERT INTO `T`\n"
            + "VALUES (ROW(1, (CURRENT VALUE FOR `MY_SEQ`)))");
  }

  @Test void testPivot() {
    final String sql = "SELECT * FROM emp\n"
        + "PIVOT (sum(sal) AS sal FOR job in ('CLERK' AS c))";
    final String expected = "SELECT *\n"
        + "FROM `EMP` PIVOT (SUM(`SAL`) AS `SAL`"
        + " FOR `JOB` IN ('CLERK' AS `C`))";
    sql(sql).ok(expected);

    // As previous, but parentheses around singleton column.
    final String sql2 = "SELECT * FROM emp\n"
        + "PIVOT (sum(sal) AS sal FOR (job) in ('CLERK' AS c))";
    sql(sql2).ok(expected);
  }

  /** As {@link #testPivot()} but composite FOR and two composite values. */
  @Test void testPivotComposite() {
    final String sql = "SELECT * FROM emp\n"
        + "PIVOT (sum(sal) AS sal FOR (job, deptno) IN\n"
        + " (('CLERK', 10) AS c10, ('MANAGER', 20) AS m20))";
    final String expected = "SELECT *\n"
        + "FROM `EMP` PIVOT (SUM(`SAL`) AS `SAL` FOR (`JOB`, `DEPTNO`)"
        + " IN (('CLERK', 10) AS `C10`, ('MANAGER', 20) AS `M20`))";
    sql(sql).ok(expected);
  }

  /** Pivot with no values. */
  @Test void testPivotWithoutValues() {
    final String sql = "SELECT * FROM emp\n"
        + "PIVOT (sum(sal) AS sal FOR job IN ())";
    final String expected = "SELECT *\n"
        + "FROM `EMP` PIVOT (SUM(`SAL`) AS `SAL` FOR `JOB` IN ())";
    sql(sql).ok(expected);
  }

  /** In PIVOT, FOR clause must contain only simple identifiers. */
  @Test void testPivotErrorExpressionInFor() {
    final String sql = "SELECT * FROM emp\n"
        + "PIVOT (sum(sal) AS sal FOR deptno ^-^10 IN (10, 20)";
    sql(sql).fails("(?s)Encountered \"-\" at .*");
  }

  /** As {@link #testPivotErrorExpressionInFor()} but more than one column. */
  @Test void testPivotErrorExpressionInCompositeFor() {
    final String sql = "SELECT * FROM emp\n"
        + "PIVOT (sum(sal) AS sal FOR (job, deptno ^-^10)\n"
        + " IN (('CLERK', 10), ('MANAGER', 20))";
    sql(sql).fails("(?s)Encountered \"-\" at .*");
  }

  /** More complex PIVOT case (multiple aggregates, composite FOR, multiple
   * values with and without aliases). */
  @Test void testPivot2() {
    final String sql = "SELECT *\n"
        + "FROM (SELECT deptno, job, sal\n"
        + "    FROM   emp)\n"
        + "PIVOT (SUM(sal) AS sum_sal, COUNT(*) AS \"COUNT\"\n"
        + "    FOR (job, deptno)\n"
        + "    IN (('CLERK', 10),\n"
        + "        ('MANAGER', 20) mgr20,\n"
        + "        ('ANALYST', 10) AS \"a10\"))\n"
        + "ORDER BY deptno";
    final String expected = "SELECT *\n"
        + "FROM (SELECT `DEPTNO`, `JOB`, `SAL`\n"
        + "FROM `EMP`) PIVOT (SUM(`SAL`) AS `SUM_SAL`, COUNT(*) AS `COUNT` "
        + "FOR (`JOB`, `DEPTNO`) "
        + "IN (('CLERK', 10),"
        + " ('MANAGER', 20) AS `MGR20`,"
        + " ('ANALYST', 10) AS `a10`))\n"
        + "ORDER BY `DEPTNO`";
    sql(sql).ok(expected);
  }

  @Test void testUnpivot() {
    final String sql = "SELECT *\n"
        + "FROM emp_pivoted\n"
        + "UNPIVOT (\n"
        + "  (sum_sal, count_star)\n"
        + "  FOR (job, deptno)\n"
        + "  IN ((c10_ss, c10_c) AS ('CLERK', 10),\n"
        + "      (c20_ss, c20_c) AS ('CLERK', 20),\n"
        + "      (a20_ss, a20_c) AS ('ANALYST', 20)))";
    final String expected = "SELECT *\n"
        + "FROM `EMP_PIVOTED` "
        + "UNPIVOT EXCLUDE NULLS ((`SUM_SAL`, `COUNT_STAR`)"
        + " FOR (`JOB`, `DEPTNO`)"
        + " IN ((`C10_SS`, `C10_C`) AS ('CLERK', 10),"
        + " (`C20_SS`, `C20_C`) AS ('CLERK', 20),"
        + " (`A20_SS`, `A20_C`) AS ('ANALYST', 20)))";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognize1() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    partition by type, price\n"
        + "    order by type asc, price desc\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PARTITION BY `TYPE`, `PRICE`\n"
        + "ORDER BY `TYPE`, `PRICE` DESC\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognize2() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+$)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)) $)\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognize3() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (^^strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (^ ((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognize4() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (^^strt down+ up+$)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (^ ((`STRT` (`DOWN` +)) (`UP` +)) $)\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognize5() {
    final String sql = "select *\n"
        + "  from (select * from t) match_recognize\n"
        + "  (\n"
        + "    pattern (strt down* up?)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM `T`) MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` (`DOWN` *)) (`UP` ?)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognize6() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (strt {-down-} up?)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` ({- `DOWN` -})) (`UP` ?)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognize7() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (strt down{2} up{3,})\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` (`DOWN` { 2 })) (`UP` { 3, })))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognize8() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (strt down{,2} up{3,5})\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` (`DOWN` { , 2 })) (`UP` { 3, 5 })))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognize9() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (strt {-down+-} {-up*-})\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` ({- (`DOWN` +) -})) ({- (`UP` *) -})))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognize10() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern ( A B C | A C B | B A C | B C A | C A B | C B A)\n"
        + "    define\n"
        + "      A as A.price > PREV(A.price),\n"
        + "      B as B.price < prev(B.price),\n"
        + "      C as C.price > prev(C.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN ((((((((`A` `B`) `C`) | ((`A` `C`) `B`)) | ((`B` `A`) `C`)) "
        + "| ((`B` `C`) `A`)) | ((`C` `A`) `B`)) | ((`C` `B`) `A`)))\n"
        + "DEFINE "
        + "`A` AS (`A`.`PRICE` > PREV(`A`.`PRICE`, 1)), "
        + "`B` AS (`B`.`PRICE` < PREV(`B`.`PRICE`, 1)), "
        + "`C` AS (`C`.`PRICE` > PREV(`C`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognize11() {
    final String sql = "select *\n"
        + "  from t match_recognize (\n"
        + "    pattern ( \"a\" \"b c\")\n"
        + "    define\n"
        + "      \"A\" as A.price > PREV(A.price),\n"
        + "      \"b c\" as \"b c\".foo\n"
        + "  ) as mr(c1, c2) join e as x on foo = baz";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN ((`a` `b c`))\n"
        + "DEFINE `A` AS (`A`.`PRICE` > PREV(`A`.`PRICE`, 1)),"
        + " `b c` AS `b c`.`FOO`) AS `MR` (`C1`, `C2`)\n"
        + "INNER JOIN `E` AS `X` ON (`FOO` = `BAZ`)";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeDefineClause() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > NEXT(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > NEXT(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeDefineClause2() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < FIRST(down.price),\n"
        + "      up as up.price > LAST(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < FIRST(`DOWN`.`PRICE`, 0)), "
        + "`UP` AS (`UP`.`PRICE` > LAST(`UP`.`PRICE`, 0))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeDefineClause3() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price,1),\n"
        + "      up as up.price > LAST(up.price + up.TAX)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > LAST((`UP`.`PRICE` + `UP`.`TAX`), 0))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeDefineClause4() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price,1),\n"
        + "      up as up.price > PREV(LAST(up.price + up.TAX),3)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(LAST((`UP`.`PRICE` + `UP`.`TAX`), 0), 3))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures1() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "   measures "
        + "   MATCH_NUMBER() as match_num,"
        + "   CLASSIFIER() as var_match,"
        + "   STRT.ts as start_ts,"
        + "   LAST(DOWN.ts) as bottom_ts,"
        + "   LAST(up.ts) as end_ts"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "MEASURES (MATCH_NUMBER ()) AS `MATCH_NUM`, "
        + "(CLASSIFIER()) AS `VAR_MATCH`, "
        + "`STRT`.`TS` AS `START_TS`, "
        + "LAST(`DOWN`.`TS`, 0) AS `BOTTOM_TS`, "
        + "LAST(`UP`.`TS`, 0) AS `END_TS`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures2() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "   measures STRT.ts as start_ts,"
        + "  FINAL LAST(DOWN.ts) as bottom_ts,"
        + "   LAST(up.ts) as end_ts"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "MEASURES `STRT`.`TS` AS `START_TS`, "
        + "FINAL LAST(`DOWN`.`TS`, 0) AS `BOTTOM_TS`, "
        + "LAST(`UP`.`TS`, 0) AS `END_TS`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures3() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "   measures STRT.ts as start_ts,"
        + "  RUNNING LAST(DOWN.ts) as bottom_ts,"
        + "   LAST(up.ts) as end_ts"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "MEASURES `STRT`.`TS` AS `START_TS`, "
        + "RUNNING LAST(`DOWN`.`TS`, 0) AS `BOTTOM_TS`, "
        + "LAST(`UP`.`TS`, 0) AS `END_TS`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures4() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "   measures "
        + "  FINAL count(up.ts) as up_ts,"
        + "  FINAL count(ts) as total_ts,"
        + "  RUNNING count(ts) as cnt_ts,"
        + "  price - strt.price as price_dif"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "MEASURES FINAL COUNT(`UP`.`TS`) AS `UP_TS`, "
        + "FINAL COUNT(`TS`) AS `TOTAL_TS`, "
        + "RUNNING COUNT(`TS`) AS `CNT_TS`, "
        + "(`PRICE` - `STRT`.`PRICE`) AS `PRICE_DIF`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))) AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures5() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "   measures "
        + "  FIRST(STRT.ts) as strt_ts,"
        + "  LAST(DOWN.ts) as down_ts,"
        + "  AVG(DOWN.ts) as avg_down_ts"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "MEASURES FIRST(`STRT`.`TS`, 0) AS `STRT_TS`, "
        + "LAST(`DOWN`.`TS`, 0) AS `DOWN_TS`, "
        + "AVG(`DOWN`.`TS`) AS `AVG_DOWN_TS`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeMeasures6() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "   measures "
        + "  FIRST(STRT.ts) as strt_ts,"
        + "  LAST(DOWN.ts) as down_ts,"
        + "  FINAL SUM(DOWN.ts) as sum_down_ts"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "MEASURES FIRST(`STRT`.`TS`, 0) AS `STRT_TS`, "
        + "LAST(`DOWN`.`TS`, 0) AS `DOWN_TS`, "
        + "FINAL SUM(`DOWN`.`TS`) AS `SUM_DOWN_TS`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternSkip1() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "     after match skip to next row\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "AFTER MATCH SKIP TO NEXT ROW\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternSkip2() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "     after match skip past last row\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "AFTER MATCH SKIP PAST LAST ROW\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternSkip3() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "     after match skip to FIRST down\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "AFTER MATCH SKIP TO FIRST `DOWN`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternSkip4() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "     after match skip to LAST down\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "AFTER MATCH SKIP TO LAST `DOWN`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizePatternSkip5() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "     after match skip to down\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "AFTER MATCH SKIP TO LAST `DOWN`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2993">[CALCITE-2993]
   * ParseException may be thrown for legal SQL queries due to incorrect
   * "LOOKAHEAD(1)" hints</a>. */
  @Test void testMatchRecognizePatternSkip6() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "     after match skip to last\n"
        + "    pattern (strt down+ up+)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "AFTER MATCH SKIP TO LAST `LAST`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeSubset1() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down)"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "SUBSET (`STDN` = (`STRT`, `DOWN`))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeSubset2() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "   measures STRT.ts as start_ts,"
        + "   LAST(DOWN.ts) as bottom_ts,"
        + "   AVG(stdn.price) as stdn_avg"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "MEASURES `STRT`.`TS` AS `START_TS`, "
        + "LAST(`DOWN`.`TS`, 0) AS `BOTTOM_TS`, "
        + "AVG(`STDN`.`PRICE`) AS `STDN_AVG`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "SUBSET (`STDN` = (`STRT`, `DOWN`))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeSubset3() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "   measures STRT.ts as start_ts,"
        + "   LAST(DOWN.ts) as bottom_ts,"
        + "   AVG(stdn.price) as stdn_avg"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down), stdn2 = (strt, down)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "MEASURES `STRT`.`TS` AS `START_TS`, "
        + "LAST(`DOWN`.`TS`, 0) AS `BOTTOM_TS`, "
        + "AVG(`STDN`.`PRICE`) AS `STDN_AVG`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "SUBSET (`STDN` = (`STRT`, `DOWN`)), (`STDN2` = (`STRT`, `DOWN`))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeRowsPerMatch1() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "   measures STRT.ts as start_ts,"
        + "   LAST(DOWN.ts) as bottom_ts,"
        + "   AVG(stdn.price) as stdn_avg"
        + "   ONE ROW PER MATCH"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down), stdn2 = (strt, down)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "MEASURES `STRT`.`TS` AS `START_TS`, "
        + "LAST(`DOWN`.`TS`, 0) AS `BOTTOM_TS`, "
        + "AVG(`STDN`.`PRICE`) AS `STDN_AVG`\n"
        + "ONE ROW PER MATCH\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "SUBSET (`STDN` = (`STRT`, `DOWN`)), (`STDN2` = (`STRT`, `DOWN`))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeRowsPerMatch2() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "   measures STRT.ts as start_ts,"
        + "   LAST(DOWN.ts) as bottom_ts,"
        + "   AVG(stdn.price) as stdn_avg"
        + "   ALL ROWS PER MATCH"
        + "    pattern (strt down+ up+)\n"
        + "    subset stdn = (strt, down), stdn2 = (strt, down)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "MEASURES `STRT`.`TS` AS `START_TS`, "
        + "LAST(`DOWN`.`TS`, 0) AS `BOTTOM_TS`, "
        + "AVG(`STDN`.`PRICE`) AS `STDN_AVG`\n"
        + "ALL ROWS PER MATCH\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +)))\n"
        + "SUBSET (`STDN` = (`STRT`, `DOWN`)), (`STDN2` = (`STRT`, `DOWN`))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testMatchRecognizeWithin() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    order by rowtime\n"
        + "    measures STRT.ts as start_ts,\n"
        + "      LAST(DOWN.ts) as bottom_ts,\n"
        + "      AVG(stdn.price) as stdn_avg\n"
        + "    pattern (strt down+ up+) within interval '3' second\n"
        + "    subset stdn = (strt, down), stdn2 = (strt, down)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "ORDER BY `ROWTIME`\n"
        + "MEASURES `STRT`.`TS` AS `START_TS`, "
        + "LAST(`DOWN`.`TS`, 0) AS `BOTTOM_TS`, "
        + "AVG(`STDN`.`PRICE`) AS `STDN_AVG`\n"
        + "PATTERN (((`STRT` (`DOWN` +)) (`UP` +))) WITHIN INTERVAL '3' SECOND\n"
        + "SUBSET (`STDN` = (`STRT`, `DOWN`)), (`STDN2` = (`STRT`, `DOWN`))\n"
        + "DEFINE `DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test void testWithinGroupClause1() {
    final String sql = "select col1,\n"
        + " collect(col2) within group (order by col3)\n"
        + "from t\n"
        + "order by col1 limit 10";
    final String expected = "SELECT `COL1`,"
        + " COLLECT(`COL2`) WITHIN GROUP (ORDER BY `COL3`)\n"
        + "FROM `T`\n"
        + "ORDER BY `COL1`\n"
        + "FETCH NEXT 10 ROWS ONLY";
    sql(sql).ok(expected);
  }

  @Test void testWithinGroupClause2() {
    final String sql = "select collect(col2) within group (order by col3)\n"
        + "from t\n"
        + "order by col1 limit 10";
    final String expected = "SELECT"
        + " COLLECT(`COL2`) WITHIN GROUP (ORDER BY `COL3`)\n"
        + "FROM `T`\n"
        + "ORDER BY `COL1`\n"
        + "FETCH NEXT 10 ROWS ONLY";
    sql(sql).ok(expected);
  }

  @Test void testWithinGroupClause3() {
    final String sql = "select collect(col2) within group (^)^ "
        + "from t order by col1 limit 10";
    sql(sql).fails("(?s).*Encountered \"\\)\" at line 1, column 36\\..*");
  }

  @Test void testWithinGroupClause4() {
    final String sql = "select col1,\n"
        + " collect(col2) within group (order by col3, col4)\n"
        + "from t\n"
        + "order by col1 limit 10";
    final String expected = "SELECT `COL1`,"
        + " COLLECT(`COL2`) WITHIN GROUP (ORDER BY `COL3`, `COL4`)\n"
        + "FROM `T`\n"
        + "ORDER BY `COL1`\n"
        + "FETCH NEXT 10 ROWS ONLY";
    sql(sql).ok(expected);
  }

  @Test void testWithinGroupClause5() {
    final String sql = "select col1,\n"
        + " collect(col2) within group (\n"
        + "  order by col3 desc nulls first, col4 asc nulls last)\n"
        + "from t\n"
        + "order by col1 limit 10";
    final String expected = "SELECT `COL1`, COLLECT(`COL2`) "
        + "WITHIN GROUP (ORDER BY `COL3` DESC NULLS FIRST, `COL4` NULLS LAST)\n"
        + "FROM `T`\n"
        + "ORDER BY `COL1`\n"
        + "FETCH NEXT 10 ROWS ONLY";
    sql(sql).ok(expected);
  }

  @Test void testStringAgg() {
    final String sql = "select\n"
        + "  string_agg(ename order by deptno, ename) as c1,\n"
        + "  string_agg(ename, '; ' order by deptno, ename desc) as c2,\n"
        + "  string_agg(ename) as c3,\n"
        + "  string_agg(ename, ':') as c4,\n"
        + "  string_agg(ename, ':' ignore nulls) as c5\n"
        + "from emp group by gender";
    final String expected = "SELECT"
        + " STRING_AGG(`ENAME` ORDER BY `DEPTNO`, `ENAME`) AS `C1`,"
        + " STRING_AGG(`ENAME`, '; ' ORDER BY `DEPTNO`, `ENAME` DESC) AS `C2`,"
        + " STRING_AGG(`ENAME`) AS `C3`,"
        + " STRING_AGG(`ENAME`, ':') AS `C4`,"
        + " STRING_AGG(`ENAME`, ':') IGNORE NULLS AS `C5`\n"
        + "FROM `EMP`\n"
        + "GROUP BY `GENDER`";
    sql(sql).ok(expected);
  }

  @Test void testArrayAgg() {
    final String sql = "select\n"
        + "  array_agg(ename respect nulls order by deptno, ename) as c1,\n"
        + "  array_concat_agg(ename order by deptno, ename desc) as c2,\n"
        + "  array_agg(ename) as c3,\n"
        + "  array_concat_agg(ename) within group (order by ename) as c4\n"
        + "from emp group by gender";
    final String expected = "SELECT"
        + " ARRAY_AGG(`ENAME` ORDER BY `DEPTNO`, `ENAME`) RESPECT NULLS AS `C1`,"
        + " ARRAY_CONCAT_AGG(`ENAME` ORDER BY `DEPTNO`, `ENAME` DESC) AS `C2`,"
        + " ARRAY_AGG(`ENAME`) AS `C3`,"
        + " ARRAY_CONCAT_AGG(`ENAME`) WITHIN GROUP (ORDER BY `ENAME`) AS `C4`\n"
        + "FROM `EMP`\n"
        + "GROUP BY `GENDER`";
    sql(sql).ok(expected);
  }

  @Test void testGroupConcat() {
    final String sql = "select\n"
        + "  group_concat(ename order by deptno, ename desc) as c2,\n"
        + "  group_concat(ename) as c3,\n"
        + "  group_concat(ename order by deptno, ename desc separator ',') as c4\n"
        + "from emp group by gender";
    final String expected = "SELECT"
        + " GROUP_CONCAT(`ENAME` ORDER BY `DEPTNO`, `ENAME` DESC) AS `C2`,"
        + " GROUP_CONCAT(`ENAME`) AS `C3`,"
        + " GROUP_CONCAT(`ENAME` ORDER BY `DEPTNO`, `ENAME` DESC SEPARATOR ',') AS `C4`\n"
        + "FROM `EMP`\n"
        + "GROUP BY `GENDER`";
    sql(sql).ok(expected);
  }

  @Test void testWithinDistinct() {
    final String sql = "select col1,\n"
        + " sum(col2) within distinct (col3 + col4, col5)\n"
        + "from t\n"
        + "order by col1 limit 10";
    final String expected = "SELECT `COL1`,"
        + " (SUM(`COL2`) WITHIN DISTINCT ((`COL3` + `COL4`), `COL5`))\n"
        + "FROM `T`\n"
        + "ORDER BY `COL1`\n"
        + "FETCH NEXT 10 ROWS ONLY";
    sql(sql).ok(expected);
  }

  @Test void testWithinDistinct2() {
    final String sql = "select col1,\n"
        + " sum(col2) within distinct (col3 + col4, col5)\n"
        + "   within group (order by col6 desc)\n"
        + "   filter (where col7 < col8) as sum2\n"
        + "from t\n"
        + "group by col9";
    final String expected = "SELECT `COL1`,"
        + " (SUM(`COL2`) WITHIN DISTINCT ((`COL3` + `COL4`), `COL5`))"
        + " WITHIN GROUP (ORDER BY `COL6` DESC)"
        + " FILTER (WHERE (`COL7` < `COL8`)) AS `SUM2`\n"
        + "FROM `T`\n"
        + "GROUP BY `COL9`";
    sql(sql).ok(expected);
  }

  @Test void testJsonValueExpressionOperator() {
    expr("foo format json")
        .ok("`FOO` FORMAT JSON");
    // Currently, encoding js not valid
    expr("foo format json encoding utf8")
        .ok("`FOO` FORMAT JSON");
    expr("foo format json encoding utf16")
        .ok("`FOO` FORMAT JSON");
    expr("foo format json encoding utf32")
        .ok("`FOO` FORMAT JSON");
    expr("null format json")
        .ok("NULL FORMAT JSON");
    // Test case to eliminate choice conflict on token <FORMAT>
    sql("select foo format from tab")
        .ok("SELECT `FOO` AS `FORMAT`\n"
            + "FROM `TAB`");
    // Test case to eliminate choice conflict on token <ENCODING>
    sql("select foo format json encoding from tab")
        .ok("SELECT `FOO` FORMAT JSON AS `ENCODING`\n"
            + "FROM `TAB`");
  }

  @Test void testJsonExists() {
    expr("json_exists('{\"foo\": \"bar\"}', 'lax $.foo')")
        .ok("JSON_EXISTS('{\"foo\": \"bar\"}', 'lax $.foo')");
    expr("json_exists('{\"foo\": \"bar\"}', 'lax $.foo' error on error)")
        .ok("JSON_EXISTS('{\"foo\": \"bar\"}', 'lax $.foo' ERROR ON ERROR)");
  }

  @Test void testJsonValue() {
    expr("json_value('{\"foo\": \"100\"}', 'lax $.foo' "
        + "returning integer)")
        .ok("JSON_VALUE('{\"foo\": \"100\"}', 'lax $.foo' "
            + "RETURNING INTEGER)");
    expr("json_value('{\"foo\": \"100\"}', 'lax $.foo' "
        + "returning integer default 10 on empty error on error)")
        .ok("JSON_VALUE('{\"foo\": \"100\"}', 'lax $.foo' "
            + "RETURNING INTEGER DEFAULT 10 ON EMPTY ERROR ON ERROR)");
  }

  @Test void testJsonQuery() {
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' WITHOUT ARRAY WRAPPER)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' WITH WRAPPER)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITH UNCONDITIONAL ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' WITH UNCONDITIONAL WRAPPER)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITH UNCONDITIONAL ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' WITH CONDITIONAL WRAPPER)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITH CONDITIONAL ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' NULL ON EMPTY)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' ERROR ON EMPTY)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER ERROR ON EMPTY NULL ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' EMPTY ARRAY ON EMPTY)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER EMPTY ARRAY ON EMPTY NULL ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' EMPTY OBJECT ON EMPTY)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER EMPTY OBJECT ON EMPTY NULL ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' NULL ON ERROR)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' ERROR ON ERROR)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY ERROR ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' EMPTY ARRAY ON ERROR)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY EMPTY ARRAY ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' EMPTY OBJECT ON ERROR)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY EMPTY OBJECT ON ERROR)");
    expr("json_query('{\"foo\": \"bar\"}', 'lax $' EMPTY ARRAY ON EMPTY "
        + "EMPTY OBJECT ON ERROR)")
        .ok("JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER EMPTY ARRAY ON EMPTY EMPTY OBJECT ON ERROR)");
  }

  @Test void testJsonObject() {
    expr("json_object('foo': 'bar')")
        .ok("JSON_OBJECT(KEY 'foo' VALUE 'bar' NULL ON NULL)");
    expr("json_object('foo': 'bar', 'foo2': 'bar2')")
        .ok("JSON_OBJECT(KEY 'foo' VALUE 'bar', KEY 'foo2' VALUE 'bar2' NULL ON NULL)");
    expr("json_object('foo' value 'bar')")
        .ok("JSON_OBJECT(KEY 'foo' VALUE 'bar' NULL ON NULL)");
    expr("json_object(key 'foo' value 'bar')")
        .ok("JSON_OBJECT(KEY 'foo' VALUE 'bar' NULL ON NULL)");
    expr("json_object('foo': null)")
        .ok("JSON_OBJECT(KEY 'foo' VALUE NULL NULL ON NULL)");
    expr("json_object('foo': null absent on null)")
        .ok("JSON_OBJECT(KEY 'foo' VALUE NULL ABSENT ON NULL)");
    expr("json_object('foo': json_object('foo': 'bar') format json)")
        .ok("JSON_OBJECT(KEY 'foo' VALUE "
            + "JSON_OBJECT(KEY 'foo' VALUE 'bar' NULL ON NULL) "
            + "FORMAT JSON NULL ON NULL)");

    if (!Bug.TODO_FIXED) {
      return;
    }
    // "LOOKAHEAD(2) list = JsonNameAndValue()" does not generate
    // valid LOOKAHEAD codes for the case "key: value".
    //
    // You can see the generated codes that are located at method
    // SqlParserImpl#JsonObjectFunctionCall. Looking ahead fails
    // immediately after seeking the tokens <KEY> and <COLON>.
    expr("json_object(key: value)")
        .ok("JSON_OBJECT(KEY `KEY` VALUE `VALUE` NULL ON NULL)");
  }

  @Test void testJsonType() {
    expr("json_type('11.56')")
        .ok("JSON_TYPE('11.56')");
    expr("json_type('{}')")
        .ok("JSON_TYPE('{}')");
    expr("json_type(null)")
        .ok("JSON_TYPE(NULL)");
    expr("json_type('[\"foo\",null]')")
        .ok("JSON_TYPE('[\"foo\",null]')");
    expr("json_type('{\"foo\": \"100\"}')")
        .ok("JSON_TYPE('{\"foo\": \"100\"}')");
  }

  @Test void testJsonDepth() {
    expr("json_depth('11.56')")
        .ok("JSON_DEPTH('11.56')");
    expr("json_depth('{}')")
        .ok("JSON_DEPTH('{}')");
    expr("json_depth(null)")
        .ok("JSON_DEPTH(NULL)");
    expr("json_depth('[\"foo\",null]')")
        .ok("JSON_DEPTH('[\"foo\",null]')");
    expr("json_depth('{\"foo\": \"100\"}')")
        .ok("JSON_DEPTH('{\"foo\": \"100\"}')");
  }

  @Test void testJsonLength() {
    expr("json_length('{\"foo\": \"bar\"}')")
        .ok("JSON_LENGTH('{\"foo\": \"bar\"}')");
    expr("json_length('{\"foo\": \"bar\"}', 'lax $')")
        .ok("JSON_LENGTH('{\"foo\": \"bar\"}', 'lax $')");
    expr("json_length('{\"foo\": \"bar\"}', 'strict $')")
        .ok("JSON_LENGTH('{\"foo\": \"bar\"}', 'strict $')");
    expr("json_length('{\"foo\": \"bar\"}', 'invalid $')")
        .ok("JSON_LENGTH('{\"foo\": \"bar\"}', 'invalid $')");
  }

  @Test void testJsonKeys() {
    expr("json_keys('{\"foo\": \"bar\"}', 'lax $')")
        .ok("JSON_KEYS('{\"foo\": \"bar\"}', 'lax $')");
    expr("json_keys('{\"foo\": \"bar\"}', 'strict $')")
        .ok("JSON_KEYS('{\"foo\": \"bar\"}', 'strict $')");
    expr("json_keys('{\"foo\": \"bar\"}', 'invalid $')")
        .ok("JSON_KEYS('{\"foo\": \"bar\"}', 'invalid $')");
  }

  @Test void testJsonRemove() {
    expr("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$')")
        .ok("JSON_REMOVE('[\"a\", [\"b\", \"c\"], \"d\"]', '$')");
    expr("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$[1]', '$[0]')")
        .ok("JSON_REMOVE('[\"a\", [\"b\", \"c\"], \"d\"]', '$[1]', '$[0]')");
  }

  @Test void testJsonObjectAgg() {
    expr("json_objectagg(k_column: v_column)")
        .ok("JSON_OBJECTAGG(KEY `K_COLUMN` VALUE `V_COLUMN` NULL ON NULL)");
    expr("json_objectagg(k_column value v_column)")
        .ok("JSON_OBJECTAGG(KEY `K_COLUMN` VALUE `V_COLUMN` NULL ON NULL)");
    expr("json_objectagg(key k_column value v_column)")
        .ok("JSON_OBJECTAGG(KEY `K_COLUMN` VALUE `V_COLUMN` NULL ON NULL)");
    expr("json_objectagg(k_column: null)")
        .ok("JSON_OBJECTAGG(KEY `K_COLUMN` VALUE NULL NULL ON NULL)");
    expr("json_objectagg(k_column: null absent on null)")
        .ok("JSON_OBJECTAGG(KEY `K_COLUMN` VALUE NULL ABSENT ON NULL)");
    expr("json_objectagg(k_column: json_object(k_column: v_column) format json)")
        .ok("JSON_OBJECTAGG(KEY `K_COLUMN` VALUE "
            + "JSON_OBJECT(KEY `K_COLUMN` VALUE `V_COLUMN` NULL ON NULL) "
            + "FORMAT JSON NULL ON NULL)");
  }

  @Test void testJsonArray() {
    expr("json_array('foo')")
        .ok("JSON_ARRAY('foo' ABSENT ON NULL)");
    expr("json_array(null)")
        .ok("JSON_ARRAY(NULL ABSENT ON NULL)");
    expr("json_array(null null on null)")
        .ok("JSON_ARRAY(NULL NULL ON NULL)");
    expr("json_array(json_array('foo', 'bar') format json)")
        .ok("JSON_ARRAY(JSON_ARRAY('foo', 'bar' ABSENT ON NULL) FORMAT JSON ABSENT ON NULL)");
  }

  @Test void testJsonPretty() {
    expr("json_pretty('foo')")
        .ok("JSON_PRETTY('foo')");
    expr("json_pretty(null)")
        .ok("JSON_PRETTY(NULL)");
  }

  @Test void testJsonStorageSize() {
    expr("json_storage_size('foo')")
        .ok("JSON_STORAGE_SIZE('foo')");
    expr("json_storage_size(null)")
        .ok("JSON_STORAGE_SIZE(NULL)");
  }

  @Test void testJsonArrayAgg1() {
    expr("json_arrayagg(\"column\")")
        .ok("JSON_ARRAYAGG(`column` ABSENT ON NULL)");
    expr("json_arrayagg(\"column\" null on null)")
        .ok("JSON_ARRAYAGG(`column` NULL ON NULL)");
    expr("json_arrayagg(json_array(\"column\") format json)")
        .ok("JSON_ARRAYAGG(JSON_ARRAY(`column` ABSENT ON NULL) FORMAT JSON ABSENT ON NULL)");
  }

  @Test void testJsonArrayAgg2() {
    expr("json_arrayagg(\"column\" order by \"column\")")
        .ok("JSON_ARRAYAGG(`column` ABSENT ON NULL) WITHIN GROUP (ORDER BY `column`)");
    expr("json_arrayagg(\"column\") within group (order by \"column\")")
        .ok("JSON_ARRAYAGG(`column` ABSENT ON NULL) WITHIN GROUP (ORDER BY `column`)");
    sql("^json_arrayagg(\"column\" order by \"column\") within group (order by \"column\")^")
        .fails("(?s).*Including both WITHIN GROUP\\(\\.\\.\\.\\) and inside ORDER BY "
            + "in a single JSON_ARRAYAGG call is not allowed.*");
  }

  @Test void testJsonPredicate() {
    expr("'{}' is json")
        .ok("('{}' IS JSON VALUE)");
    expr("'{}' is json value")
        .ok("('{}' IS JSON VALUE)");
    expr("'{}' is json object")
        .ok("('{}' IS JSON OBJECT)");
    expr("'[]' is json array")
        .ok("('[]' IS JSON ARRAY)");
    expr("'100' is json scalar")
        .ok("('100' IS JSON SCALAR)");
    expr("'{}' is not json")
        .ok("('{}' IS NOT JSON VALUE)");
    expr("'{}' is not json value")
        .ok("('{}' IS NOT JSON VALUE)");
    expr("'{}' is not json object")
        .ok("('{}' IS NOT JSON OBJECT)");
    expr("'[]' is not json array")
        .ok("('[]' IS NOT JSON ARRAY)");
    expr("'100' is not json scalar")
        .ok("('100' IS NOT JSON SCALAR)");
  }

  @Test void testParseWithReader() throws Exception {
    String query = "select * from dual";
    SqlParser sqlParserReader = getSqlParser(new StringReader(query), b -> b);
    SqlNode node1 = sqlParserReader.parseQuery();
    SqlParser sqlParserString = getSqlParser(query);
    SqlNode node2 = sqlParserString.parseQuery();
    assertEquals(node2.toString(), node1.toString());
  }

  @Test void testConfigureFromDialect() {
    // Calcite's default converts unquoted identifiers to upper case
    sql("select unquotedColumn from \"doubleQuotedTable\"")
        .withDialect(CALCITE)
        .ok("SELECT \"UNQUOTEDCOLUMN\"\n"
            + "FROM \"doubleQuotedTable\"");
    // MySQL leaves unquoted identifiers unchanged
    sql("select unquotedColumn from `doubleQuotedTable`")
        .withDialect(MYSQL)
        .ok("SELECT `unquotedColumn`\n"
            + "FROM `doubleQuotedTable`");
    // Oracle converts unquoted identifiers to upper case
    sql("select unquotedColumn from \"doubleQuotedTable\"")
        .withDialect(ORACLE)
        .ok("SELECT \"UNQUOTEDCOLUMN\"\n"
            + "FROM \"doubleQuotedTable\"");
    // PostgreSQL converts unquoted identifiers to lower case
    sql("select unquotedColumn from \"doubleQuotedTable\"")
        .withDialect(POSTGRESQL)
        .ok("SELECT \"unquotedcolumn\"\n"
            + "FROM \"doubleQuotedTable\"");
    // Redshift converts all identifiers to lower case
    sql("select unquotedColumn from \"doubleQuotedTable\"")
        .withDialect(REDSHIFT)
        .ok("SELECT \"unquotedcolumn\"\n"
            + "FROM \"doublequotedtable\"");
    // BigQuery leaves quoted and unquoted identifiers unchanged
    sql("select unquotedColumn from `doubleQuotedTable`")
        .withDialect(BIG_QUERY)
        .ok("SELECT unquotedColumn\n"
            + "FROM doubleQuotedTable");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4230">[CALCITE-4230]
   * In Babel for BigQuery, split quoted table names that contain dots</a>. */
  @Test void testSplitIdentifier() {
    final String sql = "select *\n"
        + "from `bigquery-public-data.samples.natality`";
    final String sql2 = "select *\n"
        + "from `bigquery-public-data`.`samples`.`natality`";
    final String expectedSplit = "SELECT *\n"
        + "FROM `bigquery-public-data`.samples.natality";
    final String expectedNoSplit = "SELECT *\n"
        + "FROM `bigquery-public-data.samples.natality`";
    final String expectedSplitMysql = "SELECT *\n"
        + "FROM `bigquery-public-data`.`samples`.`natality`";
    // In BigQuery, an identifier containing dots is split into sub-identifiers.
    sql(sql)
        .withDialect(BIG_QUERY)
        .ok(expectedSplit);
    // In MySQL, identifiers are not split.
    sql(sql)
        .withDialect(MYSQL)
        .ok(expectedNoSplit);
    // Query with split identifiers produces split AST. No surprise there.
    sql(sql2)
        .withDialect(BIG_QUERY)
        .ok(expectedSplit);
    // Similar to previous; we just quote simple identifiers on unparse.
    sql(sql2)
        .withDialect(MYSQL)
        .ok(expectedSplitMysql);
  }

  @Test void testParenthesizedSubQueries() {
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM `TAB`) AS `X`";

    final String sql1 = "SELECT * FROM (((SELECT * FROM tab))) X";
    sql(sql1).ok(expected);

    final String sql2 = "SELECT * FROM ((((((((((((SELECT * FROM tab)))))))))))) X";
    sql(sql2).ok(expected);
  }

  @Test void testQueryHint() {
    final String sql1 = "select "
        + "/*+ properties(k1='v1', k2='v2', 'a.b.c'='v3'), "
        + "no_hash_join, Index(idx1, idx2), "
        + "repartition(3) */ "
        + "empno, ename, deptno from emps";
    final String expected1 = "SELECT\n"
        + "/*+ `PROPERTIES`(`K1` = 'v1', `K2` = 'v2', 'a.b.c' = 'v3'), "
        + "`NO_HASH_JOIN`, "
        + "`INDEX`(`IDX1`, `IDX2`), "
        + "`REPARTITION`(3) */\n"
        + "`EMPNO`, `ENAME`, `DEPTNO`\n"
        + "FROM `EMPS`";
    sql(sql1).ok(expected1);
    // Hint item right after the token "/*+"
    final String sql2 = "select /*+properties(k1='v1', k2='v2')*/ empno from emps";
    final String expected2 = "SELECT\n"
        + "/*+ `PROPERTIES`(`K1` = 'v1', `K2` = 'v2') */\n"
        + "`EMPNO`\n"
        + "FROM `EMPS`";
    sql(sql2).ok(expected2);
    // Hint item without parentheses
    final String sql3 = "select /*+ simple_hint */ empno, ename, deptno from emps limit 2";
    final String expected3 = "SELECT\n"
        + "/*+ `SIMPLE_HINT` */\n"
        + "`EMPNO`, `ENAME`, `DEPTNO`\n"
        + "FROM `EMPS`\n"
        + "FETCH NEXT 2 ROWS ONLY";
    sql(sql3).ok(expected3);
  }

  @Test void testTableHintsInQuery() {
    final String hint = "/*+ PROPERTIES(K1 ='v1', K2 ='v2'), INDEX(IDX0, IDX1) */";
    final String sql1 = String.format(Locale.ROOT, "select * from t %s", hint);
    final String expected1 = "SELECT *\n"
        + "FROM `T`\n"
        + "/*+ `PROPERTIES`(`K1` = 'v1', `K2` = 'v2'), `INDEX`(`IDX0`, `IDX1`) */";
    sql(sql1).ok(expected1);
    final String sql2 = String.format(Locale.ROOT, "select * from\n"
        + "(select * from t %s union all select * from t %s )", hint, hint);
    final String expected2 = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM `T`\n"
        + "/*+ `PROPERTIES`(`K1` = 'v1', `K2` = 'v2'), `INDEX`(`IDX0`, `IDX1`) */\n"
        + "UNION ALL\n"
        + "SELECT *\n"
        + "FROM `T`\n"
        + "/*+ `PROPERTIES`(`K1` = 'v1', `K2` = 'v2'), `INDEX`(`IDX0`, `IDX1`) */)";
    sql(sql2).ok(expected2);
    final String sql3 = String.format(Locale.ROOT, "select * from t %s join t %s", hint, hint);
    final String expected3 = "SELECT *\n"
        + "FROM `T`\n"
        + "/*+ `PROPERTIES`(`K1` = 'v1', `K2` = 'v2'), `INDEX`(`IDX0`, `IDX1`) */\n"
        + "INNER JOIN `T`\n"
        + "/*+ `PROPERTIES`(`K1` = 'v1', `K2` = 'v2'), `INDEX`(`IDX0`, `IDX1`) */";
    sql(sql3).ok(expected3);
  }

  @Test void testTableHintsInInsert() {
    final String sql = "insert into emps\n"
        + "/*+ PROPERTIES(k1='v1', k2='v2'), INDEX(idx0, idx1) */\n"
        + "select * from emps";
    final String expected = "INSERT INTO `EMPS`\n"
        + "/*+ `PROPERTIES`(`K1` = 'v1', `K2` = 'v2'), `INDEX`(`IDX0`, `IDX1`) */\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    sql(sql).ok(expected);
  }

  @Test void testTableHintsInDelete() {
    final String sql = "delete from emps\n"
        + "/*+ properties(k1='v1', k2='v2'), index(idx1, idx2), no_hash_join */\n"
        + "where empno=12";
    final String expected = "DELETE FROM `EMPS`\n"
        + "/*+ `PROPERTIES`(`K1` = 'v1', `K2` = 'v2'), `INDEX`(`IDX1`, `IDX2`), `NO_HASH_JOIN` */\n"
        + "WHERE (`EMPNO` = 12)";
    sql(sql).ok(expected);
  }

  @Test void testTableHintsInUpdate() {
    final String sql = "update emps\n"
        + "/*+ properties(k1='v1', k2='v2'), index(idx1, idx2), no_hash_join */\n"
        + "set empno = empno + 1, sal = sal - 1\n"
        + "where empno=12";
    final String expected = "UPDATE `EMPS`\n"
        + "/*+ `PROPERTIES`(`K1` = 'v1', `K2` = 'v2'), "
        + "`INDEX`(`IDX1`, `IDX2`), `NO_HASH_JOIN` */ "
        + "SET `EMPNO` = (`EMPNO` + 1)"
        + ", `SAL` = (`SAL` - 1)\n"
        + "WHERE (`EMPNO` = 12)";
    sql(sql).ok(expected);
  }

  @Test void testTableHintsInMerge() {
    final String sql = "merge into emps\n"
        + "/*+ properties(k1='v1', k2='v2'), index(idx1, idx2), no_hash_join */ e\n"
        + "using tempemps as t\n"
        + "on e.empno = t.empno\n"
        + "when matched then update\n"
        + "set name = t.name, deptno = t.deptno, salary = t.salary * .1\n"
        + "when not matched then insert (name, dept, salary)\n"
        + "values(t.name, 10, t.salary * .15)";
    final String expected = "MERGE INTO `EMPS`\n"
        + "/*+ `PROPERTIES`(`K1` = 'v1', `K2` = 'v2'), "
        + "`INDEX`(`IDX1`, `IDX2`), `NO_HASH_JOIN` */ "
        + "AS `E`\n"
        + "USING `TEMPEMPS` AS `T`\n"
        + "ON (`E`.`EMPNO` = `T`.`EMPNO`)\n"
        + "WHEN MATCHED THEN UPDATE SET `NAME` = `T`.`NAME`"
        + ", `DEPTNO` = `T`.`DEPTNO`"
        + ", `SALARY` = (`T`.`SALARY` * 0.1)\n"
        + "WHEN NOT MATCHED THEN INSERT (`NAME`, `DEPT`, `SALARY`) "
        + "(VALUES (ROW(`T`.`NAME`, 10, (`T`.`SALARY` * 0.15))))";
    sql(sql).ok(expected);
  }

  @Test void testHintThroughShuttle() throws Exception {
    final String sql = "select * from emp /*+ options('key1' = 'val1') */";
    final SqlNode sqlNode = getSqlParser(sql).parseStmt();
    final SqlNode shuttled = sqlNode.accept(new SqlShuttle() {
      @Override public SqlNode visit(SqlIdentifier identifier) {
        // Copy the identifier in order to return a new SqlTableRef.
        return identifier.clone(identifier.getParserPosition());
      }
    });
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "/*+ `OPTIONS`('key1' = 'val1') */";
    assertThat(linux(shuttled.toString()), is(expected));
  }

  @Test void testInvalidHintFormat() {
    final String sql1 = "select "
        + "/*+ properties(^k1^=123, k2='v2'), no_hash_join() */ "
        + "empno, ename, deptno from emps";
    sql(sql1).fails("(?s).*Encountered \"k1 = 123\" at .*");
    final String sql2 = "select "
        + "/*+ properties(k1, k2^=^'v2'), no_hash_join */ "
        + "empno, ename, deptno from emps";
    sql(sql2).fails("(?s).*Encountered \"=\" at line 1, column 29.\n.*");
    final String sql3 = "select "
        + "/*+ no_hash_join() */ "
        + "empno, ename, deptno from emps";
    // Allow empty options.
    final String expected3 = "SELECT\n"
        + "/*+ `NO_HASH_JOIN` */\n"
        + "`EMPNO`, `ENAME`, `DEPTNO`\n"
        + "FROM `EMPS`";
    sql(sql3).ok(expected3);
    final String sql4 = "select "
        + "/*+ properties(^a^.b.c=123, k2='v2') */"
        + "empno, ename, deptno from emps";
    sql(sql4).fails("(?s).*Encountered \"a .\" at .*");
  }

  /** Tests {@link Hoist}. */
  @Test protected void testHoist() {
    final String sql = "select 1 as x,\n"
        + "  'ab' || 'c' as y\n"
        + "from emp /* comment with 'quoted string'? */ as e\n"
        + "where deptno < 40\n"
        + "and hiredate > date '2010-05-06'";
    final Hoist.Hoisted hoisted = Hoist.create(Hoist.config()).hoist(sql);

    // Simple toString converts each variable to '?N'
    final String expected = "select ?0 as x,\n"
        + "  ?1 || ?2 as y\n"
        + "from emp /* comment with 'quoted string'? */ as e\n"
        + "where deptno < ?3\n"
        + "and hiredate > ?4";
    assertThat(hoisted.toString(), is(expected));

    // As above, using the function explicitly.
    assertThat(hoisted.substitute(Hoist::ordinalString), is(expected));

    // Simple toString converts each variable to '?N'
    final String expected1 = "select 1 as x,\n"
        + "  ?1 || ?2 as y\n"
        + "from emp /* comment with 'quoted string'? */ as e\n"
        + "where deptno < 40\n"
        + "and hiredate > date '2010-05-06'";
    assertThat(hoisted.substitute(Hoist::ordinalStringIfChar), is(expected1));

    // Custom function converts variables to '[N:TYPE:VALUE]'
    final String expected2 = "select [0:DECIMAL:1] as x,\n"
        + "  [1:CHAR:ab] || [2:CHAR:c] as y\n"
        + "from emp /* comment with 'quoted string'? */ as e\n"
        + "where deptno < [3:DECIMAL:40]\n"
        + "and hiredate > [4:DATE:2010-05-06]";
    assertThat(hoisted.substitute(SqlParserTest::varToStr), is(expected2));
  }

  protected static String varToStr(Hoist.Variable v) {
    if (v.node instanceof SqlLiteral) {
      SqlLiteral literal = (SqlLiteral) v.node;
      return "[" + v.ordinal
          + ":" + literal.getTypeName()
          + ":" + literal.toValue()
          + "]";
    } else {
      return "[" + v.ordinal + "]";
    }
  }

  //~ Inner Interfaces -------------------------------------------------------

  /**
   * Callback to control how test actions are performed.
   */
  protected interface Tester {
    void checkList(StringAndPos sap, List<String> expected);

    void check(StringAndPos sap, SqlDialect dialect, String expected,
        Consumer<SqlParser> parserChecker);

    void checkExp(StringAndPos sap, SqlDialect dialect, String expected,
        Consumer<SqlParser> parserChecker);

    void checkFails(StringAndPos sap, SqlDialect dialect, boolean list,
        String expectedMsgPattern);

    void checkExpFails(StringAndPos sap, SqlDialect dialect,
        String expectedMsgPattern);

    void checkNode(StringAndPos sap, SqlDialect dialect,
        Matcher<SqlNode> matcher);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Default implementation of {@link Tester}.
   */
  protected class TesterImpl implements Tester {
    private void check(
        SqlNode sqlNode,
        SqlDialect dialect,
        String expected) {
      final SqlDialect dialect2 = Util.first(dialect, AnsiSqlDialect.DEFAULT);
      final SqlWriterConfig c2 = SQL_WRITER_CONFIG.withDialect(dialect2);
      final String actual = sqlNode.toSqlString(c -> c2).getSql();
      TestUtil.assertEqualsVerbose(expected, linux(actual));
    }

    @Override public void checkList(StringAndPos sap, List<String> expected) {
      final SqlNodeList sqlNodeList = parseStmtsAndHandleEx(sap.sql);
      assertThat(sqlNodeList.size(), is(expected.size()));

      for (int i = 0; i < sqlNodeList.size(); i++) {
        SqlNode sqlNode = sqlNodeList.get(i);
        check(sqlNode, null, expected.get(i));
      }
    }

    public void check(StringAndPos sap, SqlDialect dialect, String expected,
        Consumer<SqlParser> parserChecker) {
      final UnaryOperator<SqlParser.Config> transform = getTransform(dialect);
      final SqlNode sqlNode =
          parseStmtAndHandleEx(sap.sql, transform, parserChecker);
      check(sqlNode, dialect, expected);
    }

    protected SqlNode parseStmtAndHandleEx(String sql,
        UnaryOperator<SqlParser.Config> transform,
        Consumer<SqlParser> parserChecker) {
      final Reader reader = new SourceStringReader(sql);
      final SqlParser parser = getSqlParser(reader, transform);
      final SqlNode sqlNode;
      try {
        sqlNode = parser.parseStmt();
        parserChecker.accept(parser);
      } catch (SqlParseException e) {
        throw new RuntimeException("Error while parsing SQL: " + sql, e);
      }
      return sqlNode;
    }

    /** Parses a list of statements. */
    protected SqlNodeList parseStmtsAndHandleEx(String sql) {
      final SqlNodeList sqlNodeList;
      try {
        sqlNodeList = getSqlParser(sql).parseStmtList();
      } catch (SqlParseException e) {
        throw new RuntimeException("Error while parsing SQL: " + sql, e);
      }
      return sqlNodeList;
    }

    public void checkExp(StringAndPos sap, SqlDialect dialect, String expected,
        Consumer<SqlParser> parserChecker) {
      final UnaryOperator<SqlParser.Config> transform = getTransform(dialect);
      final SqlNode sqlNode =
          parseExpressionAndHandleEx(sap.sql, transform, parserChecker);
      final String actual = sqlNode.toSqlString(null, true).getSql();
      TestUtil.assertEqualsVerbose(expected, linux(actual));
    }

    protected SqlNode parseExpressionAndHandleEx(String sql,
        UnaryOperator<SqlParser.Config> transform,
        Consumer<SqlParser> parserChecker) {
      final SqlNode sqlNode;
      try {
        final SqlParser parser =
            getSqlParser(new SourceStringReader(sql), transform);
        sqlNode = parser.parseExpression();
        parserChecker.accept(parser);
      } catch (SqlParseException e) {
        throw new RuntimeException("Error while parsing expression: " + sql, e);
      }
      return sqlNode;
    }

    @Override public void checkFails(StringAndPos sap, SqlDialect dialect,
        boolean list, String expectedMsgPattern) {
      Throwable thrown = null;
      try {
        final SqlNode sqlNode;
        final UnaryOperator<SqlParser.Config> transform =
            getTransform(dialect);
        final Reader reader = new SourceStringReader(sap.sql);
        final SqlParser parser = getSqlParser(reader, transform);
        if (list) {
          sqlNode = parser.parseStmtList();
        } else {
          sqlNode = parser.parseStmt();
        }
        Util.discard(sqlNode);
      } catch (Throwable ex) {
        thrown = ex;
      }

      checkEx(expectedMsgPattern, sap, thrown);
    }

    @Override public void checkNode(StringAndPos sap, SqlDialect dialect,
        Matcher<SqlNode> matcher) {
      try {
        final UnaryOperator<SqlParser.Config> transform = getTransform(dialect);
        final Reader reader = new SourceStringReader(sap.sql);
        final SqlParser parser = getSqlParser(reader, transform);
        final SqlNode sqlNode = parser.parseStmt();
        assertThat(sqlNode, matcher);
      } catch (SqlParseException e) {
        throw TestUtil.rethrow(e);
      }
    }

    /**
     * Tests that an expression throws an exception which matches the given
     * pattern.
     */
    @Override public void checkExpFails(StringAndPos sap, SqlDialect dialect,
        String expectedMsgPattern) {
      Throwable thrown = null;
      try {
        final UnaryOperator<SqlParser.Config> transform = getTransform(dialect);
        final Reader reader = new SourceStringReader(sap.sql);
        final SqlParser parser = getSqlParser(reader, transform);
        final SqlNode sqlNode = parser.parseExpression();
        Util.discard(sqlNode);
      } catch (Throwable ex) {
        thrown = ex;
      }

      checkEx(expectedMsgPattern, sap, thrown);
    }

    protected void checkEx(String expectedMsgPattern, StringAndPos sap,
        Throwable thrown) {
      SqlTests.checkEx(thrown, expectedMsgPattern, sap,
          SqlTests.Stage.VALIDATE);
    }
  }

  private boolean isNotSubclass() {
    return this.getClass().equals(SqlParserTest.class);
  }

  /**
   * Implementation of {@link Tester} which makes sure that the results of
   * unparsing a query are consistent with the original query.
   */
  public class UnparsingTesterImpl extends TesterImpl {
    private UnaryOperator<SqlWriterConfig> simple() {
      return c -> c.withSelectListItemsOnSeparateLines(false)
          .withUpdateSetListNewline(false)
          .withIndentation(0)
          .withFromFolding(SqlWriterConfig.LineFolding.TALL);
    }

    private UnaryOperator<SqlWriterConfig> simpleWithParens() {
      return simple().andThen(withParens())::apply;
    }

    private UnaryOperator<SqlWriterConfig> simpleWithParensAnsi() {
      return simpleWithParens().andThen(withAnsi())::apply;
    }

    private UnaryOperator<SqlWriterConfig> withParens() {
      return c -> c.withAlwaysUseParentheses(true);
    }

    private UnaryOperator<SqlWriterConfig> withAnsi() {
      return c -> c.withDialect(AnsiSqlDialect.DEFAULT);
    }

    private UnaryOperator<SqlWriterConfig> randomize(Random random) {
      return c -> c.withFoldLength(random.nextInt(5) * 20 + 3)
          .withHavingFolding(nextLineFolding(random))
          .withWhereFolding(nextLineFolding(random))
          .withSelectFolding(nextLineFolding(random))
          .withFromFolding(nextLineFolding(random))
          .withGroupByFolding(nextLineFolding(random))
          .withClauseStartsLine(random.nextBoolean())
          .withClauseEndsLine(random.nextBoolean());
    }

    private String toSqlString(SqlNodeList sqlNodeList,
        UnaryOperator<SqlWriterConfig> transform) {
      return sqlNodeList.stream()
          .map(node -> node.toSqlString(transform).getSql())
          .collect(Collectors.joining(";"));
    }

    private SqlWriterConfig.LineFolding nextLineFolding(Random random) {
      return nextEnum(random, SqlWriterConfig.LineFolding.class);
    }

    private <E extends Enum<E>> E nextEnum(Random random, Class<E> enumClass) {
      final E[] constants = enumClass.getEnumConstants();
      return constants[random.nextInt(constants.length)];
    }

    private void checkList(SqlNodeList sqlNodeList, List<String> expected) {
      assertThat(sqlNodeList.size(), is(expected.size()));

      for (int i = 0; i < sqlNodeList.size(); i++) {
        SqlNode sqlNode = sqlNodeList.get(i);
        // Unparse with no dialect, always parenthesize.
        final String actual =
            sqlNode.toSqlString(simpleWithParensAnsi()).getSql();
        assertEquals(expected.get(i), linux(actual));
      }
    }

    @Override public void checkList(StringAndPos sap, List<String> expected) {
      SqlNodeList sqlNodeList = parseStmtsAndHandleEx(sap.sql);

      checkList(sqlNodeList, expected);

      // Unparse again in Calcite dialect (which we can parse), and
      // minimal parentheses.
      final String sql1 = toSqlString(sqlNodeList, simple());

      // Parse and unparse again.
      SqlNodeList sqlNodeList2;
      final Quoting q = quoting;
      try {
        quoting = Quoting.DOUBLE_QUOTE;
        sqlNodeList2 = parseStmtsAndHandleEx(sql1);
      } finally {
        quoting = q;
      }
      final String sql2 = toSqlString(sqlNodeList2, simple());

      // Should be the same as we started with.
      assertEquals(sql1, sql2);

      // Now unparse again in the null dialect.
      // If the unparser is not including sufficient parens to override
      // precedence, the problem will show up here.
      checkList(sqlNodeList2, expected);

      final Random random = new Random();
      final String sql3 = toSqlString(sqlNodeList, randomize(random));
      assertThat(sql3, notNullValue());
    }

    @Override public void check(StringAndPos sap, SqlDialect dialect,
        String expected, Consumer<SqlParser> parserChecker) {
      final UnaryOperator<SqlParser.Config> transform = getTransform(dialect);
      SqlNode sqlNode = parseStmtAndHandleEx(sap.sql, transform, parserChecker);

      // Unparse with the given dialect, always parenthesize.
      final SqlDialect dialect2 = Util.first(dialect, AnsiSqlDialect.DEFAULT);
      final UnaryOperator<SqlWriterConfig> transform2 =
          simpleWithParens().andThen(c -> c.withDialect(dialect2))::apply;
      final String actual = sqlNode.toSqlString(transform2).getSql();
      assertEquals(expected, linux(actual));

      // Unparse again in Calcite dialect (which we can parse), and
      // minimal parentheses.
      final String sql1 = sqlNode.toSqlString(simple()).getSql();

      // Parse and unparse again.
      SqlNode sqlNode2;
      final Quoting q = quoting;
      try {
        quoting = Quoting.DOUBLE_QUOTE;
        sqlNode2 = parseStmtAndHandleEx(sql1, b -> b, parser -> { });
      } finally {
        quoting = q;
      }
      final String sql2 = sqlNode2.toSqlString(simple()).getSql();

      // Should be the same as we started with.
      assertEquals(sql1, sql2);

      // Now unparse again in the given dialect.
      // If the unparser is not including sufficient parens to override
      // precedence, the problem will show up here.
      final String actual2 = sqlNode.toSqlString(transform2).getSql();
      assertEquals(expected, linux(actual2));

      // Now unparse with a randomly configured SqlPrettyWriter.
      // (This is a much a test for SqlPrettyWriter as for the parser.)
      final Random random = new Random();
      final String sql3 = sqlNode.toSqlString(randomize(random)).getSql();
      assertThat(sql3, notNullValue());
      SqlNode sqlNode4;
      try {
        quoting = Quoting.DOUBLE_QUOTE;
        sqlNode4 = parseStmtAndHandleEx(sql1, b -> b, parser -> { });
      } finally {
        quoting = q;
      }
      final String sql4 = sqlNode4.toSqlString(simple()).getSql();
      assertEquals(sql1, sql4);
    }

    @Override public void checkExp(StringAndPos sap, SqlDialect dialect,
        String expected, Consumer<SqlParser> parserChecker) {
      final UnaryOperator<SqlParser.Config> transform = getTransform(dialect);
      SqlNode sqlNode =
          parseExpressionAndHandleEx(sap.sql, transform, parserChecker);

      // Unparse with no dialect, always parenthesize.
      final UnaryOperator<SqlWriterConfig> transform2 = c ->
          simpleWithParens().apply(c).withDialect(AnsiSqlDialect.DEFAULT);
      final String actual = sqlNode.toSqlString(transform2).getSql();
      assertEquals(expected, linux(actual));

      // Unparse again in Calcite dialect (which we can parse), and
      // minimal parentheses.
      final String sql1 =
          sqlNode.toSqlString(UnaryOperator.identity()).getSql();

      // Parse and unparse again.
      // (Turn off parser checking, and use double-quotes.)
      SqlNode sqlNode2;
      final Quoting q = quoting;
      try {
        quoting = Quoting.DOUBLE_QUOTE;
        sqlNode2 = parseExpressionAndHandleEx(sql1, transform, parser -> { });
      } finally {
        quoting = q;
      }
      final String sql2 =
          sqlNode2.toSqlString(UnaryOperator.identity()).getSql();

      // Should be the same as we started with.
      assertEquals(sql1, sql2);

      // Now unparse again in the null dialect.
      // If the unparser is not including sufficient parens to override
      // precedence, the problem will show up here.
      final String actual2 = sqlNode2.toSqlString(null, true).getSql();
      assertEquals(expected, linux(actual2));
    }

    @Override public void checkFails(StringAndPos sap, SqlDialect dialect,
        boolean list, String expectedMsgPattern) {
      // Do nothing. We're not interested in unparsing invalid SQL
    }

    @Override public void checkExpFails(StringAndPos sap, SqlDialect dialect,
        String expectedMsgPattern) {
      // Do nothing. We're not interested in unparsing invalid SQL
    }
  }

  /** Converts a string to linux format (LF line endings rather than CR-LF),
   * except if disabled in {@link #LINUXIFY}. */
  private String linux(String s) {
    if (LINUXIFY.get()[0]) {
      s = Util.toLinux(s);
    }
    return s;
  }

  /** Helper class for building fluent code such as
   * {@code sql("values 1").ok();}. */
  protected class Sql {
    private final StringAndPos sap;
    private final boolean expression;
    private final SqlDialect dialect;
    private final Consumer<SqlParser> parserChecker;

    Sql(StringAndPos sap, boolean expression, SqlDialect dialect,
        Consumer<SqlParser> parserChecker) {
      this.sap = Objects.requireNonNull(sap, "sap");
      this.expression = expression;
      this.dialect = dialect;
      this.parserChecker = Objects.requireNonNull(parserChecker, "parserChecker");
    }

    public Sql same() {
      return ok(sap.sql);
    }

    public Sql ok(String expected) {
      if (expression) {
        getTester().checkExp(sap, dialect, expected, parserChecker);
      } else {
        getTester().check(sap, dialect, expected, parserChecker);
      }
      return this;
    }

    public Sql fails(String expectedMsgPattern) {
      if (expression) {
        getTester().checkExpFails(sap, dialect, expectedMsgPattern);
      } else {
        getTester().checkFails(sap, dialect, false, expectedMsgPattern);
      }
      return this;
    }

    public Sql hasWarning(Consumer<List<? extends Throwable>> messageMatcher) {
      return new Sql(sap, expression, dialect, parser ->
          messageMatcher.accept(parser.getWarnings()));
    }

    public Sql node(Matcher<SqlNode> matcher) {
      getTester().checkNode(sap, dialect, matcher);
      return this;
    }

    /** Flags that this is an expression, not a whole query. */
    public Sql expression() {
      return expression ? this : new Sql(sap, true, dialect, parserChecker);
    }

    public Sql withDialect(SqlDialect dialect) {
      return new Sql(sap, expression, dialect, parserChecker);
    }
  }

  /** Helper class for building fluent code,
   * similar to {@link Sql}, but used to manipulate
   * a list of statements, such as
   * {@code sqlList("select * from a;").ok();}. */
  protected class SqlList {
    private final StringAndPos sap;

    SqlList(String sql) {
      this.sap = StringAndPos.of(sql);
    }

    public SqlList ok(String... expected) {
      getTester().checkList(sap, ImmutableList.copyOf(expected));
      return this;
    }

    public SqlList fails(String expectedMsgPattern) {
      getTester().checkFails(sap, null, true, expectedMsgPattern);
      return this;
    }
  }

  /** Runs tests on period operators such as OVERLAPS, IMMEDIATELY PRECEDES. */
  private class Checker {
    final String op;
    final String period;

    Checker(String op, String period) {
      this.op = op;
      this.period = period;
    }

    public void checkExp(String sql, String expected) {
      expr(sql.replace("$op", op).replace("$p", period))
          .ok(expected.replace("$op", op.toUpperCase(Locale.ROOT)));
    }

    public void checkExpFails(String sql, String expected) {
      expr(sql.replace("$op", op).replace("$p", period))
          .fails(expected.replace("$op", op));
    }
  }
}
