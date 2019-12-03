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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.test.DiffTestCase;
import org.apache.calcite.test.SqlValidatorTestCase;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ConversionUtil;
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
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * A <code>SqlParserTest</code> is a unit-test for
 * {@link SqlParser the SQL parser}.
 *
 * <p>To reuse this test for an extension parser, implement the
 * {@link #parserImplFactory()} method to return the extension parser
 * implementation.
 */
public class SqlParserTest {
  //~ Static fields/initializers ---------------------------------------------

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

  Quoting quoting = Quoting.DOUBLE_QUOTE;
  Casing unquotedCasing = Casing.TO_UPPER;
  Casing quotedCasing = Casing.UNCHANGED;
  SqlConformance conformance = SqlConformanceEnum.DEFAULT;

  //~ Constructors -----------------------------------------------------------

  public SqlParserTest() {
  }

  //~ Methods ----------------------------------------------------------------

  // Helper functions -------------------------------------------------------

  protected Tester getTester() {
    return new TesterImpl();
  }

  protected void check(
      String sql,
      String expected) {
    sql(sql).ok(expected);
  }

  protected Sql sql(String sql) {
    return new Sql(sql);
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
    return getSqlParser(new SourceStringReader(sql));
  }

  protected SqlParser getSqlParser(Reader source) {
    return SqlParser.create(source,
        SqlParser.configBuilder()
            .setParserFactory(parserImplFactory())
            .setQuoting(quoting)
            .setUnquotedCasing(unquotedCasing)
            .setQuotedCasing(quotedCasing)
            .setConformance(conformance)
            .build());
  }

  protected SqlParser getDialectSqlParser(String sql, SqlDialect dialect) {
    return SqlParser.create(new SourceStringReader(sql),
        dialect.configureParser(SqlParser.configBuilder()).build());
  }

  protected void checkExp(
      String sql,
      String expected) {
    getTester().checkExp(sql, expected);
  }

  protected void checkExpSame(String sql) {
    checkExp(sql, sql);
  }

  protected void checkFails(
      String sql,
      String expectedMsgPattern) {
    sql(sql).fails(expectedMsgPattern);
  }

  /**
   * Tests that an expression throws an exception which matches the given
   * pattern.
   */
  protected void checkExpFails(
      String sql,
      String expectedMsgPattern) {
    getTester().checkExpFails(sql, expectedMsgPattern);
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
  @Nonnull private static Matcher<SqlNode> isQuoted(final int i,
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
  @Test public void testExceptionCleanup() {
    checkFails(
        "select 0.5e1^.1^ from sales.emps",
        "(?s).*Encountered \".1\" at line 1, column 13.\n"
            + "Was expecting one of:\n"
            + "    <EOF> \n"
            + "    \"AS\" \\.\\.\\.\n"
            + "    \"EXCEPT\" \\.\\.\\.\n"
            + ".*");
  }

  @Test public void testInvalidToken() {
    // Causes problems to the test infrastructure because the token mgr
    // throws a java.lang.Error. The usual case is that the parser throws
    // an exception.
    checkFails(
        "values (a^#^b)",
        "Lexical error at line 1, column 10\\.  Encountered: \"#\" \\(35\\), after : \"\"");
  }

  // TODO: should fail in parser
  @Test public void testStarAsFails() {
    sql("select * as x from emp")
        .ok("SELECT * AS `X`\n"
            + "FROM `EMP`");
  }

  @Test public void testDerivedColumnList() {
    check("select * from emp as e (empno, gender) where true",
        "SELECT *\n"
            + "FROM `EMP` AS `E` (`EMPNO`, `GENDER`)\n"
            + "WHERE TRUE");
  }

  @Test public void testDerivedColumnListInJoin() {
    check(
        "select * from emp as e (empno, gender) join dept as d (deptno, dname) on emp.deptno = dept.deptno",
        "SELECT *\n"
            + "FROM `EMP` AS `E` (`EMPNO`, `GENDER`)\n"
            + "INNER JOIN `DEPT` AS `D` (`DEPTNO`, `DNAME`) ON (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)");
  }

  /** Test case that does not reproduce but is related to
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2637">[CALCITE-2637]
   * Prefix '-' operator failed between BETWEEN and AND</a>. */
  @Test public void testBetweenAnd() {
    final String sql = "select * from emp\n"
        + "where deptno between - DEPTNO + 1 and 5";
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`DEPTNO` BETWEEN ASYMMETRIC ((- `DEPTNO`) + 1) AND 5)";
    sql(sql).ok(expected);
  }

  @Test public void testBetweenAnd2() {
    final String sql = "select * from emp\n"
        + "where deptno between - DEPTNO + 1 and - empno - 3";
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`DEPTNO` BETWEEN ASYMMETRIC ((- `DEPTNO`) + 1)"
        + " AND ((- `EMPNO`) - 3))";
    sql(sql).ok(expected);
  }

  @Ignore
  @Test public void testDerivedColumnListNoAs() {
    check("select * from emp e (empno, gender) where true", "foo");
  }

  // jdbc syntax
  @Ignore
  @Test public void testEmbeddedCall() {
    checkExp("{call foo(?, ?)}", "foo");
  }

  @Ignore
  @Test public void testEmbeddedFunction() {
    checkExp("{? = call bar (?, ?)}", "foo");
  }

  @Test public void testColumnAliasWithAs() {
    check(
        "select 1 as foo from emp",
        "SELECT 1 AS `FOO`\n"
            + "FROM `EMP`");
  }

  @Test public void testColumnAliasWithoutAs() {
    check("select 1 foo from emp",
        "SELECT 1 AS `FOO`\n"
            + "FROM `EMP`");
  }

  @Test public void testEmbeddedDate() {
    checkExp("{d '1998-10-22'}", "DATE '1998-10-22'");
  }

  @Test public void testEmbeddedTime() {
    checkExp("{t '16:22:34'}", "TIME '16:22:34'");
  }

  @Test public void testEmbeddedTimestamp() {
    checkExp("{ts '1998-10-22 16:22:34'}", "TIMESTAMP '1998-10-22 16:22:34'");
  }

  @Test public void testNot() {
    check(
        "select not true, not false, not null, not unknown from t",
        "SELECT (NOT TRUE), (NOT FALSE), (NOT NULL), (NOT UNKNOWN)\n"
            + "FROM `T`");
  }

  @Test public void testBooleanPrecedenceAndAssociativity() {
    check(
        "select * from t where true and false",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (TRUE AND FALSE)");

    check(
        "select * from t where null or unknown and unknown",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (NULL OR (UNKNOWN AND UNKNOWN))");

    check(
        "select * from t where true and (true or true) or false",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((TRUE AND (TRUE OR TRUE)) OR FALSE)");

    check(
        "select * from t where 1 and true",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (1 AND TRUE)");
  }

  @Test public void testLessThanAssociativity() {
    checkExp("NOT a = b", "(NOT (`A` = `B`))");

    // comparison operators are left-associative
    checkExp("x < y < z", "((`X` < `Y`) < `Z`)");
    checkExp("x < y <= z = a", "(((`X` < `Y`) <= `Z`) = `A`)");
    checkExp("a = x < y <= z = a", "((((`A` = `X`) < `Y`) <= `Z`) = `A`)");

    // IS NULL has lower precedence than comparison
    checkExp("a = x is null", "((`A` = `X`) IS NULL)");
    checkExp("a = x is not null", "((`A` = `X`) IS NOT NULL)");

    // BETWEEN, IN, LIKE have higher precedence than comparison
    checkExp("a = x between y = b and z = c",
        "((`A` = (`X` BETWEEN ASYMMETRIC (`Y` = `B`) AND `Z`)) = `C`)");
    checkExp("a = x like y = b",
        "((`A` = (`X` LIKE `Y`)) = `B`)");
    checkExp("a = x not like y = b",
        "((`A` = (`X` NOT LIKE `Y`)) = `B`)");
    checkExp("a = x similar to y = b",
        "((`A` = (`X` SIMILAR TO `Y`)) = `B`)");
    checkExp("a = x not similar to y = b",
        "((`A` = (`X` NOT SIMILAR TO `Y`)) = `B`)");
    checkExp("a = x not in (y, z) = b",
        "((`A` = (`X` NOT IN (`Y`, `Z`))) = `B`)");

    // LIKE has higher precedence than IS NULL
    checkExp("a like b is null", "((`A` LIKE `B`) IS NULL)");
    checkExp("a not like b is not null",
        "((`A` NOT LIKE `B`) IS NOT NULL)");

    // = has higher precedence than NOT
    checkExp("NOT a = b", "(NOT (`A` = `B`))");
    checkExp("NOT a = NOT b", "(NOT (`A` = (NOT `B`)))");

    // IS NULL has higher precedence than NOT
    checkExp("NOT a IS NULL", "(NOT (`A` IS NULL))");
    checkExp("NOT a = b IS NOT NULL", "(NOT ((`A` = `B`) IS NOT NULL))");

    // NOT has higher precedence than AND, which  has higher precedence than OR
    checkExp("NOT a AND NOT b", "((NOT `A`) AND (NOT `B`))");
    checkExp("NOT a OR NOT b", "((NOT `A`) OR (NOT `B`))");
    checkExp("NOT a = b AND NOT c = d OR NOT e = f",
        "(((NOT (`A` = `B`)) AND (NOT (`C` = `D`))) OR (NOT (`E` = `F`)))");
    checkExp("NOT a = b OR NOT c = d AND NOT e = f",
        "((NOT (`A` = `B`)) OR ((NOT (`C` = `D`)) AND (NOT (`E` = `F`))))");
    checkExp("NOT NOT a = b OR NOT NOT c = d",
        "((NOT (NOT (`A` = `B`))) OR (NOT (NOT (`C` = `D`))))");
  }

  @Test public void testIsBooleans() {
    String[] inOuts = {"NULL", "TRUE", "FALSE", "UNKNOWN"};

    for (String inOut : inOuts) {
      check(
          "select * from t where nOt fAlSe Is " + inOut,
          "SELECT *\n"
              + "FROM `T`\n"
              + "WHERE (NOT (FALSE IS " + inOut + "))");

      check(
          "select * from t where c1=1.1 IS NOT " + inOut,
          "SELECT *\n"
              + "FROM `T`\n"
              + "WHERE ((`C1` = 1.1) IS NOT " + inOut + ")");
    }
  }

  @Test public void testIsBooleanPrecedenceAndAssociativity() {
    check("select * from t where x is unknown is not unknown",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`X` IS UNKNOWN) IS NOT UNKNOWN)");

    check("select 1 from t where not true is unknown",
        "SELECT 1\n"
            + "FROM `T`\n"
            + "WHERE (NOT (TRUE IS UNKNOWN))");

    check(
        "select * from t where x is unknown is not unknown is false is not false"
            + " is true is not true is null is not null",
        "SELECT *\n"
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
    check(sql, expected);
  }

  @Test public void testEqualNotEqual() {
    checkExp("'abc'=123", "('abc' = 123)");
    checkExp("'abc'<>123", "('abc' <> 123)");
    checkExp("'abc'<>123='def'<>456", "((('abc' <> 123) = 'def') <> 456)");
    checkExp("'abc'<>123=('def'<>456)", "(('abc' <> 123) = ('def' <> 456))");
  }

  @Test public void testBangEqualIsBad() {
    // Quoth www.ocelot.ca:
    //   "Other relators besides '=' are what you'd expect if
    //   you've used any programming language: > and >= and < and <=. The
    //   only potential point of confusion is that the operator for 'not
    //   equals' is <> as in BASIC. There are many texts which will tell
    //   you that != is SQL's not-equals operator; those texts are false;
    //   it's one of those unstampoutable urban myths."
    // Therefore, we only support != with certain SQL conformance levels.
    checkExpFails("'abc'^!=^123",
        "Bang equal '!=' is not allowed under the current SQL conformance level");
  }

  @Test public void testBetween() {
    check(
        "select * from t where price between 1 and 2",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` BETWEEN ASYMMETRIC 1 AND 2)");

    check(
        "select * from t where price between symmetric 1 and 2",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` BETWEEN SYMMETRIC 1 AND 2)");

    check(
        "select * from t where price not between symmetric 1 and 2",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` NOT BETWEEN SYMMETRIC 1 AND 2)");

    check(
        "select * from t where price between ASYMMETRIC 1 and 2+2*2",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`PRICE` BETWEEN ASYMMETRIC 1 AND (2 + (2 * 2)))");

    check(
        "select * from t where price > 5 and price not between 1 + 2 and 3 * 4 AnD price is null",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (((`PRICE` > 5) AND (`PRICE` NOT BETWEEN ASYMMETRIC (1 + 2) AND (3 * 4))) AND (`PRICE` IS NULL))");

    check(
        "select * from t where price > 5 and price between 1 + 2 and 3 * 4 + price is null",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`PRICE` > 5) AND ((`PRICE` BETWEEN ASYMMETRIC (1 + 2) AND ((3 * 4) + `PRICE`)) IS NULL))");

    check(
        "select * from t where price > 5 and price between 1 + 2 and 3 * 4 or price is null",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (((`PRICE` > 5) AND (`PRICE` BETWEEN ASYMMETRIC (1 + 2) AND (3 * 4))) OR (`PRICE` IS NULL))");

    check(
        "values a between c and d and e and f between g and h",
        "VALUES (ROW((((`A` BETWEEN ASYMMETRIC `C` AND `D`) AND `E`) AND (`F` BETWEEN ASYMMETRIC `G` AND `H`))))");

    checkFails(
        "values a between b or c^",
        ".*BETWEEN operator has no terminating AND");

    checkFails(
        "values a ^between^",
        "(?s).*Encountered \"between <EOF>\" at line 1, column 10.*");

    checkFails(
        "values a between symmetric 1^",
        ".*BETWEEN operator has no terminating AND");

    // precedence of BETWEEN is higher than AND and OR, but lower than '+'
    check(
        "values a between b and c + 2 or d and e",
        "VALUES (ROW(((`A` BETWEEN ASYMMETRIC `B` AND (`C` + 2)) OR (`D` AND `E`))))");

    // '=' has slightly lower precedence than BETWEEN; both are left-assoc
    check(
        "values x = a between b and c = d = e",
        "VALUES (ROW((((`X` = (`A` BETWEEN ASYMMETRIC `B` AND `C`)) = `D`) = `E`)))");

    // AND doesn't match BETWEEN if it's between parentheses!
    check(
        "values a between b or (c and d) or e and f",
        "VALUES (ROW((`A` BETWEEN ASYMMETRIC ((`B` OR (`C` AND `D`)) OR `E`) AND `F`)))");
  }

  @Test public void testOperateOnColumn() {
    check(
        "select c1*1,c2  + 2,c3/3,c4-4,c5*c4  from t",
        "SELECT (`C1` * 1), (`C2` + 2), (`C3` / 3), (`C4` - 4), (`C5` * `C4`)\n"
            + "FROM `T`");
  }

  @Test public void testRow() {
    check(
        "select t.r.\"EXPR$1\", t.r.\"EXPR$0\" from (select (1,2) r from sales.depts) t",
        "SELECT `T`.`R`.`EXPR$1`, `T`.`R`.`EXPR$0`\n"
            + "FROM (SELECT (ROW(1, 2)) AS `R`\n"
            + "FROM `SALES`.`DEPTS`) AS `T`");

    check(
        "select t.r.\"EXPR$1\".\"EXPR$2\" "
            + "from (select ((1,2),(3,4,5)) r from sales.depts) t",
        "SELECT `T`.`R`.`EXPR$1`.`EXPR$2`\n"
            + "FROM (SELECT (ROW((ROW(1, 2)), (ROW(3, 4, 5)))) AS `R`\n"
            + "FROM `SALES`.`DEPTS`) AS `T`");

    check(
        "select t.r.\"EXPR$1\".\"EXPR$2\" "
            + "from (select ((1,2),(3,4,5,6)) r from sales.depts) t",
        "SELECT `T`.`R`.`EXPR$1`.`EXPR$2`\n"
            + "FROM (SELECT (ROW((ROW(1, 2)), (ROW(3, 4, 5, 6)))) AS `R`\n"
            + "FROM `SALES`.`DEPTS`) AS `T`");

    // Conformance DEFAULT and LENIENT support explicit row value constructor
    conformance = SqlConformanceEnum.DEFAULT;
    final String selectRow = "select ^row(t1a, t2a)^ from t1";
    final String expected = "SELECT (ROW(`T1A`, `T2A`))\n"
        + "FROM `T1`";
    sql(selectRow).sansCarets().ok(expected);
    conformance = SqlConformanceEnum.LENIENT;
    sql(selectRow).sansCarets().ok(expected);

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
    sql(whereRow).sansCarets().ok(whereExpected);
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    sql(whereRow).fails(pattern);

    final String whereRow2 = "select 1 from t2 where ^(x, y)^ < (a, b)";
    conformance = SqlConformanceEnum.DEFAULT;
    sql(whereRow2).sansCarets().ok(whereExpected);

    // After this point, SqlUnparserTest has problems.
    // We generate ROW in a dialect that does not allow ROW in all contexts.
    // So bail out.
    assumeFalse(isUnparserTest());
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    sql(whereRow2).sansCarets().ok(whereExpected);
  }

  /** Whether this is a sub-class that tests un-parsing as well as parsing. */
  protected boolean isUnparserTest() {
    return false;
  }

  @Test public void testRowWithDot() {
    check("select (1,2).a from c.t", "SELECT ((ROW(1, 2)).`A`)\nFROM `C`.`T`");
    check("select row(1,2).a from c.t", "SELECT ((ROW(1, 2)).`A`)\nFROM `C`.`T`");
    check("select tbl.foo(0).col.bar from tbl",
        "SELECT ((`TBL`.`FOO`(0).`COL`).`BAR`)\nFROM `TBL`");
  }

  @Test public void testPeriod() {
    // We don't have a PERIOD constructor currently;
    // ROW constructor is sufficient for now.
    checkExp("period (date '1969-01-05', interval '2-3' year to month)",
        "(ROW(DATE '1969-01-05', INTERVAL '2-3' YEAR TO MONTH))");
  }

  @Test public void testOverlaps() {
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
  @Test public void testStmtListWithSelect() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    sqlList("select * from emp, dept")
         .ok(expected);
  }

  @Test public void testStmtListWithSelectAndSemicolon() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    sqlList("select * from emp, dept;")
         .ok(expected);
  }

  @Test public void testStmtListWithTwoSelect() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    sqlList("select * from emp, dept ; select * from emp, dept")
        .ok(expected, expected);
  }

  @Test public void testStmtListWithTwoSelectSemicolon() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    sqlList("select * from emp, dept ; select * from emp, dept;")
        .ok(expected, expected);
  }

  @Test public void testStmtListWithSelectDelete() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    final String expected1 = "DELETE FROM `EMP`";
    sqlList("select * from emp, dept; delete from emp")
         .ok(expected, expected1);
  }

  @Test public void testStmtListWithSelectDeleteUpdate() {
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

  @Test public void testStmtListWithSemiColonInComment() {
    final String sql = ""
        + "select * from emp, dept; // comment with semicolon ; values 1\n"
        + "values 2";
    final String expected = "SELECT *\n"
        + "FROM `EMP`,\n"
        + "`DEPT`";
    final String expected1 = "VALUES (ROW(2))";
    sqlList(sql).ok(expected, expected1);
  }

  @Test public void testStmtListWithSemiColonInWhere() {
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`NAME` LIKE 'toto;')";
    final String expected1 = "DELETE FROM `EMP`";
    sqlList("select * from emp where name like 'toto;'; delete from emp")
         .ok(expected, expected1);
  }

  @Test public void testStmtListWithInsertSelectInsert() {
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

  /** Should fail since the first statement lacks semicolon */
  @Test public void testStmtListWithoutSemiColon1() {
    sqlList("select * from emp where name like 'toto' "
        + "^delete^ from emp")
        .fails("(?s).*Encountered \"delete\" at .*");
  }

  /** Should fail since the third statement lacks semicolon */
  @Test public void testStmtListWithoutSemiColon2() {
    sqlList("select * from emp where name like 'toto'; "
        + "delete from emp; "
        + "insert into dept (name, deptno) values ('a', 123) "
        + "^select^ * from dept")
        .fails("(?s).*Encountered \"select\" at .*");
  }

  @Test public void testIsDistinctFrom() {
    check(
        "select x is distinct from y from t",
        "SELECT (`X` IS DISTINCT FROM `Y`)\n"
            + "FROM `T`");

    check(
        "select * from t where x is distinct from y",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` IS DISTINCT FROM `Y`)");

    check(
        "select * from t where x is distinct from (4,5,6)",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` IS DISTINCT FROM (ROW(4, 5, 6)))");

    check(
        "select * from t where x is distinct from row (4,5,6)",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` IS DISTINCT FROM (ROW(4, 5, 6)))");

    check(
        "select * from t where true is distinct from true",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (TRUE IS DISTINCT FROM TRUE)");

    check(
        "select * from t where true is distinct from true is true",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((TRUE IS DISTINCT FROM TRUE) IS TRUE)");
  }

  @Test public void testIsNotDistinct() {
    check(
        "select x is not distinct from y from t",
        "SELECT (`X` IS NOT DISTINCT FROM `Y`)\n"
            + "FROM `T`");

    check(
        "select * from t where true is not distinct from true",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (TRUE IS NOT DISTINCT FROM TRUE)");
  }

  @Test public void testFloor() {
    checkExp("floor(1.5)", "FLOOR(1.5)");
    checkExp("floor(x)", "FLOOR(`X`)");

    checkExp("floor(x to second)", "FLOOR(`X` TO SECOND)");
    checkExp("floor(x to epoch)", "FLOOR(`X` TO EPOCH)");
    checkExp("floor(x to minute)", "FLOOR(`X` TO MINUTE)");
    checkExp("floor(x to hour)", "FLOOR(`X` TO HOUR)");
    checkExp("floor(x to day)", "FLOOR(`X` TO DAY)");
    checkExp("floor(x to dow)", "FLOOR(`X` TO DOW)");
    checkExp("floor(x to doy)", "FLOOR(`X` TO DOY)");
    checkExp("floor(x to week)", "FLOOR(`X` TO WEEK)");
    checkExp("floor(x to month)", "FLOOR(`X` TO MONTH)");
    checkExp("floor(x to quarter)", "FLOOR(`X` TO QUARTER)");
    checkExp("floor(x to year)", "FLOOR(`X` TO YEAR)");
    checkExp("floor(x to decade)", "FLOOR(`X` TO DECADE)");
    checkExp("floor(x to century)", "FLOOR(`X` TO CENTURY)");
    checkExp("floor(x to millennium)", "FLOOR(`X` TO MILLENNIUM)");

    checkExp("floor(x + interval '1:20' minute to second)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND))");
    checkExp("floor(x + interval '1:20' minute to second to second)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO SECOND)");
    checkExp("floor(x + interval '1:20' minute to second to epoch)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO EPOCH)");
    checkExp("floor(x + interval '1:20' hour to minute)",
        "FLOOR((`X` + INTERVAL '1:20' HOUR TO MINUTE))");
    checkExp("floor(x + interval '1:20' hour to minute to minute)",
        "FLOOR((`X` + INTERVAL '1:20' HOUR TO MINUTE) TO MINUTE)");
    checkExp("floor(x + interval '1:20' minute to second to hour)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO HOUR)");
    checkExp("floor(x + interval '1:20' minute to second to day)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DAY)");
    checkExp("floor(x + interval '1:20' minute to second to dow)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DOW)");
    checkExp("floor(x + interval '1:20' minute to second to doy)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DOY)");
    checkExp("floor(x + interval '1:20' minute to second to week)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO WEEK)");
    checkExp("floor(x + interval '1:20' minute to second to month)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO MONTH)");
    checkExp("floor(x + interval '1:20' minute to second to quarter)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO QUARTER)");
    checkExp("floor(x + interval '1:20' minute to second to year)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO YEAR)");
    checkExp("floor(x + interval '1:20' minute to second to decade)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DECADE)");
    checkExp("floor(x + interval '1:20' minute to second to century)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO CENTURY)");
    checkExp("floor(x + interval '1:20' minute to second to millennium)",
        "FLOOR((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO MILLENNIUM)");
  }

  @Test public void testCeil() {
    checkExp("ceil(3453.2)", "CEIL(3453.2)");
    checkExp("ceil(x)", "CEIL(`X`)");
    checkExp("ceil(x to second)", "CEIL(`X` TO SECOND)");
    checkExp("ceil(x to epoch)", "CEIL(`X` TO EPOCH)");
    checkExp("ceil(x to minute)", "CEIL(`X` TO MINUTE)");
    checkExp("ceil(x to hour)", "CEIL(`X` TO HOUR)");
    checkExp("ceil(x to day)", "CEIL(`X` TO DAY)");
    checkExp("ceil(x to dow)", "CEIL(`X` TO DOW)");
    checkExp("ceil(x to doy)", "CEIL(`X` TO DOY)");
    checkExp("ceil(x to week)", "CEIL(`X` TO WEEK)");
    checkExp("ceil(x to month)", "CEIL(`X` TO MONTH)");
    checkExp("ceil(x to quarter)", "CEIL(`X` TO QUARTER)");
    checkExp("ceil(x to year)", "CEIL(`X` TO YEAR)");
    checkExp("ceil(x to decade)", "CEIL(`X` TO DECADE)");
    checkExp("ceil(x to century)", "CEIL(`X` TO CENTURY)");
    checkExp("ceil(x to millennium)", "CEIL(`X` TO MILLENNIUM)");

    checkExp("ceil(x + interval '1:20' minute to second)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND))");
    checkExp("ceil(x + interval '1:20' minute to second to second)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO SECOND)");
    checkExp("ceil(x + interval '1:20' minute to second to epoch)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO EPOCH)");
    checkExp("ceil(x + interval '1:20' hour to minute)",
        "CEIL((`X` + INTERVAL '1:20' HOUR TO MINUTE))");
    checkExp("ceil(x + interval '1:20' hour to minute to minute)",
        "CEIL((`X` + INTERVAL '1:20' HOUR TO MINUTE) TO MINUTE)");
    checkExp("ceil(x + interval '1:20' minute to second to hour)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO HOUR)");
    checkExp("ceil(x + interval '1:20' minute to second to day)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DAY)");
    checkExp("ceil(x + interval '1:20' minute to second to dow)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DOW)");
    checkExp("ceil(x + interval '1:20' minute to second to doy)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DOY)");
    checkExp("ceil(x + interval '1:20' minute to second to week)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO WEEK)");
    checkExp("ceil(x + interval '1:20' minute to second to month)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO MONTH)");
    checkExp("ceil(x + interval '1:20' minute to second to quarter)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO QUARTER)");
    checkExp("ceil(x + interval '1:20' minute to second to year)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO YEAR)");
    checkExp("ceil(x + interval '1:20' minute to second to decade)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO DECADE)");
    checkExp("ceil(x + interval '1:20' minute to second to century)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO CENTURY)");
    checkExp("ceil(x + interval '1:20' minute to second to millennium)",
        "CEIL((`X` + INTERVAL '1:20' MINUTE TO SECOND) TO MILLENNIUM)");
  }

  @Test public void testCast() {
    checkExp("cast(x as boolean)", "CAST(`X` AS BOOLEAN)");
    checkExp("cast(x as integer)", "CAST(`X` AS INTEGER)");
    checkExp("cast(x as varchar(1))", "CAST(`X` AS VARCHAR(1))");
    checkExp("cast(x as date)", "CAST(`X` AS DATE)");
    checkExp("cast(x as time)", "CAST(`X` AS TIME)");
    checkExp("cast(x as time without time zone)", "CAST(`X` AS TIME)");
    checkExp("cast(x as time with local time zone)",
        "CAST(`X` AS TIME WITH LOCAL TIME ZONE)");
    checkExp("cast(x as timestamp without time zone)", "CAST(`X` AS TIMESTAMP)");
    checkExp("cast(x as timestamp with local time zone)",
        "CAST(`X` AS TIMESTAMP WITH LOCAL TIME ZONE)");
    checkExp("cast(x as time(0))", "CAST(`X` AS TIME(0))");
    checkExp("cast(x as time(0) without time zone)", "CAST(`X` AS TIME(0))");
    checkExp("cast(x as time(0) with local time zone)",
        "CAST(`X` AS TIME(0) WITH LOCAL TIME ZONE)");
    checkExp("cast(x as timestamp(0))", "CAST(`X` AS TIMESTAMP(0))");
    checkExp("cast(x as timestamp(0) without time zone)",
        "CAST(`X` AS TIMESTAMP(0))");
    checkExp("cast(x as timestamp(0) with local time zone)",
        "CAST(`X` AS TIMESTAMP(0) WITH LOCAL TIME ZONE)");
    checkExp("cast(x as timestamp)", "CAST(`X` AS TIMESTAMP)");
    checkExp("cast(x as decimal(1,1))", "CAST(`X` AS DECIMAL(1, 1))");
    checkExp("cast(x as char(1))", "CAST(`X` AS CHAR(1))");
    checkExp("cast(x as binary(1))", "CAST(`X` AS BINARY(1))");
    checkExp("cast(x as varbinary(1))", "CAST(`X` AS VARBINARY(1))");
    checkExp("cast(x as tinyint)", "CAST(`X` AS TINYINT)");
    checkExp("cast(x as smallint)", "CAST(`X` AS SMALLINT)");
    checkExp("cast(x as bigint)", "CAST(`X` AS BIGINT)");
    checkExp("cast(x as real)", "CAST(`X` AS REAL)");
    checkExp("cast(x as double)", "CAST(`X` AS DOUBLE)");
    checkExp("cast(x as decimal)", "CAST(`X` AS DECIMAL)");
    checkExp("cast(x as decimal(0))", "CAST(`X` AS DECIMAL(0))");
    checkExp("cast(x as decimal(1,2))", "CAST(`X` AS DECIMAL(1, 2))");

    checkExp("cast('foo' as bar)", "CAST('foo' AS `BAR`)");
  }

  @Test public void testCastFails() {
    checkExpFails("cast(x as time with ^time^ zone)",
        "(?s).*Encountered \"time\" at .*");
    checkExpFails("cast(x as time(0) with ^time^ zone)",
        "(?s).*Encountered \"time\" at .*");
    checkExpFails("cast(x as timestamp with ^time^ zone)",
        "(?s).*Encountered \"time\" at .*");
    checkExpFails("cast(x as timestamp(0) with ^time^ zone)",
        "(?s).*Encountered \"time\" at .*");
    checkExpFails("cast(x as varchar(10) ^with^ local time zone)",
        "(?s).*Encountered \"with\" at line 1, column 23.\n.*");
    checkExpFails("cast(x as varchar(10) ^without^ time zone)",
        "(?s).*Encountered \"without\" at line 1, column 23.\n.*");
  }

  @Test public void testLikeAndSimilar() {
    check(
        "select * from t where x like '%abc%'",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` LIKE '%abc%')");

    check(
        "select * from t where x+1 not siMilaR to '%abc%' ESCAPE 'e'",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`X` + 1) NOT SIMILAR TO '%abc%' ESCAPE 'e')");

    // LIKE has higher precedence than AND
    check(
        "select * from t where price > 5 and x+2*2 like y*3+2 escape (select*from t)",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`PRICE` > 5) AND ((`X` + (2 * 2)) LIKE ((`Y` * 3) + 2) ESCAPE (SELECT *\n"
            + "FROM `T`)))");

    check(
        "values a and b like c",
        "VALUES (ROW((`A` AND (`B` LIKE `C`))))");

    // LIKE has higher precedence than AND
    check(
        "values a and b like c escape d and e",
        "VALUES (ROW(((`A` AND (`B` LIKE `C` ESCAPE `D`)) AND `E`)))");

    // LIKE has same precedence as '='; LIKE is right-assoc, '=' is left
    check(
        "values a = b like c = d",
        "VALUES (ROW(((`A` = (`B` LIKE `C`)) = `D`)))");

    // Nested LIKE
    check(
        "values a like b like c escape d",
        "VALUES (ROW((`A` LIKE (`B` LIKE `C` ESCAPE `D`))))");
    check(
        "values a like b like c escape d and false",
        "VALUES (ROW(((`A` LIKE (`B` LIKE `C` ESCAPE `D`)) AND FALSE)))");
    check(
        "values a like b like c like d escape e escape f",
        "VALUES (ROW((`A` LIKE (`B` LIKE (`C` LIKE `D` ESCAPE `E`) ESCAPE `F`))))");

    // Mixed LIKE and SIMILAR TO
    check(
        "values a similar to b like c similar to d escape e escape f",
        "VALUES (ROW((`A` SIMILAR TO (`B` LIKE (`C` SIMILAR TO `D` ESCAPE `E`) ESCAPE `F`))))");

    if (isReserved("ESCAPE")) {
      checkFails(
          "select * from t where ^escape^ 'e'",
          "(?s).*Encountered \"escape\" at .*");
    }

    // LIKE with +
    check(
        "values a like b + c escape d",
        "VALUES (ROW((`A` LIKE (`B` + `C`) ESCAPE `D`)))");

    // LIKE with ||
    check(
        "values a like b || c escape d",
        "VALUES (ROW((`A` LIKE (`B` || `C`) ESCAPE `D`)))");

    // ESCAPE with no expression
    if (isReserved("ESCAPE")) {
      checkFails(
          "values a ^like^ escape d",
          "(?s).*Encountered \"like escape\" at .*");
    }

    // ESCAPE with no expression
    if (isReserved("ESCAPE")) {
      checkFails(
          "values a like b || c ^escape^ and false",
          "(?s).*Encountered \"escape and\" at line 1, column 22.*");
    }

    // basic SIMILAR TO
    check(
        "select * from t where x similar to '%abc%'",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`X` SIMILAR TO '%abc%')");

    check(
        "select * from t where x+1 not siMilaR to '%abc%' ESCAPE 'e'",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`X` + 1) NOT SIMILAR TO '%abc%' ESCAPE 'e')");

    // SIMILAR TO has higher precedence than AND
    check(
        "select * from t where price > 5 and x+2*2 SIMILAR TO y*3+2 escape (select*from t)",
        "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`PRICE` > 5) AND ((`X` + (2 * 2)) SIMILAR TO ((`Y` * 3) + 2) ESCAPE (SELECT *\n"
            + "FROM `T`)))");

    // Mixed LIKE and SIMILAR TO
    check(
        "values a similar to b like c similar to d escape e escape f",
        "VALUES (ROW((`A` SIMILAR TO (`B` LIKE (`C` SIMILAR TO `D` ESCAPE `E`) ESCAPE `F`))))");

    // SIMILAR TO with sub-query
    check(
        "values a similar to (select * from t where a like b escape c) escape d",
        "VALUES (ROW((`A` SIMILAR TO (SELECT *\n"
            + "FROM `T`\n"
            + "WHERE (`A` LIKE `B` ESCAPE `C`)) ESCAPE `D`)))");
  }

  @Test public void testFoo() {
  }

  @Test public void testArithmeticOperators() {
    checkExp("1-2+3*4/5/6-7", "(((1 - 2) + (((3 * 4) / 5) / 6)) - 7)");
    checkExp("power(2,3)", "POWER(2, 3)");
    checkExp("aBs(-2.3e-2)", "ABS(-2.3E-2)");
    checkExp("MOD(5             ,\t\f\r\n2)", "MOD(5, 2)");
    checkExp("ln(5.43  )", "LN(5.43)");
    checkExp("log10(- -.2  )", "LOG10(0.2)");
  }

  @Test public void testExists() {
    check(
        "select * from dept where exists (select 1 from emp where emp.deptno = dept.deptno)",
        "SELECT *\n"
            + "FROM `DEPT`\n"
            + "WHERE (EXISTS (SELECT 1\n"
            + "FROM `EMP`\n"
            + "WHERE (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)))");
  }

  @Test public void testExistsInWhere() {
    check(
        "select * from emp where 1 = 2 and exists (select 1 from dept) and 3 = 4",
        "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (((1 = 2) AND (EXISTS (SELECT 1\n"
            + "FROM `DEPT`))) AND (3 = 4))");
  }

  @Test public void testFromWithAs() {
    check("select 1 from emp as e where 1",
        "SELECT 1\n"
            + "FROM `EMP` AS `E`\n"
            + "WHERE 1");
  }

  @Test public void testConcat() {
    checkExp("'a' || 'b'", "('a' || 'b')");
  }

  @Test public void testReverseSolidus() {
    checkExp("'\\'", "'\\'");
  }

  @Test public void testSubstring() {
    checkExp("substring('a' \n  FROM \t  1)", "SUBSTRING('a' FROM 1)");
    checkExp("substring('a' FROM 1 FOR 3)", "SUBSTRING('a' FROM 1 FOR 3)");
    checkExp(
        "substring('a' FROM 'reg' FOR '\\')",
        "SUBSTRING('a' FROM 'reg' FOR '\\')");

    checkExp(
        "substring('a', 'reg', '\\')",
        "SUBSTRING('a' FROM 'reg' FOR '\\')");
    checkExp("substring('a', 1, 2)", "SUBSTRING('a' FROM 1 FOR 2)");
    checkExp("substring('a' , 1)", "SUBSTRING('a' FROM 1)");
  }

  @Test public void testFunction() {
    check("select substring('Eggs and ham', 1, 3 + 2) || ' benedict' from emp",
        "SELECT (SUBSTRING('Eggs and ham' FROM 1 FOR (3 + 2)) || ' benedict')\n"
            + "FROM `EMP`");
    checkExp(
        "log10(1)\r\n+power(2, mod(\r\n3\n\t\t\f\n,ln(4))*log10(5)-6*log10(7/abs(8)+9))*power(10,11)",
        "(LOG10(1) + (POWER(2, ((MOD(3, LN(4)) * LOG10(5)) - (6 * LOG10(((7 / ABS(8)) + 9))))) * POWER(10, 11)))");
  }

  @Test public void testFunctionWithDistinct() {
    checkExp("count(DISTINCT 1)", "COUNT(DISTINCT 1)");
    checkExp("count(ALL 1)", "COUNT(ALL 1)");
    checkExp("count(1)", "COUNT(1)");
    check("select count(1), count(distinct 2) from emp",
        "SELECT COUNT(1), COUNT(DISTINCT 2)\n"
            + "FROM `EMP`");
  }

  @Test public void testFunctionCallWithDot() {
    checkExp("foo(a,b).c", "(`FOO`(`A`, `B`).`C`)");
  }

  @Test public void testFunctionInFunction() {
    checkExp("ln(power(2,2))", "LN(POWER(2, 2))");
  }

  @Test public void testFunctionNamedArgument() {
    checkExp("foo(x => 1)",
        "`FOO`(`X` => 1)");
    checkExp("foo(x => 1, \"y\" => 'a', z => x <= y)",
        "`FOO`(`X` => 1, `y` => 'a', `Z` => (`X` <= `Y`))");
    checkExpFails("foo(x.y ^=>^ 1)",
        "(?s).*Encountered \"=>\" at .*");
    checkExpFails("foo(a => 1, x.y ^=>^ 2, c => 3)",
        "(?s).*Encountered \"=>\" at .*");
  }

  @Test public void testFunctionDefaultArgument() {
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
    checkExpFails("foo(x ^+^ DEFAULT)",
        "(?s).*Encountered \"\\+ DEFAULT\" at .*");
    checkExpFails("foo(0, x ^+^ DEFAULT + y)",
        "(?s).*Encountered \"\\+ DEFAULT\" at .*");
    checkExpFails("foo(0, DEFAULT ^+^ y)",
        "(?s).*Encountered \"\\+\" at .*");
  }

  @Test public void testDefault() {
    sql("select ^DEFAULT^ from emp")
        .fails("(?s)Encountered \"DEFAULT\" at .*");
    sql("select cast(empno ^+^ DEFAULT as double) from emp")
        .fails("(?s)Encountered \"\\+ DEFAULT\" at .*");
    sql("select empno ^+^ DEFAULT + deptno from emp")
        .fails("(?s)Encountered \"\\+ DEFAULT\" at .*");
    sql("select power(0, DEFAULT ^+^ empno) from emp")
        .fails("(?s)Encountered \"\\+\" at .*");
    sql("select * from emp join dept on ^DEFAULT^")
        .fails("(?s)Encountered \"DEFAULT\" at .*");
    sql("select * from emp where empno ^>^ DEFAULT or deptno < 10")
        .fails("(?s)Encountered \"> DEFAULT\" at .*");
    sql("select * from emp order by ^DEFAULT^ desc")
        .fails("(?s)Encountered \"DEFAULT\" at .*");
    final String expected = "INSERT INTO `DEPT` (`NAME`, `DEPTNO`)\n"
        + "VALUES (ROW('a', DEFAULT))";
    sql("insert into dept (name, deptno) values ('a', DEFAULT)")
        .ok(expected);
    sql("insert into dept (name, deptno) values ('a', 1 ^+^ DEFAULT)")
        .fails("(?s)Encountered \"\\+ DEFAULT\" at .*");
    sql("insert into dept (name, deptno) select 'a', ^DEFAULT^ from (values 0)")
        .fails("(?s)Encountered \"DEFAULT\" at .*");
  }

  @Test public void testAggregateFilter() {
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

  @Test public void testGroup() {
    check(
        "select deptno, min(foo) as x from emp group by deptno, gender",
        "SELECT `DEPTNO`, MIN(`FOO`) AS `X`\n"
            + "FROM `EMP`\n"
            + "GROUP BY `DEPTNO`, `GENDER`");
  }

  @Test public void testGroupEmpty() {
    check(
        "select count(*) from emp group by ()",
        "SELECT COUNT(*)\n"
            + "FROM `EMP`\n"
            + "GROUP BY ()");

    check(
        "select count(*) from emp group by () having 1 = 2 order by 3",
        "SELECT COUNT(*)\n"
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
    check(
        "select 1 from emp group by (empno + deptno)",
        "SELECT 1\n"
            + "FROM `EMP`\n"
            + "GROUP BY (`EMPNO` + `DEPTNO`)");
  }

  @Test public void testHavingAfterGroup() {
    check(
        "select deptno from emp group by deptno, emp having count(*) > 5 and 1 = 2 order by 5, 2",
        "SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "GROUP BY `DEPTNO`, `EMP`\n"
            + "HAVING ((COUNT(*) > 5) AND (1 = 2))\n"
            + "ORDER BY 5, 2");
  }

  @Test public void testHavingBeforeGroupFails() {
    checkFails(
        "select deptno from emp having count(*) > 5 and deptno < 4 ^group^ by deptno, emp",
        "(?s).*Encountered \"group\" at .*");
  }

  @Test public void testHavingNoGroup() {
    check(
        "select deptno from emp having count(*) > 5",
        "SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "HAVING (COUNT(*) > 5)");
  }

  @Test public void testGroupingSets() {
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

  @Test public void testGroupByCube() {
    sql("select deptno from emp\n"
        + "group by cube ((a, b), (c, d))")
        .ok("SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "GROUP BY CUBE((`A`, `B`), (`C`, `D`))");
  }

  @Test public void testGroupByCube2() {
    sql("select deptno from emp\n"
        + "group by cube ((a, b), (c, d)) order by a")
        .ok("SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "GROUP BY CUBE((`A`, `B`), (`C`, `D`))\n"
            + "ORDER BY `A`");
    sql("select deptno from emp\n"
        + "group by cube (^)")
        .fails("(?s)Encountered \"\\)\" at .*");
  }

  @Test public void testGroupByRollup() {
    sql("select deptno from emp\n"
        + "group by rollup (deptno, deptno + 1, gender)")
        .ok("SELECT `DEPTNO`\n"
            + "FROM `EMP`\n"
            + "GROUP BY ROLLUP(`DEPTNO`, (`DEPTNO` + 1), `GENDER`)");

    // Nested rollup not ok
    sql("select deptno from emp\n"
        + "group by rollup (deptno^, rollup(e, d))")
        .fails("(?s)Encountered \", rollup\" at .*");
  }

  @Test public void testGrouping() {
    sql("select deptno, grouping(deptno) from emp\n"
        + "group by grouping sets (deptno, (deptno, gender), ())")
        .ok("SELECT `DEPTNO`, GROUPING(`DEPTNO`)\n"
            + "FROM `EMP`\n"
            + "GROUP BY GROUPING SETS(`DEPTNO`, (`DEPTNO`, `GENDER`), ())");
  }

  @Test public void testWith() {
    check(
        "with femaleEmps as (select * from emps where gender = 'F')"
            + "select deptno from femaleEmps",
        "WITH `FEMALEEMPS` AS (SELECT *\n"
            + "FROM `EMPS`\n"
            + "WHERE (`GENDER` = 'F')) (SELECT `DEPTNO`\n"
            + "FROM `FEMALEEMPS`)");
  }

  @Test public void testWith2() {
    check(
        "with femaleEmps as (select * from emps where gender = 'F'),\n"
            + "marriedFemaleEmps(x, y) as (select * from femaleEmps where maritaStatus = 'M')\n"
            + "select deptno from femaleEmps",
        "WITH `FEMALEEMPS` AS (SELECT *\n"
            + "FROM `EMPS`\n"
            + "WHERE (`GENDER` = 'F')), `MARRIEDFEMALEEMPS` (`X`, `Y`) AS (SELECT *\n"
            + "FROM `FEMALEEMPS`\n"
            + "WHERE (`MARITASTATUS` = 'M')) (SELECT `DEPTNO`\n"
            + "FROM `FEMALEEMPS`)");
  }

  @Test public void testWithFails() {
    checkFails("with femaleEmps as ^select^ * from emps where gender = 'F'\n"
            + "select deptno from femaleEmps",
        "(?s)Encountered \"select\" at .*");
  }

  @Test public void testWithValues() {
    check(
        "with v(i,c) as (values (1, 'a'), (2, 'bb'))\n"
            + "select c, i from v",
        "WITH `V` (`I`, `C`) AS (VALUES (ROW(1, 'a')),\n"
            + "(ROW(2, 'bb'))) (SELECT `C`, `I`\n"
            + "FROM `V`)");
  }

  @Test public void testWithNestedFails() {
    // SQL standard does not allow WITH to contain WITH
    checkFails("with emp2 as (select * from emp)\n"
            + "^with^ dept2 as (select * from dept)\n"
            + "select 1 as uno from emp, dept",
        "(?s)Encountered \"with\" at .*");
  }

  @Test public void testWithNestedInSubQuery() {
    // SQL standard does not allow sub-query to contain WITH but we do
    check("with emp2 as (select * from emp)\n"
            + "(\n"
            + "  with dept2 as (select * from dept)\n"
            + "  select 1 as uno from empDept)",
        "WITH `EMP2` AS (SELECT *\n"
            + "FROM `EMP`) (WITH `DEPT2` AS (SELECT *\n"
            + "FROM `DEPT`) (SELECT 1 AS `UNO`\n"
            + "FROM `EMPDEPT`))");
  }

  @Test public void testWithUnion() {
    // Per the standard WITH ... SELECT ... UNION is valid even without parens.
    check("with emp2 as (select * from emp)\n"
            + "select * from emp2\n"
            + "union\n"
            + "select * from emp2\n",
        "WITH `EMP2` AS (SELECT *\n"
            + "FROM `EMP`) (SELECT *\n"
            + "FROM `EMP2`\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `EMP2`)");
  }

  @Test public void testIdentifier() {
    checkExp("ab", "`AB`");
    checkExp("     \"a  \"\" b!c\"", "`a  \" b!c`");
    checkExpFails("     ^`^a  \" b!c`", "(?s).*Encountered.*");
    checkExp("\"x`y`z\"", "`x``y``z`");
    checkExpFails("^`^x`y`z`", "(?s).*Encountered.*");

    checkExp("myMap[field] + myArray[1 + 2]",
        "(`MYMAP`[`FIELD`] + `MYARRAY`[(1 + 2)])");

    getTester().checkNode("VALUES a", isQuoted(0, false));
    getTester().checkNode("VALUES \"a\"", isQuoted(0, true));
    getTester().checkNode("VALUES \"a\".\"b\"", isQuoted(1, true));
    getTester().checkNode("VALUES \"a\".b", isQuoted(1, false));
  }

  @Test public void testBackTickIdentifier() {
    quoting = Quoting.BACK_TICK;
    checkExp("ab", "`AB`");
    checkExp("     `a  \" b!c`", "`a  \" b!c`");
    checkExpFails("     ^\"^a  \"\" b!c\"", "(?s).*Encountered.*");

    checkExpFails("^\"^x`y`z\"", "(?s).*Encountered.*");
    checkExp("`x``y``z`", "`x``y``z`");

    checkExp("myMap[field] + myArray[1 + 2]",
        "(`MYMAP`[`FIELD`] + `MYARRAY`[(1 + 2)])");

    getTester().checkNode("VALUES a", isQuoted(0, false));
    getTester().checkNode("VALUES `a`", isQuoted(0, true));
  }

  @Test public void testBracketIdentifier() {
    quoting = Quoting.BRACKET;
    checkExp("ab", "`AB`");
    checkExp("     [a  \" b!c]", "`a  \" b!c`");
    checkExpFails("     ^`^a  \" b!c`", "(?s).*Encountered.*");
    checkExpFails("     ^\"^a  \"\" b!c\"", "(?s).*Encountered.*");

    checkExp("[x`y`z]", "`x``y``z`");
    checkExpFails("^\"^x`y`z\"", "(?s).*Encountered.*");
    checkExpFails("^`^x``y``z`", "(?s).*Encountered.*");

    checkExp("[anything [even brackets]] is].[ok]",
        "`anything [even brackets] is`.`ok`");

    // What would be a call to the 'item' function in DOUBLE_QUOTE and BACK_TICK
    // is a table alias.
    check("select * from myMap[field], myArray[1 + 2]",
        "SELECT *\n"
            + "FROM `MYMAP` AS `field`,\n"
            + "`MYARRAY` AS `1 + 2`");
    check("select * from myMap [field], myArray [1 + 2]",
        "SELECT *\n"
            + "FROM `MYMAP` AS `field`,\n"
            + "`MYARRAY` AS `1 + 2`");

    getTester().checkNode("VALUES a", isQuoted(0, false));
    getTester().checkNode("VALUES [a]", isQuoted(0, true));
  }

  @Test public void testBackTickQuery() {
    quoting = Quoting.BACK_TICK;
    check(
        "select `x`.`b baz` from `emp` as `x` where `x`.deptno in (10, 20)",
        "SELECT `x`.`b baz`\n"
            + "FROM `emp` AS `x`\n"
            + "WHERE (`x`.`DEPTNO` IN (10, 20))");
  }

  @Test public void testInList() {
    check(
        "select * from emp where deptno in (10, 20) and gender = 'F'",
        "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE ((`DEPTNO` IN (10, 20)) AND (`GENDER` = 'F'))");
  }

  @Test public void testInListEmptyFails() {
    checkFails(
        "select * from emp where deptno in (^)^ and gender = 'F'",
        "(?s).*Encountered \"\\)\" at line 1, column 36\\..*");
  }

  @Test public void testInQuery() {
    check(
        "select * from emp where deptno in (select deptno from dept)",
        "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (`DEPTNO` IN (SELECT `DEPTNO`\n"
            + "FROM `DEPT`))");
  }

  /**
   * Tricky for the parser - looks like "IN (scalar, scalar)" but isn't.
   */
  @Test public void testInQueryWithComma() {
    check(
        "select * from emp where deptno in (select deptno from dept group by 1, 2)",
        "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (`DEPTNO` IN (SELECT `DEPTNO`\n"
            + "FROM `DEPT`\n"
            + "GROUP BY 1, 2))");
  }

  @Test public void testInSetop() {
    check(
        "select * from emp where deptno in ((select deptno from dept union select * from dept)"
            + "except select * from dept) and false",
        "SELECT *\n"
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

  @Test public void testSome() {
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
    sql(sql3).fails("(?s).*Encountered \"some\" at .*");

    final String sql4 = "select * from emp\n"
        + "where name ^like^ some (select name from emp)";
    sql(sql4).fails("(?s).*Encountered \"like some\" at .*");

    final String sql5 = "select * from emp where empno = any (10,20)";
    final String expected5 = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`EMPNO` = SOME (10, 20))";
    sql(sql5).ok(expected5);
  }

  @Test public void testAll() {
    final String sql = "select * from emp\n"
        + "where sal <= all (select comm from emp) or sal > 10";
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE ((`SAL` <= ALL (SELECT `COMM`\n"
        + "FROM `EMP`)) OR (`SAL` > 10))";
    sql(sql).ok(expected);
  }

  @Test public void testAllList() {
    final String sql = "select * from emp\n"
        + "where sal <= all (12, 20, 30)";
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`SAL` <= ALL (12, 20, 30))";
    sql(sql).ok(expected);
  }

  @Test public void testUnion() {
    check(
        "select * from a union select * from a",
        "(SELECT *\n"
            + "FROM `A`\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `A`)");
    check(
        "select * from a union all select * from a",
        "(SELECT *\n"
            + "FROM `A`\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `A`)");
    check(
        "select * from a union distinct select * from a",
        "(SELECT *\n"
            + "FROM `A`\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `A`)");
  }

  @Test public void testUnionOrder() {
    check(
        "select a, b from t "
            + "union all "
            + "select x, y from u "
            + "order by 1 asc, 2 desc",
        "(SELECT `A`, `B`\n"
            + "FROM `T`\n"
            + "UNION ALL\n"
            + "SELECT `X`, `Y`\n"
            + "FROM `U`)\n"
            + "ORDER BY 1, 2 DESC");
  }

  @Test public void testOrderUnion() {
    // ORDER BY inside UNION not allowed
    sql("select a from t order by a\n"
        + "^union^ all\n"
        + "select b from t order by b")
        .fails("(?s).*Encountered \"union\" at .*");
  }

  @Test public void testLimitUnion() {
    // LIMIT inside UNION not allowed
    sql("select a from t limit 10\n"
        + "^union^ all\n"
        + "select b from t order by b")
        .fails("(?s).*Encountered \"union\" at .*");
  }

  @Test public void testUnionOfNonQueryFails() {
    checkFails(
        "select 1 from emp union ^2^ + 5",
        "Non-query expression encountered in illegal context");
  }

  /**
   * In modern SQL, a query can occur almost everywhere that an expression
   * can. This test tests the few exceptions.
   */
  @Test public void testQueryInIllegalContext() {
    checkFails(
        "select 0, multiset[^(^select * from emp), 2] from dept",
        "Query expression encountered in illegal context");
    checkFails(
        "select 0, multiset[1, ^(^select * from emp), 2, 3] from dept",
        "Query expression encountered in illegal context");
  }

  @Test public void testExcept() {
    check(
        "select * from a except select * from a",
        "(SELECT *\n"
            + "FROM `A`\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM `A`)");
    check(
        "select * from a except all select * from a",
        "(SELECT *\n"
            + "FROM `A`\n"
            + "EXCEPT ALL\n"
            + "SELECT *\n"
            + "FROM `A`)");
    check(
        "select * from a except distinct select * from a",
        "(SELECT *\n"
            + "FROM `A`\n"
            + "EXCEPT\n"
            + "SELECT *\n"
            + "FROM `A`)");
  }

  /** Tests MINUS, which is equivalent to EXCEPT but only supported in some
   * conformance levels (e.g. ORACLE). */
  @Test public void testSetMinus() {
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
    sql(sql).sansCarets().ok(expected);

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
  @Test public void testMinusIsReserved() {
    sql("select ^minus^ from t")
        .fails("(?s).*Encountered \"minus\" at .*");
    sql("select ^minus^ select")
        .fails("(?s).*Encountered \"minus\" at .*");
    sql("select * from t as ^minus^ where x < y")
        .fails("(?s).*Encountered \"minus\" at .*");
  }

  @Test public void testIntersect() {
    check(
        "select * from a intersect select * from a",
        "(SELECT *\n"
            + "FROM `A`\n"
            + "INTERSECT\n"
            + "SELECT *\n"
            + "FROM `A`)");
    check(
        "select * from a intersect all select * from a",
        "(SELECT *\n"
            + "FROM `A`\n"
            + "INTERSECT ALL\n"
            + "SELECT *\n"
            + "FROM `A`)");
    check(
        "select * from a intersect distinct select * from a",
        "(SELECT *\n"
            + "FROM `A`\n"
            + "INTERSECT\n"
            + "SELECT *\n"
            + "FROM `A`)");
  }

  @Test public void testJoinCross() {
    check(
        "select * from a as a2 cross join b",
        "SELECT *\n"
            + "FROM `A` AS `A2`\n"
            + "CROSS JOIN `B`");
  }

  @Test public void testJoinOn() {
    check(
        "select * from a left join b on 1 = 1 and 2 = 2 where 3 = 3",
        "SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN `B` ON ((1 = 1) AND (2 = 2))\n"
            + "WHERE (3 = 3)");
  }

  @Test public void testJoinOnParentheses() {
    if (!Bug.TODO_FIXED) {
      return;
    }
    check(
        "select * from a\n"
            + " left join (b join c as c1 on 1 = 1) on 2 = 2\n"
            + "where 3 = 3",
        "SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN (`B` INNER JOIN `C` AS `C1` ON (1 = 1)) ON (2 = 2)\n"
            + "WHERE (3 = 3)");
  }

  /**
   * Same as {@link #testJoinOnParentheses()} but fancy aliases.
   */
  @Test public void testJoinOnParenthesesPlus() {
    if (!Bug.TODO_FIXED) {
      return;
    }
    check(
        "select * from a\n"
            + " left join (b as b1 (x, y) join (select * from c) c1 on 1 = 1) on 2 = 2\n"
            + "where 3 = 3",
        "SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN (`B` AS `B1` (`X`, `Y`) INNER JOIN (SELECT *\n"
            + "FROM `C`) AS `C1` ON (1 = 1)) ON (2 = 2)\n"
            + "WHERE (3 = 3)");
  }

  @Test public void testExplicitTableInJoin() {
    check(
        "select * from a left join (table b) on 2 = 2 where 3 = 3",
        "SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN (TABLE `B`) ON (2 = 2)\n"
            + "WHERE (3 = 3)");
  }

  @Test public void testSubQueryInJoin() {
    if (!Bug.TODO_FIXED) {
      return;
    }
    check(
        "select * from (select * from a cross join b) as ab\n"
            + " left join ((table c) join d on 2 = 2) on 3 = 3\n"
            + " where 4 = 4",
        "SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `A`\n"
            + "CROSS JOIN `B`) AS `AB`\n"
            + "LEFT JOIN ((TABLE `C`) INNER JOIN `D` ON (2 = 2)) ON (3 = 3)\n"
            + "WHERE (4 = 4)");
  }

  @Test public void testOuterJoinNoiseWord() {
    check(
        "select * from a left outer join b on 1 = 1 and 2 = 2 where 3 = 3",
        "SELECT *\n"
            + "FROM `A`\n"
            + "LEFT JOIN `B` ON ((1 = 1) AND (2 = 2))\n"
            + "WHERE (3 = 3)");
  }

  @Test public void testJoinQuery() {
    check(
        "select * from a join (select * from b) as b2 on true",
        "SELECT *\n"
            + "FROM `A`\n"
            + "INNER JOIN (SELECT *\n"
            + "FROM `B`) AS `B2` ON TRUE");
  }

  @Test public void testFullInnerJoinFails() {
    // cannot have more than one of INNER, FULL, LEFT, RIGHT, CROSS
    checkFails(
        "select * from a ^full^ inner join b",
        "(?s).*Encountered \"full inner\" at line 1, column 17.*");
  }

  @Test public void testFullOuterJoin() {
    // OUTER is an optional extra to LEFT, RIGHT, or FULL
    check(
        "select * from a full outer join b",
        "SELECT *\n"
            + "FROM `A`\n"
            + "FULL JOIN `B`");
  }

  @Test public void testInnerOuterJoinFails() {
    checkFails(
        "select * from a ^inner^ outer join b",
        "(?s).*Encountered \"inner outer\" at line 1, column 17.*");
  }

  @Ignore
  @Test public void testJoinAssociativity() {
    // joins are left-associative
    // 1. no parens needed
    check(
        "select * from (a natural left join b) left join c on b.c1 = c.c1",
        "SELECT *\n"
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)\n");

    // 2. parens needed
    check(
        "select * from a natural left join (b left join c on b.c1 = c.c1)",
        "SELECT *\n"
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)\n");

    // 3. same as 1
    check(
        "select * from a natural left join b left join c on b.c1 = c.c1",
        "SELECT *\n"
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)\n");
  }

  // Note: "select * from a natural cross join b" is actually illegal SQL
  // ("cross" is the only join type which cannot be modified with the
  // "natural") but the parser allows it; we and catch it at validate time
  @Test public void testNaturalCrossJoin() {
    check(
        "select * from a natural cross join b",
        "SELECT *\n"
            + "FROM `A`\n"
            + "NATURAL CROSS JOIN `B`");
  }

  @Test public void testJoinUsing() {
    check(
        "select * from a join b using (x)",
        "SELECT *\n"
            + "FROM `A`\n"
            + "INNER JOIN `B` USING (`X`)");
    checkFails(
        "select * from a join b using (^)^ where c = d",
        "(?s).*Encountered \"[)]\" at line 1, column 31.*");
  }

  /** Tests CROSS APPLY, which is equivalent to CROSS JOIN and LEFT JOIN but
   * only supported in some conformance levels (e.g. SQL Server). */
  @Test public void testApply() {
    final String pattern =
        "APPLY operator is not allowed under the current SQL conformance level";
    final String sql = "select * from dept\n"
        + "cross apply table(ramp(deptno)) as t(a^)^";
    sql(sql).fails(pattern);

    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    final String expected = "SELECT *\n"
        + "FROM `DEPT`\n"
        + "CROSS JOIN LATERAL TABLE(`RAMP`(`DEPTNO`)) AS `T` (`A`)";
    sql(sql).sansCarets().ok(expected);

    // Supported in Oracle 12 but not Oracle 10
    conformance = SqlConformanceEnum.ORACLE_10;
    sql(sql).fails(pattern);

    conformance = SqlConformanceEnum.ORACLE_12;
    sql(sql).sansCarets().ok(expected);
  }

  /** Tests OUTER APPLY. */
  @Test public void testOuterApply() {
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    final String sql = "select * from dept outer apply table(ramp(deptno))";
    final String expected = "SELECT *\n"
        + "FROM `DEPT`\n"
        + "LEFT JOIN LATERAL TABLE(`RAMP`(`DEPTNO`)) ON TRUE";
    sql(sql).ok(expected);
  }

  @Test public void testOuterApplySubQuery() {
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

  @Test public void testOuterApplyValues() {
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
  @Test public void testOuterApplyFunctionFails() {
    conformance = SqlConformanceEnum.SQL_SERVER_2008;
    final String sql = "select * from dept outer apply ramp(deptno^)^)";
    sql(sql).fails("(?s).*Encountered \"\\)\" at .*");
  }

  @Test public void testCrossOuterApply() {
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

  @Test public void testTableSample() {
    check(
        "select * from ("
            + "  select * "
            + "  from emp "
            + "  join dept on emp.deptno = dept.deptno"
            + "  where gender = 'F'"
            + "  order by sal) tablesample substitute('medium')",
        "SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `EMP`\n"
            + "INNER JOIN `DEPT` ON (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)\n"
            + "WHERE (`GENDER` = 'F')\n"
            + "ORDER BY `SAL`) TABLESAMPLE SUBSTITUTE('MEDIUM')");

    check(
        "select * "
            + "from emp as x tablesample substitute('medium') "
            + "join dept tablesample substitute('lar' /* split */ 'ge') on x.deptno = dept.deptno",
        "SELECT *\n"
            + "FROM `EMP` AS `X` TABLESAMPLE SUBSTITUTE('MEDIUM')\n"
            + "INNER JOIN `DEPT` TABLESAMPLE SUBSTITUTE('LARGE') ON (`X`.`DEPTNO` = `DEPT`.`DEPTNO`)");

    check(
        "select * "
            + "from emp as x tablesample bernoulli(50)",
        "SELECT *\n"
            + "FROM `EMP` AS `X` TABLESAMPLE BERNOULLI(50.0)");

    check(
        "select * "
            + "from emp as x "
            + "tablesample bernoulli(50) REPEATABLE(10) ",
        "SELECT *\n"
            + "FROM `EMP` AS `X` TABLESAMPLE BERNOULLI(50.0) REPEATABLE(10)");

    // test repeatable with invalid int literal.
    checkFails(
        "select * "
            + "from emp as x "
            + "tablesample bernoulli(50) REPEATABLE(^100000000000000000000^) ",
        "Literal '100000000000000000000' "
            + "can not be parsed to type 'java\\.lang\\.Integer'");

    // test repeatable with invalid negative int literal.
    checkFails(
        "select * "
            + "from emp as x "
            + "tablesample bernoulli(50) REPEATABLE(-^100000000000000000000^) ",
        "Literal '100000000000000000000' "
            + "can not be parsed to type 'java\\.lang\\.Integer'");
  }

  @Test public void testLiteral() {
    checkExpSame("'foo'");
    checkExpSame("100");
    check(
        "select 1 as uno, 'x' as x, null as n from emp",
        "SELECT 1 AS `UNO`, 'x' AS `X`, NULL AS `N`\n"
            + "FROM `EMP`");

    // Even though it looks like a date, it's just a string.
    checkExp("'2004-06-01'", "'2004-06-01'");
    checkExp("-.25", "-0.25");
    checkExpSame("TIMESTAMP '2004-06-01 15:55:55'");
    checkExpSame("TIMESTAMP '2004-06-01 15:55:55.900'");
    checkExp(
        "TIMESTAMP '2004-06-01 15:55:55.1234'",
        "TIMESTAMP '2004-06-01 15:55:55.1234'");
    checkExp(
        "TIMESTAMP '2004-06-01 15:55:55.1236'",
        "TIMESTAMP '2004-06-01 15:55:55.1236'");
    checkExp(
        "TIMESTAMP '2004-06-01 15:55:55.9999'",
        "TIMESTAMP '2004-06-01 15:55:55.9999'");
    checkExpSame("NULL");
  }

  @Test public void testContinuedLiteral() {
    checkExp(
        "'abba'\n'abba'",
        "'abba'\n'abba'");
    checkExp(
        "'abba'\n'0001'",
        "'abba'\n'0001'");
    checkExp(
        "N'yabba'\n'dabba'\n'doo'",
        "_ISO-8859-1'yabba'\n'dabba'\n'doo'");
    checkExp(
        "_iso-8859-1'yabba'\n'dabba'\n'don''t'",
        "_ISO-8859-1'yabba'\n'dabba'\n'don''t'");

    checkExp(
        "x'01aa'\n'03ff'",
        "X'01AA'\n'03FF'");

    // a bad hexstring
    checkFails(
        "x'01aa'\n^'vvvv'^",
        "Binary literal string must contain only characters '0' - '9', 'A' - 'F'");
  }

  @Test public void testMixedFrom() {
    // REVIEW: Is this syntax even valid?
    check(
        "select * from a join b using (x), c join d using (y)",
        "SELECT *\n"
            + "FROM `A`\n"
            + "INNER JOIN `B` USING (`X`),\n"
            + "`C`\n"
            + "INNER JOIN `D` USING (`Y`)");
  }

  @Test public void testMixedStar() {
    check(
        "select emp.*, 1 as foo from emp, dept",
        "SELECT `EMP`.*, 1 AS `FOO`\n"
            + "FROM `EMP`,\n"
            + "`DEPT`");
  }

  @Test public void testSchemaTableStar() {
    sql("select schem.emp.*, emp.empno * dept.deptno\n"
            + "from schem.emp, dept")
        .ok("SELECT `SCHEM`.`EMP`.*, (`EMP`.`EMPNO` * `DEPT`.`DEPTNO`)\n"
                + "FROM `SCHEM`.`EMP`,\n"
                + "`DEPT`");
  }

  @Test public void testCatalogSchemaTableStar() {
    sql("select cat.schem.emp.* from cat.schem.emp")
        .ok("SELECT `CAT`.`SCHEM`.`EMP`.*\n"
                + "FROM `CAT`.`SCHEM`.`EMP`");
  }

  @Test public void testAliasedStar() {
    // OK in parser; validator will give error
    sql("select emp.* as foo from emp")
        .ok("SELECT `EMP`.* AS `FOO`\n"
                + "FROM `EMP`");
  }

  @Test public void testNotExists() {
    check(
        "select * from dept where not not exists (select * from emp) and true",
        "SELECT *\n"
            + "FROM `DEPT`\n"
            + "WHERE ((NOT (NOT (EXISTS (SELECT *\n"
            + "FROM `EMP`)))) AND TRUE)");
  }

  @Test public void testOrder() {
    check(
        "select * from emp order by empno, gender desc, deptno asc, empno asc, name desc",
        "SELECT *\n"
            + "FROM `EMP`\n"
            + "ORDER BY `EMPNO`, `GENDER` DESC, `DEPTNO`, `EMPNO`, `NAME` DESC");
  }

  @Test public void testOrderNullsFirst() {
    check(
        "select * from emp order by gender desc nulls last, deptno asc nulls first, empno nulls last",
        "SELECT *\n"
            + "FROM `EMP`\n"
            + "ORDER BY `GENDER` DESC NULLS LAST, `DEPTNO` NULLS FIRST, `EMPNO` NULLS LAST");
  }

  @Test public void testOrderInternal() {
    check(
        "(select * from emp order by empno) union select * from emp",
        "((SELECT *\n"
            + "FROM `EMP`\n"
            + "ORDER BY `EMPNO`)\n"
            + "UNION\n"
            + "SELECT *\n"
            + "FROM `EMP`)");

    check(
        "select * from (select * from t order by x, y) where a = b",
        "SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `T`\n"
            + "ORDER BY `X`, `Y`)\n"
            + "WHERE (`A` = `B`)");
  }

  @Test public void testOrderIllegalInExpression() {
    check(
        "select (select 1 from foo order by x,y) from t where a = b",
        "SELECT (SELECT 1\n"
            + "FROM `FOO`\n"
            + "ORDER BY `X`, `Y`)\n"
            + "FROM `T`\n"
            + "WHERE (`A` = `B`)");
    checkFails(
        "select (1 ^order^ by x, y) from t where a = b",
        "ORDER BY unexpected");
  }

  @Test public void testOrderOffsetFetch() {
    check(
        "select a from foo order by b, c offset 1 row fetch first 2 row only",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 2 ROWS ONLY");
    // as above, but ROWS rather than ROW
    check(
        "select a from foo order by b, c offset 1 rows fetch first 2 rows only",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 2 ROWS ONLY");
    // as above, but NEXT (means same as FIRST)
    check(
        "select a from foo order by b, c offset 1 rows fetch next 3 rows only",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 3 ROWS ONLY");
    // as above, but omit the ROWS noise word after OFFSET. This is not
    // compatible with SQL:2008 but allows the Postgres syntax
    // "LIMIT ... OFFSET".
    check(
        "select a from foo order by b, c offset 1 fetch next 3 rows only",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 3 ROWS ONLY");
    // as above, omit OFFSET
    check(
        "select a from foo order by b, c fetch next 3 rows only",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "FETCH NEXT 3 ROWS ONLY");
    // FETCH, no ORDER BY or OFFSET
    check(
        "select a from foo fetch next 4 rows only",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "FETCH NEXT 4 ROWS ONLY");
    // OFFSET, no ORDER BY or FETCH
    check(
        "select a from foo offset 1 row",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "OFFSET 1 ROWS");
    // OFFSET and FETCH, no ORDER BY
    check(
        "select a from foo offset 1 row fetch next 3 rows only",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 3 ROWS ONLY");
    // OFFSET and FETCH, with dynamic parameters
    check(
        "select a from foo offset ? row fetch next ? rows only",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "OFFSET ? ROWS\n"
            + "FETCH NEXT ? ROWS ONLY");
    // missing ROWS after FETCH
    checkFails(
        "select a from foo offset 1 fetch next 3 ^only^",
        "(?s).*Encountered \"only\" at .*");
    // FETCH before OFFSET is illegal
    checkFails(
        "select a from foo fetch next 3 rows only ^offset^ 1",
        "(?s).*Encountered \"offset\" at .*");
  }

  /**
   * "LIMIT ... OFFSET ..." is the postgres equivalent of SQL:2008
   * "OFFSET ... FETCH". It all maps down to a parse tree that looks like
   * SQL:2008.
   */
  @Test public void testLimit() {
    check(
        "select a from foo order by b, c limit 2 offset 1",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 2 ROWS ONLY");
    check(
        "select a from foo order by b, c limit 2",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "FETCH NEXT 2 ROWS ONLY");
    check(
        "select a from foo order by b, c offset 1",
        "SELECT `A`\n"
            + "FROM `FOO`\n"
            + "ORDER BY `B`, `C`\n"
            + "OFFSET 1 ROWS");
  }

  /** Test case that does not reproduce but is related to
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1238">[CALCITE-1238]
   * Unparsing LIMIT without ORDER BY after validation</a>. */
  @Test public void testLimitWithoutOrder() {
    final String expected = "SELECT `A`\n"
        + "FROM `FOO`\n"
        + "FETCH NEXT 2 ROWS ONLY";
    sql("select a from foo limit 2")
        .ok(expected);
  }

  @Test public void testLimitOffsetWithoutOrder() {
    final String expected = "SELECT `A`\n"
        + "FROM `FOO`\n"
        + "OFFSET 1 ROWS\n"
        + "FETCH NEXT 2 ROWS ONLY";
    sql("select a from foo limit 2 offset 1")
        .ok(expected);
  }

  @Test public void testLimitStartCount() {
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

  @Test public void testSqlInlineComment() {
    check(
        "select 1 from t --this is a comment\n",
        "SELECT 1\n"
            + "FROM `T`");
    check(
        "select 1 from t--\n",
        "SELECT 1\n"
            + "FROM `T`");
    check(
        "select 1 from t--this is a comment\n"
            + "where a>b-- this is comment\n",
        "SELECT 1\n"
            + "FROM `T`\n"
            + "WHERE (`A` > `B`)");
    check(
          "select 1 from t\n--select",
          "SELECT 1\n"
                  + "FROM `T`");
  }

  @Test public void testMultilineComment() {
    // on single line
    check(
        "select 1 /* , 2 */, 3 from t",
        "SELECT 1, 3\n"
            + "FROM `T`");

    // on several lines
    check(
        "select /* 1,\n"
            + " 2, \n"
            + " */ 3 from t",
        "SELECT 3\n"
            + "FROM `T`");

    // stuff inside comment
    check(
        "values ( /** 1, 2 + ** */ 3)",
        "VALUES (ROW(3))");

    // comment in string is preserved
    check(
        "values ('a string with /* a comment */ in it')",
        "VALUES (ROW('a string with /* a comment */ in it'))");

    // SQL:2003, 5.2, syntax rule # 8 "There shall be no <separator>
    // separating the <minus sign>s of a <simple comment introducer>".

    check(
        "values (- -1\n"
            + ")",
        "VALUES (ROW(1))");

    check(
        "values (--1+\n"
            + "2)",
        "VALUES (ROW(2))");

    // end of multiline comment without start
    if (Bug.FRG73_FIXED) {
      checkFails("values (1 */ 2)", "xx");
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
      check("values (1 + /* comment /* inner comment */ */ 2)", "xx");
    }

    // single-line comment inside multiline comment is illegal
    //
    // SQL-2003, 5.2: "Note 63 - Conforming programs should not place
    // <simple comment> within a <bracketed comment> because if such a
    // <simple comment> contains the sequence of characeters "*/" without
    // a preceding "/*" in the same <simple comment>, it will prematurely
    // terminate the containing <bracketed comment>.
    if (Bug.FRG73_FIXED) {
      checkFails(
          "values /* multiline contains -- singline */ \n"
              + " (1)",
          "xxx");
    }

    // non-terminated multiline comment inside singleline comment
    if (Bug.FRG73_FIXED) {
      // Test should fail, and it does, but it should give "*/" as the
      // erroneous token.
      checkFails(
          "values ( -- rest of line /* a comment  \n"
              + " 1, ^*/^ 2)",
          "Encountered \"/\\*\" at");
    }

    check(
        "values (1 + /* comment -- rest of line\n"
            + " rest of comment */ 2)",
        "VALUES (ROW((1 + 2)))");

    // multiline comment inside singleline comment
    check(
        "values -- rest of line /* a comment */ \n"
            + "(1)",
        "VALUES (ROW(1))");

    // non-terminated multiline comment inside singleline comment
    check(
        "values -- rest of line /* a comment  \n"
            + "(1)",
        "VALUES (ROW(1))");

    // even if comment abuts the tokens at either end, it becomes a space
    check(
        "values ('abc'/* a comment*/'def')",
        "VALUES (ROW('abc'\n'def'))");

    // comment which starts as soon as it has begun
    check(
        "values /**/ (1)",
        "VALUES (ROW(1))");
  }

  // expressions
  @Test public void testParseNumber() {
    // Exacts
    checkExp("1", "1");
    checkExp("+1.", "1");
    checkExp("-1", "-1");
    checkExp("- -1", "1");
    checkExp("1.0", "1.0");
    checkExp("-3.2", "-3.2");
    checkExp("1.", "1");
    checkExp(".1", "0.1");
    checkExp("2500000000", "2500000000");
    checkExp("5000000000", "5000000000");

    // Approximates
    checkExp("1e1", "1E1");
    checkExp("+1e1", "1E1");
    checkExp("1.1e1", "1.1E1");
    checkExp("1.1e+1", "1.1E1");
    checkExp("1.1e-1", "1.1E-1");
    checkExp("+1.1e-1", "1.1E-1");
    checkExp("1.E3", "1E3");
    checkExp("1.e-3", "1E-3");
    checkExp("1.e+3", "1E3");
    checkExp(".5E3", "5E2");
    checkExp("+.5e3", "5E2");
    checkExp("-.5E3", "-5E2");
    checkExp(".5e-32", "5E-33");

    // Mix integer/decimals/approx
    checkExp("3. + 2", "(3 + 2)");
    checkExp("1++2+3", "((1 + 2) + 3)");
    checkExp("1- -2", "(1 - -2)");
    checkExp(
        "1++2.3e-4++.5e-6++.7++8",
        "((((1 + 2.3E-4) + 5E-7) + 0.7) + 8)");
    checkExp(
        "1- -2.3e-4 - -.5e-6  -\n"
            + "-.7++8",
        "((((1 - -2.3E-4) - -5E-7) - -0.7) + 8)");
    checkExp("1+-2.*-3.e-1/-4", "(1 + ((-2 * -3E-1) / -4))");
  }

  @Test public void testParseNumberFails() {
    checkFails(
        "SELECT 0.5e1^.1^ from t",
        "(?s).*Encountered .*\\.1.* at line 1.*");
  }

  @Test public void testMinusPrefixInExpression() {
    checkExp("-(1+2)", "(- (1 + 2))");
  }

  // operator precedence
  @Test public void testPrecedence0() {
    checkExp("1 + 2 * 3 * 4 + 5", "((1 + ((2 * 3) * 4)) + 5)");
  }

  @Test public void testPrecedence1() {
    checkExp("1 + 2 * (3 * (4 + 5))", "(1 + (2 * (3 * (4 + 5))))");
  }

  @Test public void testPrecedence2() {
    checkExp("- - 1", "1"); // special case for unary minus
  }

  @Test public void testPrecedence2b() {
    checkExp("not not 1", "(NOT (NOT 1))"); // two prefixes
  }

  @Test public void testPrecedence3() {
    checkExp("- 1 is null", "(-1 IS NULL)"); // prefix vs. postfix
  }

  @Test public void testPrecedence4() {
    checkExp("1 - -2", "(1 - -2)"); // infix, prefix '-'
  }

  @Test public void testPrecedence5() {
    checkExp("1++2", "(1 + 2)"); // infix, prefix '+'
    checkExp("1+ +2", "(1 + 2)"); // infix, prefix '+'
  }

  @Test public void testPrecedenceSetOps() {
    check(
        "select * from a union "
            + "select * from b intersect "
            + "select * from c intersect "
            + "select * from d except "
            + "select * from e except "
            + "select * from f union "
            + "select * from g",
        "((((SELECT *\n"
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
            + "FROM `G`)");
  }

  @Test public void testQueryInFrom() {
    // one query with 'as', the other without
    check(
        "select * from (select * from emp) as e join (select * from dept) d",
        "SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `EMP`) AS `E`\n"
            + "INNER JOIN (SELECT *\n"
            + "FROM `DEPT`) AS `D`");
  }

  @Test public void testQuotesInString() {
    checkExp("'a''b'", "'a''b'");
    checkExp("'''x'", "'''x'");
    checkExp("''", "''");
    checkExp(
        "'Quoted strings aren''t \"hard\"'",
        "'Quoted strings aren''t \"hard\"'");
  }

  @Test public void testScalarQueryInWhere() {
    check(
        "select * from emp where 3 = (select count(*) from dept where dept.deptno = emp.deptno)",
        "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE (3 = (SELECT COUNT(*)\n"
            + "FROM `DEPT`\n"
            + "WHERE (`DEPT`.`DEPTNO` = `EMP`.`DEPTNO`)))");
  }

  @Test public void testScalarQueryInSelect() {
    check(
        "select x, (select count(*) from dept where dept.deptno = emp.deptno) from emp",
        "SELECT `X`, (SELECT COUNT(*)\n"
            + "FROM `DEPT`\n"
            + "WHERE (`DEPT`.`DEPTNO` = `EMP`.`DEPTNO`))\n"
            + "FROM `EMP`");
  }

  @Test public void testSelectList() {
    check(
        "select * from emp, dept",
        "SELECT *\n"
            + "FROM `EMP`,\n"
            + "`DEPT`");
  }

  @Test public void testSelectWithoutFrom() {
    sql("select 2+2").ok("SELECT (2 + 2)");
  }

  @Test public void testSelectWithoutFrom2() {
    sql("select 2+2 as x, 'a' as y")
        .ok("SELECT (2 + 2) AS `X`, 'a' AS `Y`");
  }

  @Test public void testSelectDistinctWithoutFrom() {
    sql("select distinct 2+2 as x, 'a' as y")
        .ok("SELECT DISTINCT (2 + 2) AS `X`, 'a' AS `Y`");
  }

  @Test public void testSelectWithoutFromWhereFails() {
    sql("select 2+2 as x ^where^ 1 > 2")
        .fails("(?s).*Encountered \"where\" at line .*");
  }

  @Test public void testSelectWithoutFromGroupByFails() {
    sql("select 2+2 as x ^group^ by 1, 2")
        .fails("(?s).*Encountered \"group\" at line .*");
  }

  @Test public void testSelectWithoutFromHavingFails() {
    sql("select 2+2 as x ^having^ 1 > 2")
        .fails("(?s).*Encountered \"having\" at line .*");
  }

  @Test public void testSelectList3() {
    check(
        "select 1, emp.*, 2 from emp",
        "SELECT 1, `EMP`.*, 2\n"
            + "FROM `EMP`");
  }

  @Test public void testSelectList4() {
    checkFails(
        "select ^from^ emp",
        "(?s).*Encountered \"from\" at line .*");
  }

  @Test public void testStar() {
    check(
        "select * from emp",
        "SELECT *\n"
            + "FROM `EMP`");
  }

  @Test public void testCompoundStar() {
    final String sql = "select sales.emp.address.zipcode,\n"
        + " sales.emp.address.*\n"
        + "from sales.emp";
    final String expected = "SELECT `SALES`.`EMP`.`ADDRESS`.`ZIPCODE`,"
        + " `SALES`.`EMP`.`ADDRESS`.*\n"
        + "FROM `SALES`.`EMP`";
    sql(sql).ok(expected);
  }

  @Test public void testSelectDistinct() {
    check(
        "select distinct foo from bar",
        "SELECT DISTINCT `FOO`\n"
            + "FROM `BAR`");
  }

  @Test public void testSelectAll() {
    // "unique" is the default -- so drop the keyword
    check(
        "select * from (select all foo from bar) as xyz",
        "SELECT *\n"
            + "FROM (SELECT ALL `FOO`\n"
            + "FROM `BAR`) AS `XYZ`");
  }

  @Test public void testSelectStream() {
    sql("select stream foo from bar")
        .ok("SELECT STREAM `FOO`\n"
            + "FROM `BAR`");
  }

  @Test public void testSelectStreamDistinct() {
    sql("select stream distinct foo from bar")
        .ok("SELECT STREAM DISTINCT `FOO`\n"
                + "FROM `BAR`");
  }

  @Test public void testWhere() {
    check(
        "select * from emp where empno > 5 and gender = 'F'",
        "SELECT *\n"
            + "FROM `EMP`\n"
            + "WHERE ((`EMPNO` > 5) AND (`GENDER` = 'F'))");
  }

  @Test public void testNestedSelect() {
    check(
        "select * from (select * from emp)",
        "SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `EMP`)");
  }

  @Test public void testValues() {
    check("values(1,'two')", "VALUES (ROW(1, 'two'))");
  }

  @Test public void testValuesExplicitRow() {
    check("values row(1,'two')", "VALUES (ROW(1, 'two'))");
  }

  @Test public void testFromValues() {
    check(
        "select * from (values(1,'two'), 3, (4, 'five'))",
        "SELECT *\n"
            + "FROM (VALUES (ROW(1, 'two')),\n"
            + "(ROW(3)),\n"
            + "(ROW(4, 'five')))");
  }

  @Test public void testFromValuesWithoutParens() {
    checkFails(
        "select 1 from ^values^('x')",
        "(?s)Encountered \"values\" at line 1, column 15\\.\n"
            + "Was expecting one of:\n"
            + "    \"LATERAL\" \\.\\.\\.\n"
            + "    \"TABLE\" \\.\\.\\.\n"
            + "    \"UNNEST\" \\.\\.\\.\n"
            + "    <IDENTIFIER> \\.\\.\\.\n"
            + "    <QUOTED_IDENTIFIER> \\.\\.\\.\n"
            + "    <BACK_QUOTED_IDENTIFIER> \\.\\.\\.\n"
            + "    <BRACKET_QUOTED_IDENTIFIER> \\.\\.\\.\n"
            + "    <UNICODE_QUOTED_IDENTIFIER> \\.\\.\\.\n"
            + "    \"\\(\" \\.\\.\\.\n.*");
  }

  @Test public void testEmptyValues() {
    checkFails(
        "select * from (values(^)^)",
        "(?s).*Encountered \"\\)\" at .*");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-493">[CALCITE-493]
   * Add EXTEND clause, for defining columns and their types at query/DML
   * time</a>. */
  @Test public void testTableExtend() {
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

  @Test public void testExplicitTable() {
    check("table emp", "(TABLE `EMP`)");

    checkFails(
        "table ^123^",
        "(?s)Encountered \"123\" at line 1, column 7\\.\n.*");
  }

  @Test public void testExplicitTableOrdered() {
    check(
        "table emp order by name",
        "(TABLE `EMP`)\n"
            + "ORDER BY `NAME`");
  }

  @Test public void testSelectFromExplicitTable() {
    check(
        "select * from (table emp)",
        "SELECT *\n"
            + "FROM (TABLE `EMP`)");
  }

  @Test public void testSelectFromBareExplicitTableFails() {
    checkFails(
        "select * from table ^emp^",
        "(?s).*Encountered \"emp\" at .*");

    checkFails(
        "select * from (table ^(^select empno from emp))",
        "(?s)Encountered \"\\(\".*");
  }

  @Test public void testCollectionTable() {
    check(
        "select * from table(ramp(3, 4))",
        "SELECT *\n"
            + "FROM TABLE(`RAMP`(3, 4))");
  }

  @Test public void testCollectionTableWithCursorParam() {
    check(
        "select * from table(dedup(cursor(select * from emps),'name'))",
        "SELECT *\n"
            + "FROM TABLE(`DEDUP`((CURSOR ((SELECT *\n"
            + "FROM `EMPS`))), 'name'))");
  }

  @Test public void testCollectionTableWithColumnListParam() {
    check(
        "select * from table(dedup(cursor(select * from emps),"
            + "row(empno, name)))",
        "SELECT *\n"
            + "FROM TABLE(`DEDUP`((CURSOR ((SELECT *\n"
            + "FROM `EMPS`))), (ROW(`EMPNO`, `NAME`))))");
  }

  @Test public void testLateral() {
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
    sql("select * from lateral table(ramp(1))").ok(expected);
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
    sql("select * from lateral (select * from emp)").ok(expected2);
    sql("select * from lateral (select * from emp) as t")
        .ok(expected2 + " AS `T`");
    sql("select * from lateral (select * from emp) as t(x)")
        .ok(expected2 + " AS `T` (`X`)");
  }

  @Test public void testCollectionTableWithLateral() {
    final String sql = "select * from dept, lateral table(ramp(dept.deptno))";
    final String expected = "SELECT *\n"
        + "FROM `DEPT`,\n"
        + "LATERAL TABLE(`RAMP`(`DEPT`.`DEPTNO`))";
    sql(sql).ok(expected);
  }

  @Test public void testCollectionTableWithLateral2() {
    final String sql = "select * from dept as d\n"
        + "cross join lateral table(ramp(dept.deptno)) as r";
    final String expected = "SELECT *\n"
        + "FROM `DEPT` AS `D`\n"
        + "CROSS JOIN LATERAL TABLE(`RAMP`(`DEPT`.`DEPTNO`)) AS `R`";
    sql(sql).ok(expected);
  }

  @Test public void testCollectionTableWithLateral3() {
    // LATERAL before first table in FROM clause doesn't achieve anything, but
    // it's valid.
    final String sql = "select * from lateral table(ramp(dept.deptno)), dept";
    final String expected = "SELECT *\n"
        + "FROM LATERAL TABLE(`RAMP`(`DEPT`.`DEPTNO`)),\n"
        + "`DEPT`";
    sql(sql).ok(expected);
  }

  @Test public void testIllegalCursors() {
    checkFails(
        "select ^cursor^(select * from emps) from emps",
        "CURSOR expression encountered in illegal context");
    checkFails(
        "call list(^cursor^(select * from emps))",
        "CURSOR expression encountered in illegal context");
    checkFails(
        "select f(^cursor^(select * from emps)) from emps",
        "CURSOR expression encountered in illegal context");
  }

  @Test public void testExplain() {
    final String sql = "explain plan for select * from emps";
    final String expected = "EXPLAIN PLAN"
        + " INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
        + "SELECT *\n"
        + "FROM `EMPS`";
    sql(sql).ok(expected);
  }

  @Test public void testExplainAsXml() {
    final String sql = "explain plan as xml for select * from emps";
    final String expected = "EXPLAIN PLAN"
        + " INCLUDING ATTRIBUTES WITH IMPLEMENTATION AS XML FOR\n"
        + "SELECT *\n"
        + "FROM `EMPS`";
    sql(sql).ok(expected);
  }

  @Test public void testExplainAsJson() {
    final String sql = "explain plan as json for select * from emps";
    final String expected = "EXPLAIN PLAN"
        + " INCLUDING ATTRIBUTES WITH IMPLEMENTATION AS JSON FOR\n"
        + "SELECT *\n"
        + "FROM `EMPS`";
    sql(sql).ok(expected);
  }

  @Test public void testExplainWithImpl() {
    check(
        "explain plan with implementation for select * from emps",
        "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
            + "SELECT *\n"
            + "FROM `EMPS`");
  }

  @Test public void testExplainWithoutImpl() {
    check(
        "explain plan without implementation for select * from emps",
        "EXPLAIN PLAN INCLUDING ATTRIBUTES WITHOUT IMPLEMENTATION FOR\n"
            + "SELECT *\n"
            + "FROM `EMPS`");
  }

  @Test public void testExplainWithType() {
    check(
        "explain plan with type for (values (true))",
        "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH TYPE FOR\n"
            + "(VALUES (ROW(TRUE)))");
  }

  @Test public void testDescribeSchema() {
    check("describe schema A",
        "DESCRIBE SCHEMA `A`");
    // Currently DESCRIBE DATABASE, DESCRIBE CATALOG become DESCRIBE SCHEMA.
    // See [CALCITE-1221] Implement DESCRIBE DATABASE, CATALOG, STATEMENT
    check("describe database A",
        "DESCRIBE SCHEMA `A`");
    check("describe catalog A",
        "DESCRIBE SCHEMA `A`");
  }

  @Test public void testDescribeTable() {
    check("describe emps",
        "DESCRIBE TABLE `EMPS`");
    check("describe \"emps\"",
        "DESCRIBE TABLE `emps`");
    check("describe s.emps",
        "DESCRIBE TABLE `S`.`EMPS`");
    check("describe db.c.s.emps",
        "DESCRIBE TABLE `DB`.`C`.`S`.`EMPS`");
    check("describe emps col1",
        "DESCRIBE TABLE `EMPS` `COL1`");
    // table keyword is OK
    check("describe table emps col1",
        "DESCRIBE TABLE `EMPS` `COL1`");
    // character literal for column name not ok
    checkFails("describe emps ^'col_'^",
        "(?s).*Encountered \"\\\\'col_\\\\'\" at .*");
    // composite column name not ok
    checkFails("describe emps c1^.^c2",
        "(?s).*Encountered \"\\.\" at .*");
  }

  @Test public void testDescribeStatement() {
    // Currently DESCRIBE STATEMENT becomes EXPLAIN.
    // See [CALCITE-1221] Implement DESCRIBE DATABASE, CATALOG, STATEMENT
    final String expected0 = ""
        + "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
        + "SELECT *\n"
        + "FROM `EMPS`";
    check("describe statement select * from emps", expected0);
    final String expected1 = ""
        + "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
        + "(SELECT *\n"
        + "FROM `EMPS`\n"
        + "ORDER BY 2)";
    check("describe statement select * from emps order by 2",
        expected1);
    check("describe select * from emps", expected0);
    check("describe (select * from emps)", expected0);
    check("describe statement (select * from emps)", expected0);
    final String expected2 = ""
        + "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
        + "(SELECT `DEPTNO`\n"
        + "FROM `EMPS`\n"
        + "UNION\n"
        + "SELECT `DEPTNO`\n"
        + "FROM `DEPTS`)";
    check("describe select deptno from emps union select deptno from depts",
        expected2);
    final String expected3 = ""
        + "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION FOR\n"
        + "INSERT INTO `EMPS`\n"
        + "VALUES (ROW(1, 'a'))";
    check("describe insert into emps values (1, 'a')", expected3);
    // only allow query or DML, not explain, inside describe
    checkFails("describe ^explain^ plan for select * from emps",
        "(?s).*Encountered \"explain\" at .*");
    checkFails("describe statement ^explain^ plan for select * from emps",
        "(?s).*Encountered \"explain\" at .*");
  }

  @Test public void testSelectIsNotDdl() {
    sql("select 1 from t")
        .node(not(isDdl()));
  }

  @Test public void testInsertSelect() {
    final String expected = "INSERT INTO `EMPS`\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    sql("insert into emps select * from emps")
        .ok(expected)
        .node(not(isDdl()));
  }

  @Test public void testInsertUnion() {
    final String expected = "INSERT INTO `EMPS`\n"
        + "(SELECT *\n"
        + "FROM `EMPS1`\n"
        + "UNION\n"
        + "SELECT *\n"
        + "FROM `EMPS2`)";
    sql("insert into emps select * from emps1 union select * from emps2")
        .ok(expected);
  }

  @Test public void testInsertValues() {
    final String expected = "INSERT INTO `EMPS`\n"
        + "VALUES (ROW(1, 'Fredkin'))";
    sql("insert into emps values (1,'Fredkin')")
        .ok(expected)
        .node(not(isDdl()));
  }

  @Test public void testInsertValuesDefault() {
    final String expected = "INSERT INTO `EMPS`\n"
        + "VALUES (ROW(1, DEFAULT, 'Fredkin'))";
    sql("insert into emps values (1,DEFAULT,'Fredkin')")
        .ok(expected)
        .node(not(isDdl()));
  }

  @Test public void testInsertValuesRawDefault() {
    final String expected = "INSERT INTO `EMPS`\n"
        + "VALUES (ROW(DEFAULT))";
    sql("insert into emps values ^default^")
        .fails("(?s).*Encountered \"default\" at .*");
    sql("insert into emps values (default)")
        .ok(expected)
        .node(not(isDdl()));
  }

  @Test public void testInsertColumnList() {
    final String expected = "INSERT INTO `EMPS` (`X`, `Y`)\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    sql("insert into emps(x,y) select * from emps")
        .ok(expected);
  }

  @Test public void testInsertCaseSensitiveColumnList() {
    final String expected = "INSERT INTO `emps` (`x`, `y`)\n"
        + "(SELECT *\n"
        + "FROM `EMPS`)";
    sql("insert into \"emps\"(\"x\",\"y\") select * from emps")
        .ok(expected);
  }

  @Test public void testInsertExtendedColumnList() {
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

  @Test public void testUpdateExtendedColumnList() {
    final String expected = "UPDATE `EMPDEFAULTS` EXTEND (`EXTRA` BOOLEAN, `NOTE` VARCHAR)"
        + " SET `DEPTNO` = 1\n"
        + ", `EXTRA` = TRUE\n"
        + ", `EMPNO` = 20\n"
        + ", `ENAME` = 'Bob'\n"
        + ", `NOTE` = 'legion'\n"
        + "WHERE (`DEPTNO` = 10)";
    sql("update empdefaults(extra BOOLEAN, note VARCHAR)"
        + " set deptno = 1, extra = true, empno = 20, ename = 'Bob', note = 'legion'"
        + " where deptno = 10")
        .ok(expected);
  }


  @Test public void testUpdateCaseSensitiveExtendedColumnList() {
    final String expected = "UPDATE `EMPDEFAULTS` EXTEND (`extra` BOOLEAN, `NOTE` VARCHAR)"
        + " SET `DEPTNO` = 1\n"
        + ", `extra` = TRUE\n"
        + ", `EMPNO` = 20\n"
        + ", `ENAME` = 'Bob'\n"
        + ", `NOTE` = 'legion'\n"
        + "WHERE (`DEPTNO` = 10)";
    sql("update empdefaults(\"extra\" BOOLEAN, note VARCHAR)"
        + " set deptno = 1, \"extra\" = true, empno = 20, ename = 'Bob', note = 'legion'"
        + " where deptno = 10")
        .ok(expected);
  }

  @Test public void testInsertCaseSensitiveExtendedColumnList() {
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

  @Test public void testExplainInsert() {
    final String expected = "EXPLAIN PLAN INCLUDING ATTRIBUTES"
        + " WITH IMPLEMENTATION FOR\n"
        + "INSERT INTO `EMPS1`\n"
        + "(SELECT *\n"
        + "FROM `EMPS2`)";
    sql("explain plan for insert into emps1 select * from emps2")
        .ok(expected)
        .node(not(isDdl()));
  }

  @Test public void testUpsertValues() {
    final String expected = "UPSERT INTO `EMPS`\n"
        + "VALUES (ROW(1, 'Fredkin'))";
    final String sql = "upsert into emps values (1,'Fredkin')";
    if (isReserved("UPSERT")) {
      sql(sql)
          .ok(expected)
          .node(not(isDdl()));
    }
  }

  @Test public void testUpsertSelect() {
    final String sql = "upsert into emps select * from emp as e";
    final String expected = "UPSERT INTO `EMPS`\n"
        + "(SELECT *\n"
        + "FROM `EMP` AS `E`)";
    if (isReserved("UPSERT")) {
      sql(sql).ok(expected);
    }
  }

  @Test public void testExplainUpsert() {
    final String sql = "explain plan for upsert into emps1 values (1, 2)";
    final String expected = "EXPLAIN PLAN INCLUDING ATTRIBUTES"
        + " WITH IMPLEMENTATION FOR\n"
        + "UPSERT INTO `EMPS1`\n"
        + "VALUES (ROW(1, 2))";
    if (isReserved("UPSERT")) {
      sql(sql).ok(expected);
    }
  }

  @Test public void testDelete() {
    sql("delete from emps")
        .ok("DELETE FROM `EMPS`")
        .node(not(isDdl()));
  }

  @Test public void testDeleteWhere() {
    check(
        "delete from emps where empno=12",
        "DELETE FROM `EMPS`\n"
            + "WHERE (`EMPNO` = 12)");
  }

  @Test public void testUpdate() {
    sql("update emps set empno = empno + 1, sal = sal - 1 where empno=12")
        .ok("UPDATE `EMPS` SET `EMPNO` = (`EMPNO` + 1)\n"
                + ", `SAL` = (`SAL` - 1)\n"
                + "WHERE (`EMPNO` = 12)");
  }

  @Test public void testMergeSelectSource() {
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
        + "WHEN MATCHED THEN UPDATE SET `NAME` = `T`.`NAME`\n"
        + ", `DEPTNO` = `T`.`DEPTNO`\n"
        + ", `SALARY` = (`T`.`SALARY` * 0.1)\n"
        + "WHEN NOT MATCHED THEN INSERT (`NAME`, `DEPT`, `SALARY`) "
        + "(VALUES (ROW(`T`.`NAME`, 10, (`T`.`SALARY` * 0.15))))";
    sql(sql).ok(expected)
        .node(not(isDdl()));
  }

  /** Same as testMergeSelectSource but set with compound identifier. */
  @Test public void testMergeSelectSource2() {
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
        + "WHEN MATCHED THEN UPDATE SET `E`.`NAME` = `T`.`NAME`\n"
        + ", `E`.`DEPTNO` = `T`.`DEPTNO`\n"
        + ", `E`.`SALARY` = (`T`.`SALARY` * 0.1)\n"
        + "WHEN NOT MATCHED THEN INSERT (`NAME`, `DEPT`, `SALARY`) "
        + "(VALUES (ROW(`T`.`NAME`, 10, (`T`.`SALARY` * 0.15))))";
    sql(sql).ok(expected)
        .node(not(isDdl()));
  }

  @Test public void testMergeTableRefSource() {
    check(
        "merge into emps e "
            + "using tempemps as t "
            + "on e.empno = t.empno "
            + "when matched then update "
            + "set name = t.name, deptno = t.deptno, salary = t.salary * .1 "
            + "when not matched then insert (name, dept, salary) "
            + "values(t.name, 10, t.salary * .15)",

        "MERGE INTO `EMPS` AS `E`\n"
            + "USING `TEMPEMPS` AS `T`\n"
            + "ON (`E`.`EMPNO` = `T`.`EMPNO`)\n"
            + "WHEN MATCHED THEN UPDATE SET `NAME` = `T`.`NAME`\n"
            + ", `DEPTNO` = `T`.`DEPTNO`\n"
            + ", `SALARY` = (`T`.`SALARY` * 0.1)\n"
            + "WHEN NOT MATCHED THEN INSERT (`NAME`, `DEPT`, `SALARY`) "
            + "(VALUES (ROW(`T`.`NAME`, 10, (`T`.`SALARY` * 0.15))))");
  }

  /** Same with testMergeTableRefSource but set with compound identifier. */
  @Test public void testMergeTableRefSource2() {
    check(
        "merge into emps e "
            + "using tempemps as t "
            + "on e.empno = t.empno "
            + "when matched then update "
            + "set e.name = t.name, e.deptno = t.deptno, e.salary = t.salary * .1 "
            + "when not matched then insert (name, dept, salary) "
            + "values(t.name, 10, t.salary * .15)",

        "MERGE INTO `EMPS` AS `E`\n"
            + "USING `TEMPEMPS` AS `T`\n"
            + "ON (`E`.`EMPNO` = `T`.`EMPNO`)\n"
            + "WHEN MATCHED THEN UPDATE SET `E`.`NAME` = `T`.`NAME`\n"
            + ", `E`.`DEPTNO` = `T`.`DEPTNO`\n"
            + ", `E`.`SALARY` = (`T`.`SALARY` * 0.1)\n"
            + "WHEN NOT MATCHED THEN INSERT (`NAME`, `DEPT`, `SALARY`) "
            + "(VALUES (ROW(`T`.`NAME`, 10, (`T`.`SALARY` * 0.15))))");
  }

  @Test public void testBitStringNotImplemented() {
    // Bit-string is longer part of the SQL standard. We do not support it.
    checkFails(
        "select B^'1011'^ || 'foobar' from (values (true))",
        "(?s).*Encountered \"\\\\'1011\\\\'\" at line 1, column 9.*");
  }

  @Test public void testHexAndBinaryString() {
    checkExp("x''=X'2'", "(X'' = X'2')");
    checkExp("x'fffff'=X''", "(X'FFFFF' = X'')");
    checkExp(
        "x'1' \t\t\f\r \n"
            + "'2'--hi this is a comment'FF'\r\r\t\f \n"
            + "'34'",
        "X'1'\n'2'\n'34'");
    checkExp(
        "x'1' \t\t\f\r \n"
            + "'000'--\n"
            + "'01'",
        "X'1'\n'000'\n'01'");
    checkExp(
        "x'1234567890abcdef'=X'fFeEdDcCbBaA'",
        "(X'1234567890ABCDEF' = X'FFEEDDCCBBAA')");

    // Check the inital zeroes don't get trimmed somehow
    checkExp("x'001'=X'000102'", "(X'001' = X'000102')");
  }

  @Test public void testHexAndBinaryStringFails() {
    checkFails(
        "select ^x'FeedGoats'^ from t",
        "Binary literal string must contain only characters '0' - '9', 'A' - 'F'");
    checkFails(
        "select ^x'abcdefG'^ from t",
        "Binary literal string must contain only characters '0' - '9', 'A' - 'F'");
    checkFails(
        "select x'1' ^x'2'^ from t",
        "(?s).*Encountered .x.*2.* at line 1, column 13.*");

    // valid syntax, but should fail in the validator
    check(
        "select x'1' '2' from t",
        "SELECT X'1'\n"
            + "'2'\n"
            + "FROM `T`");
  }

  @Test public void testStringLiteral() {
    checkExp("_latin1'hi'", "_LATIN1'hi'");
    checkExp(
        "N'is it a plane? no it''s superman!'",
        "_ISO-8859-1'is it a plane? no it''s superman!'");
    checkExp("n'lowercase n'", "_ISO-8859-1'lowercase n'");
    checkExp("'boring string'", "'boring string'");
    checkExp("_iSo-8859-1'bye'", "_ISO-8859-1'bye'");
    checkExp(
        "'three' \n ' blind'\n' mice'",
        "'three'\n' blind'\n' mice'");
    checkExp(
        "'three' -- comment \n ' blind'\n' mice'",
        "'three'\n' blind'\n' mice'");
    checkExp(
        "N'bye' \t\r\f\f\n' bye'",
        "_ISO-8859-1'bye'\n' bye'");
    checkExp(
        "_iso-8859-1'bye' \n\n--\n-- this is a comment\n' bye'",
        "_ISO-8859-1'bye'\n' bye'");
    checkExp("_utf8'hi'", "_UTF8'hi'");

    // newline in string literal
    checkExp("'foo\rbar'", "'foo\rbar'");
    checkExp("'foo\nbar'", "'foo\nbar'");

    // prevent test infrastructure from converting \r\n to \n
    boolean[] linuxify = LINUXIFY.get();
    try {
      linuxify[0] = false;
      checkExp("'foo\r\nbar'", "'foo\r\nbar'");
    } finally {
      linuxify[0] = true;
    }
  }

  @Test public void testStringLiteralFails() {
    checkFails(
        "select N ^'space'^",
        "(?s).*Encountered .*space.* at line 1, column ...*");
    checkFails(
        "select _latin1 \n^'newline'^",
        "(?s).*Encountered.*newline.* at line 2, column ...*");
    checkFails(
        "select ^_unknown-charset''^ from (values(true))",
        "Unknown character set 'unknown-charset'");

    // valid syntax, but should give a validator error
    check(
        "select N'1' '2' from t",
        "SELECT _ISO-8859-1'1'\n'2'\n"
            + "FROM `T`");
  }

  @Test public void testStringLiteralChain() {
    final String fooBar =
        "'foo'\n"
            + "'bar'";
    final String fooBarBaz =
        "'foo'\n"
            + "'bar'\n"
            + "'baz'";
    checkExp("   'foo'\r'bar'", fooBar);
    checkExp("   'foo'\r\n'bar'", fooBar);
    checkExp("   'foo'\r\n\r\n'bar'  \n   'baz'", fooBarBaz);
    checkExp("   'foo' /* a comment */ 'bar'", fooBar);
    checkExp("   'foo' -- a comment\r\n 'bar'", fooBar);

    // String literals not separated by comment or newline are OK in
    // parser, should fail in validator.
    checkExp("   'foo' 'bar'", fooBar);
  }

  @Test public void testCaseExpression() {
    // implicit simple "ELSE NULL" case
    checkExp(
        "case \t col1 when 1 then 'one' end",
        "(CASE WHEN (`COL1` = 1) THEN 'one' ELSE NULL END)");

    // implicit searched "ELSE NULL" case
    checkExp(
        "case when nbr is false then 'one' end",
        "(CASE WHEN (`NBR` IS FALSE) THEN 'one' ELSE NULL END)");

    // multiple WHENs
    checkExp(
        "case col1 when \n1.2 then 'one' when 2 then 'two' else 'three' end",
        "(CASE WHEN (`COL1` = 1.2) THEN 'one' WHEN (`COL1` = 2) THEN 'two' ELSE 'three' END)");

    // sub-queries as case expression operands
    checkExp(
        "case (select * from emp) when 1 then 2 end",
        "(CASE WHEN ((SELECT *\n"
            + "FROM `EMP`) = 1) THEN 2 ELSE NULL END)");
    checkExp(
        "case 1 when (select * from emp) then 2 end",
        "(CASE WHEN (1 = (SELECT *\n"
            + "FROM `EMP`)) THEN 2 ELSE NULL END)");
    checkExp(
        "case 1 when 2 then (select * from emp) end",
        "(CASE WHEN (1 = 2) THEN (SELECT *\n"
            + "FROM `EMP`) ELSE NULL END)");
    checkExp(
        "case 1 when 2 then 3 else (select * from emp) end",
        "(CASE WHEN (1 = 2) THEN 3 ELSE (SELECT *\n"
            + "FROM `EMP`) END)");
    checkExp(
        "case x when 2, 4 then 3 else 4 end",
        "(CASE WHEN (`X` IN (2, 4)) THEN 3 ELSE 4 END)");
    // comma-list must not be empty
    checkFails(
        "case x when 2, 4 then 3 when ^then^ 5 else 4 end",
        "(?s)Encountered \"then\" at .*");
    // commas not allowed in boolean case
    checkFails(
        "case when b1, b2 ^when^ 2, 4 then 3 else 4 end",
        "(?s)Encountered \"when\" at .*");
  }

  @Test public void testCaseExpressionFails() {
    // Missing 'END'
    checkFails(
        "select case col1 when 1 then 'one' ^from^ t",
        "(?s).*from.*");

    // Wrong 'WHEN'
    checkFails(
        "select case col1 ^when1^ then 'one' end from t",
        "(?s).*when1.*");
  }

  @Test public void testNullIf() {
    checkExp(
        "nullif(v1,v2)",
        "NULLIF(`V1`, `V2`)");
    if (isReserved("NULLIF")) {
      checkExpFails(
          "1 + ^nullif^ + 3",
          "(?s)Encountered \"nullif \\+\" at line 1, column 5.*");
    }
  }

  @Test public void testCoalesce() {
    checkExp(
        "coalesce(v1)",
        "COALESCE(`V1`)");
    checkExp(
        "coalesce(v1,v2)",
        "COALESCE(`V1`, `V2`)");
    checkExp(
        "coalesce(v1,v2,v3)",
        "COALESCE(`V1`, `V2`, `V3`)");
  }

  @Test public void testLiteralCollate() {
    if (!Bug.FRG78_FIXED) {
      return;
    }

    checkExp(
        "'string' collate latin1$sv_SE$mega_strength",
        "'string' COLLATE ISO-8859-1$sv_SE$mega_strength");
    checkExp(
        "'a long '\n'string' collate latin1$sv_SE$mega_strength",
        "'a long ' 'string' COLLATE ISO-8859-1$sv_SE$mega_strength");
    checkExp(
        "x collate iso-8859-6$ar_LB$1",
        "`X` COLLATE ISO-8859-6$ar_LB$1");
    checkExp(
        "x.y.z collate shift_jis$ja_JP$2",
        "`X`.`Y`.`Z` COLLATE SHIFT_JIS$ja_JP$2");
    checkExp(
        "'str1'='str2' collate latin1$sv_SE",
        "('str1' = 'str2' COLLATE ISO-8859-1$sv_SE$primary)");
    checkExp(
        "'str1' collate latin1$sv_SE>'str2'",
        "('str1' COLLATE ISO-8859-1$sv_SE$primary > 'str2')");
    checkExp(
        "'str1' collate latin1$sv_SE<='str2' collate latin1$sv_FI",
        "('str1' COLLATE ISO-8859-1$sv_SE$primary <= 'str2' COLLATE ISO-8859-1$sv_FI$primary)");
  }

  @Test public void testCharLength() {
    checkExp("char_length('string')", "CHAR_LENGTH('string')");
    checkExp("character_length('string')", "CHARACTER_LENGTH('string')");
  }

  @Test public void testPosition() {
    checkExp(
        "posiTion('mouse' in 'house')",
        "POSITION('mouse' IN 'house')");
  }

  @Test public void testReplace() {
    checkExp("replace('x', 'y', 'z')", "REPLACE('x', 'y', 'z')");
  }

  @Test public void testDateLiteral() {
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
  @Test public void testTimeDate() {
    // CURRENT_TIME - returns time w/ timezone
    checkExp("CURRENT_TIME(3)", "CURRENT_TIME(3)");

    // checkFails("SELECT CURRENT_TIME() FROM foo",
    //     "SELECT CURRENT_TIME() FROM `FOO`");

    checkExp("CURRENT_TIME", "CURRENT_TIME");
    checkExp("CURRENT_TIME(x+y)", "CURRENT_TIME((`X` + `Y`))");

    // LOCALTIME returns time w/o TZ
    checkExp("LOCALTIME(3)", "LOCALTIME(3)");

    // checkFails("SELECT LOCALTIME() FROM foo",
    //     "SELECT LOCALTIME() FROM `FOO`");

    checkExp("LOCALTIME", "LOCALTIME");
    checkExp("LOCALTIME(x+y)", "LOCALTIME((`X` + `Y`))");

    // LOCALTIMESTAMP - returns timestamp w/o TZ
    checkExp("LOCALTIMESTAMP(3)", "LOCALTIMESTAMP(3)");

    // checkFails("SELECT LOCALTIMESTAMP() FROM foo",
    //     "SELECT LOCALTIMESTAMP() FROM `FOO`");

    checkExp("LOCALTIMESTAMP", "LOCALTIMESTAMP");
    checkExp("LOCALTIMESTAMP(x+y)", "LOCALTIMESTAMP((`X` + `Y`))");

    // CURRENT_DATE - returns DATE
    checkExp("CURRENT_DATE(3)", "CURRENT_DATE(3)");

    // checkFails("SELECT CURRENT_DATE() FROM foo",
    //     "SELECT CURRENT_DATE() FROM `FOO`");
    checkExp("CURRENT_DATE", "CURRENT_DATE");

    // checkFails("SELECT CURRENT_DATE(x+y) FROM foo",
    //     "CURRENT_DATE((`X` + `Y`))");

    // CURRENT_TIMESTAMP - returns timestamp w/ TZ
    checkExp("CURRENT_TIMESTAMP(3)", "CURRENT_TIMESTAMP(3)");

    // checkFails("SELECT CURRENT_TIMESTAMP() FROM foo",
    //     "SELECT CURRENT_TIMESTAMP() FROM `FOO`");

    checkExp("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP");
    checkExp("CURRENT_TIMESTAMP(x+y)", "CURRENT_TIMESTAMP((`X` + `Y`))");

    // Date literals
    checkExp("DATE '2004-12-01'", "DATE '2004-12-01'");

    // Time literals
    checkExp("TIME '12:01:01'", "TIME '12:01:01'");
    checkExp("TIME '12:01:01.'", "TIME '12:01:01'");
    checkExp("TIME '12:01:01.000'", "TIME '12:01:01.000'");
    checkExp("TIME '12:01:01.001'", "TIME '12:01:01.001'");
    checkExp("TIME '12:01:01.01023456789'", "TIME '12:01:01.01023456789'");

    // Timestamp literals
    checkExp(
        "TIMESTAMP '2004-12-01 12:01:01'",
        "TIMESTAMP '2004-12-01 12:01:01'");
    checkExp(
        "TIMESTAMP '2004-12-01 12:01:01.1'",
        "TIMESTAMP '2004-12-01 12:01:01.1'");
    checkExp(
        "TIMESTAMP '2004-12-01 12:01:01.'",
        "TIMESTAMP '2004-12-01 12:01:01'");
    checkExp(
        "TIMESTAMP  '2004-12-01 12:01:01.010234567890'",
        "TIMESTAMP '2004-12-01 12:01:01.010234567890'");
    checkExpSame("TIMESTAMP '2004-12-01 12:01:01.01023456789'");

    // Failures.
    checkFails("^DATE '12/21/99'^", "(?s).*Illegal DATE literal.*");
    checkFails("^TIME '1230:33'^", "(?s).*Illegal TIME literal.*");
    checkFails("^TIME '12:00:00 PM'^", "(?s).*Illegal TIME literal.*");
    checkFails(
        "^TIMESTAMP '12-21-99, 12:30:00'^",
        "(?s).*Illegal TIMESTAMP literal.*");
  }

  /**
   * Tests for casting to/from date/time types.
   */
  @Test public void testDateTimeCast() {
    //   checkExp("CAST(DATE '2001-12-21' AS CHARACTER VARYING)",
    // "CAST(2001-12-21)");
    checkExp("CAST('2001-12-21' AS DATE)", "CAST('2001-12-21' AS DATE)");
    checkExp("CAST(12 AS DATE)", "CAST(12 AS DATE)");
    checkFails(
        "CAST('2000-12-21' AS DATE ^NOT^ NULL)",
        "(?s).*Encountered \"NOT\" at line 1, column 27.*");
    checkFails(
        "CAST('foo' as ^1^)",
        "(?s).*Encountered \"1\" at line 1, column 15.*");
    checkExp(
        "Cast(DATE '2004-12-21' AS VARCHAR(10))",
        "CAST(DATE '2004-12-21' AS VARCHAR(10))");
  }

  @Test public void testTrim() {
    checkExp(
        "trim('mustache' FROM 'beard')",
        "TRIM(BOTH 'mustache' FROM 'beard')");
    checkExp("trim('mustache')", "TRIM(BOTH ' ' FROM 'mustache')");
    checkExp(
        "trim(TRAILING FROM 'mustache')",
        "TRIM(TRAILING ' ' FROM 'mustache')");
    checkExp(
        "trim(bOth 'mustache' FROM 'beard')",
        "TRIM(BOTH 'mustache' FROM 'beard')");
    checkExp(
        "trim( lEaDing       'mustache' FROM 'beard')",
        "TRIM(LEADING 'mustache' FROM 'beard')");
    checkExp(
        "trim(\r\n\ttrailing\n  'mustache' FROM 'beard')",
        "TRIM(TRAILING 'mustache' FROM 'beard')");
    checkExp(
        "trim (coalesce(cast(null as varchar(2)))||"
            + "' '||coalesce('junk ',''))",
        "TRIM(BOTH ' ' FROM ((COALESCE(CAST(NULL AS VARCHAR(2))) || "
            + "' ') || COALESCE('junk ', '')))");

    checkFails(
        "trim(^from^ 'beard')",
        "(?s).*'FROM' without operands preceding it is illegal.*");
  }

  @Test public void testConvertAndTranslate() {
    checkExp(
        "convert('abc' using conversion)",
        "CONVERT('abc' USING `CONVERSION`)");
    checkExp(
        "translate('abc' using lazy_translation)",
        "TRANSLATE('abc' USING `LAZY_TRANSLATION`)");
  }

  @Test public void testTranslate3() {
    checkExp(
        "translate('aaabbbccc', 'ab', '+-')",
        "TRANSLATE('aaabbbccc', 'ab', '+-')");
  }

  @Test public void testOverlay() {
    checkExp(
        "overlay('ABCdef' placing 'abc' from 1)",
        "OVERLAY('ABCdef' PLACING 'abc' FROM 1)");
    checkExp(
        "overlay('ABCdef' placing 'abc' from 1 for 3)",
        "OVERLAY('ABCdef' PLACING 'abc' FROM 1 FOR 3)");
  }

  @Test public void testJdbcFunctionCall() {
    checkExp("{fn apa(1,'1')}", "{fn APA(1, '1') }");
    checkExp("{ Fn apa(log10(ln(1))+2)}", "{fn APA((LOG10(LN(1)) + 2)) }");
    checkExp("{fN apa(*)}", "{fn APA(*) }");
    checkExp("{   FN\t\r\n apa()}", "{fn APA() }");
    checkExp("{fn insert()}", "{fn INSERT() }");
    checkExp("{fn convert(foo, SQL_VARCHAR)}",
        "{fn CONVERT(`FOO`, SQL_VARCHAR) }");
    checkExp("{fn convert(log10(100), integer)}",
        "{fn CONVERT(LOG10(100), SQL_INTEGER) }");
    checkExp("{fn convert(1, SQL_INTERVAL_YEAR)}",
        "{fn CONVERT(1, SQL_INTERVAL_YEAR) }");
    checkExp("{fn convert(1, SQL_INTERVAL_YEAR_TO_MONTH)}",
        "{fn CONVERT(1, SQL_INTERVAL_YEAR_TO_MONTH) }");
    checkExpFails("{fn convert(1, ^sql_interval_year_to_day^)}",
        "(?s)Encountered \"sql_interval_year_to_day\" at line 1, column 16\\.\n.*");
    checkExp("{fn convert(1, sql_interval_day)}",
        "{fn CONVERT(1, SQL_INTERVAL_DAY) }");
    checkExp("{fn convert(1, sql_interval_day_to_minute)}",
        "{fn CONVERT(1, SQL_INTERVAL_DAY_TO_MINUTE) }");
    checkExpFails("{fn convert(^)^}", "(?s)Encountered \"\\)\" at.*");
    checkExpFails("{fn convert(\"123\", SMALLINT^(^3)}",
        "(?s)Encountered \"\\(\" at.*");
    // Regular types (without SQL_) are OK for regular types, but not for
    // intervals.
    checkExp("{fn convert(1, INTEGER)}",
        "{fn CONVERT(1, SQL_INTEGER) }");
    checkExp("{fn convert(1, VARCHAR)}",
        "{fn CONVERT(1, SQL_VARCHAR) }");
    checkExpFails("{fn convert(1, VARCHAR^(^5))}",
        "(?s)Encountered \"\\(\" at.*");
    checkExpFails("{fn convert(1, ^INTERVAL^ YEAR TO MONTH)}",
        "(?s)Encountered \"INTERVAL\" at.*");
    checkExpFails("{fn convert(1, ^INTERVAL^ YEAR)}",
        "(?s)Encountered \"INTERVAL\" at.*");
  }

  @Test public void testWindowReference() {
    checkExp("sum(sal) over (w)", "(SUM(`SAL`) OVER (`W`))");

    // Only 1 window reference allowed
    checkExpFails(
        "sum(sal) over (w ^w1^ partition by deptno)",
        "(?s)Encountered \"w1\" at.*");
  }

  @Test public void testWindowInSubQuery() {
    check(
        "select * from ( select sum(x) over w, sum(y) over w from s window w as (range interval '1' minute preceding))",
        "SELECT *\n"
            + "FROM (SELECT (SUM(`X`) OVER `W`), (SUM(`Y`) OVER `W`)\n"
            + "FROM `S`\n"
            + "WINDOW `W` AS (RANGE INTERVAL '1' MINUTE PRECEDING))");
  }

  @Test public void testWindowSpec() {
    // Correct syntax
    check(
        "select count(z) over w as foo from Bids window w as (partition by y + yy, yyy order by x rows between 2 preceding and 2 following)",
        "SELECT (COUNT(`Z`) OVER `W`) AS `FOO`\n"
            + "FROM `BIDS`\n"
            + "WINDOW `W` AS (PARTITION BY (`Y` + `YY`), `YYY` ORDER BY `X` ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)");

    check(
        "select count(*) over w from emp window w as (rows 2 preceding)",
        "SELECT (COUNT(*) OVER `W`)\n"
            + "FROM `EMP`\n"
            + "WINDOW `W` AS (ROWS 2 PRECEDING)");

    // Chained string literals are valid syntax. They are unlikely to be
    // semantically valid, because intervals are usually numeric or
    // datetime.
    // Note: literal chain is not yet replaced with combined literal
    // since we are just parsing, and not validating the sql.
    check(
        "select count(*) over w from emp window w as (\n"
            + "  rows 'foo' 'bar'\n"
            + "       'baz' preceding)",
        "SELECT (COUNT(*) OVER `W`)\n"
            + "FROM `EMP`\n"
            + "WINDOW `W` AS (ROWS 'foo'\n'bar'\n'baz' PRECEDING)");

    // Partition clause out of place. Found after ORDER BY
    checkFails(
        "select count(z) over w as foo \n"
            + "from Bids window w as (partition by y order by x ^partition^ by y)",
        "(?s).*Encountered \"partition\".*");
    checkFails(
        "select count(z) over w as foo from Bids window w as (order by x ^partition^ by y)",
        "(?s).*Encountered \"partition\".*");

    // Cannot partition by sub-query
    checkFails(
        "select sum(a) over (partition by ^(^select 1 from t), x) from t2",
        "Query expression encountered in illegal context");

    // AND is required in BETWEEN clause of window frame
    checkFails(
        "select sum(x) over (order by x range between unbounded preceding ^unbounded^ following)",
        "(?s).*Encountered \"unbounded\".*");

    // WINDOW keyword is not permissible.
    checkFails(
        "select sum(x) over ^window^ (order by x) from bids",
        "(?s).*Encountered \"window\".*");

    // ORDER BY must be before Frame spec
    checkFails(
        "select sum(x) over (rows 2 preceding ^order^ by x) from emp",
        "(?s).*Encountered \"order\".*");
  }

  @Test public void testWindowSpecPartial() {
    // ALLOW PARTIAL is the default, and is omitted when the statement is
    // unparsed.
    check(
        "select sum(x) over (order by x allow partial) from bids",
        "SELECT (SUM(`X`) OVER (ORDER BY `X`))\n"
            + "FROM `BIDS`");

    check(
        "select sum(x) over (order by x) from bids",
        "SELECT (SUM(`X`) OVER (ORDER BY `X`))\n"
            + "FROM `BIDS`");

    check(
        "select sum(x) over (order by x disallow partial) from bids",
        "SELECT (SUM(`X`) OVER (ORDER BY `X` DISALLOW PARTIAL))\n"
            + "FROM `BIDS`");

    check(
        "select sum(x) over (order by x) from bids",
        "SELECT (SUM(`X`) OVER (ORDER BY `X`))\n"
            + "FROM `BIDS`");
  }

  @Test public void testNullTreatment() {
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

  @Test public void testAs() {
    // AS is optional for column aliases
    check(
        "select x y from t",
        "SELECT `X` AS `Y`\n"
            + "FROM `T`");

    check(
        "select x AS y from t",
        "SELECT `X` AS `Y`\n"
            + "FROM `T`");
    check(
        "select sum(x) y from t group by z",
        "SELECT SUM(`X`) AS `Y`\n"
            + "FROM `T`\n"
            + "GROUP BY `Z`");

    // Even after OVER
    check(
        "select count(z) over w foo from Bids window w as (order by x)",
        "SELECT (COUNT(`Z`) OVER `W`) AS `FOO`\n"
            + "FROM `BIDS`\n"
            + "WINDOW `W` AS (ORDER BY `X`)");

    // AS is optional for table correlation names
    final String expected =
        "SELECT `X`\n"
            + "FROM `T` AS `T1`";
    check("select x from t as t1", expected);
    check("select x from t t1", expected);

    // AS is required in WINDOW declaration
    checkFails(
        "select sum(x) over w from bids window w ^(order by x)",
        "(?s).*Encountered \"\\(\".*");

    // Error if OVER and AS are in wrong order
    checkFails(
        "select count(*) as foo ^over^ w from Bids window w (order by x)",
        "(?s).*Encountered \"over\".*");
  }

  @Test public void testAsAliases() {
    check(
        "select x from t as t1 (a, b) where foo",
        "SELECT `X`\n"
            + "FROM `T` AS `T1` (`A`, `B`)\n"
            + "WHERE `FOO`");

    check(
        "select x from (values (1, 2), (3, 4)) as t1 (\"a\", b) where \"a\" > b",
        "SELECT `X`\n"
            + "FROM (VALUES (ROW(1, 2)),\n"
            + "(ROW(3, 4))) AS `T1` (`a`, `B`)\n"
            + "WHERE (`a` > `B`)");

    // must have at least one column
    checkFails(
        "select x from (values (1, 2), (3, 4)) as t1 (^)^",
        "(?s).*Encountered \"\\)\" at .*");

    // cannot have expressions
    checkFails(
        "select x from t as t1 (x ^+^ y)",
        "(?s).*Was expecting one of:\n"
            + "    \"\\)\" \\.\\.\\.\n"
            + "    \",\" \\.\\.\\..*");

    // cannot have compound identifiers
    checkFails(
        "select x from t as t1 (x^.^y)",
        "(?s).*Was expecting one of:\n"
            + "    \"\\)\" \\.\\.\\.\n"
            + "    \",\" \\.\\.\\..*");
  }

  @Test public void testOver() {
    checkExp(
        "sum(sal) over ()",
        "(SUM(`SAL`) OVER ())");
    checkExp(
        "sum(sal) over (partition by x, y)",
        "(SUM(`SAL`) OVER (PARTITION BY `X`, `Y`))");
    checkExp(
        "sum(sal) over (order by x desc, y asc)",
        "(SUM(`SAL`) OVER (ORDER BY `X` DESC, `Y`))");
    checkExp(
        "sum(sal) over (rows 5 preceding)",
        "(SUM(`SAL`) OVER (ROWS 5 PRECEDING))");
    checkExp(
        "sum(sal) over (range between interval '1' second preceding and interval '1' second following)",
        "(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND INTERVAL '1' SECOND FOLLOWING))");
    checkExp(
        "sum(sal) over (range between interval '1:03' hour preceding and interval '2' minute following)",
        "(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '1:03' HOUR PRECEDING AND INTERVAL '2' MINUTE FOLLOWING))");
    checkExp(
        "sum(sal) over (range between interval '5' day preceding and current row)",
        "(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '5' DAY PRECEDING AND CURRENT ROW))");
    checkExp(
        "sum(sal) over (range interval '5' day preceding)",
        "(SUM(`SAL`) OVER (RANGE INTERVAL '5' DAY PRECEDING))");
    checkExp(
        "sum(sal) over (range between unbounded preceding and current row)",
        "(SUM(`SAL`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))");
    checkExp(
        "sum(sal) over (range unbounded preceding)",
        "(SUM(`SAL`) OVER (RANGE UNBOUNDED PRECEDING))");
    checkExp(
        "sum(sal) over (range between current row and unbounded preceding)",
        "(SUM(`SAL`) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING))");
    checkExp(
        "sum(sal) over (range between current row and unbounded following)",
        "(SUM(`SAL`) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING))");
    checkExp(
        "sum(sal) over (range between 6 preceding and interval '1:03' hour preceding)",
        "(SUM(`SAL`) OVER (RANGE BETWEEN 6 PRECEDING AND INTERVAL '1:03' HOUR PRECEDING))");
    checkExp(
        "sum(sal) over (range between interval '1' second following and interval '5' day following)",
        "(SUM(`SAL`) OVER (RANGE BETWEEN INTERVAL '1' SECOND FOLLOWING AND INTERVAL '5' DAY FOLLOWING))");
  }

  @Test public void testElementFunc() {
    checkExp("element(a)", "ELEMENT(`A`)");
  }

  @Test public void testCardinalityFunc() {
    checkExp("cardinality(a)", "CARDINALITY(`A`)");
  }

  @Test public void testMemberOf() {
    checkExp("a member of b", "(`A` MEMBER OF `B`)");
    checkExp(
        "a member of multiset[b]",
        "(`A` MEMBER OF (MULTISET[`B`]))");
  }

  @Test public void testSubMultisetrOf() {
    checkExp("a submultiset of b", "(`A` SUBMULTISET OF `B`)");
  }

  @Test public void testIsASet() {
    checkExp("b is a set", "(`B` IS A SET)");
    checkExp("a is a set", "(`A` IS A SET)");
  }

  @Test public void testMultiset() {
    checkExp("multiset[1]", "(MULTISET[1])");
    checkExp("multiset[1,2.3]", "(MULTISET[1, 2.3])");
    checkExp("multiset[1,    '2']", "(MULTISET[1, '2'])");
    checkExp("multiset[ROW(1,2)]", "(MULTISET[(ROW(1, 2))])");
    checkExp(
        "multiset[ROW(1,2),ROW(3,4)]",
        "(MULTISET[(ROW(1, 2)), (ROW(3, 4))])");

    checkExp(
        "multiset(select*from T)",
        "(MULTISET ((SELECT *\n"
            + "FROM `T`)))");
  }

  @Test public void testMultisetUnion() {
    checkExp("a multiset union b", "(`A` MULTISET UNION ALL `B`)");
    checkExp("a multiset union all b", "(`A` MULTISET UNION ALL `B`)");
    checkExp("a multiset union distinct b", "(`A` MULTISET UNION DISTINCT `B`)");
  }

  @Test public void testMultisetExcept() {
    checkExp("a multiset EXCEPT b", "(`A` MULTISET EXCEPT ALL `B`)");
    checkExp("a multiset EXCEPT all b", "(`A` MULTISET EXCEPT ALL `B`)");
    checkExp("a multiset EXCEPT distinct b", "(`A` MULTISET EXCEPT DISTINCT `B`)");
  }

  @Test public void testMultisetIntersect() {
    checkExp("a multiset INTERSECT b", "(`A` MULTISET INTERSECT ALL `B`)");
    checkExp(
        "a multiset INTERSECT all b",
        "(`A` MULTISET INTERSECT ALL `B`)");
    checkExp(
        "a multiset INTERSECT distinct b",
        "(`A` MULTISET INTERSECT DISTINCT `B`)");
  }

  @Test public void testMultisetMixed() {
    checkExp(
        "multiset[1] MULTISET union b",
        "((MULTISET[1]) MULTISET UNION ALL `B`)");
    checkExp(
        "a MULTISET union b multiset intersect c multiset except d multiset union e",
        "(((`A` MULTISET UNION ALL (`B` MULTISET INTERSECT ALL `C`)) MULTISET EXCEPT ALL `D`) MULTISET UNION ALL `E`)");
  }

  @Test public void testMapItem() {
    checkExp("a['foo']", "`A`['foo']");
    checkExp("a['x' || 'y']", "`A`[('x' || 'y')]");
    checkExp("a['foo'] ['bar']", "`A`['foo']['bar']");
    checkExp("a['foo']['bar']", "`A`['foo']['bar']");
  }

  @Test public void testMapItemPrecedence() {
    checkExp("1 + a['foo'] * 3", "(1 + (`A`['foo'] * 3))");
    checkExp("1 * a['foo'] + 3", "((1 * `A`['foo']) + 3)");
    checkExp("a['foo']['bar']", "`A`['foo']['bar']");
    checkExp("a[b['foo' || 'bar']]", "`A`[`B`[('foo' || 'bar')]]");
  }

  @Test public void testArrayElement() {
    checkExp("a[1]", "`A`[1]");
    checkExp("a[b[1]]", "`A`[`B`[1]]");
    checkExp("a[b[1 + 2] + 3]", "`A`[(`B`[(1 + 2)] + 3)]");
  }

  @Test public void testArrayElementWithDot() {
    checkExp("a[1+2].b.c[2].d", "(((`A`[(1 + 2)].`B`).`C`)[2].`D`)");
    checkExp("a[b[1]].c.f0[d[1]]", "((`A`[`B`[1]].`C`).`F0`)[`D`[1]]");
  }

  @Test public void testArrayValueConstructor() {
    checkExp("array[1, 2]", "(ARRAY[1, 2])");
    checkExp("array [1, 2]", "(ARRAY[1, 2])"); // with space

    // parser allows empty array; validator will reject it
    checkExp("array[]", "(ARRAY[])");
    checkExp(
        "array[(1, 'a'), (2, 'b')]",
        "(ARRAY[(ROW(1, 'a')), (ROW(2, 'b'))])");
  }

  @Test public void testCastAsCollectionType() {
    // test array type.
    checkExp("cast(a as int array)", "CAST(`A` AS INTEGER ARRAY)");
    checkExp("cast(a as varchar(5) array)", "CAST(`A` AS VARCHAR(5) ARRAY)");
    checkExp("cast(a as int array array)", "CAST(`A` AS INTEGER ARRAY ARRAY)");
    checkExp("cast(a as varchar(5) array array)",
        "CAST(`A` AS VARCHAR(5) ARRAY ARRAY)");
    checkExpFails("cast(a as int array^<^10>)",
        "(?s).*Encountered \"<\" at line 1, column 20.\n.*");
    // test multiset type.
    checkExp("cast(a as int multiset)", "CAST(`A` AS INTEGER MULTISET)");
    checkExp("cast(a as varchar(5) multiset)", "CAST(`A` AS VARCHAR(5) MULTISET)");
    checkExp("cast(a as int multiset array)", "CAST(`A` AS INTEGER MULTISET ARRAY)");
    checkExp("cast(a as varchar(5) multiset array)",
        "CAST(`A` AS VARCHAR(5) MULTISET ARRAY)");
    // test row type nested in collection type.
    checkExp("cast(a as row(f0 int array multiset, f1 varchar(5) array) array multiset)",
        "CAST(`A` AS "
            + "ROW(`F0` INTEGER ARRAY MULTISET, "
            + "`F1` VARCHAR(5) ARRAY) "
            + "ARRAY MULTISET)");
    // test UDT collection type.
    checkExp("cast(a as MyUDT array multiset)",
        "CAST(`A` AS `MYUDT` ARRAY MULTISET)");
  }

  @Test public void testCastAsRowType() {
    checkExp("cast(a as row(f0 int, f1 varchar))",
        "CAST(`A` AS ROW(`F0` INTEGER, `F1` VARCHAR))");
    checkExp("cast(a as row(f0 int not null, f1 varchar null))",
        "CAST(`A` AS ROW(`F0` INTEGER, `F1` VARCHAR NULL))");
    // test nested row type.
    checkExp("cast(a as row("
        + "f0 row(ff0 int not null, ff1 varchar null) null, "
        + "f1 timestamp not null))",
        "CAST(`A` AS ROW("
            + "`F0` ROW(`FF0` INTEGER, `FF1` VARCHAR NULL) NULL, "
            + "`F1` TIMESTAMP))");
    // test row type in collection data types.
    checkExp("cast(a as row(f0 bigint not null, f1 decimal null) array)",
        "CAST(`A` AS ROW(`F0` BIGINT, `F1` DECIMAL NULL) ARRAY)");
    checkExp("cast(a as row(f0 varchar not null, f1 timestamp null) multiset)",
        "CAST(`A` AS ROW(`F0` VARCHAR, `F1` TIMESTAMP NULL) MULTISET)");
  }

  @Test public void testMapValueConstructor() {
    checkExp("map[1, 'x', 2, 'y']", "(MAP[1, 'x', 2, 'y'])");
    checkExp("map [1, 'x', 2, 'y']", "(MAP[1, 'x', 2, 'y'])");
    checkExp("map[]", "(MAP[])");
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
    checkExp(
        "interval '1' year",
        "INTERVAL '1' YEAR");
    checkExp(
        "interval '99' year",
        "INTERVAL '99' YEAR");

    // explicit precision equal to default
    checkExp(
        "interval '1' year(2)",
        "INTERVAL '1' YEAR(2)");
    checkExp(
        "interval '99' year(2)",
        "INTERVAL '99' YEAR(2)");

    // max precision
    checkExp(
        "interval '2147483647' year(10)",
        "INTERVAL '2147483647' YEAR(10)");

    // min precision
    checkExp(
        "interval '0' year(1)",
        "INTERVAL '0' YEAR(1)");

    // alternate precision
    checkExp(
        "interval '1234' year(4)",
        "INTERVAL '1234' YEAR(4)");

    // sign
    checkExp(
        "interval '+1' year",
        "INTERVAL '+1' YEAR");
    checkExp(
        "interval '-1' year",
        "INTERVAL '-1' YEAR");
    checkExp(
        "interval +'1' year",
        "INTERVAL '1' YEAR");
    checkExp(
        "interval +'+1' year",
        "INTERVAL '+1' YEAR");
    checkExp(
        "interval +'-1' year",
        "INTERVAL '-1' YEAR");
    checkExp(
        "interval -'1' year",
        "INTERVAL -'1' YEAR");
    checkExp(
        "interval -'+1' year",
        "INTERVAL -'+1' YEAR");
    checkExp(
        "interval -'-1' year",
        "INTERVAL -'-1' YEAR");
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
    checkExp(
        "interval '1-2' year to month",
        "INTERVAL '1-2' YEAR TO MONTH");
    checkExp(
        "interval '99-11' year to month",
        "INTERVAL '99-11' YEAR TO MONTH");
    checkExp(
        "interval '99-0' year to month",
        "INTERVAL '99-0' YEAR TO MONTH");

    // explicit precision equal to default
    checkExp(
        "interval '1-2' year(2) to month",
        "INTERVAL '1-2' YEAR(2) TO MONTH");
    checkExp(
        "interval '99-11' year(2) to month",
        "INTERVAL '99-11' YEAR(2) TO MONTH");
    checkExp(
        "interval '99-0' year(2) to month",
        "INTERVAL '99-0' YEAR(2) TO MONTH");

    // max precision
    checkExp(
        "interval '2147483647-11' year(10) to month",
        "INTERVAL '2147483647-11' YEAR(10) TO MONTH");

    // min precision
    checkExp(
        "interval '0-0' year(1) to month",
        "INTERVAL '0-0' YEAR(1) TO MONTH");

    // alternate precision
    checkExp(
        "interval '2006-2' year(4) to month",
        "INTERVAL '2006-2' YEAR(4) TO MONTH");

    // sign
    checkExp(
        "interval '-1-2' year to month",
        "INTERVAL '-1-2' YEAR TO MONTH");
    checkExp(
        "interval '+1-2' year to month",
        "INTERVAL '+1-2' YEAR TO MONTH");
    checkExp(
        "interval +'1-2' year to month",
        "INTERVAL '1-2' YEAR TO MONTH");
    checkExp(
        "interval +'-1-2' year to month",
        "INTERVAL '-1-2' YEAR TO MONTH");
    checkExp(
        "interval +'+1-2' year to month",
        "INTERVAL '+1-2' YEAR TO MONTH");
    checkExp(
        "interval -'1-2' year to month",
        "INTERVAL -'1-2' YEAR TO MONTH");
    checkExp(
        "interval -'-1-2' year to month",
        "INTERVAL -'-1-2' YEAR TO MONTH");
    checkExp(
        "interval -'+1-2' year to month",
        "INTERVAL -'+1-2' YEAR TO MONTH");
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
    checkExp(
        "interval '1' month",
        "INTERVAL '1' MONTH");
    checkExp(
        "interval '99' month",
        "INTERVAL '99' MONTH");

    // explicit precision equal to default
    checkExp(
        "interval '1' month(2)",
        "INTERVAL '1' MONTH(2)");
    checkExp(
        "interval '99' month(2)",
        "INTERVAL '99' MONTH(2)");

    // max precision
    checkExp(
        "interval '2147483647' month(10)",
        "INTERVAL '2147483647' MONTH(10)");

    // min precision
    checkExp(
        "interval '0' month(1)",
        "INTERVAL '0' MONTH(1)");

    // alternate precision
    checkExp(
        "interval '1234' month(4)",
        "INTERVAL '1234' MONTH(4)");

    // sign
    checkExp(
        "interval '+1' month",
        "INTERVAL '+1' MONTH");
    checkExp(
        "interval '-1' month",
        "INTERVAL '-1' MONTH");
    checkExp(
        "interval +'1' month",
        "INTERVAL '1' MONTH");
    checkExp(
        "interval +'+1' month",
        "INTERVAL '+1' MONTH");
    checkExp(
        "interval +'-1' month",
        "INTERVAL '-1' MONTH");
    checkExp(
        "interval -'1' month",
        "INTERVAL -'1' MONTH");
    checkExp(
        "interval -'+1' month",
        "INTERVAL -'+1' MONTH");
    checkExp(
        "interval -'-1' month",
        "INTERVAL -'-1' MONTH");
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
    checkExp(
        "interval '1' day",
        "INTERVAL '1' DAY");
    checkExp(
        "interval '99' day",
        "INTERVAL '99' DAY");

    // explicit precision equal to default
    checkExp(
        "interval '1' day(2)",
        "INTERVAL '1' DAY(2)");
    checkExp(
        "interval '99' day(2)",
        "INTERVAL '99' DAY(2)");

    // max precision
    checkExp(
        "interval '2147483647' day(10)",
        "INTERVAL '2147483647' DAY(10)");

    // min precision
    checkExp(
        "interval '0' day(1)",
        "INTERVAL '0' DAY(1)");

    // alternate precision
    checkExp(
        "interval '1234' day(4)",
        "INTERVAL '1234' DAY(4)");

    // sign
    checkExp(
        "interval '+1' day",
        "INTERVAL '+1' DAY");
    checkExp(
        "interval '-1' day",
        "INTERVAL '-1' DAY");
    checkExp(
        "interval +'1' day",
        "INTERVAL '1' DAY");
    checkExp(
        "interval +'+1' day",
        "INTERVAL '+1' DAY");
    checkExp(
        "interval +'-1' day",
        "INTERVAL '-1' DAY");
    checkExp(
        "interval -'1' day",
        "INTERVAL -'1' DAY");
    checkExp(
        "interval -'+1' day",
        "INTERVAL -'+1' DAY");
    checkExp(
        "interval -'-1' day",
        "INTERVAL -'-1' DAY");
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
    checkExp(
        "interval '1 2' day to hour",
        "INTERVAL '1 2' DAY TO HOUR");
    checkExp(
        "interval '99 23' day to hour",
        "INTERVAL '99 23' DAY TO HOUR");
    checkExp(
        "interval '99 0' day to hour",
        "INTERVAL '99 0' DAY TO HOUR");

    // explicit precision equal to default
    checkExp(
        "interval '1 2' day(2) to hour",
        "INTERVAL '1 2' DAY(2) TO HOUR");
    checkExp(
        "interval '99 23' day(2) to hour",
        "INTERVAL '99 23' DAY(2) TO HOUR");
    checkExp(
        "interval '99 0' day(2) to hour",
        "INTERVAL '99 0' DAY(2) TO HOUR");

    // max precision
    checkExp(
        "interval '2147483647 23' day(10) to hour",
        "INTERVAL '2147483647 23' DAY(10) TO HOUR");

    // min precision
    checkExp(
        "interval '0 0' day(1) to hour",
        "INTERVAL '0 0' DAY(1) TO HOUR");

    // alternate precision
    checkExp(
        "interval '2345 2' day(4) to hour",
        "INTERVAL '2345 2' DAY(4) TO HOUR");

    // sign
    checkExp(
        "interval '-1 2' day to hour",
        "INTERVAL '-1 2' DAY TO HOUR");
    checkExp(
        "interval '+1 2' day to hour",
        "INTERVAL '+1 2' DAY TO HOUR");
    checkExp(
        "interval +'1 2' day to hour",
        "INTERVAL '1 2' DAY TO HOUR");
    checkExp(
        "interval +'-1 2' day to hour",
        "INTERVAL '-1 2' DAY TO HOUR");
    checkExp(
        "interval +'+1 2' day to hour",
        "INTERVAL '+1 2' DAY TO HOUR");
    checkExp(
        "interval -'1 2' day to hour",
        "INTERVAL -'1 2' DAY TO HOUR");
    checkExp(
        "interval -'-1 2' day to hour",
        "INTERVAL -'-1 2' DAY TO HOUR");
    checkExp(
        "interval -'+1 2' day to hour",
        "INTERVAL -'+1 2' DAY TO HOUR");
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
    checkExp(
        "interval '1 2:3' day to minute",
        "INTERVAL '1 2:3' DAY TO MINUTE");
    checkExp(
        "interval '99 23:59' day to minute",
        "INTERVAL '99 23:59' DAY TO MINUTE");
    checkExp(
        "interval '99 0:0' day to minute",
        "INTERVAL '99 0:0' DAY TO MINUTE");

    // explicit precision equal to default
    checkExp(
        "interval '1 2:3' day(2) to minute",
        "INTERVAL '1 2:3' DAY(2) TO MINUTE");
    checkExp(
        "interval '99 23:59' day(2) to minute",
        "INTERVAL '99 23:59' DAY(2) TO MINUTE");
    checkExp(
        "interval '99 0:0' day(2) to minute",
        "INTERVAL '99 0:0' DAY(2) TO MINUTE");

    // max precision
    checkExp(
        "interval '2147483647 23:59' day(10) to minute",
        "INTERVAL '2147483647 23:59' DAY(10) TO MINUTE");

    // min precision
    checkExp(
        "interval '0 0:0' day(1) to minute",
        "INTERVAL '0 0:0' DAY(1) TO MINUTE");

    // alternate precision
    checkExp(
        "interval '2345 6:7' day(4) to minute",
        "INTERVAL '2345 6:7' DAY(4) TO MINUTE");

    // sign
    checkExp(
        "interval '-1 2:3' day to minute",
        "INTERVAL '-1 2:3' DAY TO MINUTE");
    checkExp(
        "interval '+1 2:3' day to minute",
        "INTERVAL '+1 2:3' DAY TO MINUTE");
    checkExp(
        "interval +'1 2:3' day to minute",
        "INTERVAL '1 2:3' DAY TO MINUTE");
    checkExp(
        "interval +'-1 2:3' day to minute",
        "INTERVAL '-1 2:3' DAY TO MINUTE");
    checkExp(
        "interval +'+1 2:3' day to minute",
        "INTERVAL '+1 2:3' DAY TO MINUTE");
    checkExp(
        "interval -'1 2:3' day to minute",
        "INTERVAL -'1 2:3' DAY TO MINUTE");
    checkExp(
        "interval -'-1 2:3' day to minute",
        "INTERVAL -'-1 2:3' DAY TO MINUTE");
    checkExp(
        "interval -'+1 2:3' day to minute",
        "INTERVAL -'+1 2:3' DAY TO MINUTE");
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
    checkExp(
        "interval '1 2:3:4' day to second",
        "INTERVAL '1 2:3:4' DAY TO SECOND");
    checkExp(
        "interval '99 23:59:59' day to second",
        "INTERVAL '99 23:59:59' DAY TO SECOND");
    checkExp(
        "interval '99 0:0:0' day to second",
        "INTERVAL '99 0:0:0' DAY TO SECOND");
    checkExp(
        "interval '99 23:59:59.999999' day to second",
        "INTERVAL '99 23:59:59.999999' DAY TO SECOND");
    checkExp(
        "interval '99 0:0:0.0' day to second",
        "INTERVAL '99 0:0:0.0' DAY TO SECOND");

    // explicit precision equal to default
    checkExp(
        "interval '1 2:3:4' day(2) to second",
        "INTERVAL '1 2:3:4' DAY(2) TO SECOND");
    checkExp(
        "interval '99 23:59:59' day(2) to second",
        "INTERVAL '99 23:59:59' DAY(2) TO SECOND");
    checkExp(
        "interval '99 0:0:0' day(2) to second",
        "INTERVAL '99 0:0:0' DAY(2) TO SECOND");
    checkExp(
        "interval '99 23:59:59.999999' day to second(6)",
        "INTERVAL '99 23:59:59.999999' DAY TO SECOND(6)");
    checkExp(
        "interval '99 0:0:0.0' day to second(6)",
        "INTERVAL '99 0:0:0.0' DAY TO SECOND(6)");

    // max precision
    checkExp(
        "interval '2147483647 23:59:59' day(10) to second",
        "INTERVAL '2147483647 23:59:59' DAY(10) TO SECOND");
    checkExp(
        "interval '2147483647 23:59:59.999999999' day(10) to second(9)",
        "INTERVAL '2147483647 23:59:59.999999999' DAY(10) TO SECOND(9)");

    // min precision
    checkExp(
        "interval '0 0:0:0' day(1) to second",
        "INTERVAL '0 0:0:0' DAY(1) TO SECOND");
    checkExp(
        "interval '0 0:0:0.0' day(1) to second(1)",
        "INTERVAL '0 0:0:0.0' DAY(1) TO SECOND(1)");

    // alternate precision
    checkExp(
        "interval '2345 6:7:8' day(4) to second",
        "INTERVAL '2345 6:7:8' DAY(4) TO SECOND");
    checkExp(
        "interval '2345 6:7:8.9012' day(4) to second(4)",
        "INTERVAL '2345 6:7:8.9012' DAY(4) TO SECOND(4)");

    // sign
    checkExp(
        "interval '-1 2:3:4' day to second",
        "INTERVAL '-1 2:3:4' DAY TO SECOND");
    checkExp(
        "interval '+1 2:3:4' day to second",
        "INTERVAL '+1 2:3:4' DAY TO SECOND");
    checkExp(
        "interval +'1 2:3:4' day to second",
        "INTERVAL '1 2:3:4' DAY TO SECOND");
    checkExp(
        "interval +'-1 2:3:4' day to second",
        "INTERVAL '-1 2:3:4' DAY TO SECOND");
    checkExp(
        "interval +'+1 2:3:4' day to second",
        "INTERVAL '+1 2:3:4' DAY TO SECOND");
    checkExp(
        "interval -'1 2:3:4' day to second",
        "INTERVAL -'1 2:3:4' DAY TO SECOND");
    checkExp(
        "interval -'-1 2:3:4' day to second",
        "INTERVAL -'-1 2:3:4' DAY TO SECOND");
    checkExp(
        "interval -'+1 2:3:4' day to second",
        "INTERVAL -'+1 2:3:4' DAY TO SECOND");
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
    checkExp(
        "interval '1' hour",
        "INTERVAL '1' HOUR");
    checkExp(
        "interval '99' hour",
        "INTERVAL '99' HOUR");

    // explicit precision equal to default
    checkExp(
        "interval '1' hour(2)",
        "INTERVAL '1' HOUR(2)");
    checkExp(
        "interval '99' hour(2)",
        "INTERVAL '99' HOUR(2)");

    // max precision
    checkExp(
        "interval '2147483647' hour(10)",
        "INTERVAL '2147483647' HOUR(10)");

    // min precision
    checkExp(
        "interval '0' hour(1)",
        "INTERVAL '0' HOUR(1)");

    // alternate precision
    checkExp(
        "interval '1234' hour(4)",
        "INTERVAL '1234' HOUR(4)");

    // sign
    checkExp(
        "interval '+1' hour",
        "INTERVAL '+1' HOUR");
    checkExp(
        "interval '-1' hour",
        "INTERVAL '-1' HOUR");
    checkExp(
        "interval +'1' hour",
        "INTERVAL '1' HOUR");
    checkExp(
        "interval +'+1' hour",
        "INTERVAL '+1' HOUR");
    checkExp(
        "interval +'-1' hour",
        "INTERVAL '-1' HOUR");
    checkExp(
        "interval -'1' hour",
        "INTERVAL -'1' HOUR");
    checkExp(
        "interval -'+1' hour",
        "INTERVAL -'+1' HOUR");
    checkExp(
        "interval -'-1' hour",
        "INTERVAL -'-1' HOUR");
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
    checkExp(
        "interval '2:3' hour to minute",
        "INTERVAL '2:3' HOUR TO MINUTE");
    checkExp(
        "interval '23:59' hour to minute",
        "INTERVAL '23:59' HOUR TO MINUTE");
    checkExp(
        "interval '99:0' hour to minute",
        "INTERVAL '99:0' HOUR TO MINUTE");

    // explicit precision equal to default
    checkExp(
        "interval '2:3' hour(2) to minute",
        "INTERVAL '2:3' HOUR(2) TO MINUTE");
    checkExp(
        "interval '23:59' hour(2) to minute",
        "INTERVAL '23:59' HOUR(2) TO MINUTE");
    checkExp(
        "interval '99:0' hour(2) to minute",
        "INTERVAL '99:0' HOUR(2) TO MINUTE");

    // max precision
    checkExp(
        "interval '2147483647:59' hour(10) to minute",
        "INTERVAL '2147483647:59' HOUR(10) TO MINUTE");

    // min precision
    checkExp(
        "interval '0:0' hour(1) to minute",
        "INTERVAL '0:0' HOUR(1) TO MINUTE");

    // alternate precision
    checkExp(
        "interval '2345:7' hour(4) to minute",
        "INTERVAL '2345:7' HOUR(4) TO MINUTE");

    // sign
    checkExp(
        "interval '-1:3' hour to minute",
        "INTERVAL '-1:3' HOUR TO MINUTE");
    checkExp(
        "interval '+1:3' hour to minute",
        "INTERVAL '+1:3' HOUR TO MINUTE");
    checkExp(
        "interval +'2:3' hour to minute",
        "INTERVAL '2:3' HOUR TO MINUTE");
    checkExp(
        "interval +'-2:3' hour to minute",
        "INTERVAL '-2:3' HOUR TO MINUTE");
    checkExp(
        "interval +'+2:3' hour to minute",
        "INTERVAL '+2:3' HOUR TO MINUTE");
    checkExp(
        "interval -'2:3' hour to minute",
        "INTERVAL -'2:3' HOUR TO MINUTE");
    checkExp(
        "interval -'-2:3' hour to minute",
        "INTERVAL -'-2:3' HOUR TO MINUTE");
    checkExp(
        "interval -'+2:3' hour to minute",
        "INTERVAL -'+2:3' HOUR TO MINUTE");
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
    checkExp(
        "interval '2:3:4' hour to second",
        "INTERVAL '2:3:4' HOUR TO SECOND");
    checkExp(
        "interval '23:59:59' hour to second",
        "INTERVAL '23:59:59' HOUR TO SECOND");
    checkExp(
        "interval '99:0:0' hour to second",
        "INTERVAL '99:0:0' HOUR TO SECOND");
    checkExp(
        "interval '23:59:59.999999' hour to second",
        "INTERVAL '23:59:59.999999' HOUR TO SECOND");
    checkExp(
        "interval '99:0:0.0' hour to second",
        "INTERVAL '99:0:0.0' HOUR TO SECOND");

    // explicit precision equal to default
    checkExp(
        "interval '2:3:4' hour(2) to second",
        "INTERVAL '2:3:4' HOUR(2) TO SECOND");
    checkExp(
        "interval '99:59:59' hour(2) to second",
        "INTERVAL '99:59:59' HOUR(2) TO SECOND");
    checkExp(
        "interval '99:0:0' hour(2) to second",
        "INTERVAL '99:0:0' HOUR(2) TO SECOND");
    checkExp(
        "interval '23:59:59.999999' hour to second(6)",
        "INTERVAL '23:59:59.999999' HOUR TO SECOND(6)");
    checkExp(
        "interval '99:0:0.0' hour to second(6)",
        "INTERVAL '99:0:0.0' HOUR TO SECOND(6)");

    // max precision
    checkExp(
        "interval '2147483647:59:59' hour(10) to second",
        "INTERVAL '2147483647:59:59' HOUR(10) TO SECOND");
    checkExp(
        "interval '2147483647:59:59.999999999' hour(10) to second(9)",
        "INTERVAL '2147483647:59:59.999999999' HOUR(10) TO SECOND(9)");

    // min precision
    checkExp(
        "interval '0:0:0' hour(1) to second",
        "INTERVAL '0:0:0' HOUR(1) TO SECOND");
    checkExp(
        "interval '0:0:0.0' hour(1) to second(1)",
        "INTERVAL '0:0:0.0' HOUR(1) TO SECOND(1)");

    // alternate precision
    checkExp(
        "interval '2345:7:8' hour(4) to second",
        "INTERVAL '2345:7:8' HOUR(4) TO SECOND");
    checkExp(
        "interval '2345:7:8.9012' hour(4) to second(4)",
        "INTERVAL '2345:7:8.9012' HOUR(4) TO SECOND(4)");

    // sign
    checkExp(
        "interval '-2:3:4' hour to second",
        "INTERVAL '-2:3:4' HOUR TO SECOND");
    checkExp(
        "interval '+2:3:4' hour to second",
        "INTERVAL '+2:3:4' HOUR TO SECOND");
    checkExp(
        "interval +'2:3:4' hour to second",
        "INTERVAL '2:3:4' HOUR TO SECOND");
    checkExp(
        "interval +'-2:3:4' hour to second",
        "INTERVAL '-2:3:4' HOUR TO SECOND");
    checkExp(
        "interval +'+2:3:4' hour to second",
        "INTERVAL '+2:3:4' HOUR TO SECOND");
    checkExp(
        "interval -'2:3:4' hour to second",
        "INTERVAL -'2:3:4' HOUR TO SECOND");
    checkExp(
        "interval -'-2:3:4' hour to second",
        "INTERVAL -'-2:3:4' HOUR TO SECOND");
    checkExp(
        "interval -'+2:3:4' hour to second",
        "INTERVAL -'+2:3:4' HOUR TO SECOND");
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
    checkExp(
        "interval '1' minute",
        "INTERVAL '1' MINUTE");
    checkExp(
        "interval '99' minute",
        "INTERVAL '99' MINUTE");

    // explicit precision equal to default
    checkExp(
        "interval '1' minute(2)",
        "INTERVAL '1' MINUTE(2)");
    checkExp(
        "interval '99' minute(2)",
        "INTERVAL '99' MINUTE(2)");

    // max precision
    checkExp(
        "interval '2147483647' minute(10)",
        "INTERVAL '2147483647' MINUTE(10)");

    // min precision
    checkExp(
        "interval '0' minute(1)",
        "INTERVAL '0' MINUTE(1)");

    // alternate precision
    checkExp(
        "interval '1234' minute(4)",
        "INTERVAL '1234' MINUTE(4)");

    // sign
    checkExp(
        "interval '+1' minute",
        "INTERVAL '+1' MINUTE");
    checkExp(
        "interval '-1' minute",
        "INTERVAL '-1' MINUTE");
    checkExp(
        "interval +'1' minute",
        "INTERVAL '1' MINUTE");
    checkExp(
        "interval +'+1' minute",
        "INTERVAL '+1' MINUTE");
    checkExp(
        "interval +'+1' minute",
        "INTERVAL '+1' MINUTE");
    checkExp(
        "interval -'1' minute",
        "INTERVAL -'1' MINUTE");
    checkExp(
        "interval -'+1' minute",
        "INTERVAL -'+1' MINUTE");
    checkExp(
        "interval -'-1' minute",
        "INTERVAL -'-1' MINUTE");
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
    checkExp(
        "interval '2:4' minute to second",
        "INTERVAL '2:4' MINUTE TO SECOND");
    checkExp(
        "interval '59:59' minute to second",
        "INTERVAL '59:59' MINUTE TO SECOND");
    checkExp(
        "interval '99:0' minute to second",
        "INTERVAL '99:0' MINUTE TO SECOND");
    checkExp(
        "interval '59:59.999999' minute to second",
        "INTERVAL '59:59.999999' MINUTE TO SECOND");
    checkExp(
        "interval '99:0.0' minute to second",
        "INTERVAL '99:0.0' MINUTE TO SECOND");

    // explicit precision equal to default
    checkExp(
        "interval '2:4' minute(2) to second",
        "INTERVAL '2:4' MINUTE(2) TO SECOND");
    checkExp(
        "interval '59:59' minute(2) to second",
        "INTERVAL '59:59' MINUTE(2) TO SECOND");
    checkExp(
        "interval '99:0' minute(2) to second",
        "INTERVAL '99:0' MINUTE(2) TO SECOND");
    checkExp(
        "interval '99:59.999999' minute to second(6)",
        "INTERVAL '99:59.999999' MINUTE TO SECOND(6)");
    checkExp(
        "interval '99:0.0' minute to second(6)",
        "INTERVAL '99:0.0' MINUTE TO SECOND(6)");

    // max precision
    checkExp(
        "interval '2147483647:59' minute(10) to second",
        "INTERVAL '2147483647:59' MINUTE(10) TO SECOND");
    checkExp(
        "interval '2147483647:59.999999999' minute(10) to second(9)",
        "INTERVAL '2147483647:59.999999999' MINUTE(10) TO SECOND(9)");

    // min precision
    checkExp(
        "interval '0:0' minute(1) to second",
        "INTERVAL '0:0' MINUTE(1) TO SECOND");
    checkExp(
        "interval '0:0.0' minute(1) to second(1)",
        "INTERVAL '0:0.0' MINUTE(1) TO SECOND(1)");

    // alternate precision
    checkExp(
        "interval '2345:8' minute(4) to second",
        "INTERVAL '2345:8' MINUTE(4) TO SECOND");
    checkExp(
        "interval '2345:7.8901' minute(4) to second(4)",
        "INTERVAL '2345:7.8901' MINUTE(4) TO SECOND(4)");

    // sign
    checkExp(
        "interval '-3:4' minute to second",
        "INTERVAL '-3:4' MINUTE TO SECOND");
    checkExp(
        "interval '+3:4' minute to second",
        "INTERVAL '+3:4' MINUTE TO SECOND");
    checkExp(
        "interval +'3:4' minute to second",
        "INTERVAL '3:4' MINUTE TO SECOND");
    checkExp(
        "interval +'-3:4' minute to second",
        "INTERVAL '-3:4' MINUTE TO SECOND");
    checkExp(
        "interval +'+3:4' minute to second",
        "INTERVAL '+3:4' MINUTE TO SECOND");
    checkExp(
        "interval -'3:4' minute to second",
        "INTERVAL -'3:4' MINUTE TO SECOND");
    checkExp(
        "interval -'-3:4' minute to second",
        "INTERVAL -'-3:4' MINUTE TO SECOND");
    checkExp(
        "interval -'+3:4' minute to second",
        "INTERVAL -'+3:4' MINUTE TO SECOND");
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
    checkExp(
        "interval '1' second",
        "INTERVAL '1' SECOND");
    checkExp(
        "interval '99' second",
        "INTERVAL '99' SECOND");

    // explicit precision equal to default
    checkExp(
        "interval '1' second(2)",
        "INTERVAL '1' SECOND(2)");
    checkExp(
        "interval '99' second(2)",
        "INTERVAL '99' SECOND(2)");
    checkExp(
        "interval '1' second(2,6)",
        "INTERVAL '1' SECOND(2, 6)");
    checkExp(
        "interval '99' second(2,6)",
        "INTERVAL '99' SECOND(2, 6)");

    // max precision
    checkExp(
        "interval '2147483647' second(10)",
        "INTERVAL '2147483647' SECOND(10)");
    checkExp(
        "interval '2147483647.999999999' second(9,9)",
        "INTERVAL '2147483647.999999999' SECOND(9, 9)");

    // min precision
    checkExp(
        "interval '0' second(1)",
        "INTERVAL '0' SECOND(1)");
    checkExp(
        "interval '0.0' second(1,1)",
        "INTERVAL '0.0' SECOND(1, 1)");

    // alternate precision
    checkExp(
        "interval '1234' second(4)",
        "INTERVAL '1234' SECOND(4)");
    checkExp(
        "interval '1234.56789' second(4,5)",
        "INTERVAL '1234.56789' SECOND(4, 5)");

    // sign
    checkExp(
        "interval '+1' second",
        "INTERVAL '+1' SECOND");
    checkExp(
        "interval '-1' second",
        "INTERVAL '-1' SECOND");
    checkExp(
        "interval +'1' second",
        "INTERVAL '1' SECOND");
    checkExp(
        "interval +'+1' second",
        "INTERVAL '+1' SECOND");
    checkExp(
        "interval +'-1' second",
        "INTERVAL '-1' SECOND");
    checkExp(
        "interval -'1' second",
        "INTERVAL -'1' SECOND");
    checkExp(
        "interval -'+1' second",
        "INTERVAL -'+1' SECOND");
    checkExp(
        "interval -'-1' second",
        "INTERVAL -'-1' SECOND");
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
    checkExp(
        "INTERVAL '-' YEAR",
        "INTERVAL '-' YEAR");
    checkExp(
        "INTERVAL '1-2' YEAR",
        "INTERVAL '1-2' YEAR");
    checkExp(
        "INTERVAL '1.2' YEAR",
        "INTERVAL '1.2' YEAR");
    checkExp(
        "INTERVAL '1 2' YEAR",
        "INTERVAL '1 2' YEAR");
    checkExp(
        "INTERVAL '1-2' YEAR(2)",
        "INTERVAL '1-2' YEAR(2)");
    checkExp(
        "INTERVAL 'bogus text' YEAR",
        "INTERVAL 'bogus text' YEAR");

    // negative field values
    checkExp(
        "INTERVAL '--1' YEAR",
        "INTERVAL '--1' YEAR");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    checkExp(
        "INTERVAL '100' YEAR",
        "INTERVAL '100' YEAR");
    checkExp(
        "INTERVAL '100' YEAR(2)",
        "INTERVAL '100' YEAR(2)");
    checkExp(
        "INTERVAL '1000' YEAR(3)",
        "INTERVAL '1000' YEAR(3)");
    checkExp(
        "INTERVAL '-1000' YEAR(3)",
        "INTERVAL '-1000' YEAR(3)");
    checkExp(
        "INTERVAL '2147483648' YEAR(10)",
        "INTERVAL '2147483648' YEAR(10)");
    checkExp(
        "INTERVAL '-2147483648' YEAR(10)",
        "INTERVAL '-2147483648' YEAR(10)");

    // precision > maximum
    checkExp(
        "INTERVAL '1' YEAR(11)",
        "INTERVAL '1' YEAR(11)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0' YEAR(0)",
        "INTERVAL '0' YEAR(0)");
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
    checkExp(
        "INTERVAL '-' YEAR TO MONTH",
        "INTERVAL '-' YEAR TO MONTH");
    checkExp(
        "INTERVAL '1' YEAR TO MONTH",
        "INTERVAL '1' YEAR TO MONTH");
    checkExp(
        "INTERVAL '1:2' YEAR TO MONTH",
        "INTERVAL '1:2' YEAR TO MONTH");
    checkExp(
        "INTERVAL '1.2' YEAR TO MONTH",
        "INTERVAL '1.2' YEAR TO MONTH");
    checkExp(
        "INTERVAL '1 2' YEAR TO MONTH",
        "INTERVAL '1 2' YEAR TO MONTH");
    checkExp(
        "INTERVAL '1:2' YEAR(2) TO MONTH",
        "INTERVAL '1:2' YEAR(2) TO MONTH");
    checkExp(
        "INTERVAL 'bogus text' YEAR TO MONTH",
        "INTERVAL 'bogus text' YEAR TO MONTH");

    // negative field values
    checkExp(
        "INTERVAL '--1-2' YEAR TO MONTH",
        "INTERVAL '--1-2' YEAR TO MONTH");
    checkExp(
        "INTERVAL '1--2' YEAR TO MONTH",
        "INTERVAL '1--2' YEAR TO MONTH");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    checkExp(
        "INTERVAL '100-0' YEAR TO MONTH",
        "INTERVAL '100-0' YEAR TO MONTH");
    checkExp(
        "INTERVAL '100-0' YEAR(2) TO MONTH",
        "INTERVAL '100-0' YEAR(2) TO MONTH");
    checkExp(
        "INTERVAL '1000-0' YEAR(3) TO MONTH",
        "INTERVAL '1000-0' YEAR(3) TO MONTH");
    checkExp(
        "INTERVAL '-1000-0' YEAR(3) TO MONTH",
        "INTERVAL '-1000-0' YEAR(3) TO MONTH");
    checkExp(
        "INTERVAL '2147483648-0' YEAR(10) TO MONTH",
        "INTERVAL '2147483648-0' YEAR(10) TO MONTH");
    checkExp(
        "INTERVAL '-2147483648-0' YEAR(10) TO MONTH",
        "INTERVAL '-2147483648-0' YEAR(10) TO MONTH");
    checkExp(
        "INTERVAL '1-12' YEAR TO MONTH",
        "INTERVAL '1-12' YEAR TO MONTH");

    // precision > maximum
    checkExp(
        "INTERVAL '1-1' YEAR(11) TO MONTH",
        "INTERVAL '1-1' YEAR(11) TO MONTH");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0-0' YEAR(0) TO MONTH",
        "INTERVAL '0-0' YEAR(0) TO MONTH");
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
    checkExp(
        "INTERVAL '-' MONTH",
        "INTERVAL '-' MONTH");
    checkExp(
        "INTERVAL '1-2' MONTH",
        "INTERVAL '1-2' MONTH");
    checkExp(
        "INTERVAL '1.2' MONTH",
        "INTERVAL '1.2' MONTH");
    checkExp(
        "INTERVAL '1 2' MONTH",
        "INTERVAL '1 2' MONTH");
    checkExp(
        "INTERVAL '1-2' MONTH(2)",
        "INTERVAL '1-2' MONTH(2)");
    checkExp(
        "INTERVAL 'bogus text' MONTH",
        "INTERVAL 'bogus text' MONTH");

    // negative field values
    checkExp(
        "INTERVAL '--1' MONTH",
        "INTERVAL '--1' MONTH");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    checkExp(
        "INTERVAL '100' MONTH",
        "INTERVAL '100' MONTH");
    checkExp(
        "INTERVAL '100' MONTH(2)",
        "INTERVAL '100' MONTH(2)");
    checkExp(
        "INTERVAL '1000' MONTH(3)",
        "INTERVAL '1000' MONTH(3)");
    checkExp(
        "INTERVAL '-1000' MONTH(3)",
        "INTERVAL '-1000' MONTH(3)");
    checkExp(
        "INTERVAL '2147483648' MONTH(10)",
        "INTERVAL '2147483648' MONTH(10)");
    checkExp(
        "INTERVAL '-2147483648' MONTH(10)",
        "INTERVAL '-2147483648' MONTH(10)");

    // precision > maximum
    checkExp(
        "INTERVAL '1' MONTH(11)",
        "INTERVAL '1' MONTH(11)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0' MONTH(0)",
        "INTERVAL '0' MONTH(0)");
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
    checkExp(
        "INTERVAL '-' DAY",
        "INTERVAL '-' DAY");
    checkExp(
        "INTERVAL '1-2' DAY",
        "INTERVAL '1-2' DAY");
    checkExp(
        "INTERVAL '1.2' DAY",
        "INTERVAL '1.2' DAY");
    checkExp(
        "INTERVAL '1 2' DAY",
        "INTERVAL '1 2' DAY");
    checkExp(
        "INTERVAL '1:2' DAY",
        "INTERVAL '1:2' DAY");
    checkExp(
        "INTERVAL '1-2' DAY(2)",
        "INTERVAL '1-2' DAY(2)");
    checkExp(
        "INTERVAL 'bogus text' DAY",
        "INTERVAL 'bogus text' DAY");

    // negative field values
    checkExp(
        "INTERVAL '--1' DAY",
        "INTERVAL '--1' DAY");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    checkExp(
        "INTERVAL '100' DAY",
        "INTERVAL '100' DAY");
    checkExp(
        "INTERVAL '100' DAY(2)",
        "INTERVAL '100' DAY(2)");
    checkExp(
        "INTERVAL '1000' DAY(3)",
        "INTERVAL '1000' DAY(3)");
    checkExp(
        "INTERVAL '-1000' DAY(3)",
        "INTERVAL '-1000' DAY(3)");
    checkExp(
        "INTERVAL '2147483648' DAY(10)",
        "INTERVAL '2147483648' DAY(10)");
    checkExp(
        "INTERVAL '-2147483648' DAY(10)",
        "INTERVAL '-2147483648' DAY(10)");

    // precision > maximum
    checkExp(
        "INTERVAL '1' DAY(11)",
        "INTERVAL '1' DAY(11)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0' DAY(0)",
        "INTERVAL '0' DAY(0)");
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
    checkExp(
        "INTERVAL '-' DAY TO HOUR",
        "INTERVAL '-' DAY TO HOUR");
    checkExp(
        "INTERVAL '1' DAY TO HOUR",
        "INTERVAL '1' DAY TO HOUR");
    checkExp(
        "INTERVAL '1:2' DAY TO HOUR",
        "INTERVAL '1:2' DAY TO HOUR");
    checkExp(
        "INTERVAL '1.2' DAY TO HOUR",
        "INTERVAL '1.2' DAY TO HOUR");
    checkExp(
        "INTERVAL '1 x' DAY TO HOUR",
        "INTERVAL '1 x' DAY TO HOUR");
    checkExp(
        "INTERVAL ' ' DAY TO HOUR",
        "INTERVAL ' ' DAY TO HOUR");
    checkExp(
        "INTERVAL '1:2' DAY(2) TO HOUR",
        "INTERVAL '1:2' DAY(2) TO HOUR");
    checkExp(
        "INTERVAL 'bogus text' DAY TO HOUR",
        "INTERVAL 'bogus text' DAY TO HOUR");

    // negative field values
    checkExp(
        "INTERVAL '--1 1' DAY TO HOUR",
        "INTERVAL '--1 1' DAY TO HOUR");
    checkExp(
        "INTERVAL '1 -1' DAY TO HOUR",
        "INTERVAL '1 -1' DAY TO HOUR");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    checkExp(
        "INTERVAL '100 0' DAY TO HOUR",
        "INTERVAL '100 0' DAY TO HOUR");
    checkExp(
        "INTERVAL '100 0' DAY(2) TO HOUR",
        "INTERVAL '100 0' DAY(2) TO HOUR");
    checkExp(
        "INTERVAL '1000 0' DAY(3) TO HOUR",
        "INTERVAL '1000 0' DAY(3) TO HOUR");
    checkExp(
        "INTERVAL '-1000 0' DAY(3) TO HOUR",
        "INTERVAL '-1000 0' DAY(3) TO HOUR");
    checkExp(
        "INTERVAL '2147483648 0' DAY(10) TO HOUR",
        "INTERVAL '2147483648 0' DAY(10) TO HOUR");
    checkExp(
        "INTERVAL '-2147483648 0' DAY(10) TO HOUR",
        "INTERVAL '-2147483648 0' DAY(10) TO HOUR");
    checkExp(
        "INTERVAL '1 24' DAY TO HOUR",
        "INTERVAL '1 24' DAY TO HOUR");

    // precision > maximum
    checkExp(
        "INTERVAL '1 1' DAY(11) TO HOUR",
        "INTERVAL '1 1' DAY(11) TO HOUR");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0 0' DAY(0) TO HOUR",
        "INTERVAL '0 0' DAY(0) TO HOUR");
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
    checkExp(
        "INTERVAL ' :' DAY TO MINUTE",
        "INTERVAL ' :' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1' DAY TO MINUTE",
        "INTERVAL '1' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1 2' DAY TO MINUTE",
        "INTERVAL '1 2' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1:2' DAY TO MINUTE",
        "INTERVAL '1:2' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1.2' DAY TO MINUTE",
        "INTERVAL '1.2' DAY TO MINUTE");
    checkExp(
        "INTERVAL 'x 1:1' DAY TO MINUTE",
        "INTERVAL 'x 1:1' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1 x:1' DAY TO MINUTE",
        "INTERVAL '1 x:1' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1 1:x' DAY TO MINUTE",
        "INTERVAL '1 1:x' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1 1:2:3' DAY TO MINUTE",
        "INTERVAL '1 1:2:3' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1 1:1:1.2' DAY TO MINUTE",
        "INTERVAL '1 1:1:1.2' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1 1:2:3' DAY(2) TO MINUTE",
        "INTERVAL '1 1:2:3' DAY(2) TO MINUTE");
    checkExp(
        "INTERVAL '1 1' DAY(2) TO MINUTE",
        "INTERVAL '1 1' DAY(2) TO MINUTE");
    checkExp(
        "INTERVAL 'bogus text' DAY TO MINUTE",
        "INTERVAL 'bogus text' DAY TO MINUTE");

    // negative field values
    checkExp(
        "INTERVAL '--1 1:1' DAY TO MINUTE",
        "INTERVAL '--1 1:1' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1 -1:1' DAY TO MINUTE",
        "INTERVAL '1 -1:1' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1 1:-1' DAY TO MINUTE",
        "INTERVAL '1 1:-1' DAY TO MINUTE");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    checkExp(
        "INTERVAL '100 0' DAY TO MINUTE",
        "INTERVAL '100 0' DAY TO MINUTE");
    checkExp(
        "INTERVAL '100 0' DAY(2) TO MINUTE",
        "INTERVAL '100 0' DAY(2) TO MINUTE");
    checkExp(
        "INTERVAL '1000 0' DAY(3) TO MINUTE",
        "INTERVAL '1000 0' DAY(3) TO MINUTE");
    checkExp(
        "INTERVAL '-1000 0' DAY(3) TO MINUTE",
        "INTERVAL '-1000 0' DAY(3) TO MINUTE");
    checkExp(
        "INTERVAL '2147483648 0' DAY(10) TO MINUTE",
        "INTERVAL '2147483648 0' DAY(10) TO MINUTE");
    checkExp(
        "INTERVAL '-2147483648 0' DAY(10) TO MINUTE",
        "INTERVAL '-2147483648 0' DAY(10) TO MINUTE");
    checkExp(
        "INTERVAL '1 24:1' DAY TO MINUTE",
        "INTERVAL '1 24:1' DAY TO MINUTE");
    checkExp(
        "INTERVAL '1 1:60' DAY TO MINUTE",
        "INTERVAL '1 1:60' DAY TO MINUTE");

    // precision > maximum
    checkExp(
        "INTERVAL '1 1' DAY(11) TO MINUTE",
        "INTERVAL '1 1' DAY(11) TO MINUTE");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0 0' DAY(0) TO MINUTE",
        "INTERVAL '0 0' DAY(0) TO MINUTE");
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
    checkExp(
        "INTERVAL ' ::' DAY TO SECOND",
        "INTERVAL ' ::' DAY TO SECOND");
    checkExp(
        "INTERVAL ' ::.' DAY TO SECOND",
        "INTERVAL ' ::.' DAY TO SECOND");
    checkExp(
        "INTERVAL '1' DAY TO SECOND",
        "INTERVAL '1' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 2' DAY TO SECOND",
        "INTERVAL '1 2' DAY TO SECOND");
    checkExp(
        "INTERVAL '1:2' DAY TO SECOND",
        "INTERVAL '1:2' DAY TO SECOND");
    checkExp(
        "INTERVAL '1.2' DAY TO SECOND",
        "INTERVAL '1.2' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 1:2' DAY TO SECOND",
        "INTERVAL '1 1:2' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 1:2:x' DAY TO SECOND",
        "INTERVAL '1 1:2:x' DAY TO SECOND");
    checkExp(
        "INTERVAL '1:2:3' DAY TO SECOND",
        "INTERVAL '1:2:3' DAY TO SECOND");
    checkExp(
        "INTERVAL '1:1:1.2' DAY TO SECOND",
        "INTERVAL '1:1:1.2' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 1:2' DAY(2) TO SECOND",
        "INTERVAL '1 1:2' DAY(2) TO SECOND");
    checkExp(
        "INTERVAL '1 1' DAY(2) TO SECOND",
        "INTERVAL '1 1' DAY(2) TO SECOND");
    checkExp(
        "INTERVAL 'bogus text' DAY TO SECOND",
        "INTERVAL 'bogus text' DAY TO SECOND");
    checkExp(
        "INTERVAL '2345 6:7:8901' DAY TO SECOND(4)",
        "INTERVAL '2345 6:7:8901' DAY TO SECOND(4)");

    // negative field values
    checkExp(
        "INTERVAL '--1 1:1:1' DAY TO SECOND",
        "INTERVAL '--1 1:1:1' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 -1:1:1' DAY TO SECOND",
        "INTERVAL '1 -1:1:1' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 1:-1:1' DAY TO SECOND",
        "INTERVAL '1 1:-1:1' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 1:1:-1' DAY TO SECOND",
        "INTERVAL '1 1:1:-1' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 1:1:1.-1' DAY TO SECOND",
        "INTERVAL '1 1:1:1.-1' DAY TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    checkExp(
        "INTERVAL '100 0' DAY TO SECOND",
        "INTERVAL '100 0' DAY TO SECOND");
    checkExp(
        "INTERVAL '100 0' DAY(2) TO SECOND",
        "INTERVAL '100 0' DAY(2) TO SECOND");
    checkExp(
        "INTERVAL '1000 0' DAY(3) TO SECOND",
        "INTERVAL '1000 0' DAY(3) TO SECOND");
    checkExp(
        "INTERVAL '-1000 0' DAY(3) TO SECOND",
        "INTERVAL '-1000 0' DAY(3) TO SECOND");
    checkExp(
        "INTERVAL '2147483648 0' DAY(10) TO SECOND",
        "INTERVAL '2147483648 0' DAY(10) TO SECOND");
    checkExp(
        "INTERVAL '-2147483648 0' DAY(10) TO SECOND",
        "INTERVAL '-2147483648 0' DAY(10) TO SECOND");
    checkExp(
        "INTERVAL '1 24:1:1' DAY TO SECOND",
        "INTERVAL '1 24:1:1' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 1:60:1' DAY TO SECOND",
        "INTERVAL '1 1:60:1' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 1:1:60' DAY TO SECOND",
        "INTERVAL '1 1:1:60' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 1:1:1.0000001' DAY TO SECOND",
        "INTERVAL '1 1:1:1.0000001' DAY TO SECOND");
    checkExp(
        "INTERVAL '1 1:1:1.0001' DAY TO SECOND(3)",
        "INTERVAL '1 1:1:1.0001' DAY TO SECOND(3)");

    // precision > maximum
    checkExp(
        "INTERVAL '1 1' DAY(11) TO SECOND",
        "INTERVAL '1 1' DAY(11) TO SECOND");
    checkExp(
        "INTERVAL '1 1' DAY TO SECOND(10)",
        "INTERVAL '1 1' DAY TO SECOND(10)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0 0:0:0' DAY(0) TO SECOND",
        "INTERVAL '0 0:0:0' DAY(0) TO SECOND");
    checkExp(
        "INTERVAL '0 0:0:0' DAY TO SECOND(0)",
        "INTERVAL '0 0:0:0' DAY TO SECOND(0)");
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
    checkExp(
        "INTERVAL '-' HOUR",
        "INTERVAL '-' HOUR");
    checkExp(
        "INTERVAL '1-2' HOUR",
        "INTERVAL '1-2' HOUR");
    checkExp(
        "INTERVAL '1.2' HOUR",
        "INTERVAL '1.2' HOUR");
    checkExp(
        "INTERVAL '1 2' HOUR",
        "INTERVAL '1 2' HOUR");
    checkExp(
        "INTERVAL '1:2' HOUR",
        "INTERVAL '1:2' HOUR");
    checkExp(
        "INTERVAL '1-2' HOUR(2)",
        "INTERVAL '1-2' HOUR(2)");
    checkExp(
        "INTERVAL 'bogus text' HOUR",
        "INTERVAL 'bogus text' HOUR");

    // negative field values
    checkExp(
        "INTERVAL '--1' HOUR",
        "INTERVAL '--1' HOUR");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    checkExp(
        "INTERVAL '100' HOUR",
        "INTERVAL '100' HOUR");
    checkExp(
        "INTERVAL '100' HOUR(2)",
        "INTERVAL '100' HOUR(2)");
    checkExp(
        "INTERVAL '1000' HOUR(3)",
        "INTERVAL '1000' HOUR(3)");
    checkExp(
        "INTERVAL '-1000' HOUR(3)",
        "INTERVAL '-1000' HOUR(3)");
    checkExp(
        "INTERVAL '2147483648' HOUR(10)",
        "INTERVAL '2147483648' HOUR(10)");
    checkExp(
        "INTERVAL '-2147483648' HOUR(10)",
        "INTERVAL '-2147483648' HOUR(10)");

    // negative field values
    checkExp(
        "INTERVAL '--1' HOUR",
        "INTERVAL '--1' HOUR");

    // precision > maximum
    checkExp(
        "INTERVAL '1' HOUR(11)",
        "INTERVAL '1' HOUR(11)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0' HOUR(0)",
        "INTERVAL '0' HOUR(0)");
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
    checkExp(
        "INTERVAL ':' HOUR TO MINUTE",
        "INTERVAL ':' HOUR TO MINUTE");
    checkExp(
        "INTERVAL '1' HOUR TO MINUTE",
        "INTERVAL '1' HOUR TO MINUTE");
    checkExp(
        "INTERVAL '1:x' HOUR TO MINUTE",
        "INTERVAL '1:x' HOUR TO MINUTE");
    checkExp(
        "INTERVAL '1.2' HOUR TO MINUTE",
        "INTERVAL '1.2' HOUR TO MINUTE");
    checkExp(
        "INTERVAL '1 2' HOUR TO MINUTE",
        "INTERVAL '1 2' HOUR TO MINUTE");
    checkExp(
        "INTERVAL '1:2:3' HOUR TO MINUTE",
        "INTERVAL '1:2:3' HOUR TO MINUTE");
    checkExp(
        "INTERVAL '1 2' HOUR(2) TO MINUTE",
        "INTERVAL '1 2' HOUR(2) TO MINUTE");
    checkExp(
        "INTERVAL 'bogus text' HOUR TO MINUTE",
        "INTERVAL 'bogus text' HOUR TO MINUTE");

    // negative field values
    checkExp(
        "INTERVAL '--1:1' HOUR TO MINUTE",
        "INTERVAL '--1:1' HOUR TO MINUTE");
    checkExp(
        "INTERVAL '1:-1' HOUR TO MINUTE",
        "INTERVAL '1:-1' HOUR TO MINUTE");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    checkExp(
        "INTERVAL '100:0' HOUR TO MINUTE",
        "INTERVAL '100:0' HOUR TO MINUTE");
    checkExp(
        "INTERVAL '100:0' HOUR(2) TO MINUTE",
        "INTERVAL '100:0' HOUR(2) TO MINUTE");
    checkExp(
        "INTERVAL '1000:0' HOUR(3) TO MINUTE",
        "INTERVAL '1000:0' HOUR(3) TO MINUTE");
    checkExp(
        "INTERVAL '-1000:0' HOUR(3) TO MINUTE",
        "INTERVAL '-1000:0' HOUR(3) TO MINUTE");
    checkExp(
        "INTERVAL '2147483648:0' HOUR(10) TO MINUTE",
        "INTERVAL '2147483648:0' HOUR(10) TO MINUTE");
    checkExp(
        "INTERVAL '-2147483648:0' HOUR(10) TO MINUTE",
        "INTERVAL '-2147483648:0' HOUR(10) TO MINUTE");
    checkExp(
        "INTERVAL '1:24' HOUR TO MINUTE",
        "INTERVAL '1:24' HOUR TO MINUTE");

    // precision > maximum
    checkExp(
        "INTERVAL '1:1' HOUR(11) TO MINUTE",
        "INTERVAL '1:1' HOUR(11) TO MINUTE");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0:0' HOUR(0) TO MINUTE",
        "INTERVAL '0:0' HOUR(0) TO MINUTE");
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
    checkExp(
        "INTERVAL '::' HOUR TO SECOND",
        "INTERVAL '::' HOUR TO SECOND");
    checkExp(
        "INTERVAL '::.' HOUR TO SECOND",
        "INTERVAL '::.' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1' HOUR TO SECOND",
        "INTERVAL '1' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1 2' HOUR TO SECOND",
        "INTERVAL '1 2' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1:2' HOUR TO SECOND",
        "INTERVAL '1:2' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1.2' HOUR TO SECOND",
        "INTERVAL '1.2' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1 1:2' HOUR TO SECOND",
        "INTERVAL '1 1:2' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1:2:x' HOUR TO SECOND",
        "INTERVAL '1:2:x' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1:x:3' HOUR TO SECOND",
        "INTERVAL '1:x:3' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1:1:1.x' HOUR TO SECOND",
        "INTERVAL '1:1:1.x' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1 1:2' HOUR(2) TO SECOND",
        "INTERVAL '1 1:2' HOUR(2) TO SECOND");
    checkExp(
        "INTERVAL '1 1' HOUR(2) TO SECOND",
        "INTERVAL '1 1' HOUR(2) TO SECOND");
    checkExp(
        "INTERVAL 'bogus text' HOUR TO SECOND",
        "INTERVAL 'bogus text' HOUR TO SECOND");
    checkExp(
        "INTERVAL '6:7:8901' HOUR TO SECOND(4)",
        "INTERVAL '6:7:8901' HOUR TO SECOND(4)");

    // negative field values
    checkExp(
        "INTERVAL '--1:1:1' HOUR TO SECOND",
        "INTERVAL '--1:1:1' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1:-1:1' HOUR TO SECOND",
        "INTERVAL '1:-1:1' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1:1:-1' HOUR TO SECOND",
        "INTERVAL '1:1:-1' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1:1:1.-1' HOUR TO SECOND",
        "INTERVAL '1:1:1.-1' HOUR TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    checkExp(
        "INTERVAL '100:0:0' HOUR TO SECOND",
        "INTERVAL '100:0:0' HOUR TO SECOND");
    checkExp(
        "INTERVAL '100:0:0' HOUR(2) TO SECOND",
        "INTERVAL '100:0:0' HOUR(2) TO SECOND");
    checkExp(
        "INTERVAL '1000:0:0' HOUR(3) TO SECOND",
        "INTERVAL '1000:0:0' HOUR(3) TO SECOND");
    checkExp(
        "INTERVAL '-1000:0:0' HOUR(3) TO SECOND",
        "INTERVAL '-1000:0:0' HOUR(3) TO SECOND");
    checkExp(
        "INTERVAL '2147483648:0:0' HOUR(10) TO SECOND",
        "INTERVAL '2147483648:0:0' HOUR(10) TO SECOND");
    checkExp(
        "INTERVAL '-2147483648:0:0' HOUR(10) TO SECOND",
        "INTERVAL '-2147483648:0:0' HOUR(10) TO SECOND");
    checkExp(
        "INTERVAL '1:60:1' HOUR TO SECOND",
        "INTERVAL '1:60:1' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1:1:60' HOUR TO SECOND",
        "INTERVAL '1:1:60' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1:1:1.0000001' HOUR TO SECOND",
        "INTERVAL '1:1:1.0000001' HOUR TO SECOND");
    checkExp(
        "INTERVAL '1:1:1.0001' HOUR TO SECOND(3)",
        "INTERVAL '1:1:1.0001' HOUR TO SECOND(3)");

    // precision > maximum
    checkExp(
        "INTERVAL '1:1:1' HOUR(11) TO SECOND",
        "INTERVAL '1:1:1' HOUR(11) TO SECOND");
    checkExp(
        "INTERVAL '1:1:1' HOUR TO SECOND(10)",
        "INTERVAL '1:1:1' HOUR TO SECOND(10)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0:0:0' HOUR(0) TO SECOND",
        "INTERVAL '0:0:0' HOUR(0) TO SECOND");
    checkExp(
        "INTERVAL '0:0:0' HOUR TO SECOND(0)",
        "INTERVAL '0:0:0' HOUR TO SECOND(0)");
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
    checkExp(
        "INTERVAL '-' MINUTE",
        "INTERVAL '-' MINUTE");
    checkExp(
        "INTERVAL '1-2' MINUTE",
        "INTERVAL '1-2' MINUTE");
    checkExp(
        "INTERVAL '1.2' MINUTE",
        "INTERVAL '1.2' MINUTE");
    checkExp(
        "INTERVAL '1 2' MINUTE",
        "INTERVAL '1 2' MINUTE");
    checkExp(
        "INTERVAL '1:2' MINUTE",
        "INTERVAL '1:2' MINUTE");
    checkExp(
        "INTERVAL '1-2' MINUTE(2)",
        "INTERVAL '1-2' MINUTE(2)");
    checkExp(
        "INTERVAL 'bogus text' MINUTE",
        "INTERVAL 'bogus text' MINUTE");

    // negative field values
    checkExp(
        "INTERVAL '--1' MINUTE",
        "INTERVAL '--1' MINUTE");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    checkExp(
        "INTERVAL '100' MINUTE",
        "INTERVAL '100' MINUTE");
    checkExp(
        "INTERVAL '100' MINUTE(2)",
        "INTERVAL '100' MINUTE(2)");
    checkExp(
        "INTERVAL '1000' MINUTE(3)",
        "INTERVAL '1000' MINUTE(3)");
    checkExp(
        "INTERVAL '-1000' MINUTE(3)",
        "INTERVAL '-1000' MINUTE(3)");
    checkExp(
        "INTERVAL '2147483648' MINUTE(10)",
        "INTERVAL '2147483648' MINUTE(10)");
    checkExp(
        "INTERVAL '-2147483648' MINUTE(10)",
        "INTERVAL '-2147483648' MINUTE(10)");

    // precision > maximum
    checkExp(
        "INTERVAL '1' MINUTE(11)",
        "INTERVAL '1' MINUTE(11)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0' MINUTE(0)",
        "INTERVAL '0' MINUTE(0)");
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
    checkExp(
        "INTERVAL ':' MINUTE TO SECOND",
        "INTERVAL ':' MINUTE TO SECOND");
    checkExp(
        "INTERVAL ':.' MINUTE TO SECOND",
        "INTERVAL ':.' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '1' MINUTE TO SECOND",
        "INTERVAL '1' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '1 2' MINUTE TO SECOND",
        "INTERVAL '1 2' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '1.2' MINUTE TO SECOND",
        "INTERVAL '1.2' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '1 1:2' MINUTE TO SECOND",
        "INTERVAL '1 1:2' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '1:x' MINUTE TO SECOND",
        "INTERVAL '1:x' MINUTE TO SECOND");
    checkExp(
        "INTERVAL 'x:3' MINUTE TO SECOND",
        "INTERVAL 'x:3' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '1:1.x' MINUTE TO SECOND",
        "INTERVAL '1:1.x' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '1 1:2' MINUTE(2) TO SECOND",
        "INTERVAL '1 1:2' MINUTE(2) TO SECOND");
    checkExp(
        "INTERVAL '1 1' MINUTE(2) TO SECOND",
        "INTERVAL '1 1' MINUTE(2) TO SECOND");
    checkExp(
        "INTERVAL 'bogus text' MINUTE TO SECOND",
        "INTERVAL 'bogus text' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '7:8901' MINUTE TO SECOND(4)",
        "INTERVAL '7:8901' MINUTE TO SECOND(4)");

    // negative field values
    checkExp(
        "INTERVAL '--1:1' MINUTE TO SECOND",
        "INTERVAL '--1:1' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '1:-1' MINUTE TO SECOND",
        "INTERVAL '1:-1' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '1:1.-1' MINUTE TO SECOND",
        "INTERVAL '1:1.-1' MINUTE TO SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    //  plus >max value for mid/end fields
    checkExp(
        "INTERVAL '100:0' MINUTE TO SECOND",
        "INTERVAL '100:0' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '100:0' MINUTE(2) TO SECOND",
        "INTERVAL '100:0' MINUTE(2) TO SECOND");
    checkExp(
        "INTERVAL '1000:0' MINUTE(3) TO SECOND",
        "INTERVAL '1000:0' MINUTE(3) TO SECOND");
    checkExp(
        "INTERVAL '-1000:0' MINUTE(3) TO SECOND",
        "INTERVAL '-1000:0' MINUTE(3) TO SECOND");
    checkExp(
        "INTERVAL '2147483648:0' MINUTE(10) TO SECOND",
        "INTERVAL '2147483648:0' MINUTE(10) TO SECOND");
    checkExp(
        "INTERVAL '-2147483648:0' MINUTE(10) TO SECOND",
        "INTERVAL '-2147483648:0' MINUTE(10) TO SECOND");
    checkExp(
        "INTERVAL '1:60' MINUTE TO SECOND",
        "INTERVAL '1:60' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '1:1.0000001' MINUTE TO SECOND",
        "INTERVAL '1:1.0000001' MINUTE TO SECOND");
    checkExp(
        "INTERVAL '1:1:1.0001' MINUTE TO SECOND(3)",
        "INTERVAL '1:1:1.0001' MINUTE TO SECOND(3)");

    // precision > maximum
    checkExp(
        "INTERVAL '1:1' MINUTE(11) TO SECOND",
        "INTERVAL '1:1' MINUTE(11) TO SECOND");
    checkExp(
        "INTERVAL '1:1' MINUTE TO SECOND(10)",
        "INTERVAL '1:1' MINUTE TO SECOND(10)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0:0' MINUTE(0) TO SECOND",
        "INTERVAL '0:0' MINUTE(0) TO SECOND");
    checkExp(
        "INTERVAL '0:0' MINUTE TO SECOND(0)",
        "INTERVAL '0:0' MINUTE TO SECOND(0)");
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
    checkExp(
        "INTERVAL ':' SECOND",
        "INTERVAL ':' SECOND");
    checkExp(
        "INTERVAL '.' SECOND",
        "INTERVAL '.' SECOND");
    checkExp(
        "INTERVAL '1-2' SECOND",
        "INTERVAL '1-2' SECOND");
    checkExp(
        "INTERVAL '1.x' SECOND",
        "INTERVAL '1.x' SECOND");
    checkExp(
        "INTERVAL 'x.1' SECOND",
        "INTERVAL 'x.1' SECOND");
    checkExp(
        "INTERVAL '1 2' SECOND",
        "INTERVAL '1 2' SECOND");
    checkExp(
        "INTERVAL '1:2' SECOND",
        "INTERVAL '1:2' SECOND");
    checkExp(
        "INTERVAL '1-2' SECOND(2)",
        "INTERVAL '1-2' SECOND(2)");
    checkExp(
        "INTERVAL 'bogus text' SECOND",
        "INTERVAL 'bogus text' SECOND");

    // negative field values
    checkExp(
        "INTERVAL '--1' SECOND",
        "INTERVAL '--1' SECOND");
    checkExp(
        "INTERVAL '1.-1' SECOND",
        "INTERVAL '1.-1' SECOND");

    // Field value out of range
    //  (default, explicit default, alt, neg alt, max, neg max)
    checkExp(
        "INTERVAL '100' SECOND",
        "INTERVAL '100' SECOND");
    checkExp(
        "INTERVAL '100' SECOND(2)",
        "INTERVAL '100' SECOND(2)");
    checkExp(
        "INTERVAL '1000' SECOND(3)",
        "INTERVAL '1000' SECOND(3)");
    checkExp(
        "INTERVAL '-1000' SECOND(3)",
        "INTERVAL '-1000' SECOND(3)");
    checkExp(
        "INTERVAL '2147483648' SECOND(10)",
        "INTERVAL '2147483648' SECOND(10)");
    checkExp(
        "INTERVAL '-2147483648' SECOND(10)",
        "INTERVAL '-2147483648' SECOND(10)");
    checkExp(
        "INTERVAL '1.0000001' SECOND",
        "INTERVAL '1.0000001' SECOND");
    checkExp(
        "INTERVAL '1.0000001' SECOND(2)",
        "INTERVAL '1.0000001' SECOND(2)");
    checkExp(
        "INTERVAL '1.0001' SECOND(2, 3)",
        "INTERVAL '1.0001' SECOND(2, 3)");
    checkExp(
        "INTERVAL '1.000000001' SECOND(2, 9)",
        "INTERVAL '1.000000001' SECOND(2, 9)");

    // precision > maximum
    checkExp(
        "INTERVAL '1' SECOND(11)",
        "INTERVAL '1' SECOND(11)");
    checkExp(
        "INTERVAL '1.1' SECOND(1, 10)",
        "INTERVAL '1.1' SECOND(1, 10)");

    // precision < minimum allowed)
    // note: parser will catch negative values, here we
    // just need to check for 0
    checkExp(
        "INTERVAL '0' SECOND(0)",
        "INTERVAL '0' SECOND(0)");
    checkExp(
        "INTERVAL '0' SECOND(1, 0)",
        "INTERVAL '0' SECOND(1, 0)");
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
  @Test public void testIntervalLiterals() {
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

  @Test public void testUnparseableIntervalQualifiers() {
    // No qualifier
    checkExpFails(
        "interval '1^'^",
        "Encountered \"<EOF>\" at line 1, column 12\\.\n"
            + "Was expecting one of:\n"
            + "    \"DAY\" \\.\\.\\.\n"
            + "    \"HOUR\" \\.\\.\\.\n"
            + "    \"MINUTE\" \\.\\.\\.\n"
            + "    \"MONTH\" \\.\\.\\.\n"
            + "    \"SECOND\" \\.\\.\\.\n"
            + "    \"YEAR\" \\.\\.\\.\n"
            + "    ");

    // illegal qualifiers, no precision in either field
    checkExpFails(
        "interval '1' year ^to^ year",
        "(?s)Encountered \"to year\" at line 1, column 19.\n"
            + "Was expecting one of:\n"
            + "    <EOF> \n"
            + "    \"\\(\" \\.\\.\\.\n"
            + "    \"\\.\" \\.\\.\\..*");
    checkExpFails("interval '1-2' year ^to^ day", ANY);
    checkExpFails("interval '1-2' year ^to^ hour", ANY);
    checkExpFails("interval '1-2' year ^to^ minute", ANY);
    checkExpFails("interval '1-2' year ^to^ second", ANY);

    checkExpFails("interval '1-2' month ^to^ year", ANY);
    checkExpFails("interval '1-2' month ^to^ month", ANY);
    checkExpFails("interval '1-2' month ^to^ day", ANY);
    checkExpFails("interval '1-2' month ^to^ hour", ANY);
    checkExpFails("interval '1-2' month ^to^ minute", ANY);
    checkExpFails("interval '1-2' month ^to^ second", ANY);

    checkExpFails("interval '1-2' day ^to^ year", ANY);
    checkExpFails("interval '1-2' day ^to^ month", ANY);
    checkExpFails("interval '1-2' day ^to^ day", ANY);

    checkExpFails("interval '1-2' hour ^to^ year", ANY);
    checkExpFails("interval '1-2' hour ^to^ month", ANY);
    checkExpFails("interval '1-2' hour ^to^ day", ANY);
    checkExpFails("interval '1-2' hour ^to^ hour", ANY);

    checkExpFails("interval '1-2' minute ^to^ year", ANY);
    checkExpFails("interval '1-2' minute ^to^ month", ANY);
    checkExpFails("interval '1-2' minute ^to^ day", ANY);
    checkExpFails("interval '1-2' minute ^to^ hour", ANY);
    checkExpFails("interval '1-2' minute ^to^ minute", ANY);

    checkExpFails("interval '1-2' second ^to^ year", ANY);
    checkExpFails("interval '1-2' second ^to^ month", ANY);
    checkExpFails("interval '1-2' second ^to^ day", ANY);
    checkExpFails("interval '1-2' second ^to^ hour", ANY);
    checkExpFails("interval '1-2' second ^to^ minute", ANY);
    checkExpFails("interval '1-2' second ^to^ second", ANY);

    // illegal qualifiers, including precision in start field
    checkExpFails("interval '1' year(3) ^to^ year", ANY);
    checkExpFails("interval '1-2' year(3) ^to^ day", ANY);
    checkExpFails("interval '1-2' year(3) ^to^ hour", ANY);
    checkExpFails("interval '1-2' year(3) ^to^ minute", ANY);
    checkExpFails("interval '1-2' year(3) ^to^ second", ANY);

    checkExpFails("interval '1-2' month(3) ^to^ year", ANY);
    checkExpFails("interval '1-2' month(3) ^to^ month", ANY);
    checkExpFails("interval '1-2' month(3) ^to^ day", ANY);
    checkExpFails("interval '1-2' month(3) ^to^ hour", ANY);
    checkExpFails("interval '1-2' month(3) ^to^ minute", ANY);
    checkExpFails("interval '1-2' month(3) ^to^ second", ANY);

    checkExpFails("interval '1-2' day(3) ^to^ year", ANY);
    checkExpFails("interval '1-2' day(3) ^to^ month", ANY);

    checkExpFails("interval '1-2' hour(3) ^to^ year", ANY);
    checkExpFails("interval '1-2' hour(3) ^to^ month", ANY);
    checkExpFails("interval '1-2' hour(3) ^to^ day", ANY);

    checkExpFails("interval '1-2' minute(3) ^to^ year", ANY);
    checkExpFails("interval '1-2' minute(3) ^to^ month", ANY);
    checkExpFails("interval '1-2' minute(3) ^to^ day", ANY);
    checkExpFails("interval '1-2' minute(3) ^to^ hour", ANY);

    checkExpFails("interval '1-2' second(3) ^to^ year", ANY);
    checkExpFails("interval '1-2' second(3) ^to^ month", ANY);
    checkExpFails("interval '1-2' second(3) ^to^ day", ANY);
    checkExpFails("interval '1-2' second(3) ^to^ hour", ANY);
    checkExpFails("interval '1-2' second(3) ^to^ minute", ANY);

    // illegal qualfiers, including precision in end field
    checkExpFails("interval '1' year ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' year to month^(^2)", ANY);
    checkExpFails("interval '1-2' year ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' year ^to^ hour(2)", ANY);
    checkExpFails("interval '1-2' year ^to^ minute(2)", ANY);
    checkExpFails("interval '1-2' year ^to^ second(2)", ANY);
    checkExpFails("interval '1-2' year ^to^ second(2,6)", ANY);

    checkExpFails("interval '1-2' month ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' month ^to^ month(2)", ANY);
    checkExpFails("interval '1-2' month ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' month ^to^ hour(2)", ANY);
    checkExpFails("interval '1-2' month ^to^ minute(2)", ANY);
    checkExpFails("interval '1-2' month ^to^ second(2)", ANY);
    checkExpFails("interval '1-2' month ^to^ second(2,6)", ANY);

    checkExpFails("interval '1-2' day ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' day ^to^ month(2)", ANY);
    checkExpFails("interval '1-2' day ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' day to hour^(^2)", ANY);
    checkExpFails("interval '1-2' day to minute^(^2)", ANY);
    checkExpFails("interval '1-2' day to second(2^,^6)", ANY);

    checkExpFails("interval '1-2' hour ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' hour ^to^ month(2)", ANY);
    checkExpFails("interval '1-2' hour ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' hour ^to^ hour(2)", ANY);
    checkExpFails("interval '1-2' hour to minute^(^2)", ANY);
    checkExpFails("interval '1-2' hour to second(2^,^6)", ANY);

    checkExpFails("interval '1-2' minute ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' minute ^to^ month(2)", ANY);
    checkExpFails("interval '1-2' minute ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' minute ^to^ hour(2)", ANY);
    checkExpFails("interval '1-2' minute ^to^ minute(2)", ANY);
    checkExpFails("interval '1-2' minute to second(2^,^6)", ANY);

    checkExpFails("interval '1-2' second ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' second ^to^ month(2)", ANY);
    checkExpFails("interval '1-2' second ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' second ^to^ hour(2)", ANY);
    checkExpFails("interval '1-2' second ^to^ minute(2)", ANY);
    checkExpFails("interval '1-2' second ^to^ second(2)", ANY);
    checkExpFails("interval '1-2' second ^to^ second(2,6)", ANY);

    // illegal qualfiers, including precision in start and end field
    checkExpFails("interval '1' year(3) ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' year(3) to month^(^2)", ANY);
    checkExpFails("interval '1-2' year(3) ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' year(3) ^to^ hour(2)", ANY);
    checkExpFails("interval '1-2' year(3) ^to^ minute(2)", ANY);
    checkExpFails("interval '1-2' year(3) ^to^ second(2)", ANY);
    checkExpFails("interval '1-2' year(3) ^to^ second(2,6)", ANY);

    checkExpFails("interval '1-2' month(3) ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' month(3) ^to^ month(2)", ANY);
    checkExpFails("interval '1-2' month(3) ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' month(3) ^to^ hour(2)", ANY);
    checkExpFails("interval '1-2' month(3) ^to^ minute(2)", ANY);
    checkExpFails("interval '1-2' month(3) ^to^ second(2)", ANY);
    checkExpFails("interval '1-2' month(3) ^to^ second(2,6)", ANY);

    checkExpFails("interval '1-2' day(3) ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' day(3) ^to^ month(2)", ANY);
    checkExpFails("interval '1-2' day(3) ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' day(3) to hour^(^2)", ANY);
    checkExpFails("interval '1-2' day(3) to minute^(^2)", ANY);
    checkExpFails("interval '1-2' day(3) to second(2^,^6)", ANY);

    checkExpFails("interval '1-2' hour(3) ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' hour(3) ^to^ month(2)", ANY);
    checkExpFails("interval '1-2' hour(3) ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' hour(3) ^to^ hour(2)", ANY);
    checkExpFails("interval '1-2' hour(3) to minute^(^2)", ANY);
    checkExpFails("interval '1-2' hour(3) to second(2^,^6)", ANY);

    checkExpFails("interval '1-2' minute(3) ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' minute(3) ^to^ month(2)", ANY);
    checkExpFails("interval '1-2' minute(3) ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' minute(3) ^to^ hour(2)", ANY);
    checkExpFails("interval '1-2' minute(3) ^to^ minute(2)", ANY);
    checkExpFails("interval '1-2' minute(3) to second(2^,^6)", ANY);

    checkExpFails("interval '1-2' second(3) ^to^ year(2)", ANY);
    checkExpFails("interval '1-2' second(3) ^to^ month(2)", ANY);
    checkExpFails("interval '1-2' second(3) ^to^ day(2)", ANY);
    checkExpFails("interval '1-2' second(3) ^to^ hour(2)", ANY);
    checkExpFails("interval '1-2' second(3) ^to^ minute(2)", ANY);
    checkExpFails("interval '1-2' second(3) ^to^ second(2)", ANY);
    checkExpFails("interval '1-2' second(3) ^to^ second(2,6)", ANY);

    // precision of -1 (< minimum allowed)
    checkExpFails("INTERVAL '0' YEAR(^-^1)", ANY);
    checkExpFails("INTERVAL '0-0' YEAR(^-^1) TO MONTH", ANY);
    checkExpFails("INTERVAL '0' MONTH(^-^1)", ANY);
    checkExpFails("INTERVAL '0' DAY(^-^1)", ANY);
    checkExpFails("INTERVAL '0 0' DAY(^-^1) TO HOUR", ANY);
    checkExpFails("INTERVAL '0 0' DAY(^-^1) TO MINUTE", ANY);
    checkExpFails("INTERVAL '0 0:0:0' DAY(^-^1) TO SECOND", ANY);
    checkExpFails("INTERVAL '0 0:0:0' DAY TO SECOND(^-^1)", ANY);
    checkExpFails("INTERVAL '0' HOUR(^-^1)", ANY);
    checkExpFails("INTERVAL '0:0' HOUR(^-^1) TO MINUTE", ANY);
    checkExpFails("INTERVAL '0:0:0' HOUR(^-^1) TO SECOND", ANY);
    checkExpFails("INTERVAL '0:0:0' HOUR TO SECOND(^-^1)", ANY);
    checkExpFails("INTERVAL '0' MINUTE(^-^1)", ANY);
    checkExpFails("INTERVAL '0:0' MINUTE(^-^1) TO SECOND", ANY);
    checkExpFails("INTERVAL '0:0' MINUTE TO SECOND(^-^1)", ANY);
    checkExpFails("INTERVAL '0' SECOND(^-^1)", ANY);
    checkExpFails("INTERVAL '0' SECOND(1, ^-^1)", ANY);

    // These may actually be legal per SQL2003, as the first field is
    // "more significant" than the last, but we do not support them
    checkExpFails("interval '1' day(3) ^to^ day", ANY);
    checkExpFails("interval '1' hour(3) ^to^ hour", ANY);
    checkExpFails("interval '1' minute(3) ^to^ minute", ANY);
    checkExpFails("interval '1' second(3) ^to^ second", ANY);
    checkExpFails("interval '1' second(3,1) ^to^ second", ANY);
    checkExpFails("interval '1' second(2,3) ^to^ second", ANY);
    checkExpFails("interval '1' second(2,2) ^to^ second(3)", ANY);

    // Invalid units
    checkExpFails("INTERVAL '2' ^MILLENNIUM^", ANY);
    checkExpFails("INTERVAL '1-2' ^MILLENNIUM^ TO CENTURY", ANY);
    checkExpFails("INTERVAL '10' ^CENTURY^", ANY);
    checkExpFails("INTERVAL '10' ^DECADE^", ANY);
    checkExpFails("INTERVAL '4' ^QUARTER^", ANY);
  }

  @Test public void testMiscIntervalQualifier() {
    checkExp("interval '-' day", "INTERVAL '-' DAY");

    checkExpFails(
        "interval '1 2:3:4.567' day to hour ^to^ second",
        "(?s)Encountered \"to\" at.*");
    checkExpFails(
        "interval '1:2' minute to second(2^,^ 2)",
        "(?s)Encountered \",\" at.*");
    checkExp(
        "interval '1:x' hour to minute",
        "INTERVAL '1:x' HOUR TO MINUTE");
    checkExp(
        "interval '1:x:2' hour to second",
        "INTERVAL '1:x:2' HOUR TO SECOND");
  }

  @Test public void testIntervalOperators() {
    checkExp("-interval '1' day", "(- INTERVAL '1' DAY)");
    checkExp(
        "interval '1' day + interval '1' day",
        "(INTERVAL '1' DAY + INTERVAL '1' DAY)");
    checkExp(
        "interval '1' day - interval '1:2:3' hour to second",
        "(INTERVAL '1' DAY - INTERVAL '1:2:3' HOUR TO SECOND)");

    checkExp("interval -'1' day", "INTERVAL -'1' DAY");
    checkExp("interval '-1' day", "INTERVAL '-1' DAY");
    checkExpFails(
        "interval 'wael was here^'^",
        "(?s)Encountered \"<EOF>\".*");
    checkExp(
        "interval 'wael was here' HOUR",
        "INTERVAL 'wael was here' HOUR"); // ok in parser, not in validator
  }

  @Test public void testDateMinusDate() {
    checkExp("(date1 - date2) HOUR", "((`DATE1` - `DATE2`) HOUR)");
    checkExp(
        "(date1 - date2) YEAR TO MONTH",
        "((`DATE1` - `DATE2`) YEAR TO MONTH)");
    checkExp(
        "(date1 - date2) HOUR > interval '1' HOUR",
        "(((`DATE1` - `DATE2`) HOUR) > INTERVAL '1' HOUR)");
    checkExpFails(
        "^(date1 + date2) second^",
        "(?s).*Illegal expression. Was expecting ..DATETIME - DATETIME. INTERVALQUALIFIER.*");
    checkExpFails(
        "^(date1,date2,date2) second^",
        "(?s).*Illegal expression. Was expecting ..DATETIME - DATETIME. INTERVALQUALIFIER.*");
  }

  @Test public void testExtract() {
    checkExp("extract(year from x)", "EXTRACT(YEAR FROM `X`)");
    checkExp("extract(month from x)", "EXTRACT(MONTH FROM `X`)");
    checkExp("extract(day from x)", "EXTRACT(DAY FROM `X`)");
    checkExp("extract(hour from x)", "EXTRACT(HOUR FROM `X`)");
    checkExp("extract(minute from x)", "EXTRACT(MINUTE FROM `X`)");
    checkExp("extract(second from x)", "EXTRACT(SECOND FROM `X`)");
    checkExp("extract(dow from x)", "EXTRACT(DOW FROM `X`)");
    checkExp("extract(doy from x)", "EXTRACT(DOY FROM `X`)");
    checkExp("extract(week from x)", "EXTRACT(WEEK FROM `X`)");
    checkExp("extract(epoch from x)", "EXTRACT(EPOCH FROM `X`)");
    checkExp("extract(quarter from x)", "EXTRACT(QUARTER FROM `X`)");
    checkExp("extract(decade from x)", "EXTRACT(DECADE FROM `X`)");
    checkExp("extract(century from x)", "EXTRACT(CENTURY FROM `X`)");
    checkExp("extract(millennium from x)", "EXTRACT(MILLENNIUM FROM `X`)");

    checkExpFails(
        "extract(day ^to^ second from x)",
        "(?s)Encountered \"to\".*");
  }

  @Test public void testGeometry() {
    checkExpFails("cast(null as ^geometry^)",
        "Geo-spatial extensions and the GEOMETRY data type are not enabled");
    conformance = SqlConformanceEnum.LENIENT;
    checkExp("cast(null as geometry)", "CAST(NULL AS GEOMETRY)");
  }

  @Test public void testIntervalArithmetics() {
    checkExp(
        "TIME '23:59:59' - interval '1' hour ",
        "(TIME '23:59:59' - INTERVAL '1' HOUR)");
    checkExp(
        "TIMESTAMP '2000-01-01 23:59:59.1' - interval '1' hour ",
        "(TIMESTAMP '2000-01-01 23:59:59.1' - INTERVAL '1' HOUR)");
    checkExp(
        "DATE '2000-01-01' - interval '1' hour ",
        "(DATE '2000-01-01' - INTERVAL '1' HOUR)");

    checkExp(
        "TIME '23:59:59' + interval '1' hour ",
        "(TIME '23:59:59' + INTERVAL '1' HOUR)");
    checkExp(
        "TIMESTAMP '2000-01-01 23:59:59.1' + interval '1' hour ",
        "(TIMESTAMP '2000-01-01 23:59:59.1' + INTERVAL '1' HOUR)");
    checkExp(
        "DATE '2000-01-01' + interval '1' hour ",
        "(DATE '2000-01-01' + INTERVAL '1' HOUR)");

    checkExp(
        "interval '1' hour + TIME '23:59:59' ",
        "(INTERVAL '1' HOUR + TIME '23:59:59')");

    checkExp("interval '1' hour * 8", "(INTERVAL '1' HOUR * 8)");
    checkExp("1 * interval '1' hour", "(1 * INTERVAL '1' HOUR)");
    checkExp("interval '1' hour / 8", "(INTERVAL '1' HOUR / 8)");
  }

  @Test public void testIntervalCompare() {
    checkExp(
        "interval '1' hour = interval '1' second",
        "(INTERVAL '1' HOUR = INTERVAL '1' SECOND)");
    checkExp(
        "interval '1' hour <> interval '1' second",
        "(INTERVAL '1' HOUR <> INTERVAL '1' SECOND)");
    checkExp(
        "interval '1' hour < interval '1' second",
        "(INTERVAL '1' HOUR < INTERVAL '1' SECOND)");
    checkExp(
        "interval '1' hour <= interval '1' second",
        "(INTERVAL '1' HOUR <= INTERVAL '1' SECOND)");
    checkExp(
        "interval '1' hour > interval '1' second",
        "(INTERVAL '1' HOUR > INTERVAL '1' SECOND)");
    checkExp(
        "interval '1' hour >= interval '1' second",
        "(INTERVAL '1' HOUR >= INTERVAL '1' SECOND)");
  }

  @Test public void testCastToInterval() {
    checkExp("cast(x as interval year)", "CAST(`X` AS INTERVAL YEAR)");
    checkExp("cast(x as interval month)", "CAST(`X` AS INTERVAL MONTH)");
    checkExp(
        "cast(x as interval year to month)",
        "CAST(`X` AS INTERVAL YEAR TO MONTH)");
    checkExp("cast(x as interval day)", "CAST(`X` AS INTERVAL DAY)");
    checkExp("cast(x as interval hour)", "CAST(`X` AS INTERVAL HOUR)");
    checkExp("cast(x as interval minute)", "CAST(`X` AS INTERVAL MINUTE)");
    checkExp("cast(x as interval second)", "CAST(`X` AS INTERVAL SECOND)");
    checkExp(
        "cast(x as interval day to hour)",
        "CAST(`X` AS INTERVAL DAY TO HOUR)");
    checkExp(
        "cast(x as interval day to minute)",
        "CAST(`X` AS INTERVAL DAY TO MINUTE)");
    checkExp(
        "cast(x as interval day to second)",
        "CAST(`X` AS INTERVAL DAY TO SECOND)");
    checkExp(
        "cast(x as interval hour to minute)",
        "CAST(`X` AS INTERVAL HOUR TO MINUTE)");
    checkExp(
        "cast(x as interval hour to second)",
        "CAST(`X` AS INTERVAL HOUR TO SECOND)");
    checkExp(
        "cast(x as interval minute to second)",
        "CAST(`X` AS INTERVAL MINUTE TO SECOND)");
    checkExp(
        "cast(interval '3-2' year to month as CHAR(5))",
        "CAST(INTERVAL '3-2' YEAR TO MONTH AS CHAR(5))");
  }

  @Test public void testCastToVarchar() {
    checkExp("cast(x as varchar(5))", "CAST(`X` AS VARCHAR(5))");
    checkExp("cast(x as varchar)", "CAST(`X` AS VARCHAR)");
    checkExp("cast(x as varBINARY(5))", "CAST(`X` AS VARBINARY(5))");
    checkExp("cast(x as varbinary)", "CAST(`X` AS VARBINARY)");
  }

  @Test public void testTimestampAddAndDiff() {
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
          checkExp(String.format(Locale.ROOT, function, interval, ""),
              String.format(Locale.ROOT, function, intervalGroup.getKey(), "`")
                  .toUpperCase(Locale.ROOT));
        }
      }
    }

    checkExpFails("timestampadd(^incorrect^, 1, current_timestamp)",
        "(?s).*Was expecting one of.*");
    checkExpFails("timestampdiff(^incorrect^, current_timestamp, current_timestamp)",
        "(?s).*Was expecting one of.*");
  }

  @Test public void testTimestampAdd() {
    final String sql = "select * from t\n"
        + "where timestampadd(sql_tsi_month, 5, hiredate) < curdate";
    final String expected = "SELECT *\n"
        + "FROM `T`\n"
        + "WHERE (TIMESTAMPADD(MONTH, 5, `HIREDATE`) < `CURDATE`)";
    sql(sql).ok(expected);
  }

  @Test public void testTimestampDiff() {
    final String sql = "select * from t\n"
        + "where timestampdiff(frac_second, 5, hiredate) < curdate";
    final String expected = "SELECT *\n"
        + "FROM `T`\n"
        + "WHERE (TIMESTAMPDIFF(MICROSECOND, 5, `HIREDATE`) < `CURDATE`)";
    sql(sql).ok(expected);
  }

  @Test public void testUnnest() {
    check(
        "select*from unnest(x)",
        "SELECT *\n"
            + "FROM (UNNEST(`X`))");
    check(
        "select*from unnest(x) AS T",
        "SELECT *\n"
            + "FROM (UNNEST(`X`)) AS `T`");

    // UNNEST cannot be first word in query
    checkFails(
        "^unnest^(x)",
        "(?s)Encountered \"unnest\" at.*");

    // UNNEST with more than one argument
    final String sql = "select * from dept,\n"
        + "unnest(dept.employees, dept.managers)";
    final String expected = "SELECT *\n"
        + "FROM `DEPT`,\n"
        + "(UNNEST(`DEPT`.`EMPLOYEES`, `DEPT`.`MANAGERS`))";
    sql(sql).ok(expected);

    // LATERAL UNNEST is not valid
    sql("select * from dept, lateral ^unnest^(dept.employees)")
        .fails("(?s)Encountered \"unnest\" at .*");
  }

  @Test public void testUnnestWithOrdinality() {
    sql("select * from unnest(x) with ordinality")
        .ok("SELECT *\n"
            + "FROM (UNNEST(`X`) WITH ORDINALITY)");
    sql("select*from unnest(x) with ordinality AS T")
        .ok("SELECT *\n"
            + "FROM (UNNEST(`X`) WITH ORDINALITY) AS `T`");
    sql("select*from unnest(x) with ordinality AS T(c, o)")
        .ok("SELECT *\n"
            + "FROM (UNNEST(`X`) WITH ORDINALITY) AS `T` (`C`, `O`)");
    sql("select*from unnest(x) as T ^with^ ordinality")
        .fails("(?s)Encountered \"with\" at .*");
  }

  @Test public void testParensInFrom() {
    // UNNEST may not occur within parentheses.
    // FIXME should fail at "unnest"
    checkFails(
        "select *from ^(^unnest(x))",
        "(?s)Encountered \"\\( unnest\" at .*");

    // <table-name> may not occur within parentheses.
    checkFails(
        "select * from (^emp^)",
        "(?s)Non-query expression encountered in illegal context.*");

    // <table-name> may not occur within parentheses.
    checkFails(
        "select * from (^emp^ as x)",
        "(?s)Non-query expression encountered in illegal context.*");

    // <table-name> may not occur within parentheses.
    checkFails(
        "select * from (^emp^) as x",
        "(?s)Non-query expression encountered in illegal context.*");

    // Parentheses around JOINs are OK, and sometimes necessary.
    if (false) {
      // todo:
      check(
          "select * from (emp join dept using (deptno))",
          "xx");

      check(
          "select * from (emp join dept using (deptno)) join foo using (x)",
          "xx");
    }
  }

  @Test public void testProcedureCall() {
    check("call blubber(5)", "CALL `BLUBBER`(5)");
    check("call \"blubber\"(5)", "CALL `blubber`(5)");
    check("call whale.blubber(5)", "CALL `WHALE`.`BLUBBER`(5)");
  }

  @Test public void testNewSpecification() {
    checkExp("new udt()", "(NEW `UDT`())");
    checkExp("new my.udt(1, 'hey')", "(NEW `MY`.`UDT`(1, 'hey'))");
    checkExp("new udt() is not null", "((NEW `UDT`()) IS NOT NULL)");
    checkExp("1 + new udt()", "(1 + (NEW `UDT`()))");
  }

  @Test public void testMultisetCast() {
    checkExp(
        "cast(multiset[1] as double multiset)",
        "CAST((MULTISET[1]) AS DOUBLE MULTISET)");
  }

  @Test public void testAddCarets() {
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

  @Test public void testMetadata() {
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
  @Test public void testNoUnintendedNewReservedKeywords() {
    assumeTrue("don't run this test for sub-classes", isNotSubclass());
    final SqlAbstractParserImpl.Metadata metadata =
        getSqlParser("").getMetadata();

    final SortedSet<String> reservedKeywords = new TreeSet<>();
    final SortedSet<String> keywords92 = keywords("92");
    for (String s : metadata.getTokens()) {
      if (metadata.isKeyword(s) && metadata.isReservedWord(s)) {
        reservedKeywords.add(s);
      }
      if (false) {
        // Cannot enable this test yet, because the parser's list of SQL:92
        // reserved words is not consistent with keywords("92").
        assertThat(s, metadata.isSql92ReservedWord(s),
            is(keywords92.contains(s)));
      }
    }

    final String reason = "The parser has at least one new reserved keyword. "
        + "Are you sure it should be reserved? Difference:\n"
        + DiffTestCase.diffLines(ImmutableList.copyOf(getReservedKeywords()),
            ImmutableList.copyOf(reservedKeywords));
    assertThat(reason, reservedKeywords, is(getReservedKeywords()));
  }

  @Test public void testTabStop() {
    check(
        "SELECT *\n\tFROM mytable",
        "SELECT *\n"
            + "FROM `MYTABLE`");

    // make sure that the tab stops do not affect the placement of the
    // error tokens
    checkFails(
        "SELECT *\tFROM mytable\t\tWHERE x ^=^ = y AND b = 1",
        "(?s).*Encountered \"= =\" at line 1, column 32\\..*");
  }

  @Test public void testLongIdentifiers() {
    StringBuilder ident128Builder = new StringBuilder();
    for (int i = 0; i < 128; i++) {
      ident128Builder.append((char) ('a' + (i % 26)));
    }
    String ident128 = ident128Builder.toString();
    String ident128Upper = ident128.toUpperCase(Locale.US);
    String ident129 = "x" + ident128;
    String ident129Upper = ident129.toUpperCase(Locale.US);

    check(
        "select * from " + ident128,
        "SELECT *\n"
            + "FROM `" + ident128Upper + "`");
    checkFails(
        "select * from ^" + ident129 + "^",
        "Length of identifier '" + ident129Upper
            + "' must be less than or equal to 128 characters");

    check(
        "select " + ident128 + " from mytable",
        "SELECT `" + ident128Upper + "`\n"
            + "FROM `MYTABLE`");
    checkFails(
        "select ^" + ident129 + "^ from mytable",
        "Length of identifier '" + ident129Upper
            + "' must be less than or equal to 128 characters");
  }

  /**
   * Tests that you can't quote the names of builtin functions.
   *
   * @see org.apache.calcite.test.SqlValidatorTest#testQuotedFunction()
   */
  @Test public void testQuotedFunction() {
    checkExpFails(
        "\"CAST\"(1 ^as^ double)",
        "(?s).*Encountered \"as\" at .*");
    checkExpFails(
        "\"POSITION\"('b' ^in^ 'alphabet')",
        "(?s).*Encountered \"in \\\\'alphabet\\\\'\" at .*");
    checkExpFails(
        "\"OVERLAY\"('a' ^PLAcing^ 'b' from 1)",
        "(?s).*Encountered \"PLAcing\" at.*");
    checkExpFails(
        "\"SUBSTRING\"('a' ^from^ 1)",
        "(?s).*Encountered \"from\" at .*");
  }

  /**
   * Tests that applying member function of a specific type as a suffix function
   */
  @Test public void testMemberFunction() {
    check("SELECT myColumn.func(a, b) FROM tbl",
        "SELECT `MYCOLUMN`.`FUNC`(`A`, `B`)\n"
            + "FROM `TBL`");
    check("SELECT myColumn.mySubField.func() FROM tbl",
        "SELECT `MYCOLUMN`.`MYSUBFIELD`.`FUNC`()\n"
            + "FROM `TBL`");
    check("SELECT tbl.myColumn.mySubField.func() FROM tbl",
        "SELECT `TBL`.`MYCOLUMN`.`MYSUBFIELD`.`FUNC`()\n"
            + "FROM `TBL`");
    check("SELECT tbl.foo(0).col.bar(2, 3) FROM tbl",
        "SELECT ((`TBL`.`FOO`(0).`COL`).`BAR`(2, 3))\n"
            + "FROM `TBL`");
  }

  @Test public void testUnicodeLiteral() {
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
    check(in1, out1);

    // Without the U& prefix, escapes are left unprocessed
    String in2 =
        "values '"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL + "'";
    String out2 =
        "VALUES (ROW('"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL + "'))";
    check(in2, out2);

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
    check(in3, out3);
  }

  @Test public void testUnicodeEscapedLiteral() {
    // Note that here we are constructing a SQL statement which
    // contains SQL-escaped Unicode characters to be handled
    // by the SQL parser.
    String in =
        "values U&'"
            + ConversionUtil.TEST_UNICODE_SQL_ESCAPED_LITERAL + "'";
    String out =
        "VALUES (ROW(_UTF16'"
            + ConversionUtil.TEST_UNICODE_STRING + "'))";
    check(in, out);

    // Verify that we can override with an explicit escape character
    check(in.replaceAll("\\\\", "!") + "UESCAPE '!'", out);
  }

  @Test public void testIllegalUnicodeEscape() {
    checkExpFails(
        "U&'abc' UESCAPE '!!'",
        ".*must be exactly one character.*");
    checkExpFails(
        "U&'abc' UESCAPE ''",
        ".*must be exactly one character.*");
    checkExpFails(
        "U&'abc' UESCAPE '0'",
        ".*hex digit.*");
    checkExpFails(
        "U&'abc' UESCAPE 'a'",
        ".*hex digit.*");
    checkExpFails(
        "U&'abc' UESCAPE 'F'",
        ".*hex digit.*");
    checkExpFails(
        "U&'abc' UESCAPE ' '",
        ".*whitespace.*");
    checkExpFails(
        "U&'abc' UESCAPE '+'",
        ".*plus sign.*");
    checkExpFails(
        "U&'abc' UESCAPE '\"'",
        ".*double quote.*");
    checkExpFails(
        "'abc' UESCAPE ^'!'^",
        ".*without Unicode literal introducer.*");
    checkExpFails(
        "^U&'\\0A'^",
        ".*is not exactly four hex digits.*");
    checkExpFails(
        "^U&'\\wxyz'^",
        ".*is not exactly four hex digits.*");
  }

  @Test public void testSqlOptions() throws SqlParseException {
    SqlNode node = getSqlParser("alter system set schema = true").parseStmt();
    SqlSetOption opt = (SqlSetOption) node;
    assertThat(opt.getScope(), equalTo("SYSTEM"));
    SqlPrettyWriter writer = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);
    assertThat(writer.format(opt.getName()), equalTo("\"SCHEMA\""));
    writer = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);
    assertThat(writer.format(opt.getValue()), equalTo("TRUE"));
    writer = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);
    assertThat(writer.format(opt),
        equalTo("ALTER SYSTEM SET \"SCHEMA\" = TRUE"));

    sql("alter system set \"a number\" = 1")
        .ok("ALTER SYSTEM SET `a number` = 1")
        .node(isDdl());
    check("alter system set flag = false",
        "ALTER SYSTEM SET `FLAG` = FALSE");
    check("alter system set approx = -12.3450",
        "ALTER SYSTEM SET `APPROX` = -12.3450");
    check("alter system set onOff = on",
        "ALTER SYSTEM SET `ONOFF` = `ON`");
    check("alter system set onOff = off",
        "ALTER SYSTEM SET `ONOFF` = `OFF`");
    check("alter system set baz = foo",
        "ALTER SYSTEM SET `BAZ` = `FOO`");


    check("alter system set \"a\".\"number\" = 1",
        "ALTER SYSTEM SET `a`.`number` = 1");
    sql("set approx = -12.3450")
        .ok("SET `APPROX` = -12.3450")
        .node(isDdl());

    node = getSqlParser("reset schema").parseStmt();
    opt = (SqlSetOption) node;
    assertThat(opt.getScope(), equalTo(null));
    writer = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);
    assertThat(writer.format(opt.getName()), equalTo("\"SCHEMA\""));
    assertThat(opt.getValue(), equalTo(null));
    writer = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);
    assertThat(writer.format(opt),
        equalTo("RESET \"SCHEMA\""));

    check("alter system RESET flag",
        "ALTER SYSTEM RESET `FLAG`");
    sql("reset onOff")
        .ok("RESET `ONOFF`")
        .node(isDdl());
    check("reset \"this\".\"is\".\"sparta\"",
        "RESET `this`.`is`.`sparta`");
    check("alter system reset all",
        "ALTER SYSTEM RESET `ALL`");
    check("reset all",
        "RESET `ALL`");

    // expressions not allowed
    checkFails("alter system set aString = 'abc' ^||^ 'def' ",
        "(?s)Encountered \"\\|\\|\" at line 1, column 34\\..*");

    // multiple assignments not allowed
    checkFails("alter system set x = 1^,^ y = 2",
        "(?s)Encountered \",\" at line 1, column 23\\..*");
  }

  @Test public void testSequence() {
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

  @Test public void testMatchRecognize1() {
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

  @Test public void testMatchRecognize2() {
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

  @Test public void testMatchRecognize3() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (^strt down+ up+)\n"
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

  @Test public void testMatchRecognize4() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (^strt down+ up+$)\n"
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

  @Test public void testMatchRecognize5() {
    final String sql = "select *\n"
        + "  from t match_recognize\n"
        + "  (\n"
        + "    pattern (strt down* up?)\n"
        + "    define\n"
        + "      down as down.price < PREV(down.price),\n"
        + "      up as up.price > prev(up.price)\n"
        + "  ) mr";
    final String expected = "SELECT *\n"
        + "FROM `T` MATCH_RECOGNIZE(\n"
        + "PATTERN (((`STRT` (`DOWN` *)) (`UP` ?)))\n"
        + "DEFINE "
        + "`DOWN` AS (`DOWN`.`PRICE` < PREV(`DOWN`.`PRICE`, 1)), "
        + "`UP` AS (`UP`.`PRICE` > PREV(`UP`.`PRICE`, 1))"
        + ") AS `MR`";
    sql(sql).ok(expected);
  }

  @Test public void testMatchRecognize6() {
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

  @Test public void testMatchRecognize7() {
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

  @Test public void testMatchRecognize8() {
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

  @Test public void testMatchRecognize9() {
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

  @Test public void testMatchRecognize10() {
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

  @Test public void testMatchRecognize11() {
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

  @Test public void testMatchRecognizeDefineClause() {
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

  @Test public void testMatchRecognizeDefineClause2() {
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

  @Test public void testMatchRecognizeDefineClause3() {
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

  @Test public void testMatchRecognizeDefineClause4() {
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

  @Test public void testMatchRecognizeMeasures1() {
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

  @Test public void testMatchRecognizeMeasures2() {
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

  @Test public void testMatchRecognizeMeasures3() {
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

  @Test public void testMatchRecognizeMeasures4() {
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

  @Test public void testMatchRecognizeMeasures5() {
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

  @Test public void testMatchRecognizeMeasures6() {
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

  @Test public void testMatchRecognizePatternSkip1() {
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

  @Test public void testMatchRecognizePatternSkip2() {
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

  @Test public void testMatchRecognizePatternSkip3() {
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

  @Test public void testMatchRecognizePatternSkip4() {
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

  @Test public void testMatchRecognizePatternSkip5() {
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
  @Test public void testMatchRecognizePatternSkip6() {
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

  @Test public void testMatchRecognizeSubset1() {
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

  @Test public void testMatchRecognizeSubset2() {
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

  @Test public void testMatchRecognizeSubset3() {
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

  @Test public void testMatchRecognizeRowsPerMatch1() {
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

  @Test public void testMatchRecognizeRowsPerMatch2() {
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

  @Test public void testMatchRecognizeWithin() {
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

  @Test public void testWithinGroupClause1() {
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

  @Test public void testWithinGroupClause2() {
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

  @Test public void testWithinGroupClause3() {
    final String sql = "select collect(col2) within group (^)^ "
        + "from t order by col1 limit 10";
    sql(sql).fails("(?s).*Encountered \"\\)\" at line 1, column 36\\..*");
  }

  @Test public void testWithinGroupClause4() {
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

  @Test public void testWithinGroupClause5() {
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

  @Test public void testJsonValueExpressionOperator() {
    checkExp("foo format json",
        "`FOO` FORMAT JSON");
    // Currently, encoding js not valid
    checkExp("foo format json encoding utf8",
        "`FOO` FORMAT JSON");
    checkExp("foo format json encoding utf16",
        "`FOO` FORMAT JSON");
    checkExp("foo format json encoding utf32",
        "`FOO` FORMAT JSON");
    checkExp("null format json", "NULL FORMAT JSON");
    // Test case to eliminate choice conflict on token <FORMAT>
    check("select foo format from tab", "SELECT `FOO` AS `FORMAT`\n"
        + "FROM `TAB`");
    // Test case to eliminate choice conflict on token <ENCODING>
    check("select foo format json encoding from tab", "SELECT `FOO` FORMAT JSON AS `ENCODING`\n"
        + "FROM `TAB`");
  }

  @Test public void testJsonExists() {
    checkExp("json_exists('{\"foo\": \"bar\"}', 'lax $.foo')",
        "JSON_EXISTS('{\"foo\": \"bar\"}', 'lax $.foo')");
    checkExp("json_exists('{\"foo\": \"bar\"}', 'lax $.foo' error on error)",
        "JSON_EXISTS('{\"foo\": \"bar\"}', 'lax $.foo' ERROR ON ERROR)");
  }

  @Test public void testJsonValue() {
    checkExp("json_value('{\"foo\": \"100\"}', 'lax $.foo' "
            + "returning integer)",
        "JSON_VALUE('{\"foo\": \"100\"}', 'lax $.foo' "
            + "RETURNING INTEGER NULL ON EMPTY NULL ON ERROR)");
    checkExp("json_value('{\"foo\": \"100\"}', 'lax $.foo' "
            + "returning integer default 10 on empty error on error)",
        "JSON_VALUE('{\"foo\": \"100\"}', 'lax $.foo' "
            + "RETURNING INTEGER DEFAULT 10 ON EMPTY ERROR ON ERROR)");
  }

  @Test public void testJsonQuery() {
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' WITHOUT ARRAY WRAPPER)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' WITH WRAPPER)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITH UNCONDITIONAL ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' WITH UNCONDITIONAL WRAPPER)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITH UNCONDITIONAL ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' WITH CONDITIONAL WRAPPER)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITH CONDITIONAL ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' NULL ON EMPTY)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' ERROR ON EMPTY)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER ERROR ON EMPTY NULL ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' EMPTY ARRAY ON EMPTY)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER EMPTY ARRAY ON EMPTY NULL ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' EMPTY OBJECT ON EMPTY)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER EMPTY OBJECT ON EMPTY NULL ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' NULL ON ERROR)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' ERROR ON ERROR)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY ERROR ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' EMPTY ARRAY ON ERROR)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY EMPTY ARRAY ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' EMPTY OBJECT ON ERROR)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER NULL ON EMPTY EMPTY OBJECT ON ERROR)");
    checkExp("json_query('{\"foo\": \"bar\"}', 'lax $' EMPTY ARRAY ON EMPTY "
            + "EMPTY OBJECT ON ERROR)",
        "JSON_QUERY('{\"foo\": \"bar\"}', "
            + "'lax $' WITHOUT ARRAY WRAPPER EMPTY ARRAY ON EMPTY EMPTY OBJECT ON ERROR)");
  }

  @Test public void testJsonObject() {
    checkExp("json_object('foo': 'bar')",
        "JSON_OBJECT(KEY 'foo' VALUE 'bar' NULL ON NULL)");
    checkExp("json_object('foo': 'bar', 'foo2': 'bar2')",
        "JSON_OBJECT(KEY 'foo' VALUE 'bar', KEY 'foo2' VALUE 'bar2' NULL ON NULL)");
    checkExp("json_object('foo' value 'bar')",
        "JSON_OBJECT(KEY 'foo' VALUE 'bar' NULL ON NULL)");
    checkExp("json_object(key 'foo' value 'bar')",
        "JSON_OBJECT(KEY 'foo' VALUE 'bar' NULL ON NULL)");
    checkExp("json_object('foo': null)",
        "JSON_OBJECT(KEY 'foo' VALUE NULL NULL ON NULL)");
    checkExp("json_object('foo': null absent on null)",
        "JSON_OBJECT(KEY 'foo' VALUE NULL ABSENT ON NULL)");
    checkExp("json_object('foo': json_object('foo': 'bar') format json)",
        "JSON_OBJECT(KEY 'foo' VALUE "
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
    checkExp("json_object(key: value)",
        "JSON_OBJECT(KEY `KEY` VALUE `VALUE` NULL ON NULL)");
  }

  @Test public void testJsonType() {
    checkExp("json_type('11.56')", "JSON_TYPE('11.56')");
    checkExp("json_type('{}')", "JSON_TYPE('{}')");
    checkExp("json_type(null)", "JSON_TYPE(NULL)");
    checkExp("json_type('[\"foo\",null]')",
            "JSON_TYPE('[\"foo\",null]')");
    checkExp("json_type('{\"foo\": \"100\"}')",
            "JSON_TYPE('{\"foo\": \"100\"}')");
  }

  @Test public void testJsonDepth() {
    checkExp("json_depth('11.56')", "JSON_DEPTH('11.56')");
    checkExp("json_depth('{}')", "JSON_DEPTH('{}')");
    checkExp("json_depth(null)", "JSON_DEPTH(NULL)");
    checkExp("json_depth('[\"foo\",null]')",
            "JSON_DEPTH('[\"foo\",null]')");
    checkExp("json_depth('{\"foo\": \"100\"}')",
            "JSON_DEPTH('{\"foo\": \"100\"}')");
  }

  @Test public void testJsonLength() {
    checkExp("json_length('{\"foo\": \"bar\"}')",
            "JSON_LENGTH('{\"foo\": \"bar\"}')");
    checkExp("json_length('{\"foo\": \"bar\"}', 'lax $')",
            "JSON_LENGTH('{\"foo\": \"bar\"}', 'lax $')");
    checkExp("json_length('{\"foo\": \"bar\"}', 'strict $')",
            "JSON_LENGTH('{\"foo\": \"bar\"}', 'strict $')");
    checkExp("json_length('{\"foo\": \"bar\"}', 'invalid $')",
            "JSON_LENGTH('{\"foo\": \"bar\"}', 'invalid $')");
  }

  @Test public void testJsonKeys() {
    checkExp("json_keys('{\"foo\": \"bar\"}', 'lax $')",
            "JSON_KEYS('{\"foo\": \"bar\"}', 'lax $')");
    checkExp("json_keys('{\"foo\": \"bar\"}', 'strict $')",
            "JSON_KEYS('{\"foo\": \"bar\"}', 'strict $')");
    checkExp("json_keys('{\"foo\": \"bar\"}', 'invalid $')",
            "JSON_KEYS('{\"foo\": \"bar\"}', 'invalid $')");
  }

  @Test public void testJsonRemove() {
    checkExp("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$')",
            "JSON_REMOVE('[\"a\", [\"b\", \"c\"], \"d\"]', '$')");
    checkExp("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$[1]', '$[0]')",
            "JSON_REMOVE('[\"a\", [\"b\", \"c\"], \"d\"]', '$[1]', '$[0]')");
  }

  @Test public void testJsonObjectAgg() {
    checkExp("json_objectagg(k_column: v_column)",
        "JSON_OBJECTAGG(KEY `K_COLUMN` VALUE `V_COLUMN` NULL ON NULL)");
    checkExp("json_objectagg(k_column value v_column)",
        "JSON_OBJECTAGG(KEY `K_COLUMN` VALUE `V_COLUMN` NULL ON NULL)");
    checkExp("json_objectagg(key k_column value v_column)",
        "JSON_OBJECTAGG(KEY `K_COLUMN` VALUE `V_COLUMN` NULL ON NULL)");
    checkExp("json_objectagg(k_column: null)",
        "JSON_OBJECTAGG(KEY `K_COLUMN` VALUE NULL NULL ON NULL)");
    checkExp("json_objectagg(k_column: null absent on null)",
        "JSON_OBJECTAGG(KEY `K_COLUMN` VALUE NULL ABSENT ON NULL)");
    checkExp("json_objectagg(k_column: json_object(k_column: v_column) format json)",
        "JSON_OBJECTAGG(KEY `K_COLUMN` VALUE "
            + "JSON_OBJECT(KEY `K_COLUMN` VALUE `V_COLUMN` NULL ON NULL) "
            + "FORMAT JSON NULL ON NULL)");
  }

  @Test public void testJsonArray() {
    checkExp("json_array('foo')",
        "JSON_ARRAY('foo' ABSENT ON NULL)");
    checkExp("json_array(null)",
        "JSON_ARRAY(NULL ABSENT ON NULL)");
    checkExp("json_array(null null on null)",
        "JSON_ARRAY(NULL NULL ON NULL)");
    checkExp("json_array(json_array('foo', 'bar') format json)",
        "JSON_ARRAY(JSON_ARRAY('foo', 'bar' ABSENT ON NULL) FORMAT JSON ABSENT ON NULL)");
  }

  @Test public void testJsonPretty() {
    checkExp("json_pretty('foo')",
            "JSON_PRETTY('foo')");
    checkExp("json_pretty(null)",
            "JSON_PRETTY(NULL)");
  }

  @Test public void testJsonStorageSize() {
    checkExp("json_storage_size('foo')",
        "JSON_STORAGE_SIZE('foo')");
    checkExp("json_storage_size(null)",
        "JSON_STORAGE_SIZE(NULL)");
  }

  @Test public void testJsonArrayAgg1() {
    checkExp("json_arrayagg(\"column\")",
        "JSON_ARRAYAGG(`column` ABSENT ON NULL)");
    checkExp("json_arrayagg(\"column\" null on null)",
        "JSON_ARRAYAGG(`column` NULL ON NULL)");
    checkExp("json_arrayagg(json_array(\"column\") format json)",
        "JSON_ARRAYAGG(JSON_ARRAY(`column` ABSENT ON NULL) FORMAT JSON ABSENT ON NULL)");
  }

  @Test public void testJsonArrayAgg2() {
    checkExp("json_arrayagg(\"column\" order by \"column\")",
        "JSON_ARRAYAGG(`column` ABSENT ON NULL) WITHIN GROUP (ORDER BY `column`)");
    checkExp("json_arrayagg(\"column\") within group (order by \"column\")",
        "JSON_ARRAYAGG(`column` ABSENT ON NULL) WITHIN GROUP (ORDER BY `column`)");
    checkFails("^json_arrayagg(\"column\" order by \"column\") within group (order by \"column\")^",
        "(?s).*Including both WITHIN GROUP\\(\\.\\.\\.\\) and inside ORDER BY "
            + "in a single JSON_ARRAYAGG call is not allowed.*");
  }

  @Test public void testJsonPredicate() {
    checkExp("'{}' is json",
        "('{}' IS JSON VALUE)");
    checkExp("'{}' is json value",
        "('{}' IS JSON VALUE)");
    checkExp("'{}' is json object",
        "('{}' IS JSON OBJECT)");
    checkExp("'[]' is json array",
        "('[]' IS JSON ARRAY)");
    checkExp("'100' is json scalar",
        "('100' IS JSON SCALAR)");
    checkExp("'{}' is not json",
        "('{}' IS NOT JSON VALUE)");
    checkExp("'{}' is not json value",
        "('{}' IS NOT JSON VALUE)");
    checkExp("'{}' is not json object",
        "('{}' IS NOT JSON OBJECT)");
    checkExp("'[]' is not json array",
        "('[]' IS NOT JSON ARRAY)");
    checkExp("'100' is not json scalar",
        "('100' IS NOT JSON SCALAR)");
  }

  @Test public void testParseWithReader() throws Exception {
    String query = "select * from dual";
    SqlParser sqlParserReader = getSqlParser(new StringReader(query));
    SqlNode node1 = sqlParserReader.parseQuery();
    SqlParser sqlParserString = getSqlParser(query);
    SqlNode node2 = sqlParserString.parseQuery();
    assertEquals(node2.toString(), node1.toString());
  }

  @Test public void testConfigureFromDialect() throws SqlParseException {
    // Calcite's default converts unquoted identifiers to upper case
    checkDialect(SqlDialect.DatabaseProduct.CALCITE.getDialect(),
        "select unquotedColumn from \"doubleQuotedTable\"",
        is("SELECT \"UNQUOTEDCOLUMN\"\n"
            + "FROM \"doubleQuotedTable\""));
    // MySQL leaves unquoted identifiers unchanged
    checkDialect(SqlDialect.DatabaseProduct.MYSQL.getDialect(),
        "select unquotedColumn from `doubleQuotedTable`",
        is("SELECT `unquotedColumn`\n"
            + "FROM `doubleQuotedTable`"));
    // Oracle converts unquoted identifiers to upper case
    checkDialect(SqlDialect.DatabaseProduct.ORACLE.getDialect(),
        "select unquotedColumn from \"doubleQuotedTable\"",
        is("SELECT \"UNQUOTEDCOLUMN\"\n"
            + "FROM \"doubleQuotedTable\""));
    // PostgreSQL converts unquoted identifiers to lower case
    checkDialect(SqlDialect.DatabaseProduct.POSTGRESQL.getDialect(),
        "select unquotedColumn from \"doubleQuotedTable\"",
        is("SELECT \"unquotedcolumn\"\n"
            + "FROM \"doubleQuotedTable\""));
    // Redshift converts all identifiers to lower case
    checkDialect(SqlDialect.DatabaseProduct.REDSHIFT.getDialect(),
        "select unquotedColumn from \"doubleQuotedTable\"",
        is("SELECT \"unquotedcolumn\"\n"
            + "FROM \"doublequotedtable\""));
    // BigQuery leaves quoted and unquoted identifers unchanged
    checkDialect(SqlDialect.DatabaseProduct.BIG_QUERY.getDialect(),
        "select unquotedColumn from `doubleQuotedTable`",
        is("SELECT unquotedColumn\n"
            + "FROM doubleQuotedTable"));
  }

  @Test public void testParenthesizedSubQueries() {
    final String expected = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM `TAB`) AS `X`";

    final String sql1 = "SELECT * FROM (((SELECT * FROM tab))) X";
    sql(sql1).ok(expected);

    final String sql2 = "SELECT * FROM ((((((((((((SELECT * FROM tab)))))))))))) X";
    sql(sql2).ok(expected);
  }

  protected void checkDialect(SqlDialect dialect, String sql,
      Matcher<String> matcher) throws SqlParseException {
    final SqlParser parser = getDialectSqlParser(sql, dialect);
    final SqlNode node = parser.parseStmt();
    assertThat(linux(node.toSqlString(dialect).getSql()), matcher);
  }

  //~ Inner Interfaces -------------------------------------------------------

  /**
   * Callback to control how test actions are performed.
   */
  protected interface Tester {
    void checkList(String sql, List<String> expected);

    void check(String sql, String expected);

    void checkExp(String sql, String expected);

    void checkFails(String sql, boolean list, String expectedMsgPattern);

    void checkExpFails(String sql, String expectedMsgPattern);

    void checkNode(String sql, Matcher<SqlNode> matcher);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Default implementation of {@link Tester}.
   */
  protected class TesterImpl implements Tester {
    private void check(
        SqlNode sqlNode,
        String expected) {
      // no dialect, always parenthesize
      final String actual = sqlNode.toSqlString(null, true).getSql();
      TestUtil.assertEqualsVerbose(expected, linux(actual));
    }

    @Override public void checkList(
        String sql,
        List<String> expected) {
      final SqlNodeList sqlNodeList = parseStmtsAndHandleEx(sql);
      assertThat(sqlNodeList.size(), is(expected.size()));

      for (int i = 0; i < sqlNodeList.size(); i++) {
        SqlNode sqlNode = sqlNodeList.get(i);
        check(sqlNode, expected.get(i));
      }
    }

    public void check(
        String sql,
        String expected) {
      final SqlNode sqlNode = parseStmtAndHandleEx(sql);
      check(sqlNode, expected);
    }

    protected SqlNode parseStmtAndHandleEx(String sql) {
      final SqlNode sqlNode;
      try {
        sqlNode = getSqlParser(sql).parseStmt();
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

    public void checkExp(
        String sql,
        String expected) {
      final SqlNode sqlNode = parseExpressionAndHandleEx(sql);
      final String actual = sqlNode.toSqlString(null, true).getSql();
      TestUtil.assertEqualsVerbose(expected, linux(actual));
    }

    protected SqlNode parseExpressionAndHandleEx(String sql) {
      final SqlNode sqlNode;
      try {
        sqlNode = getSqlParser(sql).parseExpression();
      } catch (SqlParseException e) {
        throw new RuntimeException("Error while parsing expression: " + sql, e);
      }
      return sqlNode;
    }

    public void checkFails(
        String sql,
        boolean list,
        String expectedMsgPattern) {
      SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
      Throwable thrown = null;
      try {
        final SqlNode sqlNode;
        if (list) {
          sqlNode = getSqlParser(sap.sql).parseStmtList();
        } else {
          sqlNode = getSqlParser(sap.sql).parseStmt();
        }
        Util.discard(sqlNode);
      } catch (Throwable ex) {
        thrown = ex;
      }

      checkEx(expectedMsgPattern, sap, thrown);
    }

    public void checkNode(String sql, Matcher<SqlNode> matcher) {
      SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
      try {
        final SqlNode sqlNode = getSqlParser(sap.sql).parseStmt();
        assertThat(sqlNode, matcher);
      } catch (SqlParseException e) {
        throw TestUtil.rethrow(e);
      }
    }

    /**
     * Tests that an expression throws an exception which matches the given
     * pattern.
     */
    public void checkExpFails(
        String sql,
        String expectedMsgPattern) {
      SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
      Throwable thrown = null;
      try {
        final SqlNode sqlNode = getSqlParser(sap.sql).parseExpression();
        Util.discard(sqlNode);
      } catch (Throwable ex) {
        thrown = ex;
      }

      checkEx(expectedMsgPattern, sap, thrown);
    }

    protected void checkEx(String expectedMsgPattern, SqlParserUtil.StringAndPos sap,
        Throwable thrown) {
      SqlValidatorTestCase.checkEx(thrown, expectedMsgPattern, sap);
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

    private String toSqlString(SqlNodeList sqlNodeList) {
      List<String> sqls = sqlNodeList.getList().stream()
          .map(it -> it.toSqlString(CalciteSqlDialect.DEFAULT, false).getSql())
          .collect(Collectors.toList());
      return String.join(";", sqls);
    }

    private void checkList(SqlNodeList sqlNodeList, List<String> expected) {
      Assert.assertEquals(expected.size(), sqlNodeList.size());

      for (int i = 0; i < sqlNodeList.size(); i++) {
        SqlNode sqlNode = sqlNodeList.get(i);
        // Unparse with no dialect, always parenthesize.
        final String actual = sqlNode.toSqlString(null, true).getSql();
        assertEquals(expected.get(i), linux(actual));
      }
    }

    @Override public void checkList(String sql, List<String> expected) {
      SqlNodeList sqlNodeList = parseStmtsAndHandleEx(sql);

      checkList(sqlNodeList, expected);

      // Unparse again in Calcite dialect (which we can parse), and
      // minimal parentheses.
      final String sql1 = toSqlString(sqlNodeList);

      // Parse and unparse again.
      SqlNodeList sqlNodeList2;
      final Quoting q = quoting;
      try {
        quoting = Quoting.DOUBLE_QUOTE;
        sqlNodeList2 = parseStmtsAndHandleEx(sql1);
      } finally {
        quoting = q;
      }
      final String sql2 = toSqlString(sqlNodeList2);

      // Should be the same as we started with.
      assertEquals(sql1, sql2);

      // Now unparse again in the null dialect.
      // If the unparser is not including sufficient parens to override
      // precedence, the problem will show up here.
      checkList(sqlNodeList2, expected);
    }

    @Override public void check(String sql, String expected) {
      SqlNode sqlNode = parseStmtAndHandleEx(sql);

      // Unparse with no dialect, always parenthesize.
      final String actual = sqlNode.toSqlString(null, true).getSql();
      assertEquals(expected, linux(actual));

      // Unparse again in Calcite dialect (which we can parse), and
      // minimal parentheses.
      final String sql1 =
          sqlNode.toSqlString(CalciteSqlDialect.DEFAULT, false).getSql();

      // Parse and unparse again.
      SqlNode sqlNode2;
      final Quoting q = quoting;
      try {
        quoting = Quoting.DOUBLE_QUOTE;
        sqlNode2 = parseStmtAndHandleEx(sql1);
      } finally {
        quoting = q;
      }
      final String sql2 =
          sqlNode2.toSqlString(CalciteSqlDialect.DEFAULT, false).getSql();

      // Should be the same as we started with.
      assertEquals(sql1, sql2);

      // Now unparse again in the null dialect.
      // If the unparser is not including sufficient parens to override
      // precedence, the problem will show up here.
      final String actual2 = sqlNode2.toSqlString(null, true).getSql();
      assertEquals(expected, linux(actual2));
    }

    @Override public void checkExp(String sql, String expected) {
      SqlNode sqlNode = parseExpressionAndHandleEx(sql);

      // Unparse with no dialect, always parenthesize.
      final String actual = sqlNode.toSqlString(null, true).getSql();
      assertEquals(expected, linux(actual));

      // Unparse again in Calcite dialect (which we can parse), and
      // minimal parentheses.
      final String sql1 =
          sqlNode.toSqlString(CalciteSqlDialect.DEFAULT, false).getSql();

      // Parse and unparse again.
      SqlNode sqlNode2;
      final Quoting q = quoting;
      try {
        quoting = Quoting.DOUBLE_QUOTE;
        sqlNode2 = parseExpressionAndHandleEx(sql1);
      } finally {
        quoting = q;
      }
      final String sql2 =
          sqlNode2.toSqlString(CalciteSqlDialect.DEFAULT, false).getSql();

      // Should be the same as we started with.
      assertEquals(sql1, sql2);

      // Now unparse again in the null dialect.
      // If the unparser is not including sufficient parens to override
      // precedence, the problem will show up here.
      final String actual2 = sqlNode2.toSqlString(null, true).getSql();
      assertEquals(expected, linux(actual2));
    }

    @Override public void checkFails(String sql,
        boolean list, String expectedMsgPattern) {
      // Do nothing. We're not interested in unparsing invalid SQL
    }

    @Override public void checkExpFails(String sql, String expectedMsgPattern) {
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
    private final String sql;
    private final boolean expression;

    Sql(String sql) {
      this(sql, false);
    }

    Sql(String sql, boolean expression) {
      this.sql = sql;
      this.expression = expression;
    }

    public Sql ok(String expected) {
      if (expression) {
        getTester().checkExp(sql, expected);
      } else {
        getTester().check(sql, expected);
      }
      return this;
    }

    public Sql fails(String expectedMsgPattern) {
      if (expression) {
        getTester().checkExpFails(sql, expectedMsgPattern);
      } else {
        getTester().checkFails(sql, false, expectedMsgPattern);
      }
      return this;
    }

    public Sql node(Matcher<SqlNode> matcher) {
      getTester().checkNode(sql, matcher);
      return this;
    }

    /** Flags that this is an expression, not a whole query. */
    public Sql expression() {
      return expression ? this : new Sql(sql, true);
    }

    /** Removes the carets from the SQL string. Useful if you want to run
     * a test once at a conformance level where it fails, then run it again
     * at a conformance level where it succeeds. */
    public Sql sansCarets() {
      return new Sql(sql.replace("^", ""), expression);
    }
  }

  /** Helper class for building fluent code,
   * similar to {@link Sql}, but used to manipulate
   * a list of statements, such as
   * {@code sqlList("select * from a;").ok();}. */
  protected class SqlList {
    private final String sql;

    SqlList(String sql) {
      this.sql = sql;
    }

    public SqlList ok(String... expected) {
      getTester().checkList(sql, ImmutableList.copyOf(expected));
      return this;
    }

    public SqlList fails(String expectedMsgPattern) {
      getTester().checkFails(sql, true, expectedMsgPattern);
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
      SqlParserTest.this.checkExp(
          sql.replace("$op", op).replace("$p", period),
          expected.replace("$op", op.toUpperCase(Locale.ROOT)));
    }

    public void checkExpFails(String sql, String expected) {
      SqlParserTest.this.checkExpFails(
          sql.replace("$op", op).replace("$p", period),
          expected.replace("$op", op));
    }
  }
}

// End SqlParserTest.java
