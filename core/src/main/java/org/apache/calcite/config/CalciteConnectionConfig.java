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
package org.apache.calcite.config;

import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.model.JsonSchema;
import org.apache.calcite.sql.validate.SqlConformance;

import java.util.Properties;

/** Interface for reading connection properties within Calcite code. There is
 * a method for every property. At some point there will be similar config
 * classes for system and statement properties. */
public interface CalciteConnectionConfig extends ConnectionConfig {
  /** Default configuration. */
  CalciteConnectionConfigImpl DEFAULT =
      new CalciteConnectionConfigImpl(new Properties());
  /** Returns the value of
   * {@link CalciteConnectionProperty#APPROXIMATE_DISTINCT_COUNT}. */
  boolean approximateDistinctCount();
  /** Returns the value of
   * {@link CalciteConnectionProperty#APPROXIMATE_TOP_N}. */
  boolean approximateTopN();
  /** Returns the value of
   * {@link CalciteConnectionProperty#APPROXIMATE_DECIMAL}. */
  boolean approximateDecimal();
  /** Returns the value of
   * {@link CalciteConnectionProperty#NULL_EQUAL_TO_EMPTY}. */
  boolean nullEqualToEmpty();
  /** Returns the value of
   * {@link CalciteConnectionProperty#AUTO_TEMP}. */
  boolean autoTemp();
  /** Returns the value of
   * {@link CalciteConnectionProperty#MATERIALIZATIONS_ENABLED}. */
  boolean materializationsEnabled();
  /** Returns the value of
   * {@link CalciteConnectionProperty#CREATE_MATERIALIZATIONS}. */
  boolean createMaterializations();
  /** Returns the value of
   * {@link CalciteConnectionProperty#DEFAULT_NULL_COLLATION}. */
  NullCollation defaultNullCollation();
  /** Returns the value of {@link CalciteConnectionProperty#FUN}. */
  <T> T fun(Class<T> operatorTableClass, T defaultOperatorTable);
  /** Returns the value of {@link CalciteConnectionProperty#MODEL}. */
  String model();
  /** Returns the value of {@link CalciteConnectionProperty#LEX}. */
  Lex lex();
  /** Returns the value of {@link CalciteConnectionProperty#QUOTING}. */
  Quoting quoting();
  /** Returns the value of {@link CalciteConnectionProperty#UNQUOTED_CASING}. */
  Casing unquotedCasing();
  /** Returns the value of {@link CalciteConnectionProperty#QUOTED_CASING}. */
  Casing quotedCasing();
  /** Returns the value of {@link CalciteConnectionProperty#CASE_SENSITIVE}. */
  boolean caseSensitive();
  /** Returns the value of {@link CalciteConnectionProperty#PARSER_FACTORY}. */
  <T> T parserFactory(Class<T> parserFactoryClass, T defaultParserFactory);
  /** Returns the value of {@link CalciteConnectionProperty#SCHEMA_FACTORY}. */
  <T> T schemaFactory(Class<T> schemaFactoryClass, T defaultSchemaFactory);
  /** Returns the value of {@link CalciteConnectionProperty#SCHEMA_TYPE}. */
  JsonSchema.Type schemaType();
  /** Returns the value of {@link CalciteConnectionProperty#SPARK}. */
  boolean spark();
  /** Returns the value of
   * {@link CalciteConnectionProperty#FORCE_DECORRELATE}. */
  boolean forceDecorrelate();
  /** Returns the value of {@link CalciteConnectionProperty#TYPE_SYSTEM}. */
  <T> T typeSystem(Class<T> typeSystemClass, T defaultTypeSystem);
  /** Returns the value of {@link CalciteConnectionProperty#CONFORMANCE}. */
  SqlConformance conformance();
  /** Returns the value of {@link CalciteConnectionProperty#TIME_ZONE}. */
  @Override String timeZone();
  /** Returns the value of {@link CalciteConnectionProperty#LOCALE}. */
  String locale();
  /** Returns the value of {@link CalciteConnectionProperty#TYPE_COERCION}. */
  boolean typeCoercion();
  /** Returns the value of
   * {@link CalciteConnectionProperty#LENIENT_OPERATOR_LOOKUP}. */
  boolean lenientOperatorLookup();
  /** Returns the value of {@link CalciteConnectionProperty#TOPDOWN_OPT}. */
  boolean topDownOpt();
}
