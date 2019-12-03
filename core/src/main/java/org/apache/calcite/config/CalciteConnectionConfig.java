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

/** Interface for reading connection properties within Calcite code. There is
 * a method for every property. At some point there will be similar config
 * classes for system and statement properties. */
public interface CalciteConnectionConfig extends ConnectionConfig {
  /** @see CalciteConnectionProperty#APPROXIMATE_DISTINCT_COUNT */
  boolean approximateDistinctCount();
  /** @see CalciteConnectionProperty#APPROXIMATE_TOP_N */
  boolean approximateTopN();
  /** @see CalciteConnectionProperty#APPROXIMATE_DECIMAL */
  boolean approximateDecimal();
  /** @see CalciteConnectionProperty#NULL_EQUAL_TO_EMPTY */
  boolean nullEqualToEmpty();
  /** @see CalciteConnectionProperty#AUTO_TEMP */
  boolean autoTemp();
  /** @see CalciteConnectionProperty#MATERIALIZATIONS_ENABLED */
  boolean materializationsEnabled();
  /** @see CalciteConnectionProperty#CREATE_MATERIALIZATIONS */
  boolean createMaterializations();
  /** @see CalciteConnectionProperty#DEFAULT_NULL_COLLATION */
  NullCollation defaultNullCollation();
  /** @see CalciteConnectionProperty#FUN */
  <T> T fun(Class<T> operatorTableClass, T defaultOperatorTable);
  /** @see CalciteConnectionProperty#MODEL */
  String model();
  /** @see CalciteConnectionProperty#LEX */
  Lex lex();
  /** @see CalciteConnectionProperty#QUOTING */
  Quoting quoting();
  /** @see CalciteConnectionProperty#UNQUOTED_CASING */
  Casing unquotedCasing();
  /** @see CalciteConnectionProperty#QUOTED_CASING */
  Casing quotedCasing();
  /** @see CalciteConnectionProperty#CASE_SENSITIVE */
  boolean caseSensitive();
  /** @see CalciteConnectionProperty#PARSER_FACTORY */
  <T> T parserFactory(Class<T> parserFactoryClass, T defaultParserFactory);
  /** @see CalciteConnectionProperty#SCHEMA_FACTORY */
  <T> T schemaFactory(Class<T> schemaFactoryClass, T defaultSchemaFactory);
  /** @see CalciteConnectionProperty#SCHEMA_TYPE */
  JsonSchema.Type schemaType();
  /** @see CalciteConnectionProperty#SPARK */
  boolean spark();
  /** @see CalciteConnectionProperty#FORCE_DECORRELATE */
  boolean forceDecorrelate();
  /** @see CalciteConnectionProperty#TYPE_SYSTEM */
  <T> T typeSystem(Class<T> typeSystemClass, T defaultTypeSystem);
  /** @see CalciteConnectionProperty#CONFORMANCE */
  SqlConformance conformance();
  /** @see CalciteConnectionProperty#TIME_ZONE */
  @Override String timeZone();
  /** @see CalciteConnectionProperty#LOCALE */
  String locale();
  /** @see CalciteConnectionProperty#TYPE_COERCION */
  boolean typeCoercion();
}

// End CalciteConnectionConfig.java
