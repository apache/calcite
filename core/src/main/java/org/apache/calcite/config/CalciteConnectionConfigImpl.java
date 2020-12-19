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

import org.apache.calcite.avatica.ConnectionConfigImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.model.JsonSchema;
import org.apache.calcite.runtime.ConsList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.util.List;
import java.util.Properties;

/** Implementation of {@link CalciteConnectionConfig}. */
public class CalciteConnectionConfigImpl extends ConnectionConfigImpl
    implements CalciteConnectionConfig {
  public CalciteConnectionConfigImpl(Properties properties) {
    super(properties);
  }

  /** Returns a copy of this configuration with one property changed.
   *
   * <p>Does not modify this configuration. */
  public CalciteConnectionConfigImpl set(CalciteConnectionProperty property,
      String value) {
    final Properties newProperties = (Properties) properties.clone();
    newProperties.setProperty(property.camelName(), value);
    return new CalciteConnectionConfigImpl(newProperties);
  }

  /** Returns a copy of this configuration with the value of a property
   * removed.
   *
   * <p>Does not modify this configuration. */
  public CalciteConnectionConfigImpl unset(CalciteConnectionProperty property) {
    final Properties newProperties = (Properties) properties.clone();
    newProperties.remove(property.camelName());
    return new CalciteConnectionConfigImpl(newProperties);
  }

  /** Returns whether a given property has been assigned a value.
   *
   * <p>If not, the value returned for the property will be its default value.
   */
  public boolean isSet(CalciteConnectionProperty property) {
    return properties.containsKey(property.camelName());
  }

  @Override public boolean approximateDistinctCount() {
    return CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT.wrap(properties)
        .getBoolean();
  }

  @Override public boolean approximateTopN() {
    return CalciteConnectionProperty.APPROXIMATE_TOP_N.wrap(properties)
        .getBoolean();
  }

  @Override public boolean approximateDecimal() {
    return CalciteConnectionProperty.APPROXIMATE_DECIMAL.wrap(properties)
        .getBoolean();
  }

  @Override public boolean nullEqualToEmpty() {
    return CalciteConnectionProperty.NULL_EQUAL_TO_EMPTY.wrap(properties).getBoolean();
  }

  @Override public boolean autoTemp() {
    return CalciteConnectionProperty.AUTO_TEMP.wrap(properties).getBoolean();
  }

  @Override public boolean materializationsEnabled() {
    return CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.wrap(properties)
        .getBoolean();
  }

  @Override public boolean createMaterializations() {
    return CalciteConnectionProperty.CREATE_MATERIALIZATIONS.wrap(properties)
        .getBoolean();
  }

  @Override public NullCollation defaultNullCollation() {
    return CalciteConnectionProperty.DEFAULT_NULL_COLLATION.wrap(properties)
        .getEnum(NullCollation.class, NullCollation.HIGH);
  }

  @Override public <T> @PolyNull T fun(Class<T> operatorTableClass,
      @PolyNull T defaultOperatorTable) {
    final String fun =
        CalciteConnectionProperty.FUN.wrap(properties).getString();
    if (fun == null || fun.equals("") || fun.equals("standard")) {
      return defaultOperatorTable;
    }
    final List<SqlLibrary> libraryList = SqlLibrary.parse(fun);
    final SqlOperatorTable operatorTable =
            SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                ConsList.of(SqlLibrary.STANDARD, libraryList));
    return operatorTableClass.cast(operatorTable);
  }

  @Override public @Nullable String model() {
    return CalciteConnectionProperty.MODEL.wrap(properties).getString();
  }

  @Override public Lex lex() {
    return CalciteConnectionProperty.LEX.wrap(properties).getEnum(Lex.class);
  }

  @Override public Quoting quoting() {
    return CalciteConnectionProperty.QUOTING.wrap(properties)
        .getEnum(Quoting.class, lex().quoting);
  }

  @Override public Casing unquotedCasing() {
    return CalciteConnectionProperty.UNQUOTED_CASING.wrap(properties)
        .getEnum(Casing.class, lex().unquotedCasing);
  }

  @Override public Casing quotedCasing() {
    return CalciteConnectionProperty.QUOTED_CASING.wrap(properties)
        .getEnum(Casing.class, lex().quotedCasing);
  }

  @Override public boolean caseSensitive() {
    return CalciteConnectionProperty.CASE_SENSITIVE.wrap(properties)
        .getBoolean(lex().caseSensitive);
  }

  @Override public <T> @PolyNull T parserFactory(Class<T> parserFactoryClass,
      @PolyNull T defaultParserFactory) {
    return CalciteConnectionProperty.PARSER_FACTORY.wrap(properties)
        .getPlugin(parserFactoryClass, defaultParserFactory);
  }

  @Override public <T> @PolyNull T schemaFactory(Class<T> schemaFactoryClass,
      @PolyNull T defaultSchemaFactory) {
    return CalciteConnectionProperty.SCHEMA_FACTORY.wrap(properties)
        .getPlugin(schemaFactoryClass, defaultSchemaFactory);
  }

  @Override public JsonSchema.Type schemaType() {
    return CalciteConnectionProperty.SCHEMA_TYPE.wrap(properties)
        .getEnum(JsonSchema.Type.class);
  }

  @Override public boolean spark() {
    return CalciteConnectionProperty.SPARK.wrap(properties).getBoolean();
  }

  @Override public boolean forceDecorrelate() {
    return CalciteConnectionProperty.FORCE_DECORRELATE.wrap(properties)
        .getBoolean();
  }

  @Override public <T> @PolyNull T typeSystem(Class<T> typeSystemClass,
      @PolyNull T defaultTypeSystem) {
    return CalciteConnectionProperty.TYPE_SYSTEM.wrap(properties)
        .getPlugin(typeSystemClass, defaultTypeSystem);
  }

  @Override public SqlConformance conformance() {
    return CalciteConnectionProperty.CONFORMANCE.wrap(properties)
        .getEnum(SqlConformanceEnum.class);
  }

  @Override public String timeZone() {
    return CalciteConnectionProperty.TIME_ZONE.wrap(properties)
            .getString();
  }

  @Override public String locale() {
    return CalciteConnectionProperty.LOCALE.wrap(properties)
        .getString();
  }

  @Override public boolean typeCoercion() {
    return CalciteConnectionProperty.TYPE_COERCION.wrap(properties)
        .getBoolean();
  }

  @Override public boolean lenientOperatorLookup() {
    return CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP.wrap(properties)
        .getBoolean();
  }

  @Override public boolean topDownOpt() {
    return CalciteConnectionProperty.TOPDOWN_OPT.wrap(properties).getBoolean();
  }
}
