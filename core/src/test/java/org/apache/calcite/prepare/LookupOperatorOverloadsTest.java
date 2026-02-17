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
package org.apache.calcite.prepare;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.util.Smalls;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.apache.calcite.sql.SqlFunctionCategory.MATCH_RECOGNIZE;
import static org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR;
import static org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION;
import static org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_PROCEDURE;
import static org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_SPECIFIC_FUNCTION;
import static org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION;
import static org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_TABLE_SPECIFIC_FUNCTION;
import static org.apache.calcite.test.Matchers.isListOf;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import static java.util.Objects.requireNonNull;

/**
 * Test for lookupOperatorOverloads() in {@link CalciteCatalogReader}.
 */
class LookupOperatorOverloadsTest {

  private void checkFunctionType(int size, @Nullable String name,
      List<SqlOperator> operatorList) {
    assertThat(size, is(operatorList.size()));

    for (SqlOperator op : operatorList) {
      assertThat(op, instanceOf(SqlUserDefinedTableFunction.class));
      assertThat(op.getName(), is(name));
    }
  }

  private static void check(List<SqlFunctionCategory> actuals,
      SqlFunctionCategory... expecteds) {
    assertThat(actuals, isListOf(expecteds));
  }

  @Test void testIsUserDefined() {
    List<SqlFunctionCategory> cats = new ArrayList<>();
    for (SqlFunctionCategory c : SqlFunctionCategory.values()) {
      if (c.isUserDefined()) {
        cats.add(c);
      }
    }
    check(cats, USER_DEFINED_FUNCTION, USER_DEFINED_PROCEDURE,
        USER_DEFINED_CONSTRUCTOR, USER_DEFINED_SPECIFIC_FUNCTION,
        USER_DEFINED_TABLE_FUNCTION, USER_DEFINED_TABLE_SPECIFIC_FUNCTION);
  }

  @Test void testIsTableFunction() {
    List<SqlFunctionCategory> cats = new ArrayList<>();
    for (SqlFunctionCategory c : SqlFunctionCategory.values()) {
      if (c.isTableFunction()) {
        cats.add(c);
      }
    }
    check(cats, USER_DEFINED_TABLE_FUNCTION,
        USER_DEFINED_TABLE_SPECIFIC_FUNCTION, MATCH_RECOGNIZE);
  }

  @Test void testIsSpecific() {
    List<SqlFunctionCategory> cats = new ArrayList<>();
    for (SqlFunctionCategory c : SqlFunctionCategory.values()) {
      if (c.isSpecific()) {
        cats.add(c);
      }
    }
    check(cats, USER_DEFINED_SPECIFIC_FUNCTION,
        USER_DEFINED_TABLE_SPECIFIC_FUNCTION);
  }

  @Test void testIsUserDefinedNotSpecificFunction() {
    List<SqlFunctionCategory> cats = new ArrayList<>();
    for (SqlFunctionCategory sqlFunctionCategory : SqlFunctionCategory.values()) {
      if (sqlFunctionCategory.isUserDefinedNotSpecificFunction()) {
        cats.add(sqlFunctionCategory);
      }
    }
    check(cats, USER_DEFINED_FUNCTION, USER_DEFINED_TABLE_FUNCTION);
  }

  @Test void testLookupCaseSensitively() throws SQLException {
    checkInternal(true);
  }

  @Test void testLookupCaseInSensitively() throws SQLException {
    checkInternal(false);
  }

  // Look up MyCatalog.MySchema.MyFUNC using a lowercase 3-part identifier.
  @Test void testLookupQualifiedNameUsesResolvedCase() {
    final String catalogName = "MyCatalog";
    final String schemaName = "MySchema";
    final String funcName = "MyFUNC";
    final CalciteSchema root =
        CalciteSchema.createRootSchema(false, false, catalogName);
    final CalciteSchema schema = root.add(schemaName, new AbstractSchema());
    final TableFunction table =
        requireNonNull(TableFunctionImpl.create(Smalls.MAZE_METHOD));
    schema.plus().add(funcName, table);

    final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final Properties properties = new Properties();
    properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    final CalciteCatalogReader reader =
        new CalciteCatalogReader(root, ImmutableList.of(), typeFactory,
            new CalciteConnectionConfigImpl(properties));

    final List<SqlOperator> operatorList = new ArrayList<>();
    final SqlIdentifier lowercaseIdentifier =
        new SqlIdentifier(
            Lists.newArrayList(catalogName.toLowerCase(Locale.ROOT),
            schemaName.toLowerCase(Locale.ROOT), funcName.toLowerCase(Locale.ROOT)),
            null, SqlParserPos.ZERO, null);
    reader.lookupOperatorOverloads(lowercaseIdentifier,
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION,
        operatorList, SqlNameMatchers.withCaseSensitive(false));

    checkFunctionType(1, funcName, operatorList);
    assertThat(operatorList.get(0).getNameAsId().names,
        isListOf(catalogName, schemaName, funcName));
  }

  // Look up MySchema.MyFUNC using a lowercase 2-part identifier.
  @Test void testLookupPartiallyQualifiedNameUsesResolvedCase() {
    final String catalogName = "MyCatalog";
    final String schemaName = "MySchema";
    final String funcName = "MyFUNC";
    final CalciteSchema root =
        CalciteSchema.createRootSchema(false, false, catalogName);
    final CalciteSchema schema = root.add(schemaName, new AbstractSchema());
    final TableFunction table =
        requireNonNull(TableFunctionImpl.create(Smalls.MAZE_METHOD));
    schema.plus().add(funcName, table);

    final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final Properties properties = new Properties();
    properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    final CalciteCatalogReader reader =
        new CalciteCatalogReader(root, ImmutableList.of(), typeFactory,
            new CalciteConnectionConfigImpl(properties));

    final List<SqlOperator> operatorList = new ArrayList<>();
    final SqlIdentifier lowercaseIdentifier =
        new SqlIdentifier(
            Lists.newArrayList(schemaName.toLowerCase(Locale.ROOT),
            funcName.toLowerCase(Locale.ROOT)),
            null, SqlParserPos.ZERO, null);
    reader.lookupOperatorOverloads(lowercaseIdentifier,
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION,
        operatorList, SqlNameMatchers.withCaseSensitive(false));

    checkFunctionType(1, funcName, operatorList);
    assertThat(operatorList.get(0).getNameAsId().names,
        isListOf(schemaName, funcName));
  }

  // Look up myfunc when both MyFUNC and myfunc exist in the same schema.
  @Test void testLookupCaseInsensitiveUsesEachMatchedFunctionName() {
    final String schemaName = "MySchema";
    final String upperFuncName = "MyFUNC";
    final String lowerFuncName = "myfunc";
    final CalciteSchema root = CalciteSchema.createRootSchema(false, true);
    final CalciteSchema schema = root.add(schemaName, new AbstractSchema());
    final TableFunction table =
        requireNonNull(TableFunctionImpl.create(Smalls.MAZE_METHOD));
    schema.plus().add(upperFuncName, table);
    schema.plus().add(lowerFuncName, table);

    final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final Properties properties = new Properties();
    properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    final CalciteCatalogReader reader =
        new CalciteCatalogReader(root, ImmutableList.of(), typeFactory,
            new CalciteConnectionConfigImpl(properties));

    final List<SqlOperator> operatorList = new ArrayList<>();
    final SqlIdentifier lowercaseIdentifier =
        new SqlIdentifier(
            Lists.newArrayList(schemaName.toLowerCase(Locale.ROOT),
            lowerFuncName),
            null, SqlParserPos.ZERO, null);
    reader.lookupOperatorOverloads(lowercaseIdentifier,
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION,
        operatorList, SqlNameMatchers.withCaseSensitive(false));

    assertThat(operatorList, hasSize(2));
    boolean hasUpperName = false;
    boolean hasLowerName = false;
    for (SqlOperator operator : operatorList) {
      if (operator.getNameAsId().names.equals(Lists.newArrayList(schemaName, upperFuncName))) {
        hasUpperName = true;
      }
      if (operator.getNameAsId().names.equals(Lists.newArrayList(schemaName, lowerFuncName))) {
        hasLowerName = true;
      }
    }
    assertThat(hasUpperName, is(true));
    assertThat(hasLowerName, is(true));
  }

  // Example: lookup "myschema.myfunc" against a dynamic schema and resolve it
  // to "MySchema.MyFUNC" (fresh function instances on each lookup).
  @Test void testLookupImplicitFunctionUsesResolvedCase() {
    final String schemaName = "MySchema";
    final String funcName = "MyFUNC";
    final CalciteSchema root = CalciteSchema.createRootSchema(false, true);
    root.add(schemaName, new AbstractSchema() {
      @Override protected Multimap<String, Function> getFunctionMultimap() {
        // Return fresh instances to mimic dynamic schemas that do not preserve
        // function identity across lookups.
        return ImmutableMultimap.of(funcName,
            requireNonNull(TableFunctionImpl.create(Smalls.MAZE_METHOD)));
      }
    });

    final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    final Properties properties = new Properties();
    properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    final CalciteCatalogReader reader =
        new CalciteCatalogReader(root, ImmutableList.of(), typeFactory,
            new CalciteConnectionConfigImpl(properties));

    final List<SqlOperator> operatorList = new ArrayList<>();
    final SqlIdentifier lowercaseIdentifier =
        new SqlIdentifier(
            Lists.newArrayList(schemaName.toLowerCase(Locale.ROOT),
            funcName.toLowerCase(Locale.ROOT)),
            null, SqlParserPos.ZERO, null);
    reader.lookupOperatorOverloads(lowercaseIdentifier,
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION,
        operatorList, SqlNameMatchers.withCaseSensitive(false));

    checkFunctionType(1, funcName, operatorList);
    assertThat(operatorList.get(0).getNameAsId().names, isListOf(schemaName, funcName));
  }

  private void checkInternal(boolean caseSensitive) throws SQLException {
    final SqlNameMatcher nameMatcher =
        SqlNameMatchers.withCaseSensitive(caseSensitive);
    final String schemaName = "MySchema";
    final String funcName = "MyFUNC";
    final String anotherName = "AnotherFunc";

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus schema = rootSchema.add(schemaName, new AbstractSchema());
      final TableFunction table =
          requireNonNull(TableFunctionImpl.create(Smalls.MAZE_METHOD));
      schema.add(funcName, table);
      schema.add(anotherName, table);
      final TableFunction table2 =
          requireNonNull(TableFunctionImpl.create(Smalls.MAZE3_METHOD));
      schema.add(funcName, table2);

      final CalciteServerStatement statement =
          connection.createStatement().unwrap(CalciteServerStatement.class);
      final CalcitePrepare.Context prepareContext =
          statement.createPrepareContext();
      final JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
      CalciteCatalogReader reader =
          new CalciteCatalogReader(prepareContext.getRootSchema(),
              ImmutableList.of(), typeFactory, prepareContext.config());

      final List<SqlOperator> operatorList = new ArrayList<>();
      SqlIdentifier myFuncIdentifier =
          new SqlIdentifier(Lists.newArrayList(schemaName, funcName), null,
              SqlParserPos.ZERO, null);
      reader.lookupOperatorOverloads(myFuncIdentifier,
          SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION,
          operatorList, nameMatcher);
      checkFunctionType(2, funcName, operatorList);

      operatorList.clear();
      reader.lookupOperatorOverloads(myFuncIdentifier,
          SqlFunctionCategory.USER_DEFINED_FUNCTION, SqlSyntax.FUNCTION,
          operatorList, nameMatcher);
      checkFunctionType(0, null, operatorList);

      operatorList.clear();
      SqlIdentifier anotherFuncIdentifier =
          new SqlIdentifier(Lists.newArrayList(schemaName, anotherName), null,
              SqlParserPos.ZERO, null);
      reader.lookupOperatorOverloads(anotherFuncIdentifier,
          SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION,
          operatorList, nameMatcher);
      checkFunctionType(1, anotherName, operatorList);
    }
  }
}
