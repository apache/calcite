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

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.test.SqlTestFactory;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.UnaryOperator;

/**
 * Helper class for building fluent code,
 * similar to {@link SqlParserFixture}, but used to manipulate
 * a list of statements, such as
 * {@code sqlList("select * from a;").ok();}.
 */
class SqlParserListFixture {
  final SqlTestFactory factory;
  final SqlParserTest.Tester tester;
  final @Nullable SqlDialect dialect;
  final boolean convertToLinux;
  final StringAndPos sap;

  SqlParserListFixture(SqlTestFactory factory, SqlParserTest.Tester tester,
      @Nullable SqlDialect dialect, boolean convertToLinux,
      StringAndPos sap) {
    this.factory = factory;
    this.tester = tester;
    this.dialect = dialect;
    this.convertToLinux = convertToLinux;
    this.sap = sap;
  }

  public SqlParserListFixture ok(String... expected) {
    final UnaryOperator<String> converter = SqlParserTest.linux(convertToLinux);
    tester.checkList(factory, sap, dialect, converter,
        ImmutableList.copyOf(expected));
    return this;
  }

  public SqlParserListFixture fails(String expectedMsgPattern) {
    tester.checkFails(factory, sap, true, expectedMsgPattern);
    return this;
  }
}
