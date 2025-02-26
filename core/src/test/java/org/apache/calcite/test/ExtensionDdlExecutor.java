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
package org.apache.calcite.test;

import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.parserextensiontesting.ExtensionSqlCreateTable;
import org.apache.calcite.sql.parser.parserextensiontesting.ExtensionSqlParserImpl;

import java.io.Reader;
import java.util.function.BiConsumer;

/** Executes the few DDL commands supported by
 * {@link ExtensionSqlParserImpl}. */
public class ExtensionDdlExecutor extends MockDdlExecutor {
  static final ExtensionDdlExecutor INSTANCE = new ExtensionDdlExecutor();

  /** Parser factory. */
  @SuppressWarnings("unused") // used via reflection
  public static final SqlParserImplFactory PARSER_FACTORY =
      new SqlParserImplFactory() {
        @Override public SqlAbstractParserImpl getParser(Reader stream) {
          return ExtensionSqlParserImpl.FACTORY.getParser(stream);
        }

        @Override public DdlExecutor getDdlExecutor() {
          return ExtensionDdlExecutor.INSTANCE;
        }
      };

  @Override protected void forEachNameType(SqlCreateTable createTable, BiConsumer<SqlIdentifier,
      SqlDataTypeSpec> consumer) {
    ((ExtensionSqlCreateTable) createTable).forEachNameType(consumer);
  }
}
