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
package org.apache.calcite.sql.babel;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code CREATE TABLE} statement, with extensions for particular
 * SQL dialects supported by Babel.
 */
public class SqlBabelCreateTable extends SqlCreateTable {
  private final TableCollectionType tableCollectionType;
  // CHECKSTYLE: IGNORE 2; can't use 'volatile' because it is a Java keyword
  // but checkstyle does not like trailing '_'.
  private final boolean volatile_;

  /** Creates a SqlBabelCreateTable. */
  public SqlBabelCreateTable(SqlParserPos pos, boolean replace,
      TableCollectionType tableCollectionType, boolean volatile_,
      boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList,
      SqlNode query) {
    super(pos, replace, ifNotExists, name, columnList, query);
    this.tableCollectionType = tableCollectionType;
    this.volatile_ = volatile_;
  }

  @Override public void validate(final SqlValidator validator, final SqlValidatorScope scope) {
    // Validate the clauses that are specific to Babel's create table statement,
    // and then defers to the superclass for the rest
    if (this.volatile_) {
      throw validator.newValidationError(
          this, RESOURCE.createTableUnsupportedClause("VOLATILE"));
    }

    switch (this.tableCollectionType) {
    case SET:
      throw validator.newValidationError(
          this, RESOURCE.createTableUnsupportedClause("SET"));
    case MULTISET:
      throw validator.newValidationError(
          this, RESOURCE.createTableUnsupportedClause("MULTISET"));
    default:
      //do nothing
    }

    super.validate(validator, scope);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    switch (tableCollectionType) {
    case SET:
      writer.keyword("SET");
      break;
    case MULTISET:
      writer.keyword("MULTISET");
      break;
    default:
      break;
    }
    if (volatile_) {
      writer.keyword("VOLATILE");
    }
    writer.keyword("TABLE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }

    this.getName().unparse(writer, leftPrec, rightPrec);
    @Nullable SqlNodeList columnList = getcolumnList();
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    SqlNode query = getQuery();
    if (query != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, 0, 0);
    }
  }
}
