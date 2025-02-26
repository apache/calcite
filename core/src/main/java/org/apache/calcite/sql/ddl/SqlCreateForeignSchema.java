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
package org.apache.calcite.sql.ddl;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * Parse tree for {@code CREATE FOREIGN SCHEMA} statement.
 */
public class SqlCreateForeignSchema extends SqlCreate {
  public final SqlIdentifier name;
  public final @Nullable SqlNode type;
  public final @Nullable SqlNode library;
  private final @Nullable SqlNodeList optionList;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE FOREIGN SCHEMA",
          SqlKind.CREATE_FOREIGN_SCHEMA);

  /** Creates a SqlCreateForeignSchema. */
  SqlCreateForeignSchema(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, @Nullable SqlNode type, @Nullable SqlNode library,
      @Nullable SqlNodeList optionList) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = requireNonNull(name, "name");
    this.type = type;
    this.library = library;
    checkArgument((type == null) != (library == null),
        "of type and library, exactly one must be specified");
    this.optionList = optionList; // may be null
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, type, library, optionList);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (getReplace()) {
      writer.keyword("CREATE OR REPLACE");
    } else {
      writer.keyword("CREATE");
    }
    writer.keyword("FOREIGN SCHEMA");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (library != null) {
      writer.keyword("LIBRARY");
      library.unparse(writer, 0, 0);
    }
    if (type != null) {
      writer.keyword("TYPE");
      type.unparse(writer, 0, 0);
    }
    if (optionList != null) {
      writer.keyword("OPTIONS");
      SqlWriter.Frame frame = writer.startList("(", ")");
      int i = 0;
      for (Pair<SqlIdentifier, SqlNode> c : options()) {
        if (i++ > 0) {
          writer.sep(",");
        }
        c.left.unparse(writer, 0, 0);
        c.right.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
  }

  /** Returns options as a list of (name, value) pairs. */
  public List<Pair<SqlIdentifier, SqlNode>> options() {
    return options(optionList);
  }

  private static List<Pair<SqlIdentifier, SqlNode>> options(
      final @Nullable SqlNodeList optionList) {
    if (optionList == null) {
      return ImmutableList.of();
    }
    return new AbstractList<Pair<SqlIdentifier, SqlNode>>() {
      @Override public Pair<SqlIdentifier, SqlNode> get(int index) {
        return Pair.of((SqlIdentifier) castNonNull(optionList.get(index * 2)),
            castNonNull(optionList.get(index * 2 + 1)));
      }

      @Override public int size() {
        return optionList.size() / 2;
      }
    };
  }
}
