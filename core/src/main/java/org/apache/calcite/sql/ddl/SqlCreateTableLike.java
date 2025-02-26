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
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.Symbolizable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Parse tree for {@code CREATE TABLE LIKE} statement.
 */
public class SqlCreateTableLike extends SqlCreate {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TABLE LIKE", SqlKind.CREATE_TABLE_LIKE);

  /**
   * The LikeOption specify which additional properties of the original table to copy.
   */
  public enum LikeOption implements Symbolizable {
    ALL,
    DEFAULTS,
    GENERATED
  }

  public final SqlIdentifier name;
  public final SqlIdentifier sourceTable;
  public final SqlNodeList includingOptions;
  public final SqlNodeList excludingOptions;


  public SqlCreateTableLike(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, SqlIdentifier sourceTable,
      SqlNodeList includingOptions, SqlNodeList excludingOptions) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = name;
    this.sourceTable = sourceTable;
    this.includingOptions = includingOptions;
    this.excludingOptions = excludingOptions;

    // validate like options
    if (includingOptions.contains(LikeOption.ALL.symbol(SqlParserPos.ZERO))) {
      checkArgument(includingOptions.size() == 1 && excludingOptions.isEmpty(),
          "ALL cannot be used with other options");
    } else if (excludingOptions.contains(LikeOption.ALL.symbol(SqlParserPos.ZERO))) {
      checkArgument(excludingOptions.size() == 1 && includingOptions.isEmpty(),
          "ALL cannot be used with other options");
    }

    includingOptions.forEach(option ->
        checkArgument(!excludingOptions.contains(option),
            "Cannot include and exclude option %s at same time",
            option.toString()));
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, sourceTable, includingOptions,
        excludingOptions);
  }

  public Set<LikeOption> options() {
    return includingOptions.stream()
        .map(c -> ((SqlLiteral) c).symbolValue(LikeOption.class))
        .collect(Collectors.toSet());
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    writer.keyword("LIKE");
    sourceTable.unparse(writer, leftPrec, rightPrec);
    for (SqlNode c : new HashSet<>(includingOptions)) {
      LikeOption likeOption = ((SqlLiteral) c).getValueAs(LikeOption.class);
      writer.newlineAndIndent();
      writer.keyword("INCLUDING");
      writer.keyword(likeOption.name());
    }

    for (SqlNode c : new HashSet<>(excludingOptions)) {
      LikeOption likeOption = ((SqlLiteral) c).getValueAs(LikeOption.class);
      writer.newlineAndIndent();
      writer.keyword("EXCLUDING");
      writer.keyword(likeOption.name());
    }
  }
}
