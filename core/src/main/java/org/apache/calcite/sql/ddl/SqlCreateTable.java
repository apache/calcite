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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate {

  // These values should only be changed by calls to setOperand,
  // which should only be called during unconditional rewrite.
  private SqlIdentifier name;
  private @Nullable SqlNodeList columnList;
  private @Nullable SqlNode query;


  /* set during validation, null before that point */
  private @Nullable String outputTableName;
  private @Nullable Schema outputTableSchema;
  private @Nullable List<String> outputTableSchemaPath;

  protected CreateTableType createType;

  /** Enum describing the possible types of output table.
   * This should be reverted/moved to the bodo create table SqlNode
   * when we rewrite createRelationalAlgebraHandler to handle
   * DDL statements prior to SqlToRel conversion.
   */
  public enum CreateTableType {
    DEFAULT,
    TEMPORARY,
    TRANSIENT;

    //Helper function to convert the enum to string. Used in the BodoSQL repo when generating
    //text
    public String asStringKeyword() {
      switch (this) {
      case TEMPORARY:
        return "TEMPORARY";
      case TRANSIENT:
        return "TRANSIENT";
      case DEFAULT:
        return "";
      default:
        throw new RuntimeException("Reached unreachable code in CreateTableType.asStringKeyword");
      }
    }

  }

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

  /** Creates a SqlCreateTable. */
  protected SqlCreateTable(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name, @Nullable SqlNodeList columnList, @Nullable SqlNode query) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name, "name");
    this.columnList = columnList; // may be null
    this.query = query; // for "CREATE TABLE ... AS query"; may be null
    this.createType = CreateTableType.DEFAULT; // To handle CREATE [TEMPORARY/TRANSIENT/..] TABLE
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query);
  }

  @SuppressWarnings("assignment.type.incompatible")
  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      assert operand instanceof SqlIdentifier;
      name = (SqlIdentifier) operand;
      break;
    case 1:
      assert operand instanceof SqlNodeList;
      columnList = (SqlNodeList) operand;
      break;
    case 2:
      query = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    //Required to appease the Calcite null checker
    SqlNode queryNode = query;
    if (queryNode != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      queryNode.unparse(writer, 0, 0);
    }
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateCreateTable(this);
  }

  /**
   * Called once during validation to set the relevant fields. OutputTableName, outputTableSchema,
   * outputTableSchemaPath are all null before validation.
   */
  public void setValidationInformation(String outputTableName, Schema schema, List<String> path) {
    this.outputTableName = outputTableName;
    this.outputTableSchema = schema;
    this.outputTableSchemaPath = path;
  }

  public SqlIdentifier getName() {
    return name;
  }

  public @Nullable SqlNodeList getcolumnList() {
    return columnList;
  }
  public @Nullable SqlNode getQuery() {
    return query;
  }

  public @Nullable Schema getOutputTableSchema() {
    return outputTableSchema;
  }

  public @Nullable List<String> getOutputTableSchemaPath() {
    return outputTableSchemaPath;
  }

  public @Nullable String getOutputTableName() {
    return outputTableName;
  }

  public CreateTableType getCreateTableType() {
    return this.createType;
  }
}
