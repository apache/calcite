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
package org.apache.calcite.sql.parser.parserextensiontesting;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.test.JdbcTest;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Simple test example of a CREATE TABLE statement.
 */
public class SqlCreateTable extends SqlCreate
    implements SqlExecutableStatement {
  private final SqlIdentifier name;
  private final SqlNodeList columnList;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

  /** Creates a SqlCreateTable. */
  public SqlCreateTable(SqlParserPos pos, SqlIdentifier name,
      SqlNodeList columnList) {
    super(OPERATOR, pos, false, false);
    this.name = name;
    this.columnList = columnList;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, columnList);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    name.unparse(writer, leftPrec, rightPrec);
    SqlWriter.Frame frame = writer.startList("(", ")");
    for (Pair<SqlIdentifier, SqlDataTypeSpec> pair : nameTypes()) {
      writer.sep(",");
      pair.left.unparse(writer, leftPrec, rightPrec); // name
      pair.right.unparse(writer, leftPrec, rightPrec); // type
      if (Boolean.FALSE.equals(pair.right.getNullable())) {
        writer.keyword("NOT NULL");
      }
    }
    writer.endList(frame);
  }

  /** Creates a list of (name, type) pairs from {@link #columnList}, in which
   * they alternate. */
  private List<Pair<SqlIdentifier, SqlDataTypeSpec>> nameTypes() {
    final List list = columnList.getList();
    //noinspection unchecked
    return Pair.zip((List<SqlIdentifier>) Util.quotientList(list, 2, 0),
        Util.quotientList((List<SqlDataTypeSpec>) list, 2, 1));
  }

  public void execute(CalcitePrepare.Context context) {
    final List<String> path = context.getDefaultSchemaPath();
    CalciteSchema schema = context.getRootSchema();
    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    final SqlValidator validator = new ContextSqlValidator(context, false);
    for (Pair<SqlIdentifier, SqlDataTypeSpec> pair : nameTypes()) {
      builder.add(pair.left.getSimple(),
          pair.right.deriveType(validator, true));
    }
    final RelDataType rowType = builder.build();
    schema.add(name.getSimple(),
        new MutableArrayTable(name.getSimple(),
            RelDataTypeImpl.proto(rowType)));
  }

  /** Table backed by a Java list. */
  private static class MutableArrayTable
      extends JdbcTest.AbstractModifiableTable {
    final List list = new ArrayList();
    private final RelProtoDataType protoRowType;

    MutableArrayTable(String name, RelProtoDataType protoRowType) {
      super(name);
      this.protoRowType = protoRowType;
    }

    public Collection getModifiableCollection() {
      return list;
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      return new AbstractTableQueryable<T>(queryProvider, schema, this,
          tableName) {
        public Enumerator<T> enumerator() {
          //noinspection unchecked
          return (Enumerator<T>) Linq4j.enumerator(list);
        }
      };
    }

    public Type getElementType() {
      return Object[].class;
    }

    public Expression getExpression(SchemaPlus schema, String tableName,
        Class clazz) {
      return Schemas.tableExpression(schema, getElementType(),
          tableName, clazz);
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }
  }
}

// End SqlCreateTable.java
