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
package org.apache.calcite.sql.dml;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * A <code>SqlLoadData</code> is a node of a parse tree which represents an LOAD DATA
 * statement.
 */
public class SqlLoadData extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("LOAD DATA", SqlKind.LOAD_DATA);

  boolean local;
  private final SqlNode filepath;
  boolean overwrite;
  private final SqlIdentifier name;

  public SqlLoadData(SqlParserPos pos, boolean local, SqlNode filepath,
        boolean overwrite, SqlIdentifier name) {
    super(pos);
    this.local = local;
    this.filepath = filepath;
    this.overwrite = overwrite;
    this.name = name;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(filepath, name);
  }

  @Override  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getLocal() ? "LOAD DATA LOCAL" : "LOAD DATA");
    writer.keyword("INFILE");
    filepath.unparse(writer, 0, 0);
    writer.keyword(getOverwrite() ? "OVERWRITE INTO TABLE" : "INTO TABLE");
    name.unparse(writer, 0, 0);
  }

  public boolean getLocal() {
    return local;
  }

  public boolean getOverwrite() {
    return overwrite;
  }
}

// End SqlLoadData.java
