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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A SqlArrayWithAngleBracketsNameSpec to parse or unparse SQL ARRAY type to {@code ARRAY<VARCHAR>}.
 */
public class SqlArrayWithAngleBracketsNameSpec extends SqlCollectionTypeNameSpec {

  /**
   * Creates a {@code SqlArrayWithAngleBracketsNameSpec}.
   *
   * @param elementTypeName    Type of the collection element
   * @param collectionTypeName Collection type name
   * @param pos                Parser position, must not be null
   */
  public SqlArrayWithAngleBracketsNameSpec(SqlTypeNameSpec elementTypeName,
      SqlTypeName collectionTypeName, SqlParserPos pos) {
    super(elementTypeName, collectionTypeName, pos);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ARRAY");
    SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "<", ">");
    this.getElementTypeName().unparse(writer, leftPrec, rightPrec);
    writer.endList(frame);
  }

}
