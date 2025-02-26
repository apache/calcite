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

import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * A SQL literal representing an UUID value, for example <code>UUID
 * '123e4567-e89b-12d3-a456-426655440000'</code>.
 *
 * <p>Create values using {@link SqlLiteral#createUuid}.
 */
public class SqlUuidLiteral extends SqlLiteral {
  //~ Constructors -----------------------------------------------------------

  public SqlUuidLiteral(UUID t, SqlParserPos pos) {
    super(t, SqlTypeName.UUID, pos);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlUuidLiteral clone(SqlParserPos pos) {
    return new SqlUuidLiteral(getUuid(), pos);
  }

  public UUID getUuid() {
    return (UUID) requireNonNull(value, "value");
  }

  @Override public String toString() {
    return "UUID '" + toFormattedString() + "'";
  }

  public String toFormattedString() {
    return getUuid().toString();
  }

  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.literal(this.toString());
  }
}
