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
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;

import static java.util.Objects.requireNonNull;

/**
 * Literal whose type is not yet known.
 */
public class SqlUnknownLiteral extends SqlLiteral {
  public final String tag;

  SqlUnknownLiteral(String tag, String value, SqlParserPos pos) {
    super(requireNonNull(value, "value"), SqlTypeName.UNKNOWN, pos);
    this.tag = requireNonNull(tag, "tag");
  }

  @Override public String getValue() {
    return (String) requireNonNull(super.getValue(), "value");
  }

  @Override public SqlLiteral clone(SqlParserPos pos) {
    return new SqlUnknownLiteral(tag, getValue(), pos);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final NlsString nlsString = new NlsString(getValue(), null, null);
    writer.keyword(tag);
    writer.literal(nlsString.asSql(true, true, writer.getDialect()));
  }


  /** Converts this unknown literal to a literal of known type. */
  public SqlLiteral resolve(SqlTypeName typeName) {
    switch (typeName) {
    case DATE:
      return SqlParserUtil.parseDateLiteral(getValue(), pos);
    case TIME:
      return SqlParserUtil.parseTimeLiteral(getValue(), pos);
    case TIME_TZ:
      return SqlParserUtil.parseTimeTzLiteral(getValue(), pos);
    case TIMESTAMP:
      return SqlParserUtil.parseTimestampLiteral(getValue(), pos);
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return SqlParserUtil.parseTimestampWithLocalTimeZoneLiteral(getValue(), pos);
    case TIMESTAMP_TZ:
      return SqlParserUtil.parseTimestampTzLiteral(getValue(), pos);
    default:
      throw Util.unexpected(typeName);
    }
  }
}
