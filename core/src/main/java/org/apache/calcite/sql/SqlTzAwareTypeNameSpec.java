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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BodoTZInfo;
import org.apache.calcite.sql.type.TZAwareSqlType;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

import java.util.Objects;

/**
 * A sql type name specification of a timezone aware sql type.
 *
 */
public class SqlTzAwareTypeNameSpec extends SqlTypeNameSpec {

  public final BodoTZInfo origTz;


  /**
   * Creates a {@code SqlTzAwareTypeNameSpec} from an existing TZAwareSqlType.
   *
   * @param type The TZAwareSqlType
   */
  public SqlTzAwareTypeNameSpec(final TZAwareSqlType type) {
    super(Objects.requireNonNull(type.getSqlIdentifier()), SqlParserPos.ZERO);
    this.origTz = type.getTZInfo();
  }


  /**
   * Creates a {@code SqlTzAwareTypeNameSpec}.
   *
   * @param name Name of the type, must not be null
   * @param info TzInfo of the type, must not be null
   * @param pos  Parser position, must not be null
   */
  SqlTzAwareTypeNameSpec(SqlIdentifier name, BodoTZInfo info, SqlParserPos pos) {
    super(name, pos);
    this.origTz = info;
  }

  @Override public RelDataType deriveType(final SqlValidator validator) {
    return validator.getTypeFactory().createTZAwareSqlType(origTz);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    writer.keyword("TIMESTAMP(" + this.origTz.getPyZone() + ")");
  }

  @Override public boolean equalsDeep(final SqlTypeNameSpec spec, final Litmus litmus) {
    if (!(spec instanceof SqlTzAwareTypeNameSpec)) {
      return false;
    }
    return this.origTz == ((SqlTzAwareTypeNameSpec) spec).origTz;
  }

}
