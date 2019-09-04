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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

/**
 * A sql type name specification of user defined type.
 *
 * <p>Usually you should register the UDT into the {@link org.apache.calcite.jdbc.CalciteSchema}
 * first before referencing it in the sql statement.
 *
 * <p>Internally we may new the {@code SqlUserDefinedTypeNameSpec} instance directly
 * for some sql dialects during rel-to-sql conversion.
 */
public class SqlUserDefinedTypeNameSpec extends SqlTypeNameSpec {

  /**
   * Create a SqlUserDefinedTypeNameSpec instance.
   * @param typeName Type name as SQL identifier.
   * @param pos The parser position.
   */
  public SqlUserDefinedTypeNameSpec(SqlIdentifier typeName, SqlParserPos pos) {
    super(typeName, pos);
  }

  public SqlUserDefinedTypeNameSpec(String name, SqlParserPos pos) {
    this(new SqlIdentifier(name, pos), pos);
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    // The type name is a compound identifier, that means it is a UDT,
    // use SqlValidator to deduce its type from the Schema.
    return validator.getValidatedNodeType(getTypeName());
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final String name = getTypeName().names.get(0);
    if (name.startsWith("_")) {
      // We're generating a type for an alien system. For example,
      // UNSIGNED is a built-in type in MySQL.
      // (Need a more elegant way than '_' of flagging this.)
      writer.keyword(name.substring(1));
    } else {
      getTypeName().unparse(writer, leftPrec, rightPrec);
    }
  }

  @Override public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
    if (!(spec instanceof SqlUserDefinedTypeNameSpec)) {
      return litmus.fail("{} != {}", this, spec);
    }
    SqlUserDefinedTypeNameSpec that = (SqlUserDefinedTypeNameSpec) spec;
    if (!this.getTypeName().equalsDeep(that.getTypeName(), litmus)) {
      return litmus.fail("{} != {}", this, spec);
    }
    return litmus.succeed();
  }
}

// End SqlUserDefinedTypeNameSpec.java
