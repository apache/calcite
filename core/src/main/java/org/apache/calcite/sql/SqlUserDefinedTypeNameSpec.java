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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A sql type name specification of user defined type.
 *
 * <p>Usually you should register the UDT into the {@link org.apache.calcite.jdbc.CalciteSchema}
 * first before referencing it in the sql statement.
 */
public class SqlUserDefinedTypeNameSpec extends SqlTypeNameSpec {

  /** Like {@link SqlBasicTypeNameSpec}, user-defined types may have precision in certain cases. */
  private int precision;

  /**
   * Create a SqlUserDefinedTypeNameSpec instance.
   *
   * @param typeName Type name as SQL identifier
   * @param pos The parser position
   */
  public SqlUserDefinedTypeNameSpec(SqlIdentifier typeName, int precision, SqlParserPos pos) {
    super(typeName, pos);
    this.precision = precision;
  }

  public SqlUserDefinedTypeNameSpec(SqlIdentifier typeName, SqlParserPos pos) {
    this(typeName, -1, pos);
  }

  public SqlUserDefinedTypeNameSpec(String name, SqlParserPos pos) {
    this(new SqlIdentifier(name, pos), -1, pos);
  }

  public SqlUserDefinedTypeNameSpec(String name, int precision, SqlParserPos pos) {
    this(new SqlIdentifier(name, pos), precision, pos);
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    // The type name is a compound identifier. That means it is a UDT.
    // Use SqlValidator to deduce its type from the Schema.
    final SqlIdentifier identifier = getTypeName();
    RelDataType type = validator.getValidatedNodeTypeIfKnown(identifier);
    if (type != null) {
      return type;
    }

    // If the type does not exist in the UDT map, try a sensible default.
    try {
      SqlTypeName defaultTypeName = SqlTypeName.lookup(identifier.toString());
      return new SqlBasicTypeNameSpec(defaultTypeName, precision, getParserPos())
          .deriveType(validator);
    } catch (IllegalArgumentException e) {
      throw validator.newValidationError(
          identifier, RESOURCE.unknownIdentifier(identifier.toString()));
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    getTypeName().unparse(writer, leftPrec, rightPrec);

    if (precision >= 0) {
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      writer.print(precision);
      writer.endList(frame);
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
