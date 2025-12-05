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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A <code>SqlDialect</code> implementation for the Apache Phoenix database.
 */
public class PhoenixSqlDialect extends SqlDialect {
  public static final RelDataTypeSystem TYPE_SYSTEM =
      new RelDataTypeSystemImpl() {

        // We can refer to document of Phoenix 5.x:
        // https://phoenix.apache.org/language/datatypes.html#decimal_type
        @Override public int getMaxPrecision(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 38;
          default:
            return super.getMaxPrecision(typeName);
          }
        }

        @Override public int getMaxScale(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 38;
          default:
            return super.getMaxScale(typeName);
          }
        }

        @Override public int getMaxNumericScale() {
          return getMaxScale(SqlTypeName.DECIMAL);
        }
      };

  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.PHOENIX)
      .withIdentifierQuoteString("\"")
      .withDataTypeSystem(TYPE_SYSTEM);

  public static final SqlDialect DEFAULT = new PhoenixSqlDialect(DEFAULT_CONTEXT);

  /** Creates a PhoenixSqlDialect. */
  public PhoenixSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsApproxCountDistinct() {
    return true;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case REAL:
      return new SqlDataTypeSpec(
          new SqlAlienSystemTypeNameSpec(
              "FLOAT",
              type.getSqlTypeName(),
              SqlParserPos.ZERO),
          SqlParserPos.ZERO);
    default:
      break;
    }

    if (type instanceof AbstractSqlType) {
      switch (type.getSqlTypeName()) {
      case ARRAY:
      case MAP:
      case MULTISET:
        throw new UnsupportedOperationException("Phoenix dialect does not support cast to "
            + type.getSqlTypeName());
      default:
        break;
      }
    }

    return super.getCastSpec(type);
  }
}
