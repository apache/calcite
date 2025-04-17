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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A <code>SqlDialect</code> implementation for the Redshift database.
 */
public class RedshiftSqlDialect extends SqlDialect {
  public static final RelDataTypeSystem TYPE_SYSTEM =
      new RelDataTypeSystemImpl() {
        @Override public int getMaxPrecision(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 38;
          case VARCHAR:
            return 65535;
          case CHAR:
            return 4096;
          default:
            return super.getMaxPrecision(typeName);
          }
        }

        @Override public int getMaxNumericPrecision() {
          return getMaxPrecision(SqlTypeName.DECIMAL);
        }

        @Override public int getMaxScale(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 37;
          default:
            return super.getMaxScale(typeName);
          }
        }

        @Override public int getMaxNumericScale() {
          return getMaxScale(SqlTypeName.DECIMAL);
        }
      };

  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.REDSHIFT)
      .withIdentifierQuoteString("\"")
      .withQuotedCasing(Casing.TO_LOWER)
      .withUnquotedCasing(Casing.TO_LOWER)
      .withCaseSensitive(false)
      .withDataTypeSystem(TYPE_SYSTEM);

  public static final SqlDialect DEFAULT = new RedshiftSqlDialect(DEFAULT_CONTEXT);

  /** Creates a RedshiftSqlDialect. */
  public RedshiftSqlDialect(Context context) {
    super(context);
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    String castSpec;
    switch (type.getSqlTypeName()) {
    case TINYINT:
      // Redshift has no tinyint (1 byte), so instead cast to smallint or int2 (2 bytes).
      // smallint does not work when enclosed in quotes (i.e.) as "smallint".
      // int2 however works within quotes (i.e.) as "int2".
      // Hence using int2.
      castSpec = "int2";
      break;
    case DOUBLE:
      // Redshift has a double type but it is named differently. It is named as double precision or
      // float8.
      // double precision does not work when enclosed in quotes (i.e.) as "double precision".
      // float8 however works within quotes (i.e.) as "float8".
      // Hence using float8.
      castSpec = "float8";
      break;
    default:
      return super.getCastSpec(type);
    }

    return new SqlDataTypeSpec(
        new SqlUserDefinedTypeNameSpec(castSpec, SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }

  @Override public SqlNode rewriteMaxMinExpr(SqlNode aggCall, RelDataType relDataType) {
    return rewriteMaxMin(aggCall, relDataType);
  }

  @Override public boolean supportsGroupByLiteral() {
    return false;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }
  @Override public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {

    /*
     * Rewrites the JSON_OBJECT operator for Redshift.
     *
     * Redshift has no JSON_OBJECT function or equivalent. To account for this we rewrite the SQL
     * for each key in the JSON_OBJECT to this:
     *
     * '"key_name": ' || COALESCE(CASE WHEN %s::varchar ~'^-?\\\\d*(\\\\.\\\\d+)?$' THEN %s::varchar ELSE quote_ident(%s) END, 'null')
     *
     * Concatenate the individual statements with a " || ', ' || ". and surround the entire list with '{ }'
     *
     * When executed it produces rows as JSON OBJECTS strings, similar to PostgreSQL and others.
     *
     * This statement will convert the value clause to a string and then regex match to see if it is numeric. This can be fooled
     * by strings that happen to look like numbers. In practice this is probably not a significant issue.
     *
     * There are limitations with type checking and casting in Redshift that make it difficult to
     * get around this regex check (for example - any cast of a date to ::super will generate an error - which
     * complicates a more definitive, native type checking).
     */
    if (call.getOperator() == SqlStdOperatorTable.JSON_OBJECT) {
      assert call.operandCount() % 2 == 1;

      SqlWriter.Frame frame = writer.startFunCall("");
      SqlWriter.Frame listFrame = writer.startList("'{' || ", " || '}'");
      for(int i = 1; i < call.operandCount(); i += 2) {

        // operand 1 is a key name (need to remove surrounding single quotes)
        String keyName = call.operand(i).toSqlString(this).toString();
        keyName = keyName.substring(1, keyName.length() - 1);

        // operand 2 is the value. The value can be a literal or any SQL command that generates a value
        String valueClause = call.operand(i + 1).toSqlString(this).toString();

        // This will create the key/value pair in SQL. It will correctly put NULL as the value for NULL values.
        // It will surround non-numerics with double-quotes.
        String jsonFragment = String.format("'\"%s\": ' || COALESCE(CASE WHEN %s::varchar ~'^-?\\\\d*(\\\\.\\\\d+)?$' THEN %s::varchar ELSE quote_ident(%s) END, 'null')", keyName, valueClause, valueClause, valueClause);
        writer.print(jsonFragment);

        // List separator
        if (i != call.operandCount() - 2) {
          writer.print(" || ', ' || ");
        }
      }
      writer.endList(listFrame);
      writer.endFunCall(frame);
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }
}
