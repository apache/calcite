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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

import java.nio.charset.Charset;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A sql type name specification of basic sql type.
 *
 * <p>Supported basic sql types grammar:
 * <blockquote><pre>
 *   basicSqlType:
 *         GEOMETRY
 *     |   BOOLEAN
 *     |   [ INTEGER | INT ]
 *     |   TINYINT
 *     |   SMALLINT
 *     |   BIGINT
 *     |   REAL
 *     |   DOUBLE [ PRECISION ]
 *     |   FLOAT
 *     |   BINARY [ precision ]
 *     |   [ BINARY VARYING | VARBINARY ] [ precision ]
 *     |   [ DECIMAL | DEC | NUMERIC ] [ precision [, scale] ]
 *     |   ANY [ precision [, scale] ]
 *     |   charType [ precision ] [ charSet ]
 *     |   varcharType [ precision ] [ charSet ]
 *     |   DATE
 *     |   TIME [ precision ] [ timeZone ]
 *     |   TIMESTAMP [ precision ] [ timeZone ]
 *
 *  charType:
 *         CHARACTER
 *     |   CHAR
 *
 *  varcharType:
 *         charType VARYING
 *     |   VARCHAR
 *
 *  charSet:
 *         CHARACTER SET charSetName
 *
 *  timeZone:
 *         WITHOUT TIME ZONE
 *     |   WITH LOCAL TIME ZONE
 * </pre></blockquote>
 */
public class SqlBasicTypeNameSpec extends SqlTypeNameSpec {

  private final SqlTypeName sqlTypeName;

  private int precision;
  private int scale;

  private String charSetName;

  /**
   * Create a basic sql type name specification.
   *
   * @param typeName    Type name
   * @param precision   Precision of the type name if it is allowed, default is -1
   * @param scale       Scale of the type name if it is allowed, default is -1
   * @param charSetName Char set of the type, only works when the type
   *                    belong to CHARACTER type family
   * @param pos         The parser position
   */
  public SqlBasicTypeNameSpec(
      SqlTypeName typeName,
      int precision,
      int scale,
      @Nullable String charSetName,
      SqlParserPos pos) {
    super(new SqlIdentifier(typeName.name(), pos), pos);
    this.sqlTypeName = typeName;
    this.precision = precision;
    this.scale = scale;
    this.charSetName = charSetName;
  }

  public SqlBasicTypeNameSpec(SqlTypeName typeName, SqlParserPos pos) {
    this(typeName, -1, -1, null, pos);
  }

  public SqlBasicTypeNameSpec(SqlTypeName typeName, int precision, SqlParserPos pos) {
    this(typeName, precision, -1, null, pos);
  }

  public SqlBasicTypeNameSpec(SqlTypeName typeName, int precision,
      String charSetName, SqlParserPos pos) {
    this(typeName, precision, -1, charSetName, pos);
  }

  public SqlBasicTypeNameSpec(SqlTypeName typeName, int precision,
      int scale, SqlParserPos pos) {
    this(typeName, precision, scale, null, pos);
  }

  public int getScale() {
    return scale;
  }

  public int getPrecision() {
    return precision;
  }

  public String getCharSetName() {
    return charSetName;
  }

  @Override public boolean equalsDeep(SqlTypeNameSpec node, Litmus litmus) {
    if (!(node instanceof SqlBasicTypeNameSpec)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlBasicTypeNameSpec that = (SqlBasicTypeNameSpec) node;
    if (this.sqlTypeName != that.sqlTypeName) {
      return litmus.fail("{} != {}", this, node);
    }
    if (this.precision != that.precision) {
      return litmus.fail("{} != {}", this, node);
    }
    if (this.scale != that.scale) {
      return litmus.fail("{} != {}", this, node);
    }
    if (!Objects.equals(this.charSetName, that.charSetName)) {
      return litmus.fail("{} != {}", this, node);
    }
    return litmus.succeed();
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    // Unparse the builtin type name.
    // For some type name with extra definitions, unparse it specifically
    // instead of direct unparsing with enum name.
    // i.e. TIME_WITH_LOCAL_TIME_ZONE(3)
    // would be unparsed as "time(3) with local time zone".
    final boolean isWithLocalTimeZone = isWithLocalTimeZoneDef(sqlTypeName);
    if (isWithLocalTimeZone) {
      writer.keyword(stripLocalTimeZoneDef(sqlTypeName).name());
    } else {
      writer.keyword(getTypeName().getSimple());
    }

    if (sqlTypeName.allowsPrec() && (precision >= 0)) {
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      writer.print(precision);
      if (sqlTypeName.allowsScale() && (scale >= 0)) {
        writer.sep(",", true);
        writer.print(scale);
      }
      writer.endList(frame);
    }

    if (isWithLocalTimeZone) {
      writer.keyword("WITH LOCAL TIME ZONE");
    }

    if (charSetName != null) {
      writer.keyword("CHARACTER SET");
      writer.identifier(charSetName, true);
    }
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    if (sqlTypeName == null) {
      return null;
    }
    RelDataType type;
    // NOTE jvs 15-Jan-2009:  earlier validation is supposed to
    // have caught these, which is why it's OK for them
    // to be assertions rather than user-level exceptions.
    if ((precision >= 0) && (scale >= 0)) {
      assert sqlTypeName.allowsPrecScale(true, true);
      type = typeFactory.createSqlType(sqlTypeName, precision, scale);
    } else if (precision >= 0) {
      assert sqlTypeName.allowsPrecNoScale();
      type = typeFactory.createSqlType(sqlTypeName, precision);
    } else {
      assert sqlTypeName.allowsNoPrecNoScale();
      type = typeFactory.createSqlType(sqlTypeName);
    }

    if (SqlTypeUtil.inCharFamily(type)) {
      // Applying Syntax rule 10 from SQL:99 spec section 6.22 "If TD is a
      // fixed-length, variable-length or large object character string,
      // then the collating sequence of the result of the <cast
      // specification> is the default collating sequence for the
      // character repertoire of TD and the result of the <cast
      // specification> has the Coercible coercibility characteristic."
      SqlCollation collation = SqlCollation.COERCIBLE;

      Charset charset;
      if (null == this.charSetName) {
        charset = typeFactory.getDefaultCharset();
      } else {
        String javaCharSetName =
            Objects.requireNonNull(
                SqlUtil.translateCharacterSetName(charSetName), charSetName);
        charset = Charset.forName(javaCharSetName);
      }
      type =
          typeFactory.createTypeWithCharsetAndCollation(
              type,
              charset,
              collation);
    }
    return type;
  }

  //~ Tools ------------------------------------------------------------------

  /**
   * @return true if this type name has "local time zone" definition.
   */
  private static boolean isWithLocalTimeZoneDef(SqlTypeName typeName) {
    switch (typeName) {
    case TIME_WITH_LOCAL_TIME_ZONE:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return true;
    default:
      return false;
    }
  }

  /**
   * Remove the local time zone definition of the {@code typeName}.
   *
   * @param typeName Type name
   * @return new type name without local time zone definition
   */
  private SqlTypeName stripLocalTimeZoneDef(SqlTypeName typeName) {
    switch (typeName) {
    case TIME_WITH_LOCAL_TIME_ZONE:
      return SqlTypeName.TIME;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return SqlTypeName.TIMESTAMP;
    default:
      throw new AssertionError(typeName);
    }
  }
}

// End SqlBasicTypeNameSpec.java
