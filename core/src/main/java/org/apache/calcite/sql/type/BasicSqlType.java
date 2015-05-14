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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.util.SerializableCharset;
import org.apache.calcite.util.Util;

import java.nio.charset.Charset;

/**
 * BasicSqlType represents a standard atomic SQL type (excluding interval
 * types).
 */
public class BasicSqlType extends AbstractSqlType {
  //~ Static fields/initializers ---------------------------------------------

  //~ Instance fields --------------------------------------------------------

  private final int precision;
  private final int scale;
  private final RelDataTypeSystem typeSystem;
  private SqlCollation collation;
  private SerializableCharset wrappedCharset;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs a type with no parameters. This should only be called from a
   * factory method.
   *
   * @param typeName Type name
   */
  public BasicSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName) {
    this(typeSystem, typeName, false, PRECISION_NOT_SPECIFIED,
        SCALE_NOT_SPECIFIED);
    assert typeName.allowsPrecScale(false, false)
        : "typeName.allowsPrecScale(false,false), typeName=" + typeName.name();
    computeDigest();
  }

  /**
   * Constructs a type with precision/length but no scale.
   *
   * @param typeName Type name
   */
  public BasicSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName,
      int precision) {
    this(typeSystem, typeName, false, precision, SCALE_NOT_SPECIFIED);
    assert typeName.allowsPrecScale(true, false)
        : "typeName.allowsPrecScale(true, false)";
    computeDigest();
  }

  /**
   * Constructs a type with precision/length and scale.
   *
   * @param typeName Type name
   */
  public BasicSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName,
      int precision, int scale) {
    this(typeSystem, typeName, false, precision, scale);
    assert typeName.allowsPrecScale(true, true);
    computeDigest();
  }

  /** Internal constructor. */
  private BasicSqlType(
      RelDataTypeSystem typeSystem,
      SqlTypeName typeName,
      boolean nullable,
      int precision,
      int scale) {
    super(typeName, nullable, null);
    this.typeSystem = typeSystem;
    this.precision = precision;
    this.scale = scale;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Constructs a type with nullablity
   */
  BasicSqlType createWithNullability(boolean nullable) {
    BasicSqlType ret;
    try {
      ret = (BasicSqlType) this.clone();
    } catch (CloneNotSupportedException e) {
      throw Util.newInternal(e);
    }
    ret.isNullable = nullable;
    ret.computeDigest();
    return ret;
  }

  /**
   * Constructs a type with charset and collation
   *
   * @pre SqlTypeUtil.inCharFamily(this)
   */
  BasicSqlType createWithCharsetAndCollation(
      Charset charset,
      SqlCollation collation) {
    Util.pre(SqlTypeUtil.inCharFamily(this), "Not an chartype");
    BasicSqlType ret;
    try {
      ret = (BasicSqlType) this.clone();
    } catch (CloneNotSupportedException e) {
      throw Util.newInternal(e);
    }
    ret.wrappedCharset = SerializableCharset.forCharset(charset);
    ret.collation = collation;
    ret.computeDigest();
    return ret;
  }

  // implement RelDataType
  public int getPrecision() {
    if (precision == PRECISION_NOT_SPECIFIED) {
      return typeSystem.getDefaultPrecision(typeName);
    }
    return precision;
  }

  // implement RelDataType
  public int getScale() {
    if (scale == SCALE_NOT_SPECIFIED) {
      switch (typeName) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case DECIMAL:
        return 0;
      default:
        // fall through
      }
    }
    return scale;
  }

  // implement RelDataType
  public Charset getCharset() {
    return wrappedCharset == null ? null : wrappedCharset.getCharset();
  }

  // implement RelDataType
  public SqlCollation getCollation() {
    return collation;
  }

  // implement RelDataTypeImpl
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    // Called to make the digest, which equals() compares;
    // so equivalent data types must produce identical type strings.

    sb.append(typeName.name());
    boolean printPrecision = precision != PRECISION_NOT_SPECIFIED;
    boolean printScale = scale != SCALE_NOT_SPECIFIED;

    // for the digest, print the precision when defaulted,
    // since (for instance) TIME is equivalent to TIME(0).
    if (withDetail) {
      // -1 means there is no default value for precision
      if (typeName.allowsPrec()
        && typeSystem.getDefaultPrecision(typeName) > -1) {
        printPrecision = true;
      }
      if (typeName.getDefaultScale() > -1) {
        printScale = true;
      }
    }

    if (printPrecision) {
      sb.append('(');
      sb.append(getPrecision());
      if (printScale) {
        sb.append(", ");
        sb.append(getScale());
      }
      sb.append(')');
    }
    if (!withDetail) {
      return;
    }
    if (wrappedCharset != null) {
      sb.append(" CHARACTER SET \"");
      sb.append(wrappedCharset.getCharset().name());
      sb.append("\"");
    }
    if (collation != null) {
      sb.append(" COLLATE \"");
      sb.append(collation.getCollationName());
      sb.append("\"");
    }
  }

  /**
   * Returns a value which is a limit for this type.
   *
   * <p>For example,
   *
   * <table border="1">
   * <caption>Limits</caption>
   * <tr>
   * <th>Datatype</th>
   * <th>sign</th>
   * <th>limit</th>
   * <th>beyond</th>
   * <th>precision</th>
   * <th>scale</th>
   * <th>Returns</th>
   * </tr>
   * <tr>
   * <td>Integer</td>
   * <td>true</td>
   * <td>true</td>
   * <td>false</td>
   * <td>-1</td>
   * <td>-1</td>
   * <td>2147483647 (2 ^ 31 -1 = MAXINT)</td>
   * </tr>
   * <tr>
   * <td>Integer</td>
   * <td>true</td>
   * <td>true</td>
   * <td>true</td>
   * <td>-1</td>
   * <td>-1</td>
   * <td>2147483648 (2 ^ 31 = MAXINT + 1)</td>
   * </tr>
   * <tr>
   * <td>Integer</td>
   * <td>false</td>
   * <td>true</td>
   * <td>false</td>
   * <td>-1</td>
   * <td>-1</td>
   * <td>-2147483648 (-2 ^ 31 = MININT)</td>
   * </tr>
   * <tr>
   * <td>Boolean</td>
   * <td>true</td>
   * <td>true</td>
   * <td>false</td>
   * <td>-1</td>
   * <td>-1</td>
   * <td>TRUE</td>
   * </tr>
   * <tr>
   * <td>Varchar</td>
   * <td>true</td>
   * <td>true</td>
   * <td>false</td>
   * <td>10</td>
   * <td>-1</td>
   * <td>'ZZZZZZZZZZ'</td>
   * </tr>
   * </table>
   *
   * @param sign   If true, returns upper limit, otherwise lower limit
   * @param limit  If true, returns value at or near to overflow; otherwise
   *               value at or near to underflow
   * @param beyond If true, returns the value just beyond the limit, otherwise
   *               the value at the limit
   * @return Limit value
   */
  public Object getLimit(
      boolean sign,
      SqlTypeName.Limit limit,
      boolean beyond) {
    int precision = typeName.allowsPrec() ? this.getPrecision() : -1;
    int scale = typeName.allowsScale() ? this.getScale() : -1;
    return typeName.getLimit(
        sign,
        limit,
        beyond,
        precision,
        scale);
  }
}

// End BasicSqlType.java
