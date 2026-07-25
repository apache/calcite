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
import org.apache.calcite.util.Comment;
import org.apache.calcite.util.SerializableCharset;

import com.google.common.base.Preconditions;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * BasicSqlType represents a standard atomic SQL type (excluding interval
 * types).
 *
 * <p>Instances of this class are immutable.
 */
public class BasicSqlType extends AbstractSqlType {
  //~ Static fields/initializers ---------------------------------------------

  //~ Instance fields --------------------------------------------------------

  private final int precision;
  private final int scale;
  protected final RelDataTypeSystem typeSystem;
  private final @Nullable SqlCollation collation;
  private final @Nullable SerializableCharset wrappedCharset;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs a type with no parameters. This should only be called from a
   * factory method.
   *
   * @param typeSystem Type system
   * @param typeName Type name
   */
  public BasicSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName) {
    this(typeSystem, typeName, false);
  }

  protected BasicSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName,
      boolean nullable) {
    this(typeSystem, typeName, nullable, PRECISION_NOT_SPECIFIED,
        SCALE_NOT_SPECIFIED, null, null);
    checkPrecScale(typeName, false, false);
  }

  /**
   * Constructs a type with precision/length but no scale.
   *
   * @param typeSystem Type system
   * @param typeName Type name
   * @param precision Precision (called length for some types)
   */
  public BasicSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName,
      int precision) {
    this(typeSystem, typeName, false, precision, SCALE_NOT_SPECIFIED, null,
        null);
    checkPrecScale(typeName, true, false);
  }

  /**
   * Constructs a type with precision/length and scale.
   *
   * @param typeSystem Type system
   * @param typeName Type name
   * @param precision Precision (called length for some types)
   * @param scale Scale
   */
  public BasicSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName,
      int precision, int scale) {
    this(typeSystem, typeName, false, precision, scale, null, null);
    checkPrecScale(typeName, true, true);
  }

  /** Internal constructor. */
  private BasicSqlType(
      RelDataTypeSystem typeSystem,
      SqlTypeName typeName,
      boolean nullable,
      int precision,
      int scale,
      @Nullable SqlCollation collation,
      @Nullable SerializableCharset wrappedCharset) {
    this(typeSystem, typeName, nullable, precision, scale, collation, wrappedCharset,
        new HashSet<>());
  }

  /** Internal constructor with comments (for subclasses such as {@link BasicSqlTypeWithFormat}). */
  protected BasicSqlType(
      RelDataTypeSystem typeSystem,
      SqlTypeName typeName,
      boolean nullable,
      int precision,
      int scale,
      @Nullable SqlCollation collation,
      @Nullable SerializableCharset wrappedCharset,
      Set<Comment> comments) {
    super(typeName, nullable, null, comments);
    this.typeSystem = Objects.requireNonNull(typeSystem, "typeSystem");
    this.precision = precision;
    this.scale = scale;
    this.collation = collation;
    this.wrappedCharset = wrappedCharset;
    computeDigest();
  }

  @Override public BasicSqlType copy(Set<Comment> comments) {
    return new BasicSqlType(typeSystem, typeName, isNullable, precision, scale, collation,
        wrappedCharset, comments);
  }

  /** Throws if {@code typeName} does not allow the given combination of
   * precision and scale. */
  protected static void checkPrecScale(SqlTypeName typeName,
      boolean precisionSpecified, boolean scaleSpecified) {
    if (!typeName.allowsPrecScale(precisionSpecified, scaleSpecified)) {
      throw new AssertionError("typeName.allowsPrecScale("
          + precisionSpecified + ", " + scaleSpecified + "): " + typeName);
    }
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Constructs a type with nullablity.
   */
  public BasicSqlType createWithNullability(boolean nullable) {
    if (nullable == this.isNullable) {
      return this;
    }
    return new BasicSqlType(this.typeSystem, this.typeName, nullable,
        this.precision, this.scale, this.collation, this.wrappedCharset, getComment());
  }

  /**
   * Constructs a type with charset and collation.
   *
   * <p>This must be a character type.
   */
  BasicSqlType createWithCharsetAndCollation(Charset charset,
      SqlCollation collation) {
    Preconditions.checkArgument(SqlTypeUtil.inCharFamily(this));
    return new BasicSqlType(this.typeSystem, this.typeName, this.isNullable,
        this.precision, this.scale, collation,
        SerializableCharset.forCharset(charset), getComment());
  }

  @Override public int getPrecision() {
    if (precision == PRECISION_NOT_SPECIFIED) {
      return typeSystem.getDefaultPrecision(typeName);
    }
    return precision;
  }

  @Override public int getMaxNumericPrecision() {
    return typeSystem.getMaxNumericPrecision();
  }

  @Override public int getScale() {
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

  @Override public @Nullable Charset getCharset() {
    return wrappedCharset == null ? null : wrappedCharset.getCharset();
  }

  @Override public @Nullable SqlCollation getCollation() {
    return collation;
  }

  // implement RelDataTypeImpl
  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    // Called to make the digest, which equals() compares;
    // so equivalent data types must produce identical type strings.

    sb.append(typeName.name());
    boolean printPrecision = precision != PRECISION_NOT_SPECIFIED;
    boolean printScale = scale != SCALE_NOT_SPECIFIED;

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
    if (wrappedCharset != null
        && !SqlCollation.IMPLICIT.getCharset().equals(wrappedCharset.getCharset())) {
      sb.append(" CHARACTER SET \"");
      sb.append(wrappedCharset.getCharset().name());
      sb.append("\"");
    }
    if (collation != null
        && collation != SqlCollation.IMPLICIT && collation != SqlCollation.COERCIBLE) {
      sb.append(" COLLATE \"");
      sb.append(collation.getCollationName());
      sb.append("\"");
    }
  }

  // A BasicSqlType with unspecified precision derives getPrecision() from the ATTACHED type system
  // (typeSystem.getDefaultPrecision(typeName); DECIMAL additionally reads typeSystem.getMaxNumericPrecision()).
  // That type system is NOT part of the type digest, yet RelDataTypeFactoryImpl interns types in a JVM-wide
  // static weak cache (DATATYPE_CACHE) keyed via equals()/hashCode(). Two types with the same
  // (typeName, unspecified precision) built under DIFFERENT type systems therefore collapse into a single
  // interned instance, making getPrecision()/getMaxNumericPrecision() depend on whichever was interned
  // first -- a source of order-dependent (parallel-fork flaky) behaviour. Known manifestations:
  //   * leastRestrictive(BIGINT, INTEGER) picks the wider integer via getPrecision(); a collapsed BIGINT
  //     whose attached type system reports a small/unspecified default precision flips the result to
  //     INTEGER, changing integer arithmetic result width.
  //   * BigQuery NUMERIC vs BIGNUMERIC keys off unspecified DECIMAL getMaxNumericPrecision().
  // Fold the RESOLVED precision (and, for DECIMAL, the max numeric precision) into equals()/hashCode() so
  // the interner keeps type-system-distinct instances separate. This deliberately does NOT touch
  // generateTypeString(): the rendered type digest (used in CAST/Rel output) stays byte-for-byte identical.
  //
  // SCOPE: restricted to unspecified-precision DECIMAL and the exact integer types -- the types whose
  // type-system precision actually drives a downstream decision (leastRestrictive integer width;
  // NUMERIC vs BIGNUMERIC). All other unspecified-precision types (e.g. VARCHAR, whose default precision
  // varies wildly by dialect -- Snowflake 16 MB vs unspecified) keep digest-only equality, so their
  // existing interner collapse is preserved and output that relies on it does not change.
  private boolean typeSystemAffectsInterning() {
    if (precision != PRECISION_NOT_SPECIFIED) {
      return false;
    }
    switch (typeName) {
    case DECIMAL:
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
      return true;
    default:
      return false;
    }
  }

  @Override public int hashCode() {
    if (!typeSystemAffectsInterning()) {
      return super.hashCode();
    }
    int maxNumeric = typeName == SqlTypeName.DECIMAL ? typeSystem.getMaxNumericPrecision() : 0;
    return Objects.hash(getFullTypeString(), getPrecision(), maxNumeric);
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof BasicSqlType)) {
      return false;
    }
    BasicSqlType that = (BasicSqlType) obj;
    if (!getFullTypeString().equals(that.getFullTypeString())) {
      return false;
    }
    // Digest matches here, so this and that share a typeName -> same typeSystemAffectsInterning().
    if (!typeSystemAffectsInterning()) {
      return true;
    }
    if (getPrecision() != that.getPrecision()) {
      return false;
    }
    return typeName != SqlTypeName.DECIMAL
        || typeSystem.getMaxNumericPrecision() == that.typeSystem.getMaxNumericPrecision();
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
  public @Nullable Object getLimit(
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
