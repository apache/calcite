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
package org.eigenbase.sarg;

import java.math.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import net.hydromatic.avatica.ByteString;

/**
 * SargEndpoint represents an endpoint of a ({@link SargInterval}).
 *
 * <p>Instances of SargEndpoint are immutable from outside this package.
 * Subclass {@link SargMutableEndpoint} is provided for manipulation from
 * outside the package.
 */
public class SargEndpoint implements Comparable<SargEndpoint> {
  // TODO jvs 16-Jan-2006:  special pattern prefix support for LIKE operator

  //~ Instance fields --------------------------------------------------------

  /**
   * Factory which produced this endpoint.
   */
  protected final SargFactory factory;

  /**
   * Datatype for endpoint value.
   */
  protected final RelDataType dataType;

  /**
   * Coordinate for this endpoint, constrained to be either {@link
   * RexLiteral}, {@link RexInputRef}, {@link RexDynamicParam}, or null to
   * represent infinity (positive or negative infinity is implied by
   * boundType).
   */
  protected RexNode coordinate;

  /**
   * @see #getBoundType
   */
  protected SargBoundType boundType;

  /**
   * @see #getStrictness
   */
  protected SargStrictness strictness;

  //~ Constructors -----------------------------------------------------------

  /**
   * @see SargFactory#newEndpoint
   */
  SargEndpoint(SargFactory factory, RelDataType dataType) {
    this.factory = factory;
    this.dataType = dataType;
    boundType = SargBoundType.LOWER;
    strictness = SargStrictness.OPEN;
  }

  //~ Methods ----------------------------------------------------------------

  void copyFrom(SargEndpoint other) {
    assert getDataType() == other.getDataType();
    if (other.isFinite()) {
      setFinite(
          other.getBoundType(),
          other.getStrictness(),
          other.getCoordinate());
    } else {
      setInfinity(other.getInfinitude());
    }
  }

  /**
   * Sets this endpoint to either negative or positive infinity. An infinite
   * endpoint implies an open bound (negative infinity implies a lower bound,
   * while positive infinity implies an upper bound).
   *
   * @param infinitude either -1 or +1
   */
  void setInfinity(int infinitude) {
    assert (infinitude == -1) || (infinitude == 1);

    if (infinitude == -1) {
      boundType = SargBoundType.LOWER;
    } else {
      boundType = SargBoundType.UPPER;
    }
    strictness = SargStrictness.OPEN;
    coordinate = null;
  }

  /**
   * Sets a finite value for this endpoint.
   *
   * @param boundType  bound type (upper/lower)
   * @param strictness boundary strictness
   * @param coordinate endpoint position
   */
  void setFinite(
      SargBoundType boundType,
      SargStrictness strictness,
      RexNode coordinate) {
    // validate the input
    assert coordinate != null;
    if (!(coordinate instanceof RexDynamicParam)
        && !(coordinate instanceof RexInputRef)) {
      assert coordinate instanceof RexLiteral;
      RexLiteral literal = (RexLiteral) coordinate;
      if (!RexLiteral.isNullLiteral(literal)) {
        assert SqlTypeUtil.canAssignFrom(
            dataType,
            literal.getType());
      }
    }

    this.boundType = boundType;
    this.coordinate = coordinate;
    this.strictness = strictness;
    convertToTargetType();
  }

  private void convertToTargetType() {
    if (!(coordinate instanceof RexLiteral)) {
      // Dynamic parameters and RexInputRefs are always cast to the
      // target type before comparison, so they are guaranteed not to
      // need any special conversion logic.
      return;
    }

    RexLiteral literal = (RexLiteral) coordinate;

    if (RexLiteral.isNullLiteral(literal)) {
      return;
    }

    Comparable value = literal.getValue();
    int roundingCompensation;

    if (value instanceof BigDecimal) {
      roundingCompensation = convertNumber((BigDecimal) value);
    } else if (value instanceof NlsString) {
      roundingCompensation = convertString((NlsString) value);
    } else if (value instanceof ByteString) {
      roundingCompensation = convertBytes((ByteString) value);
    } else {
      // NOTE jvs 19-Feb-2006:  Once we support fractional time
      // precision, need to handle that here.
      return;
    }

    // rounding takes precedence over the strictness flag.
    //  Input        round    strictness    output    effective strictness
    //    >5.9        down            -1       >=6        0
    //    >=5.9       down             0       >=6        0
    //    >6.1          up            -1        >6        1
    //    >=6.1         up             0        >6        1
    //    <6.1          up             1       <=6        0
    //    <=6.1         up             0       <=6        0
    //    <5.9        down             1        <6       -1
    //    <=5.9       down             0        <6       -1
    if (roundingCompensation == 0) {
      return;
    }
    if (boundType == SargBoundType.LOWER) {
      if (roundingCompensation < 0) {
        strictness = SargStrictness.CLOSED;
      } else {
        strictness = SargStrictness.OPEN;
      }
    } else if (boundType == SargBoundType.UPPER) {
      if (roundingCompensation > 0) {
        strictness = SargStrictness.CLOSED;
      } else {
        strictness = SargStrictness.OPEN;
      }
    }
  }

  private int convertString(NlsString value) {
    // For character strings, have to deal with truncation (complicated by
    // padding rules).

    boolean fixed = dataType.getSqlTypeName() == SqlTypeName.CHAR;

    String s = value.getValue();
    String trimmed = Util.rtrim(s);

    if (fixed) {
      // For CHAR, canonical representation is padded.  This is
      // required during execution of comparisons.
      s = Util.rpad(s, dataType.getPrecision());
    } else {
      // For PAD SPACE, we can use trimmed representation as
      // canonical for VARCHAR.  This may shave off cycles
      // during execution of comparisons.  If we ever support the NO PAD
      // attribute, we'll have to do something different for collations
      // with that attribute enabled.
      s = trimmed;
    }

    // Truncate if needed
    if (s.length() > dataType.getPrecision()) {
      s = s.substring(0, dataType.getPrecision());

      // Post-truncation, need to trim again if truncation
      // left spaces on the end
      if (!fixed) {
        s = Util.rtrim(s);
      }
    }
    coordinate =
        factory.getRexBuilder().makeCharLiteral(
            new NlsString(
                s,
                value.getCharsetName(),
                value.getCollation()));

    if (trimmed.length() > dataType.getPrecision()) {
      // Truncation is always "down" in coordinate space, so rounding
      // compensation is always "up".  Note that this calculation is
      // intentionally with respect to the pre-truncation trim (not the
      // post-truncation trim) because that's what tells us whether
      // we truncated anything of significance.
      return 1;
    } else {
      // For PAD SPACE, trailing spaces have no effect on comparison,
      // so it's the same as if no truncation took place.
      return 0;
    }
  }

  private int convertBytes(ByteString value) {
    // REVIEW jvs 11-Sept-2006:  What about 0-padding for BINARY?

    // For binary strings, have to deal with truncation.

    if (value.length() <= dataType.getPrecision()) {
      // No truncation required.
      return 0;
    }
    ByteString truncated = value.substring(0, dataType.getPrecision());
    coordinate = factory.getRexBuilder().makeBinaryLiteral(truncated);

    // Truncation is always "down" in coordinate space, so rounding
    // compensation is always "up".
    return 1;
  }

  private int convertNumber(BigDecimal value) {
    if (SqlTypeUtil.isApproximateNumeric(dataType)) {
      // REVIEW jvs 18-Jan-2006: is it necessary to do anything for
      // approx types here?  Wait until someone complains.  May at least
      // have to deal with case of double->float overflow.
      return 0;
    }

    // For exact numerics, we have to deal with rounding/overflow fun and
    // games.

    // REVIEW jvs 19-Feb-2006: Why do we make things complicated with
    // RoundingMode.HALF_UP?  We could just use RoundingMode.FLOOR so the
    // compensation direction would always be the same.  Maybe because this
    // code was ported from Broadbase, where the conversion library didn't
    // support multiple rounding modes.

    BigDecimal roundedValue =
        value.setScale(
            dataType.getScale(),
            RoundingMode.HALF_UP);

    if (roundedValue.precision() > dataType.getPrecision()) {
      // Overflow.  Convert to infinity.  Note that this may
      // flip the bound type, which isn't really correct,
      // but we handle that outside in SargIntervalExpr.evaluate.
      setInfinity(roundedValue.signum());
      return 0;
    }

    coordinate =
        factory.getRexBuilder().makeExactLiteral(
            roundedValue,
            dataType);

    // The sign of roundingCompensation should be the opposite of the
    // rounding direction, so subtract post-rounding value from
    // pre-rounding.
    return value.compareTo(roundedValue);
  }

  /**
   * @return true if this endpoint represents a closed (exact) bound; false if
   * open (strict)
   */
  public boolean isClosed() {
    return strictness == SargStrictness.CLOSED;
  }

  /**
   * @return opposite of isClosed
   */
  public boolean isOpen() {
    return strictness == SargStrictness.OPEN;
  }

  /**
   * @return false if this endpoint represents infinity (either positive or
   * negative); true if a finite coordinate
   */
  public boolean isFinite() {
    return coordinate != null;
  }

  /**
   * @return -1 for negative infinity, +1 for positive infinity, 0 for a
   * finite endpoint
   */
  public int getInfinitude() {
    if (!isFinite()) {
      if (boundType == SargBoundType.LOWER) {
        return -1;
      } else {
        return 1;
      }
    } else {
      return 0;
    }
  }

  /**
   * @return coordinate of this endpoint
   */
  public RexNode getCoordinate() {
    return coordinate;
  }

  /**
   * @return true if this endpoint has the null value for its coordinate
   */
  public boolean isNull() {
    if (!isFinite()) {
      return false;
    }
    return RexLiteral.isNullLiteral(coordinate);
  }

  /**
   * @return target datatype for coordinate
   */
  public RelDataType getDataType() {
    return dataType;
  }

  /**
   * @return boundary type this endpoint represents
   */
  public SargBoundType getBoundType() {
    return boundType;
  }

  /**
   * Tests whether this endpoint "touches" another one (not necessarily
   * overlapping). For example, the upper bound of the interval (1, 10)
   * touches the lower bound of the interval [10, 20), but not of the interval
   * (10, 20).
   *
   * @param other the other endpoint to test
   * @return true if touching; false if discontinuous
   */
  public boolean isTouching(SargEndpoint other) {
    assert getDataType() == other.getDataType();

    if (!isFinite() || !other.isFinite()) {
      return false;
    }

    if ((coordinate instanceof RexDynamicParam)
        || (other.coordinate instanceof RexDynamicParam)) {
      if ((coordinate instanceof RexDynamicParam)
          && (other.coordinate instanceof RexDynamicParam)) {
        // make sure it's the same param
        RexDynamicParam p1 = (RexDynamicParam) coordinate;
        RexDynamicParam p2 = (RexDynamicParam) other.coordinate;
        if (p1.getIndex() != p2.getIndex()) {
          return false;
        }
      } else {
        // one is a dynamic param but the other isn't
        return false;
      }
    } else if (
        (coordinate instanceof RexInputRef)
            || (other.coordinate instanceof RexInputRef)) {
      if ((coordinate instanceof RexInputRef)
          && (other.coordinate instanceof RexInputRef)) {
        // make sure it's the same RexInputRef
        RexInputRef r1 = (RexInputRef) coordinate;
        RexInputRef r2 = (RexInputRef) other.coordinate;
        if (r1.getIndex() != r2.getIndex()) {
          return false;
        }
      } else {
        // one is a RexInputRef but the other isn't
        return false;
      }
    } else if (compareCoordinates(coordinate, other.coordinate) != 0) {
      return false;
    }

    return isClosed() || other.isClosed();
  }

  static int compareCoordinates(RexNode coord1, RexNode coord2) {
    assert coord1 instanceof RexLiteral;
    assert coord2 instanceof RexLiteral;

    // null values always sort lowest
    boolean isNull1 = RexLiteral.isNullLiteral(coord1);
    boolean isNull2 = RexLiteral.isNullLiteral(coord2);
    if (isNull1 && isNull2) {
      return 0;
    } else if (isNull1) {
      return -1;
    } else if (isNull2) {
      return 1;
    } else {
      RexLiteral lit1 = (RexLiteral) coord1;
      RexLiteral lit2 = (RexLiteral) coord2;
      return lit1.getValue().compareTo(lit2.getValue());
    }
  }

  // implement Object
  public String toString() {
    if (!isFinite()) {
      if (boundType == SargBoundType.LOWER) {
        return "-infinity";
      } else {
        return "+infinity";
      }
    }
    StringBuilder sb = new StringBuilder();
    if (boundType == SargBoundType.LOWER) {
      if (isClosed()) {
        sb.append(">=");
      } else {
        sb.append(">");
      }
    } else {
      if (isClosed()) {
        sb.append("<=");
      } else {
        sb.append("<");
      }
    }
    sb.append(" ");
    sb.append(coordinate);
    return sb.toString();
  }

  // implement Comparable
  public int compareTo(SargEndpoint other) {
    if (getInfinitude() != other.getInfinitude()) {
      // at least one is infinite; result is based on comparison of
      // infinitudes
      return getInfinitude() - other.getInfinitude();
    }

    if (!isFinite()) {
      // both are the same infinity:  equals
      return 0;
    }

    // both are finite:  compare coordinates
    int c =
        compareCoordinates(
            getCoordinate(),
            other.getCoordinate());

    if (c != 0) {
      return c;
    }

    // if coordinates are the same, then result is based on comparison of
    // strictness
    return getStrictnessSign() - other.getStrictnessSign();
  }

  /**
   * @return SargStrictness of this bound
   */
  public SargStrictness getStrictness() {
    return strictness;
  }

  /**
   * @return complement of SargStrictness of this bound
   */
  public SargStrictness getStrictnessComplement() {
    return (strictness == SargStrictness.OPEN) ? SargStrictness.CLOSED
        : SargStrictness.OPEN;
  }

  /**
   * @return -1 for infinitesimally below (open upper bound, strictly less
   * than), 0 for exact equality (closed bound), 1 for infinitesimally above
   * (open lower bound, strictly greater than)
   */
  public int getStrictnessSign() {
    if (strictness == SargStrictness.CLOSED) {
      return 0;
    } else {
      if (boundType == SargBoundType.LOWER) {
        return 1;
      } else {
        return -1;
      }
    }
  }

  // override Object
  public boolean equals(Object other) {
    if (!(other instanceof SargEndpoint)) {
      return false;
    }
    return compareTo((SargEndpoint) other) == 0;
  }

  // override Object
  public int hashCode() {
    return toString().hashCode();
  }
}

// End SargEndpoint.java
