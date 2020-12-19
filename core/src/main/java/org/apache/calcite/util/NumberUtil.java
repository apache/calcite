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
package org.apache.calcite.util;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Utility functions for working with numbers.
 */
public class NumberUtil {
  private NumberUtil() {}

  //~ Static fields/initializers ---------------------------------------------

  private static final DecimalFormat FLOAT_FORMATTER;
  private static final DecimalFormat DOUBLE_FORMATTER;
  private static final BigInteger[] BIG_INT_TEN_POW;
  private static final BigInteger[] BIG_INT_MIN_UNSCALED;
  private static final BigInteger[] BIG_INT_MAX_UNSCALED;

  static {
    // TODO: DecimalFormat uses ROUND_HALF_EVEN, not ROUND_HALF_UP
    // Float: precision of 7 (6 digits after .)
    FLOAT_FORMATTER = decimalFormat("0.######E0");

    // Double: precision of 16 (15 digits after .)
    DOUBLE_FORMATTER = decimalFormat("0.###############E0");

    BIG_INT_TEN_POW = new BigInteger[20];
    BIG_INT_MIN_UNSCALED = new BigInteger[20];
    BIG_INT_MAX_UNSCALED = new BigInteger[20];

    for (int i = 0; i < BIG_INT_TEN_POW.length; i++) {
      BIG_INT_TEN_POW[i] = BigInteger.TEN.pow(i);
      if (i < 19) {
        BIG_INT_MAX_UNSCALED[i] = BIG_INT_TEN_POW[i].subtract(BigInteger.ONE);
        BIG_INT_MIN_UNSCALED[i] = BIG_INT_MAX_UNSCALED[i].negate();
      } else {
        BIG_INT_MAX_UNSCALED[i] = BigInteger.valueOf(Long.MAX_VALUE);
        BIG_INT_MIN_UNSCALED[i] = BigInteger.valueOf(Long.MIN_VALUE);
      }
    }
  }

  //~ Methods ----------------------------------------------------------------

  /** Creates a format. Locale-independent. */
  public static DecimalFormat decimalFormat(String pattern) {
    return new DecimalFormat(pattern,
        DecimalFormatSymbols.getInstance(Locale.ROOT));
  }

  public static BigInteger powTen(int exponent) {
    if ((exponent >= 0) && (exponent < BIG_INT_TEN_POW.length)) {
      return BIG_INT_TEN_POW[exponent];
    } else {
      return BigInteger.TEN.pow(exponent);
    }
  }

  public static BigInteger getMaxUnscaled(int precision) {
    return BIG_INT_MAX_UNSCALED[precision];
  }

  public static BigInteger getMinUnscaled(int precision) {
    return BIG_INT_MIN_UNSCALED[precision];
  }

  /** Sets the scale of a BigDecimal {@code bd} if it is not null;
   * always returns {@code bd}. */
  public static @PolyNull BigDecimal rescaleBigDecimal(@PolyNull BigDecimal bd,
      int scale) {
    if (bd != null) {
      bd = bd.setScale(scale, RoundingMode.HALF_UP);
    }
    return bd;
  }

  public static BigDecimal toBigDecimal(Number number, int scale) {
    BigDecimal bd = toBigDecimal(number);
    return rescaleBigDecimal(bd, scale);
  }

  /** Converts a number to a BigDecimal with the same value;
   * returns null if and only if the number is null. */
  public static @PolyNull BigDecimal toBigDecimal(@PolyNull Number number) {
    if (number == null) {
      return castNonNull(null);
    }
    if (number instanceof BigDecimal) {
      return (BigDecimal) number;
    } else if ((number instanceof Double)
        || (number instanceof Float)) {
      return BigDecimal.valueOf(number.doubleValue());
    } else if (number instanceof BigInteger) {
      return new BigDecimal((BigInteger) number);
    } else {
      return new BigDecimal(number.longValue());
    }
  }

  /** Returns whether a {@link BigDecimal} is a valid Farrago decimal. If a
   * BigDecimal's unscaled value overflows a long, then it is not a valid
   * Farrago decimal. */
  public static boolean isValidDecimal(BigDecimal bd) {
    BigInteger usv = bd.unscaledValue();
    long usvl = usv.longValue();
    return usv.equals(BigInteger.valueOf(usvl));
  }

  public static NumberFormat getApproxFormatter(boolean isFloat) {
    return isFloat ? FLOAT_FORMATTER : DOUBLE_FORMATTER;
  }

  public static long round(double d) {
    if (d < 0) {
      return (long) (d - 0.5);
    } else {
      return (long) (d + 0.5);
    }
  }

  /** Returns the sum of two numbers, or null if either is null. */
  public static @PolyNull Double add(@PolyNull Double a, @PolyNull Double b) {
    if (a == null || b == null) {
      return null;
    }

    return a + b;
  }

  /** Returns the difference of two numbers,
   * or null if either is null. */
  public static @PolyNull Double subtract(@PolyNull Double a, @PolyNull Double b) {
    if (a == null || b == null) {
      return castNonNull(null);
    }

    return a - b;
  }

  /** Returns the quotient of two numbers,
   * or null if either is null or the divisor is zero. */
  public static @Nullable Double divide(@Nullable Double a, @Nullable Double b) {
    if ((a == null) || (b == null) || (b == 0D)) {
      return castNonNull(null);
    }

    return a / b;
  }

  /** Returns the product of two numbers,
   * or null if either is null. */
  public static @PolyNull Double multiply(@PolyNull Double a, @PolyNull Double b) {
    if (a == null || b == null) {
      return castNonNull(null);
    }

    return a * b;
  }

  /** Like {@link Math#min} but null safe;
   * returns the lesser of two numbers,
   * ignoring numbers that are null,
   * or null if both are null. */
  public static @PolyNull Double min(@PolyNull Double a, @PolyNull Double b) {
    if (a == null) {
      return b;
    } else if (b == null) {
      return a;
    } else {
      return Math.min(a, b);
    }
  }

  /** Like {@link Math#max} but null safe;
   * returns the greater of two numbers,
   * or null if either is null. */
  public static @PolyNull Double max(@PolyNull Double a, @PolyNull Double b) {
    if (a == null || b == null) {
      return castNonNull(null);
    }
    return Math.max(a, b);
  }
}
