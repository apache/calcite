/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.util14;

import java.math.*;

import java.text.*;

/**
 * Utility functions for working with numbers This class is JDK 1.4 compatible.
 */
public class NumberUtil
{
    //~ Static fields/initializers ---------------------------------------------

    private static final DecimalFormat floatFormatter;
    private static final DecimalFormat doubleFormatter;
    private static final BigInteger [] bigIntTenPow;
    private static final BigInteger [] bigIntMinUnscaled;
    private static final BigInteger [] bigIntMaxUnscaled;

    // For JDK 1.4 compatibility
    private static final BigInteger bigIntTen = BigInteger.valueOf(10);

    public static final Byte MIN_BYTE = new Byte(Byte.MIN_VALUE);
    public static final Byte MAX_BYTE = new Byte(Byte.MAX_VALUE);
    public static final Integer MIN_INTEGER = new Integer(Integer.MIN_VALUE);
    public static final Integer MAX_INTEGER = new Integer(Integer.MAX_VALUE);
    public static final Short MIN_SHORT = new Short(Short.MIN_VALUE);
    public static final Short MAX_SHORT = new Short(Short.MAX_VALUE);
    public static final Long MIN_LONG = new Long(Long.MIN_VALUE);
    public static final Long MAX_LONG = new Long(Long.MAX_VALUE);
    public static final Float MIN_FLOAT = new Float(-Float.MAX_VALUE);
    public static final Float MAX_FLOAT = new Float(Float.MAX_VALUE);
    public static final Double MIN_DOUBLE = new Double(-Double.MAX_VALUE);
    public static final Double MAX_DOUBLE = new Double(Double.MAX_VALUE);

    public static final Integer INTEGER_ZERO = new Integer(0);
    public static final Integer INTEGER_ONE = new Integer(1);

    static {
        // TODO: DecimalFormat uses ROUND_HALF_EVEN, not ROUND_HALF_UP
        // Float: precision of 7 (6 digits after .)
        floatFormatter = new DecimalFormat();
        floatFormatter.applyPattern("0.######E0");

        // Double: precision of 16 (15 digits after .)
        doubleFormatter = new DecimalFormat();
        doubleFormatter.applyPattern("0.###############E0");

        bigIntTenPow = new BigInteger[20];
        bigIntMinUnscaled = new BigInteger[20];
        bigIntMaxUnscaled = new BigInteger[20];

        for (int i = 0; i < bigIntTenPow.length; i++) {
            bigIntTenPow[i] = bigIntTen.pow(i);
            if (i < 19) {
                bigIntMaxUnscaled[i] = bigIntTenPow[i].subtract(BigInteger.ONE);
                bigIntMinUnscaled[i] = bigIntMaxUnscaled[i].negate();
            } else {
                bigIntMaxUnscaled[i] = BigInteger.valueOf(Long.MAX_VALUE);
                bigIntMinUnscaled[i] = BigInteger.valueOf(Long.MIN_VALUE);
            }
        }
    }

    //~ Methods ----------------------------------------------------------------

    public static final BigInteger powTen(int exponent)
    {
        if ((exponent >= 0) && (exponent < bigIntTenPow.length)) {
            return bigIntTenPow[exponent];
        } else {
            return bigIntTen.pow(exponent);
        }
    }

    public static final BigInteger getMaxUnscaled(int precision)
    {
        return bigIntMaxUnscaled[precision];
    }

    public static final BigInteger getMinUnscaled(int precision)
    {
        return bigIntMinUnscaled[precision];
    }

    public static final BigDecimal rescaleBigDecimal(BigDecimal bd, int scale)
    {
        if (bd != null) {
            bd = bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
        }
        return bd;
    }

    public static final BigDecimal toBigDecimal(Number number, int scale)
    {
        BigDecimal bd = toBigDecimal(number);
        return rescaleBigDecimal(bd, scale);
    }

    public static final BigDecimal toBigDecimal(Number number)
    {
        if (number == null) {
            return null;
        }
        if (number instanceof BigDecimal) {
            return (BigDecimal) number;
        } else if ((number instanceof Double)
            || (number instanceof Float))
        {
            // For JDK 1.4 compatibility
            return new BigDecimal(((Number) number).doubleValue());
                //return BigDecimal.valueOf(((Number) number).doubleValue());
        } else if (number instanceof BigInteger) {
            return new BigDecimal((BigInteger) number);
        } else {
            return new BigDecimal(((Number) number).longValue());
        }
    }

    /**
     * @return whether a BigDecimal is a valid Farrago decimal. If a
     * BigDecimal's unscaled value overflows a long, then it is not a valid
     * Farrago decimal.
     */
    public static boolean isValidDecimal(BigDecimal bd)
    {
        BigInteger usv = bd.unscaledValue();
        long usvl = usv.longValue();
        return usv.equals(BigInteger.valueOf(usvl));
    }

    public static NumberFormat getApproxFormatter(boolean isFloat)
    {
        return (isFloat) ? floatFormatter : doubleFormatter;
    }

    public static long round(double d)
    {
        if (d < 0) {
            return (long) (d - 0.5);
        } else {
            return (long) (d + 0.5);
        }
    }

    public static Double add(Double a, Double b)
    {
        if ((a == null) || (b == null)) {
            return null;
        }

        // For JDK 1.4 compatibility
        return new Double(a.doubleValue() + b.doubleValue());
            //return Double.valueOf(a.doubleValue() + b.doubleValue());
    }

    public static Double divide(Double a, Double b)
    {
        if ((a == null) || (b == null) || (b.doubleValue() == 0.0)) {
            return null;
        }

        // For JDK 1.4 compatibility
        return new Double(a.doubleValue() / b.doubleValue());
            // return Double.valueOf(a.doubleValue() / b.doubleValue());
    }

    public static Double multiply(Double a, Double b)
    {
        if ((a == null) || (b == null)) {
            return null;
        }

        // For JDK 1.4 compatibility
        return new Double(a.doubleValue() * b.doubleValue());
            //return Double.valueOf(a.doubleValue() * b.doubleValue());
    }
}

// End NumberUtil.java
