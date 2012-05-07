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
package org.eigenbase.util.property;

import java.util.*;


/**
 * Definition and accessor for a double-precision property.
 *
 * @author jhyde
 * @version $Id$
 * @since July 5, 2005
 */
public class DoubleProperty
    extends Property
{
    //~ Instance fields --------------------------------------------------------

    private final double minValue;
    private final double maxValue;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Double property.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value.
     */
    public DoubleProperty(
        Properties properties,
        String path,
        double defaultValue)
    {
        this(
            properties,
            path,
            defaultValue,
            -Double.MAX_VALUE,
            Double.MAX_VALUE);
    }

    /**
     * Creates a Double property which has no default value.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     */
    public DoubleProperty(
        Properties properties,
        String path)
    {
        this(properties, path, -Double.MAX_VALUE, Double.MAX_VALUE);
    }

    /**
     * Creates a Double property.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value.
     *
     * @throws IllegalArgumentException if <code>defaultValue</code> is not in
     * the range [<code>minValue</code>, <code>maxValue</code>].
     */
    public DoubleProperty(
        Properties properties,
        String path,
        double defaultValue,
        double minValue,
        double maxValue)
    {
        super(
            properties,
            path,
            Double.toString(defaultValue));

        if (minValue > maxValue) {
            double temp = minValue;
            minValue = maxValue;
            maxValue = temp;
        }

        if ((defaultValue < minValue) || (defaultValue > maxValue)) {
            throw new IllegalArgumentException(
                "invalid default value " + defaultValue);
        }

        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    /**
     * Creates a Double property which has no default value.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     */
    public DoubleProperty(
        Properties properties,
        String path,
        double minValue,
        double maxValue)
    {
        super(properties, path, null);

        if (minValue > maxValue) {
            double temp = minValue;
            minValue = maxValue;
            maxValue = temp;
        }

        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves the value of this double property according to these rules.
     *
     * <ul>
     * <li>If the property has no value, returns the default value.</li>
     * <li>If there is no default value and {@link #minValue} &lt;= 0.0 &lt;=
     * {@link #maxValue}, returns 0.0.</li>
     * <li>If there is no default value and 0.0 is not in the min/max range,
     * returns {@link #minValue}.</li>
     * </ul>
     */
    public double get()
    {
        final String value = getInternal(null, false);
        if (value == null) {
            return noValue();
        }

        double v = Double.parseDouble(value);

        return limit(v);
    }

    /**
     * Retrieves the value of this double property. If the property has no
     * value, returns the default value. If there is no default value, returns
     * the given default value. In all cases, the returned value is limited to
     * the min/max value range given during construction.
     */
    public double get(double defaultValue)
    {
        final String value =
            getInternal(
                Double.toString(defaultValue),
                false);
        if (value == null) {
            return limit(defaultValue);
        }

        double v = Double.parseDouble(value);

        // need to limit value in case setString() was called directly with
        // an out-of-range value
        return limit(v);
    }

    /**
     * Sets the value of this double property. The value is limited to the
     * min/max range given during construction.
     *
     * @return the previous value, or if not set: the default value. If no
     * default value exists, 0.0 if that value is in the range [minValue,
     * maxValue], or minValue if 0.0 is not in the range
     */
    public double set(double value)
    {
        String prevValue = setString(Double.toString(limit(value)));
        if (prevValue == null) {
            prevValue = getDefaultValue();
            if (prevValue == null) {
                return noValue();
            }
        }

        double v = Double.parseDouble(prevValue);

        return limit(v);
    }

    /**
     * Returns value limited to the range [minValue, maxValue].
     *
     * @param value the value to limit
     *
     * @return value limited to the range [minValue, maxValue].
     */
    private double limit(double value)
    {
        return Math.min(
            Math.max(value, minValue),
            maxValue);
    }

    /**
     * Returns 0.0 if that value is in the range [minValue, maxValue].
     * Otherwise, returns minValue.
     */
    private double noValue()
    {
        if ((minValue <= 0.0) && (maxValue >= 0.0)) {
            return 0.0;
        } else {
            return minValue;
        }
    }
}

// End DoubleProperty.java
