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
 * Definition and accessor for an integer property.
 *
 * @author jhyde
 * @version $Id$
 * @since May 4, 2004
 */
public class IntegerProperty
    extends Property
{
    //~ Instance fields --------------------------------------------------------

    private final int minValue;
    private final int maxValue;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an Integer property. Minimum and maximum values are set to {@link
     * Integer#MIN_VALUE} and {@link Integer#MAX_VALUE}.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value.
     */
    public IntegerProperty(
        Properties properties,
        String path,
        int defaultValue)
    {
        this(
            properties,
            path,
            defaultValue,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE);
    }

    /**
     * Creates an Integer property which has no default value. Minimum and
     * maximum values are set to {@link Integer#MIN_VALUE} and {@link
     * Integer#MAX_VALUE}.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     */
    public IntegerProperty(
        Properties properties,
        String path)
    {
        this(properties, path, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Creates an Integer property with a default value and fixed minimum and
     * maximum values.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value.
     * @param minValue the minimum value of this property (inclusive)
     * @param maxValue the maximum value of this property (inclusive)
     *
     * @throws IllegalArgumentException if <code>defaultValue</code> is not in
     * the range [<code>minValue</code>, <code>maxValue</code>].
     */
    public IntegerProperty(
        Properties properties,
        String path,
        int defaultValue,
        int minValue,
        int maxValue)
    {
        super(
            properties,
            path,
            Integer.toString(defaultValue));

        if (minValue > maxValue) {
            int temp = minValue;
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
     * Creates an Integer property with fixed minimum and maximum values.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     * @param minValue the minimum value of this property (inclusive)
     * @param maxValue the maximum value of this property (inclusive)
     */
    public IntegerProperty(
        Properties properties,
        String path,
        int minValue,
        int maxValue)
    {
        super(properties, path, null);

        if (minValue > maxValue) {
            int temp = minValue;
            minValue = maxValue;
            maxValue = temp;
        }

        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves the value of this integer property according to these rules.
     *
     * <ul>
     * <li>If the property has no value, returns the default value.</li>
     * <li>If there is no default value and {@link #minValue} &lt;= 0 &lt;=
     * {@link #maxValue}, returns 0.</li>
     * <li>If there is no default value and 0 is not in the min/max range,
     * returns {@link #minValue}.</li>
     * </ul>
     */
    public int get()
    {
        final String value = getInternal(null, false);
        if (value == null) {
            return noValue();
        }

        int v = Integer.parseInt(value);

        // need to limit value in case setString() was called directly with
        // an out-of-range value
        return limit(v);
    }

    /**
     * Retrieves the value of this integer property. If the property has no
     * value, returns the default value. If there is no default value, returns
     * the given default value. In all cases, the returned value is limited to
     * the min/max value range given during construction.
     */
    public int get(int defaultValue)
    {
        final String value =
            getInternal(
                Integer.toString(defaultValue),
                false);
        if (value == null) {
            return limit(defaultValue);
        }

        int v = Integer.parseInt(value);

        return limit(v);
    }

    /**
     * Sets the value of this integer property. The value is limited to the
     * min/max range given during construction.
     *
     * @return the previous value, or if not set: the default value. If no
     * default value exists, 0 if that value is in the range [minValue,
     * maxValue], or minValue if 0 is not in the range
     */
    public int set(int value)
    {
        String prevValue = setString(Integer.toString(limit(value)));
        if (prevValue == null) {
            prevValue = getDefaultValue();
            if (prevValue == null) {
                return noValue();
            }
        }

        int v = Integer.parseInt(prevValue);

        return limit(v);
    }

    /**
     * Returns value limited to the range [minValue, maxValue].
     *
     * @param value the value to limit
     *
     * @return value limited to the range [minValue, maxValue].
     */
    private int limit(int value)
    {
        return Math.min(
            Math.max(value, minValue),
            maxValue);
    }

    /**
     * Returns 0 if that value is in the range [minValue, maxValue]. Otherwise,
     * returns minValue.
     */
    private int noValue()
    {
        if ((minValue <= 0) && (maxValue >= 0)) {
            return 0;
        } else {
            return minValue;
        }
    }
}

// End IntegerProperty.java
