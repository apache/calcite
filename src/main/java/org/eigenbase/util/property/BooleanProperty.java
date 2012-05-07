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
 * Definition and accessor for a boolean property.
 *
 * @author jhyde
 * @version $Id$
 * @since May 4, 2004
 */
public class BooleanProperty
    extends Property
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Boolean property.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value, null if there is no default.
     */
    public BooleanProperty(
        Properties properties,
        String path,
        boolean defaultValue)
    {
        super(properties, path, defaultValue ? "true" : "false");
    }

    /**
     * Creates a Boolean property which has no default value.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     */
    public BooleanProperty(
        Properties properties,
        String path)
    {
        super(properties, path, null);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves the value of this boolean property.
     *
     * <p>Returns <code>true</code> if the property exists, and its value is
     * <code>1</code>, <code>true</code> or <code>yes</code>; returns <code>
     * false</code> otherwise.
     */
    public boolean get()
    {
        return booleanValue();
    }

    /**
     * Retrieves the value of this boolean property.
     *
     * <p>Returns <code>true</code> if the property exists, and its value is
     * <code>1</code>, <code>true</code> or <code>yes</code>; returns <code>
     * false</code> otherwise.
     */
    public boolean get(boolean defaultValue)
    {
        final String value =
            getInternal(
                Boolean.toString(defaultValue),
                false);
        if (value == null) {
            return defaultValue;
        }
        return toBoolean(value);
    }

    /**
     * Sets the value of this boolean property.
     *
     * @return The previous value, or the default value if not set.
     */
    public boolean set(boolean value)
    {
        String prevValue = setString(Boolean.toString(value));
        if (prevValue == null) {
            prevValue = getDefaultValue();
            if (prevValue == null) {
                return false;
            }
        }
        return toBoolean(prevValue);
    }
}

// End BooleanProperty.java
