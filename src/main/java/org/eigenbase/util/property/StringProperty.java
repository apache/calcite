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
 * Definition and accessor for a string property.
 *
 * @author jhyde
 * @version $Id$
 * @since May 4, 2004
 */
public class StringProperty
    extends Property
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a string property.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value, null if there is no default.
     */
    public StringProperty(
        Properties properties,
        String path,
        String defaultValue)
    {
        super(properties, path, defaultValue);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves the value of this property. Returns the property's default
     * value if the property set has no value for this property.
     */
    public String get()
    {
        return stringValue();
    }

    /**
     * Retrieves the value of this property, optionally failing if there is no
     * value. Returns the property's default value if the property set has no
     * value for this property.
     */
    public String get(boolean required)
    {
        return getInternal(null, required);
    }

    /**
     * Retrieves the value of this property, or the default value if none is
     * found.
     */
    public String get(String defaultValue)
    {
        return getInternal(defaultValue, false);
    }

    /**
     * Sets the value of this property.
     *
     * @return The previous value, or the default value if not set.
     */
    public String set(String value)
    {
        String prevValue = setString(value);
        if (prevValue == null) {
            prevValue = getDefaultValue();
        }
        return prevValue;
    }
}

// End StringProperty.java
