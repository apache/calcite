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
package org.eigenbase.util;

import java.io.*;

import java.util.*;

import org.eigenbase.util14.*;


/**
 * <code>EnumeratedValues</code> is a helper class for declaring a set of
 * symbolic constants which have names, ordinals, and possibly descriptions. The
 * ordinals do not have to be contiguous.
 *
 * <p>Typically, for a particular set of constants, you derive a class from this
 * interface, and declare the constants as <code>public static final</code>
 * members. Give it a private constructor, and a <code>public static final <i>
 * ClassName</i> instance</code> member to hold the singleton instance.</p>
 */
public class EnumeratedValues
    extends Enum14
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new empty, mutable enumeration.
     */
    public EnumeratedValues()
    {
    }

    /**
     * Creates an enumeration, with an array of values, and freezes it.
     */
    public EnumeratedValues(Value [] values)
    {
        super(values);
    }

    /**
     * Creates an enumeration, initialize it with an array of strings, and
     * freezes it.
     */
    public EnumeratedValues(String [] names)
    {
        super(names);
    }

    /**
     * Create an enumeration, initializes it with arrays of code/name pairs, and
     * freezes it.
     */
    public EnumeratedValues(
        String [] names,
        int [] codes)
    {
        super(names, codes);
    }

    /**
     * Create an enumeration, initializes it with arrays of code/name pairs, and
     * freezes it.
     */
    public EnumeratedValues(
        String [] names,
        int [] codes,
        String [] descriptions)
    {
        super(names, codes, descriptions);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a mutable enumeration from an existing enumeration, which may
     * already be immutable.
     */
    public EnumeratedValues getMutableClone()
    {
        return (EnumeratedValues) clone();
    }
}

// End EnumeratedValues.java
