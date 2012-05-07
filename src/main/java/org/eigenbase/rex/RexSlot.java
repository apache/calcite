/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.rex;

import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * Abstract base class for {@link RexInputRef} and {@link RexLocalRef}.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 25, 2005
 */
public abstract class RexSlot
    extends RexVariable
{
    //~ Instance fields --------------------------------------------------------

    protected final int index;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a slot.
     *
     * @param index Index of the field in the underlying rowtype
     * @param type Type of the column
     *
     * @pre type != null
     * @pre index >= 0
     */
    protected RexSlot(
        String name,
        int index,
        RelDataType type)
    {
        super(name, type);
        Util.pre(type != null, "type != null");
        Util.pre(index >= 0, "index >= 0");
        this.index = index;
    }

    //~ Methods ----------------------------------------------------------------

    public int getIndex()
    {
        return index;
    }

    protected static String [] makeArray(int length, String prefix)
    {
        final String [] a = new String[length];
        for (int i = 0; i < a.length; i++) {
            a[i] = prefix + i;
        }
        return a;
    }
}

// End RexSlot.java
