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
 * Local variable.
 *
 * <p>Identity is based upon type and index. We want multiple references to the
 * same slot in the same context to be equal. A side effect is that references
 * to slots in different contexts which happen to have the same index and type
 * will be considered equal; this is not desired, but not too damaging, because
 * of the immutability.
 *
 * <p>Variables are immutable.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 25, 2005
 */
public class RexLocalRef
    extends RexSlot
{
    //~ Static fields/initializers ---------------------------------------------

    // array of common names, to reduce memory allocations
    private static final String [] names = makeArray(32, "$t");

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a local variable.
     *
     * @param index Index of the field in the underlying rowtype
     * @param type Type of the column
     *
     * @pre type != null
     * @pre index >= 0
     */
    public RexLocalRef(
        int index,
        RelDataType type)
    {
        super(
            createName(index),
            index,
            type);
    }

    //~ Methods ----------------------------------------------------------------

    public RexLocalRef clone()
    {
        // Since refs are immutable and identity is based on value,
        // there's no point returning a copy.
        return this;
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof RexLocalRef) {
            RexLocalRef that = (RexLocalRef) obj;
            return (this.type == that.type) && (this.index == that.index);
        }
        return false;
    }

    public int hashCode()
    {
        return Util.hash(
            type.hashCode(),
            index);
    }

    public <R> R accept(RexVisitor<R> visitor)
    {
        return visitor.visitLocalRef(this);
    }

    private static String createName(int index)
    {
        return (index < names.length) ? names[index] : ("$t" + index);
    }
}

// End RexLocalRef.java
