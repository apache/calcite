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
package org.eigenbase.rex;

import org.eigenbase.reltype.*;


/**
 * Dynamic parameter reference in a row-expression.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RexDynamicParam
    extends RexVariable
{
    //~ Instance fields --------------------------------------------------------

    private final int index;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a dynamic parameter.
     *
     * @param type inferred type of parameter
     * @param index 0-based index of dynamic parameter in statement
     */
    public RexDynamicParam(
        RelDataType type,
        int index)
    {
        super("?" + index, type);
        this.index = index;
    }

    //~ Methods ----------------------------------------------------------------

    public RexDynamicParam clone()
    {
        return new RexDynamicParam(type, index);
    }

    public RexKind getKind()
    {
        return RexKind.DynamicParam;
    }

    public int getIndex()
    {
        return index;
    }

    public <R> R accept(RexVisitor<R> visitor)
    {
        return visitor.visitDynamicParam(this);
    }
}

// End RexDynamicParam.java
