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

import org.eigenbase.util.*;


/**
 * Shuttle which applies a permutation to its input fields.
 *
 * @author jhyde
 * @version $Id$
 * @see RexPermutationShuttle
 */
public class RexPermuteInputsShuttle
    extends RexShuttle
{
    //~ Instance fields --------------------------------------------------------

    private final Permutation permutation;

    //~ Constructors -----------------------------------------------------------

    public RexPermuteInputsShuttle(Permutation permutation)
    {
        this.permutation = permutation;
    }

    //~ Methods ----------------------------------------------------------------

    public RexNode visitInputRef(RexInputRef local)
    {
        final int index = local.getIndex();
        int target = permutation.getTarget(index);
        return new RexInputRef(
            target,
            local.getType());
    }
}

// End RexPermuteInputsShuttle.java
