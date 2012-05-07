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
package org.eigenbase.oj.rel;

import java.util.*;
import java.util.List;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.runtime.*;


/**
 * <code>IterOneRowRel</code> is an iterator implementation of {@link
 * OneRowRel}.
 */
public class IterOneRowRel
    extends OneRowRelBase
    implements JavaRel
{
    //~ Constructors -----------------------------------------------------------

    public IterOneRowRel(RelOptCluster cluster)
    {
        super(
            cluster,
            cluster.getEmptyTraitSet().plus(CallingConvention.ITERATOR));
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs)
    {
        assert traitSet.comprises(CallingConvention.ITERATOR);
        assert inputs.isEmpty();
        return this;
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        OJClass outputRowClass =
            OJUtil.typeToOJClass(
                getRowType(),
                getCluster().getTypeFactory());

        Expression newRowExp =
            new AllocationExpression(
                TypeName.forOJClass(outputRowClass),
                new ExpressionList());

        Expression iterExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(
                    RestartableCollectionTupleIter.class),
                new ExpressionList(
                    new MethodCall(
                        OJUtil.typeNameForClass(Collections.class),
                        "singletonList",
                        new ExpressionList(newRowExp))));
        return iterExp;
    }
}

// End IterOneRowRel.java
