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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

/**
 * A relational expression representing a set of window aggregates.
 *
 * <p>A window rel can handle several window aggregate functions, over several
 * partitions, with pre- and post-expressions, and an optional post-filter.
 * Each of the partitions is defined by a partition key (zero or more columns)
 * and a range (logical or physical). The partitions expect the data to be
 * sorted correctly on input to the relational expression.
 *
 * <p>Each {@link org.eigenbase.rel.WindowRelBase.Window} has a set of
 * {@link org.eigenbase.rel.WindowRelBase.Partition} objects, and each partition
 * has a set of {@link org.eigenbase.rex.RexOver} objects.
 *
 * <p>Created by {@link org.eigenbase.rel.rules.WindowedAggSplitterRule}.
 */
public final class WindowRel extends WindowRelBase {
    /**
     * Creates a WindowRel.
     *
     * @param cluster Cluster
     * @param child Input relational expression
     * @param rowType Output row type
     * @param windows Windows
     */
    public WindowRel(
        RelOptCluster cluster, RelTraitSet traits, RelNode child,
        RelDataType rowType, List<Window> windows)
    {
        super(cluster, traits, child, rowType, windows);
    }

    @Override public WindowRel copy(RelTraitSet traitSet, List<RelNode> inputs)
    {
        return new WindowRel(
            getCluster(), traitSet, sole(inputs), rowType, windows);
    }

    /** Creates a WindowRel. */
    public static WindowRel create(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexProgram program,
        RelDataType rowType)
    {
        // TODO:
        return new WindowRel(cluster, traitSet, child, rowType, null);
    }
}

// End WindowRel.java
