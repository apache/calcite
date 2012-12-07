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

import java.util.List;

import org.eigenbase.relopt.*;


/**
 * SamplingRel represents the TABLESAMPLE BERNOULLI or SYSTEM keyword applied to
 * a table, view or subquery.
 *
 * @author Stephan Zuercher
 */
public class SamplingRel
    extends SingleRel
{
    //~ Instance fields --------------------------------------------------------

    private final RelOptSamplingParameters params;

    //~ Constructors -----------------------------------------------------------

    public SamplingRel(
        RelOptCluster cluster,
        RelNode child,
        RelOptSamplingParameters params)
    {
        super(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            child);
        this.params = params;
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.comprises(Convention.NONE);
        return new SamplingRel(
            getCluster(),
            sole(inputs),
            params);
    }

    /**
     * Retrieve the sampling parameters for this SamplingRel.
     */
    public RelOptSamplingParameters getSamplingParameters()
    {
        return params;
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child", "mode", "rate", "repeatableSeed" },
            new Object[] {
                params.isBernoulli() ? "bernoulli" : "system",
                params.getSamplingPercentage(),
                params.isRepeatable() ? params.getRepeatableSeed() : "-"
            });
    }
}

// End SamplingRel.java
