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

import java.util.AbstractList;
import java.util.List;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;

import net.hydromatic.linq4j.Ord;


/**
 * <code>SetOpRel</code> is an abstract base for relational set operators such
 * as union, minus, and intersect.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class SetOpRel
    extends AbstractRelNode
{
    //~ Instance fields --------------------------------------------------------

    protected List<RelNode> inputs;
    protected boolean all;

    //~ Constructors -----------------------------------------------------------

    protected SetOpRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        List<RelNode> inputs,
        boolean all)
    {
        super(cluster, traits);
        this.inputs = inputs;
        this.all = all;
    }

    //~ Methods ----------------------------------------------------------------

    public abstract SetOpRel copy(
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all);

    public SetOpRel copy(
        RelTraitSet traitSet,
        List<RelNode> inputs)
    {
        return copy(traitSet, inputs, all);
    }

    public boolean isDistinct()
    {
        return !all;
    }

    public List<RelNode> getInputs()
    {
        return inputs;
    }

    public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
        super.explainTerms(pw);
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e);
        }
        return pw.item("all", all);
    }

    protected RelDataType deriveRowType()
    {
        return getCluster().getTypeFactory().leastRestrictive(
            new AbstractList<RelDataType>() {
                public RelDataType get(int index) {
                    return inputs.get(index).getRowType();
                }

                public int size() {
                    return inputs.size();
                }
            });
    }

    /**
     * Returns whether all the inputs of this set operator have the same row
     * type as its output row.
     *
     * @param compareNames whether or not column names are important in the
     * homogeneity comparison
     */
    public boolean isHomogeneous(boolean compareNames)
    {
        RelDataType unionType = getRowType();
        List<RelNode> inputs = getInputs();
        for (int i = 0; i < inputs.size(); ++i) {
            RelDataType inputType = inputs.get(i).getRowType();
            if (!RelOptUtil.areRowTypesEqual(
                    inputType,
                    unionType,
                    compareNames))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether all the inputs of this set operator have the same row
     * type as its output row. Equivalent to {@link #isHomogeneous(boolean)
     * isHomogeneous(true)}.
     */
    public boolean isHomogeneous()
    {
        return isHomogeneous(true);
    }
}

// End SetOpRel.java
