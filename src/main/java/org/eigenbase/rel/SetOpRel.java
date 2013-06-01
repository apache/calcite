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

import net.hydromatic.optiq.runtime.FlatLists;

import net.hydromatic.linq4j.Ord;


/**
 * <code>SetOpRel</code> is an abstract base for relational set operators such
 * as UNION, MINUS (aka EXCEPT), and INTERSECT.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class SetOpRel
    extends AbstractRelNode
{
    //~ Instance fields --------------------------------------------------------

    protected List<RelNode> inputs;
    public final boolean all;

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

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        final RelNode[] newInputs = inputs.toArray(new RelNode[inputs.size()]);
        newInputs[ordinalInParent] = p;
        inputs = FlatLists.of(newInputs);
        recomputeDigest();
    }

    @Override
    public boolean isKey(BitSet columns) {
        // If not ALL then the rows are distinct.
        // Therefore the set of all columns is a key.
        return !all && columns.nextClearBit(0) >= getRowType().getFieldCount();
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
        for (RelNode input : getInputs()) {
            if (!RelOptUtil.areRowTypesEqual(
                    input.getRowType(), unionType, compareNames))
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
