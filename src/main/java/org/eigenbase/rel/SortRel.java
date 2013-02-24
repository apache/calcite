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
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

import net.hydromatic.linq4j.Ord;


/**
 * Relational expression which imposes a particular sort order on its input
 * without otherwise changing its content.
 */
public class SortRel
    extends SingleRel
{
    //~ Instance fields --------------------------------------------------------

    protected final List<RelFieldCollation> collations;
    protected final RexNode [] fieldExps;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a sorter.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits Traits
     * @param child input relational expression
     * @param collations array of sort specifications
     */
    public SortRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        List<RelFieldCollation> collations)
    {
        super(cluster, traits, child);
        this.collations = collations;

        fieldExps = new RexNode[collations.size()];
        final RelDataTypeField [] fields = getRowType().getFields();
        for (int i = 0; i < collations.size(); ++i) {
            int iField = collations.get(i).getFieldIndex();
            fieldExps[i] =
                cluster.getRexBuilder().makeInputRef(
                    fields[iField].getType(),
                    iField);
        }
    }

    //~ Methods ----------------------------------------------------------------

    public SortRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), collations);
    }

    public SortRel copy(
        RelTraitSet traitSet,
        RelNode newInput,
        List<RelFieldCollation> newCollations)
    {
        assert traitSet.comprises(Convention.NONE);
        return new SortRel(
            getCluster(),
            getCluster().traitSetOf(Convention.NONE),
            newInput,
            newCollations);
    }

    public RexNode [] getChildExps()
    {
        return fieldExps;
    }

    /**
     * @return array of RelFieldCollations, from most significant to least
     * significant
     */
    public List<RelFieldCollation> getCollations()
    {
        return collations;
    }

    public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
        super.explainTerms(pw);
        assert fieldExps.length == collations.size();
        for (Ord<RexNode> ord : Ord.zip(fieldExps)) {
            pw.item("sort" + ord.i, ord.e);
        }
        for (Ord<RelFieldCollation> ord : Ord.zip(collations)) {
            pw.item("dir" + ord.i, ord.e.shortString());
        }
        return pw;
    }
}

// End SortRel.java
