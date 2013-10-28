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

import java.util.Collections;
import java.util.List;

import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

import net.hydromatic.linq4j.Ord;

import com.google.common.collect.ImmutableList;

/**
 * Relational expression which imposes a particular sort order on its input
 * without otherwise changing its content.
 */
public class SortRel
    extends SingleRel
{
    //~ Instance fields --------------------------------------------------------

    protected final RelCollation collation;
    protected final ImmutableList<RexNode> fieldExps;
    public final RexNode offset;
    public final RexNode fetch;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a sorter.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits Traits
     * @param child input relational expression
     * @param collation array of sort specifications
     */
    public SortRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation)
    {
        this(cluster, traits, child, collation, null, null);
    }

    /**
     * Creates a sorter.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits Traits
     * @param child input relational expression
     * @param collation array of sort specifications
     * @param offset Expression for number of rows to discard before returning
     *               first row
     * @param fetch Expression for number of rows to fetch
     */
    public SortRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation,
        RexNode offset,
        RexNode fetch)
    {
        super(cluster, traits, child);
        this.collation = collation;
        this.offset = offset;
        this.fetch = fetch;

        assert traits.containsIfApplicable(collation)
            : "traits=" + traits + ", collation=" + collation;
        assert !(fetch == null
            && offset == null
            && collation.getFieldCollations().isEmpty())
            : "trivial sort";
        ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        for (RelFieldCollation field : collation.getFieldCollations()) {
            int index = field.getFieldIndex();
            builder.add(
                cluster.getRexBuilder().makeInputRef(
                    getRowType().getFieldList().get(index).getType(), index));
        }
        fieldExps = builder.build();
    }

    //~ Methods ----------------------------------------------------------------

    public SortRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), collation);
    }

    public SortRel copy(
        RelTraitSet traitSet,
        RelNode newInput,
        RelCollation newCollation)
    {
      return copy(traitSet, newInput, newCollation, offset, fetch);
    }

    public SortRel copy(
        RelTraitSet traitSet,
        RelNode newInput,
        RelCollation newCollation,
        RexNode offset,
        RexNode fetch)
    {
        assert traitSet.contains(Convention.NONE);
        return new SortRel(
            getCluster(),
            traitSet,
            newInput,
            newCollation,
            offset,
            fetch);
    }

    @Override public List<RexNode> getChildExps() {
        return fieldExps;
    }

    /**
     * Returns the array of {@link RelFieldCollation}s asked for by the sort
     * specification, from most significant to least significant.
     *
     * <p>See also {@link #getCollationList()}, inherited from {@link RelNode},
     * which lists all known collations. For example,
     * <code>ORDER BY time_id</code> might also be sorted by
     * <code>the_year, the_month</code> because of a known monotonicity
     * constraint among the columns. {@code getCollations} would return
     * <code>[time_id]</code> and {@code getCollationList} would return
     * <code>[ [time_id], [the_year, the_month] ]</code>.</p>
     */
    public RelCollation getCollation()
    {
        return collation;
    }

    @Override
    public List<RelCollation> getCollationList() {
        // TODO: include each prefix of the collation, e.g [[x, y], [x], []]
        return Collections.singletonList(getCollation());
    }

    public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
        super.explainTerms(pw);
        assert fieldExps.size() == collation.getFieldCollations().size();
        for (Ord<RexNode> ord : Ord.zip(fieldExps)) {
            pw.item("sort" + ord.i, ord.e);
        }
        for (Ord<RelFieldCollation> ord
            : Ord.zip(collation.getFieldCollations()))
        {
            pw.item("dir" + ord.i, ord.e.shortString());
        }
        pw.itemIf("offset", offset, offset != null);
        pw.itemIf("fetch", fetch, fetch != null);
        return pw;
    }
}

// End SortRel.java
