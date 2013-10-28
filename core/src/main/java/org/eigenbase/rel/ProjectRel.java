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
import org.eigenbase.util.*;

import com.google.common.collect.ImmutableList;

/**
 * <code>ProjectRel</code> is a relational expression which computes a set of
 * 'select expressions' from its input relational expression.
 *
 * <p>The result is usually 'boxed' as a record with one named field for each
 * column; if there is precisely one expression, the result may be 'unboxed',
 * and consist of the raw value type.</p>
 *
 * @version $Id$
 * @author jhyde
 * @since March, 2004
 */
public final class ProjectRel
    extends ProjectRelBase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ProjectRel with no sort keys.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param child input relational expression
     * @param exps set of expressions for the input columns
     * @param fieldNames aliases of the expressions
     * @param flags values as in {@link ProjectRelBase.Flags}
     */
    public ProjectRel(
        RelOptCluster cluster,
        RelNode child,
        List<RexNode> exps,
        List<String> fieldNames,
        int flags)
    {
        this(
            cluster,
            child,
            exps,
            RexUtil.createStructType(
                cluster.getTypeFactory(),
                exps,
                fieldNames),
            flags,
            Collections.<RelCollation>emptyList());
    }

    /**
     * Creates a ProjectRel, deriving a trait set.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param child input relational expression
     * @param exps List of expressions for the input columns
     * @param rowType output row type
     * @param flags values as in {@link ProjectRelBase.Flags}
     * @param collationList List of sort keys
     *
     * @deprecated Use constructor without explicit collation-list;
     * collations can be derived from the trait-set;
     * this constructor will be removed after optiq-0.4.16.
     */
    public ProjectRel(
        RelOptCluster cluster,
        RelNode child,
        List<RexNode> exps,
        RelDataType rowType,
        int flags,
        final List<RelCollation> collationList)
    {
        this(
            cluster,
            cluster.traitSetOf(
                collationList.isEmpty()
                    ? RelCollationImpl.EMPTY
                    : collationList.get(0)),
            child,
            exps,
            rowType,
            flags,
            collationList);
        Bug.upgrade("remove after optiq-0.4.16");
    }

    /**
     * Creates a ProjectRel.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet traits of this rel
     * @param child input relational expression
     * @param exps List of expressions for the input columns
     * @param rowType output row type
     * @param flags values as in {@link ProjectRelBase.Flags}
     * @param collationList List of sort keys
     */
    public ProjectRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        List<RexNode> exps,
        RelDataType rowType,
        int flags,
        final List<RelCollation> collationList)
    {
        super(
            cluster,
            traitSet,
            child,
            exps,
            rowType,
            flags,
            collationList);
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new ProjectRel(
            getCluster(),
            traitSet,
            sole(inputs),
            getProjects(),
            rowType,
            getFlags(),
            ImmutableList.of(
                Util.first(
                    traitSet.getTrait(RelCollationTraitDef.INSTANCE),
                    RelCollationImpl.EMPTY)));
    }

    /**
     * Returns a permutation, if this projection is merely a permutation of its
     * input fields, otherwise null.
     */
    public Permutation getPermutation()
    {
        final int fieldCount = rowType.getFieldList().size();
        if (fieldCount != getChild().getRowType().getFieldList().size()) {
            return null;
        }
        Permutation permutation = new Permutation(fieldCount);
        for (int i = 0; i < fieldCount; ++i) {
            final RexNode exp = exps.get(i);
            if (exp instanceof RexInputRef) {
                permutation.set(i, ((RexInputRef) exp).getIndex());
            } else {
                return null;
            }
        }
        return permutation;
    }
}

// End ProjectRel.java
