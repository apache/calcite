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
        RexNode [] exps,
        String [] fieldNames,
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
     * Creates a ProjectRel.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param child input relational expression
     * @param exps set of expressions for the input columns
     * @param rowType output row type
     * @param flags values as in {@link ProjectRelBase.Flags}
     * @param collationList List of sort keys
     */
    public ProjectRel(
        RelOptCluster cluster,
        RelNode child,
        RexNode [] exps,
        RelDataType rowType,
        int flags,
        final List<RelCollation> collationList)
    {
        super(
            cluster,
            cluster.traitSetOf(Convention.NONE),
            child,
            exps,
            rowType,
            flags,
            collationList);
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.comprises(Convention.NONE);
        return new ProjectRel(
            getCluster(),
            sole(inputs),
            exps,
            rowType,
            getFlags(),
            Collections.<RelCollation>emptyList());
    }

    /**
     * Returns a permutation, if this projection is merely a permutation of its
     * input fields, otherwise null.
     */
    public Permutation getPermutation()
    {
        final int fieldCount = rowType.getFields().length;
        if (fieldCount != getChild().getRowType().getFields().length) {
            return null;
        }
        Permutation permutation = new Permutation(fieldCount);
        for (int i = 0; i < fieldCount; ++i) {
            if (exps[i] instanceof RexInputRef) {
                permutation.set(i, ((RexInputRef) exps[i]).getIndex());
            } else {
                return null;
            }
        }
        return permutation;
    }
}

// End ProjectRel.java
