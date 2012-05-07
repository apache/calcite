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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * <code>ValuesRelBase</code> is an abstract base class for implementations of
 * {@link ValuesRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class ValuesRelBase
    extends AbstractRelNode
{
    //~ Instance fields --------------------------------------------------------

    protected final List<List<RexLiteral>> tuples;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new ValuesRelBase. Note that tuples passed in become owned by
     * this rel (without a deep copy), so caller must not modify them after this
     * call, otherwise bad things will happen.
     *
     * @param cluster .
     * @param rowType row type for tuples produced by this rel
     * @param tuples 2-dimensional array of tuple values to be produced; outer
     * list contains tuples; each inner list is one tuple; all tuples must be of
     * same length, conforming to rowType
     */
    protected ValuesRelBase(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples,
        RelTraitSet traits)
    {
        super(cluster, traits);
        this.rowType = rowType;
        this.tuples = tuples;
        assert (assertRowType());
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return rows of literals represented by this rel
     */
    public List<List<RexLiteral>> getTuples()
    {
        return tuples;
    }

    /**
     * @return true if all tuples match rowType; otherwise, assert on mismatch
     */
    private boolean assertRowType()
    {
        for (List<RexLiteral> tuple : tuples) {
            RelDataTypeField [] fields = rowType.getFields();
            assert (tuple.size() == fields.length);
            int i = 0;
            for (RexLiteral literal : tuple) {
                RelDataType fieldType = fields[i++].getType();

                // TODO jvs 19-Feb-2006: strengthen this a bit.  For example,
                // overflow, rounding, and padding/truncation must already have
                // been dealt with.
                if (!RexLiteral.isNullLiteral(literal)) {
                    assert (SqlTypeUtil.canAssignFrom(
                        fieldType,
                        literal.getType()));
                }
            }
        }
        return true;
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        return rowType;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = RelMetadataQuery.getRowCount(this);

        // Assume CPU is negligible since values are precomputed.
        double dCpu = 1;
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    // implement RelNode
    public double getRows()
    {
        return tuples.size();
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        // A little adapter just to get the tuples to come out
        // with curly brackets instead of square brackets.  Plus
        // more whitespace for readability.
        List<String> renderList = new ArrayList<String>();
        for (List<RexLiteral> tuple : tuples) {
            String s = tuple.toString();
            assert (s.startsWith("["));
            assert (s.endsWith("]"));
            renderList.add("{ " + s.substring(1, s.length() - 1) + " }");
        }
        if (pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES) {
            // For rel digest, include the row type since a rendered
            // literal may leave the type ambiguous (e.g. "null").
            pw.explain(
                this,
                new String[] { "type", "tuples" },
                new Object[] { rowType, renderList });
        } else {
            // For normal EXPLAIN PLAN, omit the type.
            pw.explain(
                this,
                new String[] { "tuples" },
                new Object[] { renderList });
        }
    }
}

// End ValuesRelBase.java
