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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * A MultiJoinRel represents a join of N inputs, whereas other join relnodes
 * represent strictly binary joins.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public final class MultiJoinRel
    extends AbstractRelNode
{
    //~ Instance fields --------------------------------------------------------

    private List<RelNode> inputs;
    private RexNode joinFilter;
    private RelDataType rowType;
    private boolean isFullOuterJoin;
    private RexNode [] outerJoinConditions;
    private JoinRelType [] joinTypes;
    private BitSet [] projFields;
    private Map<Integer, int[]> joinFieldRefCountsMap;
    private RexNode postJoinFilter;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a MultiJoinRel.
     *
     * @param cluster cluster that join belongs to
     * @param inputs inputs into this multirel join
     * @param joinFilter join filter applicable to this join node
     * @param rowType row type of the join result of this node
     * @param isFullOuterJoin true if the join is a full outer join
     * @param outerJoinConditions outer join condition associated with each join
     * input, if the input is null-generating in a left or right outer join;
     * null otherwise
     * @param joinTypes the join type corresponding to each input; if an input
     * is null-generating in a left or right outer join, the entry indicates the
     * type of outer join; otherwise, the entry is set to INNER
     * @param projFields fields that will be projected from each input; if null,
     * projection information is not available yet so it's assumed that all
     * fields from the input are projected
     * @param joinFieldRefCountsMap counters of the number of times each field
     * is referenced in join conditions, indexed by the input #
     * @param postJoinFilter filter to be applied after the joins are executed
     */
    public MultiJoinRel(
        RelOptCluster cluster,
        List<RelNode> inputs,
        RexNode joinFilter,
        RelDataType rowType,
        boolean isFullOuterJoin,
        RexNode [] outerJoinConditions,
        JoinRelType [] joinTypes,
        BitSet [] projFields,
        Map<Integer, int[]> joinFieldRefCountsMap,
        RexNode postJoinFilter)
    {
        super(
            cluster,
            cluster.traitSetOf(CallingConvention.NONE));
        this.inputs = inputs;
        this.joinFilter = joinFilter;
        this.rowType = rowType;
        this.isFullOuterJoin = isFullOuterJoin;
        this.outerJoinConditions = outerJoinConditions;
        this.joinTypes = joinTypes;
        this.projFields = projFields;
        this.joinFieldRefCountsMap = joinFieldRefCountsMap;
        this.postJoinFilter = postJoinFilter;
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.comprises(CallingConvention.NONE);
        return new MultiJoinRel(
            getCluster(),
            inputs,
            joinFilter.clone(),
            rowType,
            isFullOuterJoin,
            RexUtil.clone(outerJoinConditions),
            joinTypes.clone(),
            projFields.clone(),
            cloneJoinFieldRefCountsMap(),
            postJoinFilter);
    }

    /**
     * Returns a deep copy of {@link #joinFieldRefCountsMap}.
     */
    private Map<Integer, int[]> cloneJoinFieldRefCountsMap()
    {
        Map<Integer, int[]> clonedMap = new HashMap<Integer, int[]>();
        for (int i = 0; i < inputs.size(); i++) {
            clonedMap.put(i, joinFieldRefCountsMap.get(i).clone());
        }
        return clonedMap;
    }

    public void explain(RelOptPlanWriter pw)
    {
        int nInputs = inputs.size();
        int nExtraTerms = (postJoinFilter != null) ? 6 : 5;
        String [] terms = new String[nInputs + nExtraTerms];
        for (int i = 0; i < nInputs; i++) {
            terms[i] = "input#" + i;
        }
        terms[nInputs] = "joinFilter";
        terms[nInputs + 1] = "isFullOuterJoin";
        terms[nInputs + 2] = "joinTypes";
        terms[nInputs + 3] = "outerJoinConditions";
        terms[nInputs + 4] = "projFields";
        if (postJoinFilter != null) {
            terms[nInputs + 5] = "postJoinFilter";
        }
        List<String> joinTypeNames = new ArrayList<String>();
        List<String> outerJoinConds = new ArrayList<String>();
        List<String> projFieldObjects = new ArrayList<String>();
        for (int i = 0; i < nInputs; i++) {
            joinTypeNames.add(joinTypes[i].name());
            if (outerJoinConditions[i] == null) {
                outerJoinConds.add("NULL");
            } else {
                outerJoinConds.add(outerJoinConditions[i].toString());
            }
            if (projFields[i] == null) {
                projFieldObjects.add("ALL");
            } else {
                projFieldObjects.add(projFields[i].toString());
            }
        }

        // Note that we don't need to include the join field reference counts
        // in the digest because that field does not change for a given set
        // of inputs
        Object [] objects = new Object[nExtraTerms - 1];
        objects[0] = isFullOuterJoin;
        objects[1] = joinTypeNames;
        objects[2] = outerJoinConds;
        objects[3] = projFieldObjects;
        if (postJoinFilter != null) {
            objects[4] = postJoinFilter;
        }

        pw.explain(this, terms, objects);
    }

    public RelDataType deriveRowType()
    {
        return rowType;
    }

    public List<RelNode> getInputs()
    {
        return inputs;
    }

    public RexNode [] getChildExps()
    {
        return new RexNode[] { joinFilter };
    }

    /**
     * @return join filters associated with this MultiJoinRel
     */
    public RexNode getJoinFilter()
    {
        return joinFilter;
    }

    /**
     * @return true if the MultiJoinRel corresponds to a full outer join.
     */
    public boolean isFullOuterJoin()
    {
        return isFullOuterJoin;
    }

    /**
     * @return outer join conditions for null-generating inputs
     */
    public RexNode [] getOuterJoinConditions()
    {
        return outerJoinConditions;
    }

    /**
     * @return join types of each input
     */
    public JoinRelType [] getJoinTypes()
    {
        return joinTypes;
    }

    /**
     * @return bitmaps representing the fields projected from each input; if an
     * entry is null, all fields are projected
     */
    public BitSet [] getProjFields()
    {
        return projFields;
    }

    /**
     * @return the map of reference counts for each input, representing the
     * fields accessed in join conditions
     */
    public Map<Integer, int[]> getJoinFieldRefCountsMap()
    {
        return joinFieldRefCountsMap;
    }

    /**
     * @return a copy of the map of reference counts for each input,
     * representing the fields accessed in join conditions
     */
    public Map<Integer, int[]> getCopyJoinFieldRefCountsMap()
    {
        return cloneJoinFieldRefCountsMap();
    }

    /**
     * @return post-join filter associated with this MultiJoinRel
     */
    public RexNode getPostJoinFilter()
    {
        return postJoinFilter;
    }
}

// End MultiJoinRel.java
