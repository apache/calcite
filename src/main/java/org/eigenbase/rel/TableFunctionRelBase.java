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


/**
 * <code>TableFunctionRelBase</code> is an abstract base class for
 * implementations of {@link TableFunctionRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class TableFunctionRelBase
    extends AbstractRelNode
{
    //~ Instance fields --------------------------------------------------------

    private final RexNode rexCall;

    private final RelDataType rowType;

    protected final List<RelNode> inputs;

    protected final Set<RelColumnMapping> columnMappings;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>TableFunctionRelBase</code>.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param inputs 0 or more relational inputs
     * @param rexCall function invocation expression
     * @param rowType row type produced by function
     * @param columnMappings column mappings associated with this function
     */
    protected TableFunctionRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        List<RelNode> inputs,
        RexNode rexCall,
        RelDataType rowType,
        Set<RelColumnMapping> columnMappings)
    {
        super(cluster, traits);
        this.rexCall = rexCall;
        this.rowType = rowType;
        this.inputs = inputs;
        this.columnMappings = columnMappings;
    }

    //~ Methods ----------------------------------------------------------------

    public List<RelNode> getInputs()
    {
        return inputs;
    }

    public RexNode [] getChildExps()
    {
        return new RexNode[] {rexCall};
    }

    public double getRows()
    {
        // Calculate result as the sum of the input rowcount estimates,
        // assuming there are any, otherwise use the superclass default.  So
        // for a no-input UDX, behave like an AbstractRelNode; for a one-input
        // UDX, behave like a SingleRel; for a multi-input UDX, behave like
        // UNION ALL.  TODO jvs 10-Sep-2007: UDX-supplied costing metadata.
        if (inputs.size() == 0) {
            return super.getRows();
        }
        double nRows = 0.0;
        for (int i = 0; i < inputs.size(); i++) {
            Double d = RelMetadataQuery.getRowCount(inputs.get(i));
            if (d != null) {
                nRows += d;
            }
        }
        return nRows;
    }

    public RexNode getCall()
    {
        // NOTE jvs 7-May-2006:  Within this rexCall, instances
        // of RexInputRef refer to entire input RelNodes rather
        // than their fields.
        return rexCall;
    }

    public void explain(RelOptPlanWriter pw)
    {
        String [] terms = new String[inputs.size() + 1];
        for (int i = 0; i < inputs.size(); i++) {
            terms[i] = "input#" + i;
        }
        terms[inputs.size()] = "invocation";

        pw.explain(this, terms);
    }

    /**
     * @return set of mappings known for this table function, or null if unknown
     * (not the same as empty!)
     */
    public Set<RelColumnMapping> getColumnMappings()
    {
        return columnMappings;
    }

    protected RelDataType deriveRowType()
    {
        return rowType;
    }
}

// End TableFunctionRelBase.java
