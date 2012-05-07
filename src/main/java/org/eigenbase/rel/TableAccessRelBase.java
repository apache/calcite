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

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * <code>TableAccessRelBase</code> is an abstract base class for implementations
 * of {@link TableAccessRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class TableAccessRelBase
    extends AbstractRelNode
{
    //~ Instance fields --------------------------------------------------------

    /**
     * The connection to the optimizing session.
     */
    protected RelOptConnection connection;

    /**
     * The table definition.
     */
    protected RelOptTable table;

    //~ Constructors -----------------------------------------------------------

    protected TableAccessRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable table,
        RelOptConnection connection)
    {
        super(cluster, traits);
        this.table = table;
        this.connection = connection;
        if (table.getRelOptSchema() != null) {
            cluster.getPlanner().registerSchema(table.getRelOptSchema());
        }
    }

    //~ Methods ----------------------------------------------------------------

    public RelOptConnection getConnection()
    {
        return connection;
    }

    public double getRows()
    {
        return table.getRowCount();
    }

    public RelOptTable getTable()
    {
        return table;
    }

    public List<RelCollation> getCollationList()
    {
        return table.getCollationList();
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = table.getRowCount();
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    public RelDataType deriveRowType()
    {
        return table.getRowType();
    }

    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "table" },
            new Object[] { Arrays.asList(table.getQualifiedName()) });
    }
}

// End TableAccessRelBase.java
