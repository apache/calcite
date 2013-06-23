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
package org.eigenbase.relopt;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * A <code>RelOptQuery</code> represents a set of {@link RelNode relational
 * expressions} which derive from the same <code>select</code> statement.
 *
 * @version $Id$
 */
public class RelOptQuery
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Prefix to the name of correlating variables.
     */
    public static final String correlPrefix = "$cor";

    //~ Instance fields --------------------------------------------------------

    /**
     * Maps a from-list expression to the name of the correlating variable which
     * references it. This is for forward-references, caused when from items
     * have correlating variables. We will later resolve to a {@link RelNode}.
     */
    private final Map<DeferredLookup, String> mapDeferredToCorrel =
        new HashMap<DeferredLookup, String>();

    /**
     * Maps name of correlating variable (e.g. "$cor3") to the {@link RelNode}
     * which implements it.
     */
    final Map<String, RelNode> mapCorrelToRel = new HashMap<String, RelNode>();

    private final RelOptPlanner planner;
    private int nextCorrel = 0;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a query.
     *
     * @param planner Planner
     */
    public RelOptQuery(RelOptPlanner planner)
    {
        this.planner = planner;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Converts a correlating variable name into an ordinal, unqiue within the
     * query.
     *
     * @param correlName Name of correlating variable
     *
     * @return Correlating variable ordinal
     */
    public static int getCorrelOrdinal(String correlName)
    {
        assert (correlName.startsWith(correlPrefix));
        return Integer.parseInt(correlName.substring(correlPrefix.length()));
    }

    /**
     * Returns the map which identifies which correlating variable each {@link
     * org.eigenbase.relopt.RelOptQuery.DeferredLookup} will set.
     *
     * @return Map of deferred lookups
     */
    public Map<DeferredLookup, String> getMapDeferredToCorrel()
    {
        return mapDeferredToCorrel;
    }

    /**
     * Creates a cluster.
     *
     * @param typeFactory Type factory
     * @param rexBuilder Expression builder
     * @param emptyTraitSet Empty trait set. The prototype for other trait sets
     * @return New cluster
     */
    public RelOptCluster createCluster(
        RelDataTypeFactory typeFactory,
        RexBuilder rexBuilder,
        RelTraitSet emptyTraitSet)
    {
        return new RelOptCluster(
            this, planner, typeFactory, rexBuilder, emptyTraitSet);
    }

    /**
     * Constructs a new name for a correlating variable. It is unique within the
     * whole query.
     */
    public String createCorrel()
    {
        int n = nextCorrel++;
        return correlPrefix + n;
    }

    /**
     * Creates a name for a correlating variable for which no {@link RelNode}
     * has been created yet.
     *
     * @param deferredLookup contains the information required to resolve the
     * variable later
     */
    public String createCorrelUnresolved(DeferredLookup deferredLookup)
    {
        int n = nextCorrel++;
        String name = correlPrefix + n;
        mapDeferredToCorrel.put(deferredLookup, name);
        return name;
    }

    /**
     * Returns the relational expression which populates a correlating variable.
     */
    public RelNode lookupCorrel(String name)
    {
        return mapCorrelToRel.get(name);
    }

    /**
     * Maps a correlating variable to a {@link RelNode}.
     */
    public void mapCorrel(
        String name,
        RelNode rel)
    {
        mapCorrelToRel.put(name, rel);
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * Contains the information necessary to repeat a call to {@link
     * org.eigenbase.sql2rel.SqlToRelConverter.Blackboard#lookup}.
     */
    public interface DeferredLookup
    {
        /**
         * Creates an expression which accesses a particular field of this
         * lookup.
         *
         * <p>For example, when resolving
         *
         * <pre>
         * select *
         * from dept
         * where exists (
         *   select *
         *   from emp
         *   where deptno = dept.deptno
         *   and specialty = 'Karate')</pre>
         *
         * the expression <code>dept.deptno</code> would be handled using a
         * deferred lookup for <code>dept</code> (because the sub-query is
         * validated before the outer query) and the translator would call
         * <code>getFieldAccess("DEPTNO")</code> on that lookup.
         *
         * @param name Name of field
         *
         * @return Expression which retrieves the given field of this lookup's
         * correlating variable
         */
        RexFieldAccess getFieldAccess(String name);
    }
}

// End RelOptQuery.java
