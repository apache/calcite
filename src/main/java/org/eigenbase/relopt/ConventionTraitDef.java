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
import org.eigenbase.rel.convert.*;
import org.eigenbase.util.*;


/**
 * Definition of the the convention trait.
 * A new set of conversion information is created for
 * each planner that registers at least one {@link ConverterRule} instance.
 *
 * <p>Conversion data is held in a {@link WeakHashMap} so that the JVM's garbage
 * collector may reclaim the conversion data after the planner itself has been
 * garbage collected. The conversion information consists of a graph of
 * conversions (from one calling convention to another) and a map of graph arcs
 * to {@link ConverterRule}s.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class ConventionTraitDef
    extends RelTraitDef<Convention>
{
    //~ Static fields/initializers ---------------------------------------------

    public static final ConventionTraitDef instance =
        new ConventionTraitDef();

    //~ Instance fields --------------------------------------------------------

    /**
     * Weak-key map of RelOptPlanner to ConversionData. The idea is that when
     * the planner goes away, so does the map entry.
     */
    private final WeakHashMap<RelOptPlanner, ConversionData>
        plannerConversionMap = new WeakHashMap<RelOptPlanner, ConversionData>();

    //~ Constructors -----------------------------------------------------------

    private ConventionTraitDef()
    {
        super();
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelTraitDef
    public Class<Convention> getTraitClass()
    {
        return Convention.class;
    }

    public String getSimpleName()
    {
        return "convention";
    }

    public void registerConverterRule(
        RelOptPlanner planner,
        ConverterRule converterRule)
    {
        if (converterRule.isGuaranteed()) {
            ConversionData conversionData = getConversionData(planner);

            final Graph<Convention> conversionGraph =
                conversionData.conversionGraph;
            final MultiMap<Graph.Arc, ConverterRule> mapArcToConverterRule =
                conversionData.mapArcToConverterRule;

            final Graph.Arc arc =
                conversionGraph.createArc(
                    (Convention) converterRule.getInTrait(),
                    (Convention) converterRule.getOutTrait());

            mapArcToConverterRule.putMulti(arc, converterRule);
        }
    }

    public void deregisterConverterRule(
        RelOptPlanner planner,
        ConverterRule converterRule)
    {
        if (converterRule.isGuaranteed()) {
            ConversionData conversionData = getConversionData(planner);

            final Graph<Convention> conversionGraph =
                conversionData.conversionGraph;
            final MultiMap<Graph.Arc, ConverterRule> mapArcToConverterRule =
                conversionData.mapArcToConverterRule;

            final Graph.Arc arc =
                conversionGraph.deleteArc(
                    (Convention) converterRule.getInTrait(),
                    (Convention) converterRule.getOutTrait());
            assert arc != null;

            mapArcToConverterRule.removeMulti(arc, converterRule);
        }
    }

    // implement RelTraitDef
    public RelNode convert(
        RelOptPlanner planner,
        RelNode rel,
        RelTrait toTrait,
        boolean allowInfiniteCostConverters)
    {
        final ConversionData conversionData = getConversionData(planner);
        final Graph<Convention> conversionGraph =
            conversionData.conversionGraph;
        final MultiMap<Graph.Arc, ConverterRule> mapArcToConverterRule =
            conversionData.mapArcToConverterRule;

        final Convention fromConvention = rel.getConvention();
        final Convention toConvention = (Convention) toTrait;

        Iterator<Graph.Arc<Convention>[]> conversionPaths =
            conversionGraph.getPaths(fromConvention, toConvention);

loop:
        while (conversionPaths.hasNext()) {
            Graph.Arc [] arcs = conversionPaths.next();
            assert (arcs[0].from == fromConvention);
            assert (arcs[arcs.length - 1].to == toConvention);
            RelNode converted = rel;
            for (Graph.Arc arc : arcs) {
                if (planner.getCost(converted).isInfinite()
                    && !allowInfiniteCostConverters)
                {
                    continue loop;
                }
                converted = changeConvention(
                    converted, arc, mapArcToConverterRule);
                if (converted == null) {
                    throw Util.newInternal(
                        "Converter from "
                        + arc.from
                        + " to "
                        + arc.to
                        + " guaranteed that it could convert any relexp");
                }
            }
            return converted;
        }

        return null;
    }

    /**
     * Tries to convert a relational expression to the target convention of an
     * arc.
     */
    private RelNode changeConvention(
        RelNode rel,
        Graph.Arc arc,
        final MultiMap<Graph.Arc, ConverterRule> mapArcToConverterRule)
    {
        assert (arc.from == rel.getConvention());

        // Try to apply each converter rule for this arc's source/target calling
        // conventions.
        for (ConverterRule rule : mapArcToConverterRule.getMulti(arc)) {
            assert rule.getInTrait() == arc.from;
            assert rule.getOutTrait() == arc.to;
            RelNode converted = rule.convert(rel);
            if (converted != null) {
                return converted;
            }
        }
        return null;
    }

    public boolean canConvert(
        RelOptPlanner planner,
        RelTrait fromTrait,
        RelTrait toTrait)
    {
        ConversionData conversionData = getConversionData(planner);

        Convention fromConvention = (Convention) fromTrait;
        Convention toConvention = (Convention) toTrait;

        return conversionData.conversionGraph.getShortestPath(
            fromConvention,
            toConvention) != null;
    }

    private ConversionData getConversionData(RelOptPlanner planner)
    {
        if (plannerConversionMap.containsKey(planner)) {
            return plannerConversionMap.get(planner);
        }

        // Create new, empty ConversionData
        ConversionData conversionData = new ConversionData();
        plannerConversionMap.put(planner, conversionData);
        return conversionData;
    }

    //~ Inner Classes ----------------------------------------------------------

    private static final class ConversionData
    {
        final Graph<Convention> conversionGraph =
            new Graph<Convention>();

        /**
         * For a given source/target convention, there may be several possible
         * conversion rules. Maps {@link org.eigenbase.util.Graph.Arc} to a
         * collection of {@link ConverterRule} objects.
         */
        final MultiMap<Graph.Arc, ConverterRule> mapArcToConverterRule =
            new MultiMap<Graph.Arc, ConverterRule>();
    }
}

// End ConventionTraitDef.java
