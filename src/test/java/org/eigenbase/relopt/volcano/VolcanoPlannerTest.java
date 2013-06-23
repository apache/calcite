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
package org.eigenbase.relopt.volcano;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit test for {@link VolcanoPlanner the optimizer}.
 */
public class VolcanoPlannerTest {
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Private calling convention representing a physical implementation.
     */
    private static final Convention PHYS_CALLING_CONVENTION =
        new Convention.Impl(
            "PHYS",
            RelNode.class);

    //~ Constructors -----------------------------------------------------------

    public VolcanoPlannerTest() {
    }

    //~ Methods ----------------------------------------------------------------

    static RelOptCluster newCluster(VolcanoPlanner planner)
    {
        RelOptQuery query = new RelOptQuery(planner);
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
        return query.createCluster(
            typeFactory,
            new RexBuilder(typeFactory),
            RelTraitSet.createEmpty());
    }

    /**
     * Tests transformation of a leaf from NONE to PHYS.
     */
    @Test public void testTransformLeaf() {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.addRelTraitDef(ConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());

        RelOptCluster cluster = newCluster(planner);
        NoneLeafRel leafRel =
            new NoneLeafRel(
                cluster,
                "a");
        RelNode convertedRel =
            planner.changeTraits(
                leafRel,
                cluster.traitSetOf(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysLeafRel);
    }

    /**
     * Tests transformation of a single+leaf from NONE to PHYS.
     */
    @Test public void testTransformSingleGood() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());
        planner.addRule(new GoodSingleRule());

        RelOptCluster cluster = newCluster(planner);
        NoneLeafRel leafRel =
            new NoneLeafRel(
                cluster,
                "a");
        NoneSingleRel singleRel =
            new NoneSingleRel(
                cluster,
                leafRel);
        RelNode convertedRel =
            planner.changeTraits(
                singleRel,
                cluster.traitSetOf(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysSingleRel);
    }

    /**
     * Tests transformation of a single+leaf from NONE to PHYS. In the past,
     * this one didn't work due to the definition of ReformedSingleRule.
     */
    @Test public void testTransformSingleReformed() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());
        planner.addRule(new ReformedSingleRule());

        RelOptCluster cluster = newCluster(planner);
        NoneLeafRel leafRel =
            new NoneLeafRel(
                cluster,
                "a");
        NoneSingleRel singleRel =
            new NoneSingleRel(
                cluster,
                leafRel);
        RelNode convertedRel =
            planner.changeTraits(
                singleRel,
                cluster.traitSetOf(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysSingleRel);
    }

    private void removeTrivialProject(boolean useRule)
    {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.ambitious = true;

        planner.addRelTraitDef(ConventionTraitDef.instance);

        if (useRule) {
            planner.addRule(RemoveTrivialProjectRule.instance);
        }

        planner.addRule(new PhysLeafRule());
        planner.addRule(new GoodSingleRule());
        planner.addRule(new PhysProjectRule());

        planner.addRule(
            new ConverterRule(
                RelNode.class,
                PHYS_CALLING_CONVENTION,
                EnumerableConvention.ARRAY,
                "PhysToIteratorRule")
            {
                public RelNode convert(RelNode rel)
                {
                    return new PhysToIteratorConverter(
                        rel.getCluster(),
                        rel);
                }
            });

        RelOptCluster cluster = newCluster(planner);
        PhysLeafRel leafRel =
            new PhysLeafRel(
                cluster,
                "a");
        RexInputRef inputRef =
            new RexInputRef(
                0,
                leafRel.getRowType().getFields()[0].getType());
        RelNode projectRel =
            CalcRel.createProject(
                leafRel,
                new RexNode[] { inputRef },
                new String[] { "this" });
        NoneSingleRel singleRel =
            new NoneSingleRel(
                cluster,
                projectRel);
        RelNode convertedRel =
            planner.changeTraits(
                singleRel,
                cluster.traitSetOf(EnumerableConvention.ARRAY));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysToIteratorConverter);
    }

    // NOTE:  this used to fail but now works
    @Test public void testWithRemoveTrivialProject() {
        removeTrivialProject(true);
    }

    // NOTE:  this always worked; it's here as constrast to
    // testWithRemoveTrivialProject()
    @Test public void testWithoutRemoveTrivialProject() {
        removeTrivialProject(false);
    }

    /**
     * Previously, this didn't work because ReformedRemoveSingleRule uses a
     * pattern which spans calling conventions.
     */
    @Test public void testRemoveSingleReformed() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.ambitious = true;
        planner.addRelTraitDef(ConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());
        planner.addRule(new ReformedRemoveSingleRule());

        RelOptCluster cluster = newCluster(planner);
        NoneLeafRel leafRel =
            new NoneLeafRel(
                cluster,
                "a");
        NoneSingleRel singleRel =
            new NoneSingleRel(
                cluster,
                leafRel);
        RelNode convertedRel =
            planner.changeTraits(
                singleRel,
                cluster.traitSetOf(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysLeafRel);
        PhysLeafRel resultLeaf = (PhysLeafRel) result;
        assertEquals(
            "c",
            resultLeaf.getLabel());
    }

    /**
     * This always worked (in contrast to testRemoveSingleReformed) because it
     * uses a completely-physical pattern (requiring GoodSingleRule to fire
     * first).
     */
    @Test public void testRemoveSingleGood() {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.ambitious = true;
        planner.addRelTraitDef(ConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());
        planner.addRule(new GoodSingleRule());
        planner.addRule(new GoodRemoveSingleRule());

        RelOptCluster cluster = newCluster(planner);
        NoneLeafRel leafRel =
            new NoneLeafRel(
                cluster,
                "a");
        NoneSingleRel singleRel =
            new NoneSingleRel(
                cluster,
                leafRel);
        RelNode convertedRel =
            planner.changeTraits(
                singleRel,
                cluster.traitSetOf(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysLeafRel);
        PhysLeafRel resultLeaf = (PhysLeafRel) result;
        assertEquals(
            "c",
            resultLeaf.getLabel());
    }

    /**
     * Tests whether planner correctly notifies listeners of events.
     */
    @Ignore
    @Test public void testListener() {
        TestListener listener = new TestListener();

        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addListener(listener);

        planner.addRelTraitDef(ConventionTraitDef.instance);

        planner.addRule(new PhysLeafRule());

        RelOptCluster cluster = newCluster(planner);
        NoneLeafRel leafRel =
            new NoneLeafRel(
                cluster,
                "a");
        RelNode convertedRel =
            planner.changeTraits(
                leafRel,
                cluster.traitSetOf(PHYS_CALLING_CONVENTION));
        planner.setRoot(convertedRel);
        RelNode result = planner.chooseDelegate().findBestExp();
        assertTrue(result instanceof PhysLeafRel);

        List<RelOptListener.RelEvent> eventList = listener.getEventList();

        // add node
        checkEvent(
            eventList,
            0,
            RelOptListener.RelEquivalenceEvent.class,
            leafRel,
            null);

        // internal subset
        checkEvent(
            eventList,
            1,
            RelOptListener.RelEquivalenceEvent.class,
            null,
            null);

        // before rule
        checkEvent(
            eventList,
            2,
            RelOptListener.RuleAttemptedEvent.class,
            leafRel,
            PhysLeafRule.class);

        // before rule
        checkEvent(
            eventList,
            3,
            RelOptListener.RuleProductionEvent.class,
            result,
            PhysLeafRule.class);

        // result of rule
        checkEvent(
            eventList,
            4,
            RelOptListener.RelEquivalenceEvent.class,
            result,
            null);

        // after rule
        checkEvent(
            eventList,
            5,
            RelOptListener.RuleProductionEvent.class,
            result,
            PhysLeafRule.class);

        // after rule
        checkEvent(
            eventList,
            6,
            RelOptListener.RuleAttemptedEvent.class,
            leafRel,
            PhysLeafRule.class);

        // choose plan
        checkEvent(
            eventList,
            7,
            RelOptListener.RelChosenEvent.class,
            result,
            null);

        // finish choosing plan
        checkEvent(
            eventList,
            8,
            RelOptListener.RelChosenEvent.class,
            null,
            null);
    }

    private void checkEvent(
        List<RelOptListener.RelEvent> eventList,
        int iEvent,
        Class expectedEventClass,
        RelNode expectedRel,
        Class<? extends RelOptRule> expectedRuleClass)
    {
        assertTrue(iEvent < eventList.size());
        RelOptListener.RelEvent event = eventList.get(iEvent);
        assertSame(
            expectedEventClass,
            event.getClass());
        if (expectedRel != null) {
            assertSame(
                expectedRel,
                event.getRel());
        }
        if (expectedRuleClass != null) {
            RelOptListener.RuleEvent ruleEvent =
                (RelOptListener.RuleEvent) event;
            assertSame(
                expectedRuleClass,
                ruleEvent.getRuleCall().getRule().getClass());
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static abstract class TestLeafRel
        extends AbstractRelNode
    {
        private String label;

        protected TestLeafRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            String label)
        {
            super(cluster, traits);
            this.label = label;
        }

        public String getLabel()
        {
            return label;
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeInfiniteCost();
        }

        // implement RelNode
        protected RelDataType deriveRowType()
        {
            return getCluster().getTypeFactory().createStructType(
                new RelDataType[] {
                    getCluster().getTypeFactory().createJavaType(Void.TYPE)
                },
                new String[] { "this" });
        }

        public RelOptPlanWriter explainTerms(RelOptPlanWriter pw)
        {
            return super.explainTerms(pw)
                .item("label", label);
        }
    }

    private static abstract class TestSingleRel
        extends SingleRel
    {
        protected TestSingleRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child)
        {
            super(cluster, traits, child);
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeInfiniteCost();
        }

        // implement RelNode
        protected RelDataType deriveRowType()
        {
            return getChild().getRowType();
        }
    }

    private static class NoneSingleRel
        extends TestSingleRel
    {
        protected NoneSingleRel(
            RelOptCluster cluster,
            RelNode child)
        {
            super(
                cluster,
                cluster.traitSetOf(Convention.NONE),
                child);
        }

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            assert traitSet.comprises(Convention.NONE);
            return new NoneSingleRel(
                getCluster(),
                sole(inputs));
        }
    }

    private static class NoneLeafRel
        extends TestLeafRel
    {
        protected NoneLeafRel(
            RelOptCluster cluster,
            String label)
        {
            super(
                cluster,
                cluster.traitSetOf(Convention.NONE),
                label);
        }

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            assert traitSet.comprises(Convention.NONE);
            assert inputs.isEmpty();
            return this;
        }
    }

    private static class PhysLeafRel
        extends TestLeafRel
    {
        PhysLeafRel(
            RelOptCluster cluster,
            String label)
        {
            super(
                cluster,
                cluster.traitSetOf(PHYS_CALLING_CONVENTION),
                label);
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeTinyCost();
        }

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            assert traitSet.comprises(PHYS_CALLING_CONVENTION);
            assert inputs.isEmpty();
            return this;
        }
    }

    private static class PhysSingleRel
        extends TestSingleRel
    {
        PhysSingleRel(
            RelOptCluster cluster,
            RelNode child)
        {
            super(
                cluster,
                cluster.traitSetOf(PHYS_CALLING_CONVENTION),
                child);
        }

        // implement RelNode
        public RelOptCost computeSelfCost(RelOptPlanner planner)
        {
            return planner.makeTinyCost();
        }

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            assert traitSet.comprises(PHYS_CALLING_CONVENTION);
            return new PhysSingleRel(
                getCluster(),
                sole(inputs));
        }
    }

    class PhysToIteratorConverter
        extends ConverterRelImpl
    {
        public PhysToIteratorConverter(
            RelOptCluster cluster,
            RelNode child)
        {
            super(
                cluster,
                ConventionTraitDef.instance,
                cluster.traitSetOf(EnumerableConvention.ARRAY),
                child);
        }

        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            assert traitSet.comprises(EnumerableConvention.ARRAY);
            return new PhysToIteratorConverter(
                getCluster(),
                sole(inputs));
        }
    }

    private static class PhysLeafRule
        extends RelOptRule
    {
        PhysLeafRule()
        {
            super(any(NoneLeafRel.class));
        }

        // implement RelOptRule
        public Convention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneLeafRel leafRel = call.rel(0);
            call.transformTo(
                new PhysLeafRel(
                    leafRel.getCluster(),
                    leafRel.getLabel()));
        }
    }

    private static class GoodSingleRule
        extends RelOptRule
    {
        GoodSingleRule()
        {
            super(any(NoneSingleRel.class));
        }

      // implement RelOptRule
        public Convention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneSingleRel singleRel = call.rel(0);
            RelNode childRel = singleRel.getChild();
            RelNode physInput =
                convert(
                    childRel,
                    singleRel.getTraitSet().replace(PHYS_CALLING_CONVENTION));
            call.transformTo(
                new PhysSingleRel(
                    singleRel.getCluster(),
                    physInput));
        }
    }

    // NOTE: Previously, ReformedSingleRule did't work because it explicitly
    // specifies PhysLeafRel rather than RelNode for the single input.  Since
    // the PhysLeafRel is in a different subset from the original NoneLeafRel,
    // ReformedSingleRule never saw it.  (GoodSingleRule saw the NoneLeafRel
    // instead and fires off of that; later the NoneLeafRel gets converted into
    // a PhysLeafRel).  Now Volcano supports rules which match across subsets.
    private static class ReformedSingleRule
        extends RelOptRule
    {
        ReformedSingleRule()
        {
            super(
                some(
                    NoneSingleRel.class, any(PhysLeafRel.class)));
        }

        // implement RelOptRule
        public Convention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneSingleRel singleRel = call.rel(0);
            RelNode childRel = call.rel(1);
            RelNode physInput =
                convert(
                    childRel,
                    singleRel.getTraitSet().replace(PHYS_CALLING_CONVENTION));
            call.transformTo(
                new PhysSingleRel(
                    singleRel.getCluster(),
                    physInput));
        }
    }

    private static class PhysProjectRule
        extends RelOptRule
    {
        PhysProjectRule()
        {
            super(any(ProjectRel.class));
        }

        // implement RelOptRule
        public Convention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            final ProjectRel project = call.rel(0);
            RelNode childRel = project.getChild();
            call.transformTo(
                new PhysLeafRel(
                    childRel.getCluster(),
                    "b"));
        }
    }

    private static class GoodRemoveSingleRule
        extends RelOptRule
    {
        GoodRemoveSingleRule()
        {
            super(
                some(
                    PhysSingleRel.class, any(PhysLeafRel.class)));
        }

        // implement RelOptRule
        public Convention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            PhysSingleRel singleRel = call.rel(0);
            PhysLeafRel leafRel = call.rel(1);
            call.transformTo(
                new PhysLeafRel(
                    singleRel.getCluster(),
                    "c"));
        }
    }

    private static class ReformedRemoveSingleRule
        extends RelOptRule
    {
        ReformedRemoveSingleRule()
        {
            super(
                some(
                    NoneSingleRel.class, any(PhysLeafRel.class)));
        }

        // implement RelOptRule
        public Convention getOutConvention()
        {
            return PHYS_CALLING_CONVENTION;
        }

        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            NoneSingleRel singleRel = call.rel(0);
            PhysLeafRel leafRel = call.rel(1);
            call.transformTo(
                new PhysLeafRel(
                    singleRel.getCluster(),
                    "c"));
        }
    }

    private static class TestListener
        implements RelOptListener
    {
        private List<RelEvent> eventList;

        TestListener()
        {
            eventList = new ArrayList<RelEvent>();
        }

        List<RelEvent> getEventList()
        {
            return eventList;
        }

        private void recordEvent(RelEvent event)
        {
            eventList.add(event);
        }

        // implement RelOptListener
        public void relChosen(RelChosenEvent event)
        {
            recordEvent(event);
        }

        // implement RelOptListener
        public void relDiscarded(RelDiscardedEvent event)
        {
            // Volcano is quite a packrat--it never discards anything!
            throw Util.newInternal(event.toString());
        }

        // implement RelOptListener
        public void relEquivalenceFound(RelEquivalenceEvent event)
        {
            if (!event.isPhysical()) {
                return;
            }
            recordEvent(event);
        }

        // implement RelOptListener
        public void ruleAttempted(RuleAttemptedEvent event)
        {
            recordEvent(event);
        }

        // implement RelOptListener
        public void ruleProductionSucceeded(RuleProductionEvent event)
        {
            recordEvent(event);
        }
    }
}

// End VolcanoPlannerTest.java
