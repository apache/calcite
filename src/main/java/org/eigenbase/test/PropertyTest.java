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
package org.eigenbase.test;

import java.lang.ref.*;

import junit.framework.*;

import org.eigenbase.util.property.*;


/**
 * Unit test for properties system ({@link TriggerableProperties}, {@link
 * IntegerProperty} and the like).
 *
 * @author jhyde
 * @version $Id$
 * @since July 6, 2005
 */
public class PropertyTest
    extends TestCase
{
    //~ Static fields/initializers ---------------------------------------------

    private static final boolean [] FalseTrue = new boolean[] { false, true };

    //~ Methods ----------------------------------------------------------------

    public void testInt()
    {
        final MyProperties props = new MyProperties();

        // Default value.
        Assert.assertEquals(
            5,
            props.intProp.get());
        Assert.assertEquals(
            789,
            props.intProp.get(789));

        int prev = props.intProp.set(8);
        Assert.assertEquals(
            8,
            props.intProp.get());
        Assert.assertEquals(5, prev);

        prev = props.intProp.set(0);
        Assert.assertEquals(8, prev);
    }

    public void testIntNoDefault()
    {
        final MyProperties props = new MyProperties();

        // As above, on property with no default value.
        Assert.assertEquals(
            0,
            props.intPropNoDefault.get());
        Assert.assertEquals(
            17,
            props.intPropNoDefault.get(17));

        int prev = props.intPropNoDefault.set(-56);
        Assert.assertEquals(0, prev);
        Assert.assertEquals(-56, props.intPropNoDefault.get());
        Assert.assertEquals(-56, props.intPropNoDefault.get(17));

        // Second time set returns the previous value.
        prev = props.intPropNoDefault.set(12345);
        Assert.assertEquals(-56, prev);

        // Setting null is not OK.
        try {
            props.intPropNoDefault.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }
    }

    public void testIntLimit()
    {
        final MyProperties props = new MyProperties();

        // Default value.
        assertEquals(
            10,
            props.intPropLimit.get());

        // Specified default value w/limit
        assertEquals(
            11,
            props.intPropLimit.get(11));
        assertEquals(
            50,
            props.intPropLimit.get(51));
        assertEquals(
            5,
            props.intPropLimit.get(4));
        assertEquals(
            50,
            props.intPropLimit.get(Integer.MAX_VALUE));
        assertEquals(
            5,
            props.intPropLimit.get(Integer.MIN_VALUE));

        int prev = props.intPropLimit.set(8);
        assertEquals(
            8,
            props.intPropLimit.get());
        assertEquals(10, prev);

        prev = props.intPropLimit.set(100);
        assertEquals(
            50,
            props.intPropLimit.get());
        assertEquals(8, prev);

        prev = props.intPropLimit.set(-100);
        assertEquals(
            5,
            props.intPropLimit.get());
        assertEquals(50, prev);

        // set string isn't limited until read
        props.intPropLimit.setString("99");
        assertEquals(
            50,
            props.intPropLimit.get());

        props.intPropLimit.setString("-2");
        assertEquals(
            5,
            props.intPropLimit.get());

        // Setting null is not OK.
        try {
            props.intPropLimit.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // expected
        }
    }

    public void testIntLimitNoDefault()
    {
        final MyProperties props = new MyProperties();

        assertEquals(
            0,
            props.intPropLimitNoDefault.get());
        assertEquals(
            1,
            props.intPropLimitNoDefault2.get());

        // prev is "no value" == 0; set value is limited
        int prev = props.intPropLimitNoDefault.set(-100);
        assertEquals(0, prev);
        assertEquals(-5, props.intPropLimitNoDefault.get());
        assertEquals(-5, props.intPropLimitNoDefault.get(0));

        // prev is "no value" == 1; set value is limited
        prev = props.intPropLimitNoDefault2.set(100);
        assertEquals(1, prev);
        assertEquals(
            2,
            props.intPropLimitNoDefault2.get());
        assertEquals(
            2,
            props.intPropLimitNoDefault2.get(1));

        prev = props.intPropLimitNoDefault.set(2);
        assertEquals(-5, prev);

        // Setting null is not OK.
        try {
            props.intPropLimitNoDefault.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // expected
        }
    }

    public void testDouble()
    {
        final MyProperties props = new MyProperties();

        // Default value.
        Assert.assertEquals(-3.14, props.doubleProp.get());
        Assert.assertEquals(.789, props.doubleProp.get(.789));

        double prev = props.doubleProp.set(.8);
        Assert.assertEquals(.8, props.doubleProp.get());
        Assert.assertEquals(-3.14, prev);

        prev = props.doubleProp.set(.0);
        Assert.assertEquals(.8, prev);
    }

    public void testDoubleNoDefault()
    {
        final MyProperties props = new MyProperties();

        // As above, on property with no default value.
        Assert.assertEquals(
            .0,
            props.doublePropNoDefault.get());
        Assert.assertEquals(
            .17,
            props.doublePropNoDefault.get(.17));

        double prev = props.doublePropNoDefault.set(-.56);
        Assert.assertEquals(.0, prev);
        Assert.assertEquals(-.56, props.doublePropNoDefault.get());
        Assert.assertEquals(-.56, props.doublePropNoDefault.get(.17));

        // Second time set returns the previous value.
        prev = props.doublePropNoDefault.set(.12345);
        Assert.assertEquals(-.56, prev);

        // Setting null is not OK.
        try {
            props.doublePropNoDefault.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }
    }

    public void testDoubleLimit()
    {
        final MyProperties props = new MyProperties();

        // Default value.
        assertEquals(
            Math.E,
            props.doublePropLimit.get());

        // Specified default value w/limit
        assertEquals(
            1.1,
            props.doublePropLimit.get(1.1));
        assertEquals(
            Math.PI,
            props.doublePropLimit.get(5.1));
        assertEquals(-Math.PI, props.doublePropLimit.get(-4.0));
        assertEquals(
            Math.PI,
            props.doublePropLimit.get(Double.MAX_VALUE));
        assertEquals(-Math.PI, props.doublePropLimit.get(-Double.MAX_VALUE));

        double prev = props.doublePropLimit.set(2.5);
        assertEquals(
            2.5,
            props.doublePropLimit.get());
        assertEquals(Math.E, prev);

        prev = props.doublePropLimit.set(10.0);
        assertEquals(
            Math.PI,
            props.doublePropLimit.get());
        assertEquals(2.5, prev);

        prev = props.doublePropLimit.set(-10.0);
        assertEquals(-Math.PI, props.doublePropLimit.get());
        assertEquals(Math.PI, prev);

        // set string isn't limited until read
        props.doublePropLimit.setString("99.0");
        assertEquals(
            Math.PI,
            props.doublePropLimit.get());

        props.doublePropLimit.setString("-20.2");
        assertEquals(-Math.PI, props.doublePropLimit.get());

        // Setting null is not OK.
        try {
            props.doublePropLimit.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // expected
        }
    }

    public void testDoubleLimitNoDefault()
    {
        final MyProperties props = new MyProperties();

        assertEquals(
            0.0,
            props.doublePropLimitNoDefault.get());
        assertEquals(
            1.0,
            props.doublePropLimitNoDefault2.get());

        // prev is "no value" == 0.0; set value is limited
        double prev = props.doublePropLimitNoDefault.set(-100.0);
        assertEquals(0.0, prev);
        assertEquals(-1.0, props.doublePropLimitNoDefault.get());
        assertEquals(-1.0, props.doublePropLimitNoDefault.get(0));

        // prev is "no value" == 1.0; set value is limited
        prev = props.doublePropLimitNoDefault2.set(100);
        assertEquals(1.0, prev);
        assertEquals(
            10.0,
            props.doublePropLimitNoDefault2.get());
        assertEquals(
            10.0,
            props.doublePropLimitNoDefault2.get(1.1));

        prev = props.doublePropLimitNoDefault.set(-0.5);
        assertEquals(-1.0, prev);

        // Setting null is not OK.
        try {
            props.doublePropLimitNoDefault.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // expected
        }
    }

    public void testString()
    {
        final MyProperties props = new MyProperties();

        // Default value.
        Assert.assertEquals(
            "foo",
            props.stringProp.get());
        Assert.assertEquals(
            "xxxxx",
            props.stringProp.get("xxxxx"));

        // First time set returns the default value.
        String prev = props.stringProp.set("bar");
        Assert.assertEquals(
            "bar",
            props.stringProp.get());
        Assert.assertEquals("foo", prev);

        // Second time set returns the previous value.
        prev = props.stringProp.set("baz");
        Assert.assertEquals("bar", prev);

        // Setting null is not OK.
        try {
            prev = props.stringProp.set(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }
    }

    public void testStringNoDefault()
    {
        final MyProperties props = new MyProperties();

        // As above, on property with no default value.
        Assert.assertEquals(
            null,
            props.stringPropNoDefault.get());
        Assert.assertEquals(
            "xx",
            props.stringPropNoDefault.get("xx"));

        String prev = props.stringPropNoDefault.set("paul");
        Assert.assertEquals(null, prev);
        Assert.assertEquals(
            "paul",
            props.stringPropNoDefault.get());
        Assert.assertEquals(
            "paul",
            props.stringPropNoDefault.get("xx"));

        // Second time set returns the previous value.
        prev = props.stringPropNoDefault.set("ringo");
        Assert.assertEquals("paul", prev);

        // Setting null is not OK.
        try {
            prev = props.stringPropNoDefault.set(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }
    }

    public void testBoolean()
    {
        final MyProperties props = new MyProperties();

        // Default value.
        Assert.assertEquals(
            true,
            props.booleanProp.get());
        Assert.assertEquals(
            false,
            props.booleanProp.get(false));

        // First time set returns the default value.
        boolean prev = props.booleanProp.set(false);
        Assert.assertEquals(
            false,
            props.booleanProp.get());
        Assert.assertEquals(true, prev);

        // Second time set returns the previous value.
        prev = props.booleanProp.set(true);
        Assert.assertEquals(false, prev);

        // Various values all mean true.
        String prevString = props.booleanProp.setString("1");
        Assert.assertEquals(
            true,
            props.booleanProp.get());
        prevString = props.booleanProp.setString("true");
        Assert.assertEquals(
            true,
            props.booleanProp.get());
        prevString = props.booleanProp.setString("TRUE");
        Assert.assertEquals(
            true,
            props.booleanProp.get());
        prevString = props.booleanProp.setString("yes");
        Assert.assertEquals(
            true,
            props.booleanProp.get());
        prevString = props.booleanProp.setString("Yes");
        Assert.assertEquals(
            true,
            props.booleanProp.get());

        // Leading and trailing spaces are ignored.
        prevString = props.booleanProp.setString("  yes  ");
        Assert.assertEquals(
            true,
            props.booleanProp.get());
        prevString = props.booleanProp.setString("false   ");
        Assert.assertEquals(
            false,
            props.booleanProp.get());
        prevString = props.booleanProp.setString("true ");
        Assert.assertEquals(
            true,
            props.booleanProp.get());

        // All other values mean false.
        prevString = props.booleanProp.setString("");
        Assert.assertEquals(
            false,
            props.booleanProp.get());
        prevString = props.booleanProp.setString("no");
        Assert.assertEquals(
            false,
            props.booleanProp.get());
        prevString = props.booleanProp.setString("wombat");
        Assert.assertEquals(
            false,
            props.booleanProp.get());
        prevString = props.booleanProp.setString("0");
        Assert.assertEquals(
            false,
            props.booleanProp.get());
        prevString = props.booleanProp.setString("false");
        Assert.assertEquals(
            false,
            props.booleanProp.get());

        // Setting null is not OK.
        try {
            props.booleanProp.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }
    }

    public void testBooleanNoDefault()
    {
        final MyProperties props = new MyProperties();

        // As above, on property with no default value.
        Assert.assertEquals(
            false,
            props.booleanPropNoDefault.get());
        Assert.assertEquals(
            true,
            props.booleanPropNoDefault.get(true));
        Assert.assertEquals(
            false,
            props.booleanPropNoDefault.get(false));

        boolean prev = props.booleanPropNoDefault.set(true);
        Assert.assertEquals(false, prev);
        Assert.assertEquals(
            true,
            props.booleanPropNoDefault.get());
        Assert.assertEquals(
            true,
            props.booleanPropNoDefault.get(false));

        // Second time set returns the previous value.
        prev = props.booleanPropNoDefault.set(false);
        Assert.assertEquals(true, prev);

        // Setting null is not OK.
        try {
            props.booleanPropNoDefault.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }
    }

    public void testTrigger()
    {
        final MyProperties props = new MyProperties();
        final int [] ints = { 0 };
        final Trigger trigger =
            new Trigger() {
                public boolean isPersistent()
                {
                    return false;
                }

                public int phase()
                {
                    return 0;
                }

                public void execute(Property property, String value)
                    throws VetoRT
                {
                    int intValue = Integer.parseInt(value);
                    if (intValue > 10) {
                        ints[0] = intValue;
                    }
                    if (intValue > 100) {
                        throw new VetoRT("too big");
                    }
                }
            };
        props.intProp.addTrigger(trigger);
        props.intProp.set(5);
        assertEquals(0, ints[0]); // unchanged
        props.intProp.set(15);
        assertEquals(15, ints[0]); // changed by trigger
        try {
            props.intProp.set(120);
            fail("expecting exception");
        } catch (Trigger.VetoRT e) {
            assertEquals(
                "too big",
                e.getMessage());
        }
        Assert.assertEquals(
            15,
            props.intProp.get()); // change was rolled back
    }

    /**
     * Tests that trigger is called after the value is changed
     */
    public void testValueChange()
    {
        final MyProperties props = new MyProperties();

        String path = "test.mondrian.properties.change.value";
        BooleanProperty boolProp =
            new BooleanProperty(
                props,
                path,
                false);

        assertTrue("Check property value NOT false", !boolProp.get());

        // set via the 'set' method
        final boolean prevBoolean = boolProp.set(true);
        assertEquals(false, prevBoolean);

        // now explicitly set the property
        final Object prevObject = props.setProperty(path, "false");
        assertEquals("true", prevObject);

        String v = props.getProperty(path);
        assertTrue("Check property value is null", v != null);
        assertTrue(
            "Check property value is true",
            (!Boolean.valueOf(v).booleanValue()));

        final State state = new State();
        state.triggerCalled = false;
        state.triggerValue = null;

        final Trigger trigger =
            new Trigger() {
                public boolean isPersistent()
                {
                    return false;
                }

                public int phase()
                {
                    return Trigger.PRIMARY_PHASE;
                }

                public void execute(Property property, String value)
                {
                    state.triggerCalled = true;
                    state.triggerValue = value;
                }
            };
        boolProp.addTrigger(trigger);

        String falseStr = "false";
        props.setProperty(path, falseStr);
        assertTrue("Check trigger was called", !state.triggerCalled);

        String trueStr = "true";
        props.setProperty(path, trueStr);

        assertTrue(
            "Check trigger was NOT called",
            state.triggerCalled);
        assertTrue(
            "Check trigger value was null",
            (state.triggerValue != null));
        assertTrue(
            "Check trigger value is NOT correct",
            state.triggerValue.equals(trueStr));
    }

    /**
     * Checks that triggers are called in the correct order.
     */
    public void testTriggerCallOrder()
    {
        final MyProperties props = new MyProperties();
        String path = "test.mondrian.properties.call.order";
        BooleanProperty boolProp =
            new BooleanProperty(
                props,
                path,
                false);

        final State2 state = new State2();
        state.callCounter = 0;

        // now explicitly set the property
        props.setProperty(path, "false");

        String v = props.getProperty(path);
        assertTrue(
            "Check property value is null",
            (v != null));
        assertTrue(
            "Check property value is true",
            (!Boolean.valueOf(v).booleanValue()));

        // primaryOne
        Trigger primaryOneTrigger =
            new Trigger() {
                public boolean isPersistent()
                {
                    return false;
                }

                public int phase()
                {
                    return Trigger.PRIMARY_PHASE;
                }

                public void execute(Property property, String value)
                {
                    state.primaryOne = state.callCounter++;
                }
            };
        boolProp.addTrigger(primaryOneTrigger);

        // secondaryOne
        Trigger secondaryOneTrigger =
            new Trigger() {
                public boolean isPersistent()
                {
                    return false;
                }

                public int phase()
                {
                    return Trigger.SECONDARY_PHASE;
                }

                public void execute(Property property, String value)
                {
                    state.secondaryOne = state.callCounter++;
                }
            };
        boolProp.addTrigger(secondaryOneTrigger);

        // tertiaryOne
        Trigger tertiaryOneTrigger =
            new Trigger() {
                public boolean isPersistent()
                {
                    return false;
                }

                public int phase()
                {
                    return Trigger.TERTIARY_PHASE;
                }

                public void execute(Property property, String value)
                {
                    state.tertiaryOne = state.callCounter++;
                }
            };
        boolProp.addTrigger(tertiaryOneTrigger);

        // tertiaryTwo
        Trigger tertiaryTwoTrigger =
            new Trigger() {
                public boolean isPersistent()
                {
                    return false;
                }

                public int phase()
                {
                    return Trigger.TERTIARY_PHASE;
                }

                public void execute(Property property, String value)
                {
                    state.tertiaryTwo = state.callCounter++;
                }
            };
        boolProp.addTrigger(tertiaryTwoTrigger);

        // secondaryTwo
        Trigger secondaryTwoTrigger =
            new Trigger() {
                public boolean isPersistent()
                {
                    return false;
                }

                public int phase()
                {
                    return Trigger.SECONDARY_PHASE;
                }

                public void execute(Property property, String value)
                {
                    state.secondaryTwo = state.callCounter++;
                }
            };
        boolProp.addTrigger(secondaryTwoTrigger);

        // primaryTwo
        Trigger primaryTwoTrigger =
            new Trigger() {
                public boolean isPersistent()
                {
                    return false;
                }

                public int phase()
                {
                    return Trigger.PRIMARY_PHASE;
                }

                public void execute(Property property, String value)
                {
                    state.primaryTwo = state.callCounter++;
                }
            };
        boolProp.addTrigger(primaryTwoTrigger);

        String falseStr = "false";
        props.setProperty(path, falseStr);
        assertTrue(
            "Check trigger was called",
            (state.callCounter == 0));

        String trueStr = "true";
        props.setProperty(path, trueStr);

        assertTrue(
            "Check trigger was NOT called",
            (state.callCounter != 0));
        assertTrue(
            "Check triggers was NOT called correct number of times",
            (state.callCounter == 6));

        // now make sure that primary are called before secondary which are
        // before tertiary
        assertTrue(
            "Check primaryOne > secondaryOne",
            (state.primaryOne < state.secondaryOne));
        assertTrue(
            "Check primaryOne > secondaryTwo",
            (state.primaryOne < state.secondaryTwo));
        assertTrue(
            "Check primaryOne > tertiaryOne",
            (state.primaryOne < state.tertiaryOne));
        assertTrue(
            "Check primaryOne > tertiaryTwo",
            (state.primaryOne < state.tertiaryTwo));

        assertTrue(
            "Check primaryTwo > secondaryOne",
            (state.primaryTwo < state.secondaryOne));
        assertTrue(
            "Check primaryTwo > secondaryTwo",
            (state.primaryTwo < state.secondaryTwo));
        assertTrue(
            "Check primaryTwo > tertiaryOne",
            (state.primaryTwo < state.tertiaryOne));
        assertTrue(
            "Check primaryTwo > tertiaryTwo",
            (state.primaryTwo < state.tertiaryTwo));

        assertTrue(
            "Check secondaryOne > tertiaryOne",
            (state.secondaryOne < state.tertiaryOne));
        assertTrue(
            "Check secondaryOne > tertiaryTwo",
            (state.secondaryOne < state.tertiaryTwo));

        assertTrue(
            "Check secondaryTwo > tertiaryOne",
            (state.secondaryTwo < state.tertiaryOne));
        assertTrue(
            "Check secondaryTwo > tertiaryTwo",
            (state.secondaryTwo < state.tertiaryTwo));

        // remove some of the triggers
        boolProp.removeTrigger(primaryTwoTrigger);
        boolProp.removeTrigger(secondaryTwoTrigger);
        boolProp.removeTrigger(tertiaryTwoTrigger);

        // reset
        state.callCounter = 0;
        state.primaryOne = 0;
        state.primaryTwo = 0;
        state.secondaryOne = 0;
        state.secondaryTwo = 0;
        state.tertiaryOne = 0;
        state.tertiaryTwo = 0;

        props.setProperty(path, falseStr);
        assertTrue(
            "Check trigger was NOT called",
            (state.callCounter != 0));
        assertTrue(
            "Check triggers was NOT called correct number of times",
            (state.callCounter == 3));

        // now make sure that primary are called before secondary which are
        // before tertiary
        assertTrue(
            "Check primaryOne > secondaryOne",
            (state.primaryOne < state.secondaryOne));
        assertTrue(
            "Check primaryOne > tertiaryOne",
            (state.primaryOne < state.tertiaryOne));

        assertTrue(
            "Check secondaryOne > tertiaryOne",
            (state.secondaryOne < state.tertiaryOne));
    }

    public void testVetoChangeValue()
        throws Exception
    {
        checkVetoChangeValue(false, true);
    }

    public void testVetoChangeValuePersistent()
        throws Exception
    {
        checkVetoChangeValue(true, true);
    }

    /**
     * Checks that one can veto a property change.
     *
     * @param persistent Whether to make strong references to triggers, to
     * prevent them from being garbage collected
     * @param save Whether to keep a pointer to each trigger on the stack, to
     * prevent them from being garbage collected
     */
    private void checkVetoChangeValue(
        final boolean persistent,
        boolean save)
        throws Exception
    {
        final MyProperties props = new MyProperties();
        String path = "test.mondrian.properties.veto.change.value";
        IntegerProperty intProp =
            new IntegerProperty(
                props,
                path,
                -1);

        assertTrue(
            "Check property value NOT false",
            (intProp.get() == -1));

        // now explicitly set the property
        props.setProperty(path, "-1");

        String v = props.getProperty(path);
        assertTrue(
            "Check property value is null",
            (v != null));

        assertTrue(
            "Check property value is -1",
            (Integer.decode(v).intValue() == -1));

        final State3 state = new State3();
        state.callCounter = 0;

        // Add a trigger. Keep it on the stack to prevent it from being
        // garbage-collected.
        final Trigger trigger1 =
            new Trigger() {
                public boolean isPersistent()
                {
                    return persistent;
                }

                public int phase()
                {
                    return Trigger.PRIMARY_PHASE;
                }

                public void execute(Property property, String value)
                {
                    state.triggerCalled = true;
                    state.triggerValue = value;
                }
            };
        intProp.addTrigger(trigger1);
        SoftReference<Trigger> ref1 = new SoftReference<Trigger>(trigger1);

        final Trigger trigger2 =
            new Trigger() {
                public boolean isPersistent()
                {
                    return persistent;
                }

                public int phase()
                {
                    return Trigger.SECONDARY_PHASE;
                }

                public void execute(Property property, String value)
                    throws VetoRT
                {
                    // even numbers are rejected
                    state.callCounter++;
                    int ival = Integer.decode(value).intValue();
                    if ((ival % 2) == 0) {
                        // throw on even
                        throw new VetoRT("have a nice day");
                    } else {
                        // ok
                    }
                }
            };
        intProp.addTrigger(trigger2);
        SoftReference<Trigger> ref2 = new SoftReference<Trigger>(trigger2);

        // Holder object prevents triggers from being garbage-collected even
        // if persistent=false.
        Object saver;
        if (save) {
            saver = new Trigger[] { trigger1, trigger2 };
        } else {
            saver = "dummy";
        }

        for (int i = 0; i < 10; i++) {
            // reset values
            state.triggerCalled = false;
            state.triggerValue = null;

            boolean isEven = ((i % 2) == 0);

            try {
                props.setProperty(
                    path,
                    Integer.toString(i));

                // If triggers have been gc'ed - only possible if persistent =
                // save = false - then we can't guarantee that state has been
                // changed.
                if (!persistent
                    && !save
                    && ((ref1.get() == null) || (ref2.get() == null)))
                {
                    continue;
                }

                // should only be here if odd
                if (isEven) {
                    fail("Did not pass odd number: " + i);
                }
                int val = Integer.decode(state.triggerValue).intValue();

                assertTrue("Odd counter not value", (i == val));
            } catch (Trigger.VetoRT ex) {
                // If triggers have been gc'ed - only possible if persistent =
                // save = false - then we can't guarantee that state has been
                // changed.
                if (!persistent
                    && !save
                    && ((ref1.get() == null) || (ref2.get() == null)))
                {
                    continue;
                }

                // Trigger rejects even numbers so if even its ok
                if (!isEven) {
                    fail("Did not reject even number: " + i);
                }
                int val = Integer.decode(state.triggerValue).intValue();

                // the property value was reset to the previous value of "i"
                // so we add "1" to it to get the current value.
                if (i != (val + 1)) {
                    fail("Even counter not value plus one: " + i + ", " + val);
                }
            }
        }

        // Refer to the saver object at the end of the routine so that it
        // cannot be garbage-collected. (Some VMs try to be smart.)
        assertTrue(saver != null);
    }

    /**
     * Runs {@link #testVetoChangeValue} many times, to test concurrency.
     */
    public void testVetoChangeValueManyTimes()
        throws Exception
    {
        final int count = 10000;
        for (boolean persistent : FalseTrue) {
            for (boolean save : FalseTrue) {
                for (int i = 0; i < count; ++i) {
                    checkVetoChangeValue(persistent, save);
                }
            }
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class State
    {
        boolean triggerCalled;
        String triggerValue;
    }

    private static class State2
    {
        int callCounter;
        int primaryOne;
        int primaryTwo;
        int secondaryOne;
        int secondaryTwo;
        int tertiaryOne;
        int tertiaryTwo;
    }

    private static class State3
    {
        int callCounter;
        boolean triggerCalled;
        String triggerValue;
    }

    private static class MyProperties
        extends TriggerableProperties
    {
        public final IntegerProperty intProp =
            new IntegerProperty(
                this,
                "props.int",
                5);

        public final IntegerProperty intPropNoDefault =
            new IntegerProperty(this, "props.int.nodefault");

        public final IntegerProperty intPropLimit =
            new IntegerProperty(this, "props.int.limit", 10, 5, 50);

        public final IntegerProperty intPropLimitNoDefault =
            new IntegerProperty(this, "props.int.limit.nodefault", -5, 5);

        public final IntegerProperty intPropLimitNoDefault2 =
            new IntegerProperty(this, "props.int.limit.nodefault2", 1, 2);

        public final StringProperty stringProp =
            new StringProperty(this, "props.string", "foo");

        public final StringProperty stringPropNoDefault =
            new StringProperty(this, "props.string.nodefault", null);

        public final DoubleProperty doubleProp =
            new DoubleProperty(this, "props.double", -3.14);

        public final DoubleProperty doublePropNoDefault =
            new DoubleProperty(this, "props.double.nodefault");

        public final DoubleProperty doublePropLimit =
            new DoubleProperty(
                this,
                "props.double.limit",
                Math.E,
                -Math.PI,
                Math.PI);

        public final DoubleProperty doublePropLimitNoDefault =
            new DoubleProperty(
                this,
                "props.double.limit.nodefault",
                -1.0,
                1.0);

        public final DoubleProperty doublePropLimitNoDefault2 =
            new DoubleProperty(
                this,
                "props.double.limit.nodefault2",
                1.0,
                10.0);

        public final BooleanProperty booleanProp =
            new BooleanProperty(this, "props.boolean", true);

        public final BooleanProperty booleanPropNoDefault =
            new BooleanProperty(this, "props.boolean.nodefault");
    }
}

// End PropertyTest.java
