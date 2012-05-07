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
package org.eigenbase.util;

import java.io.*;

import junit.framework.*;


/**
 * Unit test for {@link OptionsList}.
 *
 * @author Julian Hyde
 * @version $Id$
 * @since Sep 4, 2003
 */
public class OptionsListTest
    extends TestCase
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String NL = System.getProperty("line.separator");

    //~ Methods ----------------------------------------------------------------

    public void _testBooleanArg()
    {
        checkIt(
            "flag=true",
            new OptionsList.Option[] {
                new OptionsList.BooleanOption(
                    "flag",
                    null,
                    "xxx",
                    false,
                    false,
                    false,
                    null)
            },
            new String[] { "-flag" });
    }

    public void _testBooleanArgMissing()
    {
        checkIt(
            "",
            new OptionsList.Option[] {
                new OptionsList.BooleanOption(
                    "flag",
                    null,
                    "xxx",
                    false,
                    false,
                    false,
                    null)
            },
            new String[] {});
    }

    public void _testUnknownArg()
    {
        checkIt(
            "?",
            new OptionsList.Option[] {
                new OptionsList.BooleanOption(
                    "flag",
                    null,
                    "xxx",
                    false,
                    false,
                    false,
                    null)
            },
            new String[] { "-unknown" });
    }

    public void _testUnknownArgWithEqualsSyntax()
    {
        checkIt(
            "?",
            new OptionsList.Option[] {
                new OptionsList.BooleanOption(
                    "flag",
                    null,
                    "xxx",
                    false,
                    false,
                    false,
                    null)
            },
            new String[] { "unknown=foo" });
    }

    public void _testStringArgSameAsDefault()
    {
        checkIt(
            "foo=default",
            new OptionsList.Option[] {
                new OptionsList.StringOption(
                    "foo",
                    "foo",
                    "xxx",
                    false,
                    false,
                    "default",
                    null)
            },
            new String[] { "foo=default" });
    }

    public void _testStringArgUsesDefaultValue()
    {
        checkIt(
            "foo=default (default)",
            new OptionsList.Option[] {
                new OptionsList.StringOption(
                    "foo",
                    "foo",
                    "xxx",
                    false,
                    false,
                    "default",
                    null)
            },
            new String[] { "" });
    }

    public void _testEnumeratedArgShort()
    {
        checkIt(
            "color=GREEN",
            new OptionsList.Option[] {
                new OptionsList.EnumeratedOption(
                    "c",
                    "color",
                    "",
                    false,
                    false,
                    Color.RED,
                    Color.RED.getEnumeratedType(),
                    null)
            },
            new String[] { "-c GREEN" });
    }

    public void _testEnumeratedArgLong()
    {
        checkIt(
            "color=BLUE",
            new OptionsList.Option[] {
                new OptionsList.EnumeratedOption(
                    "c",
                    "color",
                    "",
                    false,
                    false,
                    Color.RED,
                    Color.RED.getEnumeratedType(),
                    null)
            },
            new String[] { "color=BLUE" });
    }

    public void _testEnumeratedArgWrong()
    {
        checkIt(
            "PURPLE is not a valid value",
            new OptionsList.Option[] {
                new OptionsList.EnumeratedOption(
                    "c",
                    "color",
                    "",
                    false,
                    false,
                    Color.RED,
                    Color.RED.getEnumeratedType(),
                    null)
            },
            new String[] { "-c PURPLE" });
    }

    public void _testEnumeratedArgDefault()
    {
        checkIt(
            "color=RED (default)",
            new OptionsList.Option[] {
                new OptionsList.EnumeratedOption(
                    "c",
                    "color",
                    "",
                    false,
                    false,
                    Color.RED,
                    Color.RED.getEnumeratedType(),
                    null),
                new OptionsList.NumberOption(
                    "x",
                    "x",
                    "",
                    false,
                    false,
                    null,
                    null)
            },
            new String[] { "-c PURPLE" });
    }

    public void _testMissingMandatory()
    {
        checkIt(
            "missing x",
            new OptionsList.Option[] {
                new OptionsList.NumberOption(
                    "x",
                    "x",
                    "",
                    true,
                    false,
                    null,
                    null)
            },
            new String[] {});
    }

    public void _testGroupOptionsMustBeOptional()
    {
        final OptionsList.NumberOption optionX =
            new OptionsList.NumberOption(
                "x",
                "x",
                "",
                false,
                false,
                null,
                null);
        final OptionsList.NumberOption optionY =
            new OptionsList.NumberOption("y", "y", "", true, false, null, null);
        OptionsList optionsList =
            new OptionsList(new OptionsList.Option[] { optionX, optionY });
        try {
            optionsList.constrain(
                new OptionsList.Option[] { optionX, optionY },
                0,
                1);
            assertTrue("Expected an error", false);
        } catch (Exception e) {
            assertContains(
                "fewer than 1...",
                e.toString());
        }
    }

    public void _testGroupMissing()
    {
        final StringBufferOptionsHandler handler =
            new StringBufferOptionsHandler();
        final OptionsList.NumberOption optionX =
            new OptionsList.NumberOption(
                "x",
                "x",
                "",
                false,
                false,
                null,
                null);
        final OptionsList.NumberOption optionY =
            new OptionsList.NumberOption(
                "y",
                "y",
                "",
                false,
                false,
                null,
                null);
        final OptionsList.NumberOption optionZ =
            new OptionsList.NumberOption(
                "z",
                "z",
                "",
                false,
                false,
                null,
                null);
        OptionsList optionsList =
            new OptionsList(
                new OptionsList.Option[] { optionX, optionY, optionZ });
        optionsList.constrain(
            new OptionsList.Option[] { optionX, optionY },
            0,
            1);
        String [] args = { "-z" };
        optionsList.parse(args);
        assertEquals(
            "foo",
            handler.toString());
    }

    public void _testAnonymousOption()
    {
        checkIt(
            "verbose=true" + NL + "file=file.txt",
            new OptionsList.Option[] {
                new OptionsList.BooleanOption(
                    "flag",
                    "flag",
                    "",
                    false,
                    false,
                    false,
                    null),
                new OptionsList.StringOption(
                    "file",
                    "file",
                    "",
                    false,
                    true,
                    "foo.txt",
                    null)
            },
            new String[] { "-v", "bar.txt" });
    }

    public void _testRepeatingOption()
    {
        checkIt(
            "verbose=true" + NL + "file=foo.txt" + NL + "file=bar.txt",
            new OptionsList.Option[] {
                new OptionsList.BooleanOption(
                    "flag",
                    "flag",
                    "",
                    false,
                    false,
                    false,
                    null),
                new OptionsList.StringOption(
                    "file",
                    "file",
                    "",
                    false,
                    true,
                    "foo.txt",
                    null)
            },
            new String[] { "-v", "-f", "foo.txt", "-f", "bar.txt" });
    }

    public void testAlwaysSucceeds()
    {
        // TODO enable the other tests, and remove this test -- it only exists
        //   to stop junit complaining that there are no tests!
    }

    // -------------------------------------------------------------------------
    // Utility methods and classes
    private void assertContains(
        String expected,
        String actual)
    {
        if (actual.indexOf(expected) < 0) {
            fail("Expected '" + actual + "' to contain '" + expected + "'");
        }
    }

    private void checkIt(
        final String expected,
        final OptionsList.Option [] options,
        final String [] args)
    {
        final StringBufferOptionsHandler handler =
            new StringBufferOptionsHandler();
        final OptionsList optionsList = new OptionsList();
        for (int i = 0; i < options.length; i++) {
            options[i].setHandler(handler);
            optionsList.add(options[i]);
        }
        optionsList.parse(args);
        assertEquals(
            expected,
            handler.toString());
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class Color
        extends EnumeratedValues.BasicValue
    {
        public static final Color RED = new Color("RED", 0);
        public static final Color GREEN = new Color("GREEN", 0);
        public static final Color BLUE = new Color("BLUE", 0);
        public static final EnumeratedValues enumeration =
            new EnumeratedValues(new Color[] { RED, GREEN, BLUE });

        public Color(
            String name,
            int ordinal)
        {
            super(name, ordinal, null);
        }

        public EnumeratedValues getEnumeratedType()
        {
            return enumeration;
        }
    }

    /**
     * Implementation of {@link OptionsList.OptionHandler} which writes to a
     * buffer. For testing purposes.
     */
    public static class StringBufferOptionsHandler
        implements OptionsList.OptionHandler
    {
        private StringWriter buf = new StringWriter();

        public void set(
            OptionsList.Option option,
            Object value,
            boolean isExplicit)
        {
            buf.write(
                option.getName() + "=" + value
                + (isExplicit ? "" : " (default)") + NL);
        }

        public void invalidValue(
            OptionsList.Option option,
            String value)
        {
            buf.write(value + " is not valid for " + option.getName() + NL);
        }

        public String toString()
        {
            return buf.toString();
        }
    }
}

// End OptionsListTest.java
