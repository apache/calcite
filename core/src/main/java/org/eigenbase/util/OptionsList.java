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

import java.util.*;


/**
 * Command-line option parser.
 *
 * <p>For example, given the options</p>
 *
 * <table>
 * <tr>
 * <th>Short name</td>
 * <th>Long name</td>
 * <th>Type</td>
 * <th>Default</td>
 * <th>Anonymous</td>
 * </tr>
 * <tr>
 * <td>v</td>
 * <td>verbose</td>
 * <td>Boolean</td>
 * <td>true</td>
 * <td>false</td>
 * </tr>
 * <tr>
 * <td>f</td>
 * <td>&nbsp;</td>
 * <td>String</td>
 * <td>&nbsp;</td>
 * <td>true</td>
 * </tr>
 * <tr>
 * <td>c</td>
 * <td>count</td>
 * <td>Number</td>
 * <td>&nbsp;</td>
 * <td>false</td>
 * </tr>
 * </table>
 *
 * <p>and the command line
 *
 * <pre>-v count=5 Foo.txt</pre>
 *
 * the parser will set <tt>verbose = true</tt>, <tt>count = 5</tt>, and <tt>file
 * = Foo.txt</tt>.</p>
 *
 * <p>Options can generally be specified using <dfn>flag syntax</dfn> (for
 * example <tt>-v</tt> or <tt>-count 5</tt>) or <dfn>property synax</dfn> (for
 * example <tt>verbose=true</tt> or <tt>count=5</tt>).</p>
 *
 * <p>Boolean options do not have a value following the flag. <tt>-v</tt> means
 * the same as <tt>verbose=true</tt>, and <tt>+v</tt> means the same as <tt>
 * verbose=false</tt>.</p>
 *
 * <p>One of the options in a list can be <em>anonymous</em>. Arguments which
 * are not flagged with an option name are assumed to be for this option.<br>
 * <em>Defining options</em><br>
 * </p>
 *
 * <p>You first define what options are available. Options all implement
 * interface {@link Option}. You can use one of the built-in option types (
 * {@link StringOption}, {@link NumberOption}, {@link BooleanOption}, {@link
 * EnumeratedOption}) or write one of your own.<br>
 * <em>Parsing options</em><br>
 * </p>
 *
 * <p>Once you have defined the options, you can parse an argument list by
 * calling {@link #parse}.</p>
 *
 * <p>There are two ways of handling options. By default, when you parse an
 * array of command-line parameters, the values of those parameters are stored
 * in the options themselves. Alternatively, you can specify a {@link
 * OptionsList.OptionHandler handler}.</p>
 *
 * @author Julian Hyde
 * @version $Id$
 * @since Sep 4, 2003
 */
public class OptionsList
{
    //~ Instance fields --------------------------------------------------------

    private final ArrayList<Group> optionGroups = new ArrayList<Group>();
    private final ArrayList<Option> options = new ArrayList<Option>();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an options list with no options.
     */
    public OptionsList()
    {
    }

    /**
     * Creates an options list with an array of options.
     */
    public OptionsList(Option [] options)
    {
        for (Option option : options) {
            this.options.add(option);
        }
    }

    //~ Methods ----------------------------------------------------------------

    public void add(Option option)
    {
        options.add(option);
    }

    /**
     * Tells the options list that the given options are mutually exclusive.
     * This means that at most one of the given options can be specified.
     *
     * <p>To create a set mutually exclusive options, specify minCount = 0 or 1,
     * maxCount = 1. To create a set of mutually inclusive options, specify
     * minCount = 1, maxCount = -1.</p>
     *
     * @param options List of mutually exclusive options
     * @param minCount Minimum number of these options which must be specified.
     * @param maxCount Maximum number of these options which must be specified.
     *
     * @pre None of the options must be mandatory.
     */
    public void constrain(
        Option [] options,
        int minCount,
        int maxCount)
    {
        for (Option option : options) {
            if (option.required) {
                throw new AssertionError("!options[i].required");
            }
        }
        optionGroups.add(new Group(minCount, maxCount, options));
    }

    public void parse(String [] args)
    {
        final Option [] options = toArray();
        final Set<Option> usedOptions = new HashSet<Option>();
        for (Option option : options) {
            option.setHandler(
                new OptionHandler() {
                    public void invalidValue(Option option, String value) {
                    }
                    public void set(
                        Option option, Object value, boolean explicit)
                    {
                        usedOptions.add(option);
                    }
                });
        }
        for (int i = 0; i < args.length; i++) {
            for (Option option : options) {
                if (usedOptions.contains(option)) {
                    // Each option can only be used once.
                    continue;
                }
                int k = option.match(args, i);
                if (k > i) {
                    break;
                }
            }
        }

        // Check mandatory options.
        for (Option option : options) {
            if (!option.required) {
                continue;
            }
            if (!usedOptions.contains(option)) {
                throw new RuntimeException(
                    "Mandatory option '"
                    + option.getName() + "' was not specified");
            }
        }

        // Check inclusivity/exclusivity.
        for (Group group : optionGroups) {
            int count = 0;
            for (int j = 0; j < options.length; j++) {
                Option option = group.options[j];
                if (usedOptions.contains(option)) {
                    count++;
                }
            }
            if (count > group.maxCount) {
                throw new RuntimeException(
                    "More than " + group.maxCount
                    + " of the following options were specified: "
                    + group.description);
            }
            if (count < group.minCount) {
                throw new RuntimeException(
                    "Fewer than " + group.minCount
                    + " of the following options were specified: "
                    + group.description);
            }
        }

        // Set options to their default value if they have a default value and
        // have not been seen while parsing.
        for (Option option : options) {
            if (!usedOptions.contains(option)
                && (option.defaultValue != null))
            {
                option.set(option.defaultValue, false);
            }
        }
    }

    public Option [] toArray()
    {
        return options.toArray(new Option[options.size()]);
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * Handles the event of setting options. One implementation stores options
     * in a property list; another implementation uses reflection.
     */
    interface OptionHandler
    {
        void invalidValue(
            Option option,
            String value);

        void set(
            Option option,
            Object value,
            boolean isExplicit);
    }

    //~ Inner Classes ----------------------------------------------------------

    public class BasicOptionHandler
        implements OptionHandler
    {
        public void invalidValue(
            Option option,
            String value)
        {
        }

        public void set(
            Option option,
            Object value,
            boolean isExplicit)
        {
        }
    }

    /**
     * Definition of a command-line option, including its short and long names,
     * description, default value, and whether it is mandatory.
     *
     * <p>You can optionally provide a {@link OptionsList.OptionHandler handler}
     * to handle events such as the option receiving a value, or a value being
     * of the wrong format. If you do not provide a handler, the value is stored
     * inside the option, and can be retrieved via</p>
     */
    public static abstract class Option
    {
        /**
         * Holds the runtime value of this option. Set by the default
         * implementation {@link #set}. If the user has supplied an {@link
         * OptionHandler}, or overridden the <code>set</code> method, this field
         * is not assigned.
         *
         * <p>Several derived classes have typesafe methods to access this
         * field: see {@link OptionsList.BooleanOption#booleanValue}, {@link
         * OptionsList.StringOption#stringValue}, {@link
         * OptionsList.NumberOption#intValue}, {@link
         * OptionsList.NumberOption#doubleValue}.</p>
         */
        protected Object value;

        /**
         * Default value of option, or null if there is no default value.
         */
        private final Object defaultValue;
        private final String description;

        /**
         * Short name of option, used as a flag, e.g. "c".
         */
        private final String flag;

        /**
         * Long name of option, e.g. "color".
         */
        private final String name;
        private final boolean required;
        private List<OptionHandler> handlers = new ArrayList<OptionHandler>();

        Option(
            String flag,
            String option,
            String description,
            boolean required,
            boolean anonymous,
            Object defaultValue)
        {
            this.flag = flag;
            name = option;
            this.required = required;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        public String getDescription()
        {
            return description;
        }

        public void setHandler(OptionHandler handler)
        {
            this.handlers.add(handler);
        }

        public String getName()
        {
            return name != null ? name : flag;
        }

        /**
         * Returns the value of this option for the most recent call to {@link
         * #parse}.
         *
         * <p>If you specified an {@link OptionsList.OptionHandler}, this value
         * will not be set. Also note that this method is unsafe if the same
         * options are shared between multiple threads.</p>
         *
         * <p>Some derived classes have methods which return the same
         * information in a typesafe manner. For example: {@link
         * OptionsList.BooleanOption#booleanValue}, {@link
         * OptionsList.NumberOption#intValue}.</p>
         */
        public Object getValue()
        {
            return value;
        }

        /**
         * Tries to apply this option to the <tt>i</tt>th member of
         * <tt>args</tt>.
         *
         * @param args Argument list
         * @param i Offset of argument in argument list
         *
         * @return If matched, the offset of the argument after the last one
         * matched, otherwise <tt>i</tt>.
         */
        public int match(
            String [] args,
            int i)
        {
            final String arg = args[i];
            if (arg.startsWith("-")
                && (flag != null)
                && arg.equals("-" + flag))
            {
                if (this instanceof BooleanOption) {
                    // e.g. "-nolog"
                    set(true, true);
                    return i + 1;
                }
                if (i < args.length - 1) {
                    // e.g. "-threads 5"
                    readArg(args[i + 1]);
                    return i + 2;
                }
            }
            if ((name != null) && arg.startsWith(name + "=")) {
                // e.g. "threads=5"
                readArg(arg.substring((name + "=").length()));
                return i + 1;
            }
            return i;
        }

        public void set(
            Object value,
            boolean isExplicit)
        {
            this.value = value;
            for (OptionHandler handler : handlers) {
                handler.set(this, value, isExplicit);
            }
        }

        /**
         * Converts an argument to the correct value type, and acts on the
         * value.
         *
         * <p>What action is taken depends upon whether the value is valid for
         * this argument type, and whether there is a handler. If there is a
         * handler, this method calls either {@link
         * OptionsList.OptionHandler#set} or {@link
         * OptionsList.OptionHandler#invalidValue}. If there is no handler, the
         * method should execute a reasonable default action like assigning to a
         * field via reflection.</p>
         *
         * @param arg Argument
         */
        protected abstract void readArg(String arg);

        /**
         * Called by the parser when an argument is not a valid value for this
         * type of argument.
         *
         * <p>For example, if "flag" is a boolean argument and they specify
         * "flag=oui" on the command-line, the parser will call <code>
         * valueError("oui")</code>.</p>
         *
         * <p>The default implementation calls {@link
         * OptionsList.OptionHandler#invalidValue} if there is a handler, or
         * prints a message to {@link System#out} if there is not. Derived
         * option classes can override this method.</p>
         *
         * @param arg String value which is supposed to match the parameter, but
         * doesn't.
         */
        protected void valueError(String arg)
        {
            for (OptionHandler handler : handlers) {
                handler.invalidValue(this, arg);
            }
        }
    }

    public static class BooleanOption
        extends Option
    {
        public BooleanOption(
            String flag,
            String option,
            String description,
            boolean required,
            boolean anonymous,
            boolean defaultValue)
        {
            super(
                flag,
                option,
                description,
                required,
                anonymous,
                Boolean.valueOf(defaultValue));
        }

        public boolean booleanValue()
        {
            return ((Boolean) value).booleanValue();
        }

        protected void readArg(String arg)
        {
            if (arg.equals("true")) {
                set(Boolean.TRUE, true);
            } else if (arg.equals("false")) {
                set(Boolean.FALSE, true);
            } else {
                valueError(arg);
            }
        }
    }

    public static class EnumeratedOption
        extends Option
    {
        private final EnumeratedValues enumeration;

        public EnumeratedOption(
            String flag,
            String option,
            String description,
            boolean required,
            boolean anonymous,
            EnumeratedValues.Value defaultValue,
            EnumeratedValues enumeration)
        {
            super(
                flag,
                option,
                description,
                required,
                anonymous,
                defaultValue);
            this.enumeration = enumeration;
        }

        protected void readArg(String arg)
        {
            final EnumeratedValues.Value value = enumeration.getValue(arg);
            if (value == null) {
                valueError(arg);
            } else {
                set(value, true);
            }
        }
    }

    public static class NumberOption
        extends Option
    {
        public NumberOption(
            String flag,
            String option,
            String description,
            boolean required,
            boolean anonymous,
            Number defaultValue)
        {
            super(
                flag,
                option,
                description,
                required,
                anonymous,
                defaultValue);
        }

        public double doubleValue()
        {
            return ((Number) value).doubleValue();
        }

        public int intValue()
        {
            return ((Number) value).intValue();
        }

        protected void readArg(String arg)
        {
            try {
                final long value = Long.parseLong(arg);
                set(
                    new Long(value),
                    true);
            } catch (NumberFormatException e) {
                try {
                    final double doubleValue = Double.parseDouble(arg);
                    set(
                        new Double(doubleValue),
                        true);
                } catch (NumberFormatException e1) {
                    valueError(arg);
                }
            }
        }
    }

    public static class StringOption
        extends Option
    {
        public StringOption(
            String flag,
            String option,
            String description,
            boolean required,
            boolean anonymous,
            String defaultValue)
        {
            super(
                flag,
                option,
                description,
                required,
                anonymous,
                defaultValue);
        }

        public String stringValue()
        {
            return (String) value;
        }

        protected void readArg(String arg)
        {
            set(arg, true);
        }
    }

    private static class Group
    {
        private final String description;
        private final Option [] options;
        private final int maxCount;
        private final int minCount;

        Group(
            int maxCount,
            int minCount,
            Option [] options)
        {
            this.maxCount = maxCount;
            this.minCount = minCount;
            this.options = options;

            // derive description
            StringBuilder buf = new StringBuilder("{");
            for (int j = 0; j < this.options.length; j++) {
                Option option = this.options[j];
                if (j > 0) {
                    buf.append(", ");
                }
                buf.append(option.getName());
            }
            buf.append("}");
            description = buf.toString();
        }
    }
}

// End OptionsList.java
