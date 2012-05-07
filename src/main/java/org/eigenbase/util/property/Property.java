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
package org.eigenbase.util.property;

import java.lang.ref.*;

import java.util.*;


/**
 * Definition and accessor for a property.
 *
 * <p>For example:
 *
 * <blockquote><code>
 * <pre>
 * class MyProperties extends Properties {
 *     public final IntegerProperty DebugLevel =
 *         new IntegerProperty(this, "com.acme.debugLevel", 10);
 * }
 *
 * MyProperties props = new MyProperties();
 * System.out.println(props.DebugLevel.get()); // prints "10", the default
 * props.DebugLevel.set(20);
 * System.out.println(props.DebugLevel.get()); // prints "20"
 * </pre>
 * </code></blockquote>
 *
 * @author jhyde
 * @version $Id$
 * @since May 4, 2004
 */
public abstract class Property
{
    //~ Instance fields --------------------------------------------------------

    protected final Properties properties;
    private final String path;
    private final String defaultValue;

    /**
     * List of triggers on this property. Access must be synchronized on this
     * Property object.
     */
    private final TriggerList triggerList = new TriggerList();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Property and associates it with an underlying properties
     * object.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value, null if there is no default.
     */
    protected Property(
        Properties properties,
        String path,
        String defaultValue)
    {
        this.properties = properties;
        this.path = path;
        this.defaultValue = defaultValue;
        if (properties instanceof TriggerableProperties) {
            ((TriggerableProperties) properties).register(this);
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the name of this property. Typically a dotted path such as
     * "com.acme.foo.Bar".
     *
     * @return this property's name (typically a dotted path)
     */
    public String getPath()
    {
        return path;
    }

    /**
     * Returns the default value of this property. Derived classes (for example
     * those with special rules) can override.
     */
    public String getDefaultValue()
    {
        return defaultValue;
    }

    /**
     * Retrieves the value of a property, using a given default value, and
     * optionally failing if there is no value.
     */
    protected String getInternal(
        String defaultValue,
        boolean required)
    {
        String value = properties.getProperty(path, defaultValue);
        if (value != null) {
            return value;
        }
        if (defaultValue == null) {
            value = getDefaultValue();
            if (value != null) {
                return value;
            }
        }
        if (required) {
            throw new RuntimeException("Property " + path + " must be set");
        }
        return value;
    }

    /**
     * Adds a trigger to this property.
     */
    public synchronized void addTrigger(Trigger trigger)
    {
        triggerList.add(trigger);
    }

    /**
     * Removes a trigger from this property.
     */
    public synchronized void removeTrigger(Trigger trigger)
    {
        triggerList.remove(trigger);
    }

    /**
     * Called when a property's value has just changed.
     *
     * <p>If one of the triggers on the property throws a {@link
     * org.eigenbase.util.property.Trigger.VetoRT} exception, this method passes
     * it on.
     *
     * @param oldValue Previous value of the property
     * @param value New value of the property
     *
     * @throws org.eigenbase.util.property.Trigger.VetoRT if one of the triggers
     * threw a VetoRT
     */
    public void onChange(String oldValue, String value)
    {
        if (TriggerableProperties.equals(oldValue, value)) {
            return;
        }

        triggerList.execute(this, value);
    }

    /**
     * Sets a property directly as a string.
     *
     * @return the previous value
     */
    public String setString(String value)
    {
        return (String) properties.setProperty(path, value);
    }

    /**
     * Returns whether this property has a value assigned.
     */
    public boolean isSet()
    {
        return properties.get(path) != null;
    }

    /**
     * Returns the value of this property as a string.
     */
    public String getString()
    {
        return (String) properties.getProperty(path, defaultValue);
    }

    /**
     * Returns the boolean value of this property.
     */
    public boolean booleanValue()
    {
        final String value = getInternal(null, false);
        if (value == null) {
            return false;
        }
        return toBoolean(value);
    }

    /**
     * Converts a string to a boolean.
     *
     * <p/>Note that {@link Boolean#parseBoolean(String)} is similar, but only
     * exists from JDK 1.5 onwards, and only accepts 'true'.
     *
     * @return true if the string is "1" or "true" or "yes", ignoring case and
     * any leading or trailing spaces
     */
    public static boolean toBoolean(final String value)
    {
        String trimmedLowerValue = value.toLowerCase().trim();
        return trimmedLowerValue.equals("1")
            || trimmedLowerValue.equals("true")
            || trimmedLowerValue.equals("yes");
    }

    /**
     * Returns the value of the property as a string, or null if the property is
     * not set.
     */
    public String stringValue()
    {
        return getInternal(null, false);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * A trigger list a list of triggers associated with a given property.
     *
     * <p/>A trigger list is associated with a property key, and contains zero
     * or more {@link Trigger} objects.
     *
     * <p/>Each {@link Trigger} is stored in a {@link WeakReference} so that
     * when the Trigger is only reachable via weak references the Trigger will
     * be be collected and the contents of the WeakReference will be set to
     * null.
     */
    private static class TriggerList
        extends ArrayList
    {
        /**
         * Adds a Trigger, wrapping it in a WeakReference.
         *
         * @param trigger
         */
        void add(final Trigger trigger)
        {
            // this is the object to add to list
            Object o =
                (trigger.isPersistent()) ? trigger
                : (Object) new WeakReference /*<Trigger>*/(trigger);

            // Add a Trigger in the correct group of phases in the list
            for (ListIterator /*<Object>*/ it = listIterator(); it.hasNext();) {
                Trigger t = convert(it.next());

                if (t == null) {
                    it.remove();
                } else if (trigger.phase() < t.phase()) {
                    // add it before
                    it.hasPrevious();
                    it.add(o);
                    return;
                } else if (trigger.phase() == t.phase()) {
                    // add it after
                    it.add(o);
                    return;
                }
            }
            super.add(o);
        }

        /**
         * Removes the given Trigger.
         *
         * <p/>In addition, removes any {@link WeakReference} that is empty.
         *
         * @param trigger
         */
        void remove(final Trigger trigger)
        {
            for (Iterator it = iterator(); it.hasNext();) {
                Trigger t = convert(it.next());

                if (t == null) {
                    it.remove();
                } else if (t.equals(trigger)) {
                    it.remove();
                }
            }
        }

        /**
         * Executes every {@link Trigger} in this {@link TriggerList}, passing
         * in the property key whose change was the casue.
         *
         * <p/>In addition, removes any {@link WeakReference} that is empty.
         *
         * <p>Synchronizes on {@code property} while modifying the trigger list.
         *
         * @param property The property whose change caused this property to
         * fire
         */
        void execute(Property property, String value)
            throws Trigger.VetoRT
        {
            // Make a copy so that if during the execution of a trigger a
            // Trigger is added or removed, we do not get a concurrent
            // modification exception. We do an explicit copy (rather than
            // a clone) so that we can remove any WeakReference whose
            // content has become null. Synchronize, per the locking strategy,
            // while the copy is being made.
            List /*<Trigger>*/ l = new ArrayList /*<Trigger>*/();
            synchronized (property) {
                for (Iterator /*<Object>*/ it = iterator(); it.hasNext();) {
                    Trigger t = convert(it.next());
                    if (t == null) {
                        it.remove();
                    } else {
                        l.add(t);
                    }
                }
            }

            for (int i = 0; i < l.size(); i++) {
                Trigger t = (Trigger) l.get(i);
                t.execute(property, value);
            }
        }

        /**
         * Converts a trigger or a weak reference to a trigger into a trigger.
         * The result may be null.
         */
        private Trigger convert(Object o)
        {
            if (o instanceof WeakReference) {
                o = ((WeakReference) o).get();
            }
            return (Trigger) o;
        }
    }
}

// End Property.java
