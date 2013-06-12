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
package org.eigenbase.util14;

import java.io.*;
import java.util.*;

import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.parser.SqlParserPos;


/**
 * <code>Enum14</code> is a pre-JDK1.5-enum helper class for declaring a set of
 * symbolic constants which have names, ordinals, and possibly descriptions. The
 * ordinals do not have to be contiguous.
 *
 * <p>Typically, for a particular set of constants, you derive a class from this
 * interface, and declare the constants as <code>public static final</code>
 * members. Give it a private constructor, and a <code>public static final <i>
 * ClassName</i> instance</code> member to hold the singleton instance.</p>
 */
public class Enum14
    implements Cloneable
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String [] emptyStringArray = new String[0];

    //~ Instance fields --------------------------------------------------------

    /**
     * map symbol names to values
     */
    private HashMap valuesByName = new HashMap();

    // the variables below are only set AFTER makeImmutable() has been called

    /**
     * An array mapping ordinals to {@link Value}s. It is biased by the min
     * value. It is built by {@link #makeImmutable}.
     */
    private Value [] ordinalToValueMap;

    /**
     * the largest ordinal value
     */
    private int max = Integer.MIN_VALUE;

    /**
     * the smallest ordinal value
     */
    private int min = Integer.MAX_VALUE;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new empty, mutable enumeration.
     */
    public Enum14()
    {
    }

    /**
     * Creates an enumeration, with an array of values, and freezes it.
     */
    public Enum14(Value [] values)
    {
        for (int i = 0; i < values.length; i++) {
            register(values[i]);
        }
        makeImmutable();
    }

    /**
     * Creates an enumeration, initialize it with an array of strings, and
     * freezes it.
     */
    public Enum14(String [] names)
    {
        for (int i = 0; i < names.length; i++) {
            register(new BasicValue(names[i], i, names[i]));
        }
        makeImmutable();
    }

    /**
     * Create an enumeration, initializes it with arrays of code/name pairs, and
     * freezes it.
     */
    public Enum14(
        String [] names,
        int [] codes)
    {
        for (int i = 0; i < names.length; i++) {
            register(new BasicValue(names[i], codes[i], names[i]));
        }
        makeImmutable();
    }

    /**
     * Create an enumeration, initializes it with arrays of code/name pairs, and
     * freezes it.
     */
    public Enum14(
        String [] names,
        int [] codes,
        String [] descriptions)
    {
        for (int i = 0; i < names.length; i++) {
            register(new BasicValue(names[i], codes[i], descriptions[i]));
        }
        makeImmutable();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the description associated with an ordinal; the return value is
     * null if the ordinal is not a member of the enumeration.
     *
     * @pre isImmutable()
     */
    public final String getDescription(int ordinal)
    {
        assert (isImmutable());
        final Value value = ordinalToValueMap[ordinal - min];
        if (value == null) {
            return null;
        } else {
            return value.getDescription();
        }
    }

    public final boolean isImmutable()
    {
        return (ordinalToValueMap != null);
    }

    /**
     * Returns the number of enumerated values currently contained in this
     * enumeration
     */
    public final int getSize()
    {
        return valuesByName.size();
    }

    /**
     * Returns the largest ordinal defined by this enumeration.
     */
    public final int getMax()
    {
        return max;
    }

    /**
     * Returns the smallest ordinal defined by this enumeration.
     */
    public final int getMin()
    {
        return min;
    }

    /**
     * Creates a mutable enumeration from an existing enumeration, which may
     * already be immutable.
     */
    public Enum14 getMutableClone14()
    {
        return (Enum14) clone();
    }

    /**
     * Returns the name associated with an ordinal; the return value is null if
     * the ordinal is not a member of the enumeration.
     *
     * @pre isImmutable()
     */
    public final String getName(int ordinal)
    {
        final Value value = getValue(ordinal);
        return (value == null) ? null : value.getName();
    }

    /**
     * Returns the value associated with an ordinal; the return value is null if
     * the ordinal is not a member of the enumeration.
     *
     * @pre isImmutable()
     */
    public final Value getValue(int ordinal)
    {
        assert (isImmutable());
        final Value value = ordinalToValueMap[ordinal - min];
        if (value == null) {
            return null;
        } else {
            return value;
        }
    }

    /**
     * Returns the ordinal associated with a name
     *
     * @throws Error if the name is not a member of the enumeration
     */
    public final int getOrdinal(String name)
    {
        return getValue(name).getOrdinal();
    }

    /**
     * Returns whether <code>ordinal</code> is valid for this enumeration. This
     * method is particularly useful in pre- and post-conditions, for example
     *
     * <blockquote>
     * <pre>&#64;param axisCode Axis code, must be a {&#64;link AxisCode} value
     * &#64;pre AxisCode.instance.isValid(axisCode)</pre>
     * </blockquote>
     *
     * @param ordinal Suspected ordinal from this enumeration.
     *
     * @return Whether <code>ordinal</code> is valid.
     */
    public final boolean isValid(int ordinal)
    {
        if ((ordinal < min) || (ordinal > max)) {
            return false;
        }
        if (getName(ordinal) == null) {
            return false;
        }
        return true;
    }

    /**
     * Returns an iterator over the values of this enumeration.
     */
    public Iterator iterator()
    {
        final Collection values =
            Collections.unmodifiableCollection(
                valuesByName.values());
        return values.iterator();
    }

    /**
     * Returns the names in this enumeration, in no particular order.
     */
    public String [] getNames()
    {
        return (String []) valuesByName.keySet().toArray(emptyStringArray);
    }

    /**
     * Returns the ordinal associated with a name.
     *
     * @throws Error if the name is not a member of the enumeration
     */
    public Value getValue(String name)
    {
        final Value value = (Value) valuesByName.get(name);
        if (value == null) {
            throw new Error("Unknown enum name:  " + name);
        }
        return value;
    }

    /**
     * Returns true if this enumerationr contains <code>name</code>, else false.
     */
    public boolean containsName(String name)
    {
        return valuesByName.containsKey(name);
    }

    /**
     * Returns an error indicating that the value is illegal. (The client needs
     * to throw the error.)
     */
    public Error badValue(int ordinal)
    {
        return new AssertionError(
            "bad value " + ordinal + "("
            + getName(ordinal) + ") for enumeration '"
            + getClass().getName()
            + "'");
    }

    /**
     * Freezes the enumeration, preventing it from being further modified.
     */
    public void makeImmutable()
    {
        ordinalToValueMap = new Value[(1 + max) - min];
        for (
            Iterator values = valuesByName.values().iterator();
            values.hasNext();)
        {
            Value value = (Value) values.next();
            final int index = value.getOrdinal() - min;
            if (ordinalToValueMap[index] != null) {
                throw new AssertionError(
                    "Enumeration has more than one value with ordinal "
                    + value.getOrdinal());
            }
            ordinalToValueMap[index] = value;
        }
    }

    /**
     * Associates a symbolic name with an ordinal value.
     *
     * @pre value != null
     * @pre !isImmutable()
     * @pre value.getName() != null
     */
    public void register(Value value)
    {
        assert (value != null);
        assert (!isImmutable());
        final String name = value.getName();
        assert (name != null);
        Value old = (Value) valuesByName.put(name, value);
        if (old != null) {
            throw new AssertionError(
                "Enumeration already contained a value '"
                + old.getName() + "'");
        }
        final int ordinal = value.getOrdinal();
        min = Math.min(min, ordinal);
        max = Math.max(max, ordinal);
    }

    /**
     * Returns an exception indicating that we didn't expect to find this value
     * here.
     *
     * @see org.eigenbase.util.Util#unexpected
     */
    public Error unexpected(Value value)
    {
        return new AssertionError(
            "Was not expecting value '" + value
            + "' for enumeration '" + getClass().getName()
            + "' in this context");
    }

    protected Object clone()
    {
        Enum14 clone = null;
        try {
            clone = (Enum14) super.clone();
        } catch (CloneNotSupportedException ex) {
            // IMPLEMENT internal error?
        }
        clone.valuesByName = (HashMap) valuesByName.clone();
        clone.ordinalToValueMap = null;
        return clone;
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * A <code>Value</code> represents a member of an enumerated type. If an
     * enumerated type is not based upon an explicit array of values, an array
     * of {@link Enum14.BasicValue}s will implicitly be created.
     */
    public interface Value
        extends Comparable
    {
        String getDescription();

        String getName();

        int getOrdinal();
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * <code>BasicValue</code> is an obvious implementation of {@link
     * Enum14.Value}. This class is marked Serializable so that serializable
     * subclasses can be supported without requiring a default (no-argument)
     * constructor. However, note that while <code>BasicValue</code> is marked
     * Serializable, deserialized instances will be new instances rather than
     * members of the original enumeration. In other words, <code>
     * deserializedBasicValue == origBasicValue</code> will be false. Use {@link
     * Enum14.SerializableValue} for instances that deserialize into members of
     * the original enumeration so that <code>deserializedBasicValue ==
     * origBasicValue</code> will be true.
     */
    public static class BasicValue
        implements Value,
            Serializable
    {
        /**
         * SerialVersionUID created with JDK 1.5 serialver tool.
         */
        private static final long serialVersionUID = -7944099370846909699L;

        private final String description;
        private final String name;
        private final int ordinal;

        /**
         * @pre name != null
         */
        public BasicValue(
            String name,
            int ordinal,
            String description)
        {
            assert (name != null);
            this.name = name;
            this.ordinal = ordinal;
            this.description = description;
        }

        public String getDescription()
        {
            return description;
        }

        public String getName()
        {
            return name;
        }

        public int getOrdinal()
        {
            return ordinal;
        }

        /**
         * Returns whether this value is equal to a given string.
         *
         * @deprecated I bet you meant to write <code>
         * value.name.equals(s)</code> rather than <code>value.equals(s)</code>,
         * didn't you?
         */
        public boolean equals(String s)
        {
            return super.equals(s);
        }

        // forwarding function for super.equals
        public final boolean equals(Object o)
        {
            return super.equals(o);
        }

        // keep checkstyle happy
        public final int hashCode()
        {
            return super.hashCode();
        }

        // implement Comparable
        public int compareTo(Object other)
        {
            assert (other instanceof BasicValue);
            BasicValue otherValue = (BasicValue) other;
            return ordinal - otherValue.ordinal;
        }

        /**
         * Returns the value's name.
         */
        public String toString()
        {
            return name;
        }

        public Error unexpected()
        {
            return new AssertionError(
                "Value " + name + " of class "
                + getClass() + " unexpected here");
        }
    }

    /**
     * <code>SerializableValue</code> extends <code>BasicValue</code> to provide
     * better support for serializable subclasses. Instances of <code>
     * SerializableValue</code> will deserialize into members of the original
     * enumeration so that <code>deserializedBasicValue == origBasicValue</code>
     * will be true.
     */
    public static abstract class SerializableValue
        extends BasicValue
        implements Serializable
    {
        /**
         * SerialVersionUID created with JDK 1.5 serialver tool.
         */
        private static final long serialVersionUID = 1534436036499327177L;

        /**
         * Ordinal value which, when deserialized, can be used by {@link
         * #readResolve} to locate a matching instance in the original
         * enumeration.
         */
        protected int _ordinal;

        /**
         * Creates a new SerializableValue.
         */
        public SerializableValue(
            String name,
            int ordinal,
            String description)
        {
            super(name, ordinal, description);
        }

        /**
         * Subclass must implement this method to retrieve a matching instance
         * based on the <code>_ordinal</code> deserialized by {@link
         * #readObject}. This would typically be an instance from the original
         * enumeration. Current instance is the candidate object deserialized
         * from the ObjectInputStream. It is incomplete, cannot be used as-is,
         * and this method must return a valid replacement. For example,<br>
         * <code>return SqlTypeName.get(_ordinal);</code>
         *
         * @return replacement instance that matches <code>_ordinal</code>
         *
         * @throws java.io.ObjectStreamException
         */
        protected abstract Object readResolve()
            throws ObjectStreamException;

        /**
         * Deserialization method reads the <code>_ordinal</code> value.
         */
        private void readObject(ObjectInputStream in)
            throws IOException
        {
            this._ordinal = in.readInt();
        }

        /**
         * Serialization method writes just the ordinal value.
         */
        private void writeObject(ObjectOutputStream out)
            throws IOException
        {
            out.writeInt(this.getOrdinal());
        }
    }
}

// End Enum14.java
