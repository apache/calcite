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
package org.eigenbase.util.mapping;

/**
 * Describes the type of a mapping, from the most general {@link #MultiFunction}
 * (every element in the source and target domain can participate in many
 * mappings) to the most retricted {@link #Bijection} (every element in the
 * source and target domain must be paired with precisely one element in the
 * other domain).
 *
 * <p>Some common types:
 *
 * <ul>
 * <li>A surjection is a mapping if every target has at least one source; also
 * known as an 'onto' mapping.
 * <li>A mapping is a partial function if every source has at most one target.
 * <li>A mapping is a function if every source has precisely one target.
 * <li>An injection is a mapping where a target has at most one source; also
 * somewhat confusingly known as a 'one-to-one' mapping.
 * <li>A bijection is a mapping which is both an injection and a surjection.
 * Every source has precisely one target, and vice versa.
 * </ul>
 *
 * <p>Once you know what type of mapping you want, call {@link
 * Mappings#create(MappingType, int, int)} to create an efficient implementation
 * of that mapping.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 24, 2006
 */
public enum MappingType
{
    //            ordinal source target function inverse
    //            ======= ====== ====== ======== =================

    //                  0      1      1 true     0 Bijection
    Bijection,

    //                  1   <= 1      1 true     4 InverseSurjection
    Surjection,

    //                  2   >= 1      1 true     8 InverseInjection
    Injection,

    //                  3    any      1 true     12 InverseFunction
    Function,

    /**
     * An inverse surjection has a source for every target, and no source has
     * more than one target.
     */
    //                  4      1   <= 1 partial  1 Surjection
    InverseSurjection,

    /**
     * A partial surjection has no more than one source for any target, and no
     * more than one target for any source.
     */
    //                  5   <= 1   <= 1 partial  5 PartialSurjection
    PartialSurjection,

    //                  6   >= 1   <= 1 partial  9 InversePartialInjection
    PartialInjection,

    //                  7    any   <= 1 partial  13 InversePartialFunction
    PartialFunction,

    //                  8      1   >= 1 multi    2 Injection
    InverseInjection,

    //                  9   <= 1   >= 1 multi    6 PartialInjection
    InversePartialInjection,

    //                 10   >= 1   >= 1 multi    10
    Ten,

    //                 11    any   >= 1 multi    14
    Eleven,

    /**
     * An inverse function has a source for every target, but a source might
     * have 0, 1 or more targets.
     *
     * <p>Obeys the constaints {@link MappingType#isMandatorySource()}, {@link
     * MappingType#isSingleSource()}.
     *
     * <p>Similar types:
     *
     * <ul>
     * <li> {@link #InverseSurjection} is stronger (a source may not have
     * multiple targets);
     * <li>{@link #InversePartialFunction} is weaker (a target may have 0 or 1
     * sources).
     * </ul>
     */
    //                 12      1    any multi    3 Function
    InverseFunction,

    //                 13   <= 1    any multi    7 PartialFunction
    InversePartialFunction,

    //                 14   >= 1    any multi    11
    Fourteen,

    //                 15    any    any multi    15 MultiFunction
    MultiFunction;

    private final int inverseOrdinal;

    private MappingType()
    {
        this.inverseOrdinal = ((ordinal() & 3) << 2)
            | ((ordinal() & 12) >> 2);
    }

    public MappingType inverse()
    {
        return MappingType.values()[this.inverseOrdinal];
    }

    /**
     * Returns whether this mapping type is (possibly a weaker form of) a given
     * mapping type.
     *
     * <p>For example, a {@link #Bijection} is a {@link #Function}, but not
     * every {link #Function} is a {@link #Bijection}.
     */
    public boolean isA(MappingType mappingType)
    {
        return (ordinal() & mappingType.ordinal()) == ordinal();
    }

    /**
     * A mapping is a total function if every source has precisely one target.
     */
    public boolean isFunction()
    {
        return (ordinal() & (OptionalTarget | MultipleTarget)) == 0;
    }

    /**
     * A mapping is a partial function if every source has at most one target.
     */
    public boolean isPartialFunction()
    {
        return (ordinal() & MultipleTarget) == 0;
    }

    /**
     * A mapping is a surjection if it is a function and every target has at
     * least one source.
     */
    public boolean isSurjection()
    {
        return (ordinal() & (OptionalTarget | MultipleTarget | OptionalSource))
            == 0;
    }

    /**
     * A mapping is an injection if it is a function and no target has more than
     * one source. (In other words, every source has precisely one target.)
     */
    public boolean isInjection()
    {
        return (ordinal() & (OptionalTarget | MultipleTarget | MultipleSource))
            == 0;
    }

    /**
     * A mapping is a bijection if it is a surjection and it is an injection.
     * (In other words,
     */
    public boolean isBijection()
    {
        return (ordinal()
            & (OptionalTarget | MultipleTarget | OptionalSource
                | MultipleSource)) == 0;
    }

    /**
     * Constraint that every source has at least one target.
     */
    public boolean isMandatoryTarget()
    {
        return !((ordinal() & OptionalTarget) == OptionalTarget);
    }

    /**
     * Constraint that every source has at most one target.
     */
    public boolean isSingleTarget()
    {
        return !((ordinal() & MultipleTarget) == MultipleTarget);
    }

    /**
     * Constraint that every target has at least one source.
     */
    public boolean isMandatorySource()
    {
        return !((ordinal() & OptionalSource) == OptionalSource);
    }

    /**
     * Constraint that every target has at most one source.
     */
    public boolean isSingleSource()
    {
        return !((ordinal() & MultipleSource) == MultipleSource);
    }

    /**
     * Allow less than one source for a given target.
     */
    private static final int OptionalSource = 1;

    /**
     * Allow more than one source for a given target.
     */
    private static final int MultipleSource = 2;

    /**
     * Allow less than one target for a given source.
     */
    private static final int OptionalTarget = 4;

    /**
     * Allow more than one target for a given source.
     */
    private static final int MultipleTarget = 8;
}

// End MappingType.java
