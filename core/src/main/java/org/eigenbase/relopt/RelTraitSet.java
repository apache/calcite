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

import org.eigenbase.util.Pair;

/**
 * RelTraitSet represents an ordered set of {@link RelTrait}s.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public final class RelTraitSet extends AbstractList<RelTrait> {
    private static final RelTrait[] EMPTY_TRAITS = new RelTrait[0];

    //~ Instance fields --------------------------------------------------------

    private final Cache cache;
    private final RelTrait[] traits;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a RelTraitSet with the given set of RelTraits.
     *
     * @param cache Trait set cache (and indirectly cluster) that this set
     *              belongs to
     * @param traits Traits
     */
    private RelTraitSet(Cache cache, RelTrait[] traits)
    {
        // NOTE: We do not copy the array. It is important that the array is not
        //   shared. However, since this constructor is private, we assume that
        //   the caller has made a copy.
        this.cache = cache;
        this.traits = traits;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates an empty trait set.
     *
     * <p>It has a new cache, which will be shared by any trait set created from
     * it. Thus each empty trait set is the start of a new ancestral line.
     */
    public static RelTraitSet createEmpty()
    {
        return new RelTraitSet(new Cache(), EMPTY_TRAITS);
    }

    /**
     * Retrieves a RelTrait from the set.
     *
     * @param index 0-based index into ordered RelTraitSet
     *
     * @return the RelTrait
     *
     * @throws ArrayIndexOutOfBoundsException if index greater than or equal to
     * {@link #size()} or less than 0.
     */
    public RelTrait getTrait(int index)
    {
        return traits[index];
    }

    public RelTrait get(int index) {
        return getTrait(index);
    }

    /**
     * Retrieves a RelTrait of the given type from the set.
     *
     * @param traitDef the type of RelTrait to retrieve
     *
     * @return the RelTrait, or null if not found
     */
    public <T extends RelTrait> T getTrait(RelTraitDef<T> traitDef)
    {
        int index = findIndex(traitDef);
        if (index >= 0) {
            //noinspection unchecked
            return (T) getTrait(index);
        }

        return null;
    }

    /**
     * Replaces an existing RelTrait in the set.
     * Returns a different trait set; does not modify this trait set.
     *
     * @param index 0-based index into ordered RelTraitSet
     * @param trait the new RelTrait
     *
     * @return the old RelTrait at the index
     */
    public RelTraitSet replace(int index, RelTrait trait)
    {
        assert traits[index].getTraitDef() == trait.getTraitDef()
            : "RelTrait has different RelTraitDef than replacement";

        RelTrait canonizedTrait = canonize(trait);
        if (traits[index] == canonizedTrait) {
            return this;
        }
        RelTrait[] newTraits = traits.clone();
        newTraits[index] = canonizedTrait;
        return cache.getOrAdd(new RelTraitSet(cache, newTraits));
    }

    /**
     * Returns a trait set consisting of the current set plus a new trait.
     *
     * <p>If the set does not contain a trait of the same {@link RelTraitDef},
     * the trait is ignored, and this trait set is returned.
     *
     * @param trait the new trait
     * @return New set
     *
     * @see #plus(RelTrait)
     */
    public RelTraitSet replace(
        RelTrait trait)
    {
        final RelTraitDef traitDef = trait.getTraitDef();
        int index = findIndex(traitDef);
        if (index < 0) {
            // Trait is not present. Ignore it.
            return this;
        }

        return replace(index, trait);
    }

    /**
     * Returns the size of the RelTraitSet.
     *
     * @return the size of the RelTraitSet.
     */
    public int size()
    {
        return traits.length;
    }

    /**
     * Converts a trait to canonical form.
     *
     * <p>After canonization, t1.equals(t2) if and only if t1 == t2.
     *
     * @param trait Trait
     *
     * @return Trait in canonical form
     */
    public <T extends RelTrait> T canonize(T trait)
    {
        if (trait == null) {
            return null;
        }

        //noinspection unchecked
        return (T) trait.getTraitDef().canonize(trait);
    }

    /**
     * Compares two RelTraitSet objects for equality.
     *
     * @param obj another RelTraitSet
     *
     * @return true if traits are equal and in the same order, false otherwise
     */
    public boolean equals(Object obj)
    {
        if (obj instanceof RelTraitSet) {
            RelTraitSet other = (RelTraitSet) obj;
            return Arrays.equals(traits, other.traits);
        }
        return false;
    }

    /**
     * Returns whether this trait set subsumes another trait set.
     *
     * <p>For that to happen, each trait subsumes the corresponding trait in the
     * other set. In particular, each trait set subsumes itself, because each
     * trait subsumes itself.</p>
     *
     * <p>Intuitively, if a relational expression is needed that has trait set
     * S, and trait set S1 subsumes S, then a relational expression R in S1
     * meets that need. For example, if we need a relational expression that has
     * trait set S = {enumerable convention, sorted on [C1 asc]}, and R
     * has {enumerable convention, sorted on [C1 asc, C2]}</p>
     *
     * @param that another RelTraitSet
     *
     * @return whether this trait set subsumes other trait set
     */
    public boolean subsumes(RelTraitSet that) {
        for (Pair<RelTrait, RelTrait> pair : Pair.zip(traits, that.traits)) {
            if (!pair.left.subsumes(pair.right)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares two RelTraitSet objects to see if they match for the purposes of
     * firing a rule. A null RelTrait within a RelTraitSet indicates a wildcard:
     * any RelTrait in the other RelTraitSet will match. If one RelTraitSet is
     * smaller than the other, comparison stops when the last RelTrait from the
     * smaller set has been examined and the remaining RelTraits in the larger
     * set are assumed to match.
     *
     * @param that another RelTraitSet
     *
     * @return true if the RelTraitSets match, false otherwise
     */
    public boolean matches(RelTraitSet that)
    {
        final int n =
            Math.min(
                this.size(),
                that.size());

        for (int i = 0; i < n; i++) {
            RelTrait thisTrait = this.traits[i];
            RelTrait thatTrait = that.traits[i];

            if ((thisTrait == null) || (thatTrait == null)) {
                continue;
            }

            if (thisTrait != thatTrait) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns whether this trait set contains a given trait.
     *
     * @param trait Sought trait
     *
     * @return Whether set contains given trait
     */
    public boolean contains(RelTrait trait)
    {
        for (RelTrait relTrait : traits) {
            if (trait == relTrait) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether this trait set contains the given trait, or whether the
     * trait is not present because its {@link RelTraitDef} is not enabled.
     * Returns false if another trait of the same {@code RelTraitDef} is
     * present.
     *
     * @param trait Trait
     * @return Whether trait is present, or is absent because disabled
     */
    public boolean containsIfApplicable(RelTrait trait) {
        // Note that '==' is sufficient, because trait should be canonized.
        final RelTrait trait1 = getTrait(trait.getTraitDef());
        return trait1 == null || trait1 == trait;
    }

    /**
     * Returns whether this trait set comprises precisely the list of given
     * traits.
     *
     * @param relTraits Traits
     * @return Whether this trait set's traits are the same as the argument
     */
    public boolean comprises(RelTrait... relTraits) {
        return Arrays.equals(traits, relTraits);
    }

    /**
     * Outputs the traits of this set as a String. Traits are output in order,
     * separated by periods.
     */
    public String toString()
    {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < traits.length; i++) {
            final RelTrait trait = traits[i];
            if (i > 0) {
                s.append('.');
            }
            if ((trait == null)
                && (traits.length == 1))
            {
                // Special format for a list containing a single null trait;
                // otherwise its string appears as "null", which is the same
                // as if the whole trait set were null, and so confusing.
                s.append("{null}");
            } else {
                s.append(trait);
            }
        }
        return s.toString();
    }

    /**
     * Finds the index of a trait of a given type in this set.
     *
     * @param traitDef Sought trait definition
     *
     * @return index of trait, or -1 if not found
     */
    private int findIndex(RelTraitDef traitDef)
    {
        for (int i = 0; i < traits.length; i++) {
            RelTrait trait = traits[i];
            if ((trait != null) && (trait.getTraitDef() == traitDef)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Returns this trait set with a given trait added or overridden. Does not
     * modify this trait set.
     *
     * @param trait Trait
     * @return Trait set with given trait
     */
    public RelTraitSet plus(RelTrait trait) {
        if (contains(trait)) {
            return this;
        }
        int i = findIndex(trait.getTraitDef());
        if (i >= 0) {
            return replace(i, trait);
        }
        final RelTrait canonizedTrait = canonize(trait);
        RelTrait[] newTraits = Arrays.copyOf(traits, traits.length + 1);
        newTraits[newTraits.length - 1] = canonizedTrait;
        return cache.getOrAdd(new RelTraitSet(cache, newTraits));
    }

    public RelTraitSet plusAll(RelTrait[] traits) {
        RelTraitSet t = this;
        for (RelTrait trait : traits) {
            t = t.plus(trait);
        }
        return t;
    }

    public RelTraitSet merge(RelTraitSet additionalTraits) {
        return plusAll(additionalTraits.traits);
    }

    private static class Cache {
        final Map<RelTraitSet, RelTraitSet> map =
            new HashMap<RelTraitSet, RelTraitSet>();

        Cache() {
        }

        RelTraitSet getOrAdd(RelTraitSet traitSet) {
            RelTraitSet traitSet1 = map.get(traitSet);
            if (traitSet1 != null) {
                return traitSet1;
            }
            map.put(traitSet, traitSet);
            return traitSet;
        }
    }
}

// End RelTraitSet.java
