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

/**
 * RelTrait represents the manifestation of a relational expression trait within
 * a trait definition. For example, a {@code CallingConvention.JAVA} is a trait
 * of the {@link ConventionTraitDef} trait definition.
 *
 * <h3><a name="EqualsHashCodeNote">Note about equals() and hashCode()</a></h3>
 *
 * <p>If all instances of RelTrait for a particular RelTraitDef are defined in
 * an {@code enum} and no new RelTraits can be introduced at runtime, you need
 * not override {@link #hashCode()} and {@link #equals(Object)}. If, however,
 * new RelTrait instances are generated at runtime (e.g. based on state external
 * to the planner), you must implement {@link #hashCode()} and {@link
 * #equals(Object)} for proper {@link RelTraitDef#canonize canonization} of your
 * RelTrait objects.</p>
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public interface RelTrait
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the RelTraitDef that defines this RelTrait.
     *
     * @return the RelTraitDef that defines this RelTrait
     */
    abstract RelTraitDef getTraitDef();

    /**
     * See <a href="#EqualsHashCodeNote">note about equals() and hashCode()</a>.
     */
    abstract int hashCode();

    /**
     * See <a href="#EqualsHashCodeNote">note about equals() and hashCode()</a>.
     */
    abstract boolean equals(Object o);

    /**
     * Returns a succinct name for this trait. The planner may use this String
     * to describe the trait.
     */
    abstract String toString();
}

// End RelTrait.java
