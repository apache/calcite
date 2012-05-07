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

/**
 * A Trigger is a callback which allows a subscriber to be notified when a
 * property value changes.
 *
 * <p>If the user wishes to be able to remove a Trigger at some time after it
 * has been added, then either 1) the user has to keep the instance of the
 * Trigger that was added and use it when calling the remove method or 2) the
 * Trigger must implement the equals method and the Trigger used during the call
 * to the remove method must be equal to the original Trigger added.
 *
 * <p>Each non-persistent Trigger is wrapped in a {@link
 * java.lang.ref.WeakReference}, so that is can be garbage-collected. But this
 * means that the user had better keep a reference to the Trigger, otherwise it
 * will be removed (garbage collected) without the user knowing.
 *
 * <p>Persistent Triggers (those that refer to objects in their {@link #execute}
 * method that will never be garbage-collected) are not wrapped in a
 * WeakReference.
 *
 * <p>What does all this mean, well, objects that might be garbage collected
 * that create a Trigger must keep a reference to the Trigger (otherwise the
 * Trigger will be garbage-collected out from under the object). A common usage
 * pattern is to implement the Trigger interface using an anonymous class -
 * anonymous classes are non-static classes when created in an instance object.
 * But, remember, the anonymous class instance holds a reference to the outer
 * instance object but not the other way around; the instance object does not
 * have an implicit reference to the anonymous class instance - the reference
 * must be explicit. And, if the anonymous Trigger is created with the
 * isPersistent method returning true, then, surprise, the outer instance object
 * will never be garbage collected!!!
 *
 * <p>Note that it is up to the creator of a set of triggers to make sure that
 * they are ordered correctly. This is done by either having order independent
 * Triggers (they all have the same phase) or by assigning phases to the
 * Triggers where primaries execute before secondary which execute before
 * tertiary.
 *
 * <p>If a finer level of execution order granularity is needed, then the
 * implementation should be changed so that the {@link #phase} method returns
 * just some integer and it's up to the users to coordinate their values.
 */
public interface Trigger
{
    //~ Instance fields --------------------------------------------------------

    int PRIMARY_PHASE = 1;
    int SECONDARY_PHASE = 2;
    int TERTIARY_PHASE = 3;

    //~ Methods ----------------------------------------------------------------

    /**
     * If a Trigger is associated with a class or singleton, then it should
     * return true because its associated object is not subject to garbage
     * collection. On the other hand, if a Trigger is associated with an object
     * which will be garbage collected, then this method must return false so
     * that the Trigger will be wrapped in a WeakReference and thus can itself
     * be garbage collected.
     *
     * @return whether trigger is persistent
     */
    boolean isPersistent();

    /**
     * Which phase does this Trigger belong to.
     *
     * @return phase trigger belongs to
     */
    int phase();

    /**
     * Executes the trigger, passing in the key of the property whose change
     * triggered the execution.
     *
     * @param property Property being changed
     * @param value New value of property
     */
    void execute(Property property, String value)
        throws VetoRT;

    //~ Inner Classes ----------------------------------------------------------

    public static class VetoRT
        extends RuntimeException
    {
        public VetoRT(String msg)
        {
            super(msg);
        }

        public VetoRT(Exception ex)
        {
            super(ex);
        }
    }
}

// End Trigger.java
