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

/**
 * <code>HashableArray</code> provides a <code>Object[]</code> with a {@link
 * #hashCode} and an {@link #equals} function, so it can be used as a key in a
 * {@link java.util.Hashtable}.
 */
public class HashableArray
{
    //~ Instance fields --------------------------------------------------------

    Object [] a;

    //~ Constructors -----------------------------------------------------------

    public HashableArray(Object [] a)
    {
        this.a = a;
    }

    //~ Methods ----------------------------------------------------------------

    // override Object
    public int hashCode()
    {
        return arrayHashCode(a);
    }

    // override Object
    public boolean equals(Object o)
    {
        return (o instanceof HashableArray)
            && arraysAreEqual(this.a, ((HashableArray) o).a);
    }

    public static int arrayHashCode(Object [] a)
    {
        // hash algorithm borrowed from java.lang.String
        int h = 0;
        for (int i = 0; i < a.length; i++) {
            h = (31 * h) + a[i].hashCode();
        }
        return h;
    }

    /**
     * Returns whether two arrays are equal (deep compare).
     */
    public static boolean arraysAreEqual(Object [] a1, Object [] a2)
    {
        if (a1.length != a2.length) {
            return false;
        }
        for (int i = 0; i < a1.length; i++) {
            if (!a1[i].equals(a2[i])) {
                return false;
            }
        }
        return true;
    }
}

// End HashableArray.java
