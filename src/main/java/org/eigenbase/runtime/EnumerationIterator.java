/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.runtime;

import java.util.*;


/**
 * <code>EnumerationIterator</code> is an adapter which converts an {@link
 * Enumeration} into an {@link Iterator}.
 *
 * @author jhyde
 * @version $Id$
 * @since 16 December, 2001
 */
public class EnumerationIterator
    implements Iterator
{
    //~ Instance fields --------------------------------------------------------

    Enumeration enumeration;

    //~ Constructors -----------------------------------------------------------

    public EnumerationIterator(Enumeration enumeration)
    {
        this.enumeration = enumeration;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean hasNext()
    {
        return enumeration.hasMoreElements();
    }

    public Object next()
    {
        return enumeration.nextElement();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}

// End EnumerationIterator.java
