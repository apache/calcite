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

/**
 * A base class for implementations of {@link TupleIter}, with default
 * implementations of some of its methods.
 *
 * @author Marc Berkowitz
 * @version $Id$
 */
public abstract class AbstractTupleIter
    implements TupleIter
{
    //~ Methods ----------------------------------------------------------------

    public boolean setTimeout(long timeout, boolean asUnderflow)
    {
        return false; // by default, don't provide a timeout
    }

    public boolean addListener(MoreDataListener c)
    {
        return false;
    }

    public void restart()
    {
    }

    public StringBuilder printStatus(StringBuilder b)
    {
        return b;
    }
}

// End AbstractTupleIter.java
