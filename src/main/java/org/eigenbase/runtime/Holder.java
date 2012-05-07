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
 * A set of classes for holding primitive objects.
 *
 * @author jhyde
 * @version $Id$
 * @since 8 February, 2002
 */
public class Holder
{
    //~ Inner Classes ----------------------------------------------------------

    public static final class int_Holder
    {
        public int value;

        public int_Holder(int value)
        {
            this.value = value;
        }

        void setGreater(int other)
        {
            if (other > value) {
                value = other;
            }
        }

        void setLesser(int other)
        {
            if (other < value) {
                value = other;
            }
        }
    }
}

// End Holder.java
