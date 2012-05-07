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
package org.eigenbase.reltype;

/**
 * RelDataTypePrecedenceList defines a type precedence list for a particular
 * type.
 *
 * @author John V. Sichi
 * @version $Id$
 * @sql.99 Part 2 Section 9.5
 */
public interface RelDataTypePrecedenceList
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Determines whether a type appears in this precedence list.
     *
     * @param type type to check
     *
     * @return true iff this list contains type
     */
    public boolean containsType(RelDataType type);

    /**
     * Compares the precedence of two types.
     *
     * @param type1 first type to compare
     * @param type2 second type to compare
     *
     * @return positive if type1 has higher precedence; negative if type2 has
     * higher precedence; 0 if types have equal precedence
     *
     * @pre containsType(type1) && containsType(type2)
     */
    public int compareTypePrecedence(RelDataType type1, RelDataType type2);
}

// End RelDataTypePrecedenceList.java
