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
package org.eigenbase.reltype;

/**
 * RelDataTypeField represents the definition of a field in a structured {@link
 * RelDataType}.
 *
 * @author jhyde
 * @version $Id$
 * @since May 29, 2003
 */
public interface RelDataTypeField
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Gets the name of this field, which is unique within its containing type.
     *
     * @return field name
     */
    public String getName();

    /**
     * Gets the ordinal of this field within its containing type.
     *
     * @return 0-based ordinal
     */
    public int getIndex();

    /**
     * Gets the type of this field.
     *
     * @return field type
     */
    public RelDataType getType();
}

// End RelDataTypeField.java
