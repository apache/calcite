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
package org.eigenbase.rel.metadata;

/**
 * RelColumnMapping records a mapping from an input column of a RelNode to one
 * of its output columns.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RelColumnMapping
{
    //~ Instance fields --------------------------------------------------------

    /**
     * 0-based ordinal of mapped output column.
     */
    public int iOutputColumn;

    /**
     * 0-based ordinal of mapped input rel.
     */
    public int iInputRel;

    /**
     * 0-based ordinal of mapped column within input rel.
     */
    public int iInputColumn;

    /**
     * Whether the column mapping transforms the input.
     */
    public boolean isDerived;
}

// End RelColumnMapping.java
