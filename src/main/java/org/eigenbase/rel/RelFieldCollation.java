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
package org.eigenbase.rel;

/**
 * RelFieldCollation defines the ordering for one field of a RelNode whose
 * output is to be sorted.
 *
 * <p>TODO: collation sequence (including ASC/DESC)
 */
public class RelFieldCollation
{
    //~ Static fields/initializers ---------------------------------------------

    public static final RelFieldCollation [] emptyCollationArray =
        new RelFieldCollation[0];

    //~ Enums ------------------------------------------------------------------

    /**
     * Direction that a field is ordered in.
     */
    public static enum Direction
    {
        /**
         * Ascending direction: A value is always followed by a greater or equal
         * value.
         */
        Ascending,

        /**
         * Strictly ascending direction: A value is always followed by a greater
         * value.
         */
        StrictlyAscending,

        /**
         * Descending direction: A value is always followed by a lesser or equal
         * value.
         */
        Descending,

        /**
         * Strictly descending direction: A value is always followed by a lesser
         * value.
         */
        StrictlyDescending,

        /**
         * Clustered direction: Values occur in no particular order, and the
         * same value may occur in contiguous groups, but never occurs after
         * that. This sort order tends to occur when values are ordered
         * according to a hash-key.
         */
        Clustered,
    }

    //~ Instance fields --------------------------------------------------------

    /**
     * 0-based index of field being sorted.
     */
    private final int fieldIndex;

    /**
     * Direction of sorting.
     */
    private final Direction direction;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an ascending field collation.
     */
    public RelFieldCollation(int fieldIndex)
    {
        this(fieldIndex, Direction.Ascending);
    }

    /**
     * Creates a field collation.
     */
    public RelFieldCollation(int fieldIndex, Direction direction)
    {
        this.fieldIndex = fieldIndex;
        this.direction = direction;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Object
    public boolean equals(Object obj)
    {
        if (!(obj instanceof RelFieldCollation)) {
            return false;
        }
        RelFieldCollation other = (RelFieldCollation) obj;
        return (fieldIndex == other.fieldIndex)
            && (direction == other.direction);
    }

    // implement Object
    public int hashCode()
    {
        return (this.direction.ordinal() << 4) | this.fieldIndex;
    }

    public int getFieldIndex()
    {
        return fieldIndex;
    }

    public RelFieldCollation.Direction getDirection()
    {
        return direction;
    }

    public String toString()
    {
        return fieldIndex + " " + direction;
    }
}

// End RelFieldCollation.java
