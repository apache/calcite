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

import java.util.*;

import org.eigenbase.relopt.RelTraitDef;
import org.eigenbase.reltype.*;


/**
 * Simple implementation of {@link RelCollation}.
 *
 * @author jhyde
 * @version $Id$
 * @since March 6, 2006
 */
public class RelCollationImpl
    implements RelCollation
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * An ordering by the zeroth column.
     */
    public static final List<RelCollation> Singleton0 = createSingleton(0);

    //~ Instance fields --------------------------------------------------------

    private final List<RelFieldCollation> fieldCollations;

    //~ Constructors -----------------------------------------------------------

    public RelCollationImpl(List<RelFieldCollation> fieldCollations)
    {
        this.fieldCollations = fieldCollations;
    }

    //~ Methods ----------------------------------------------------------------

    public RelTraitDef getTraitDef() {
        return RelCollationTraitDef.INSTANCE;
    }

    public List<RelFieldCollation> getFieldCollations()
    {
        return fieldCollations;
    }

    public int hashCode()
    {
        return fieldCollations.hashCode();
    }

    public boolean equals(Object obj)
    {
        if (obj instanceof RelCollationImpl) {
            RelCollationImpl that = (RelCollationImpl) obj;
            return this.fieldCollations.equals(that.fieldCollations);
        }
        return false;
    }

    public String toString()
    {
        return fieldCollations.toString();
    }

    /**
     * Creates a list containing one collation containing one field.
     */
    public static List<RelCollation> createSingleton(int fieldIndex)
    {
        return Collections.singletonList(
            (RelCollation) new RelCollationImpl(
                Collections.singletonList(
                    new RelFieldCollation(
                        fieldIndex,
                        RelFieldCollation.Direction.Ascending,
                        RelFieldCollation.NullDirection.UNSPECIFIED))));
    }

    /**
     * Checks that a collection of collations is valid.
     *
     * @param rowType Row type of the relational expression
     * @param collationList List of collations
     * @param fail Whether to fail if invalid
     *
     * @return Whether valid
     */
    public static boolean isValid(
        RelDataType rowType,
        List<RelCollation> collationList,
        boolean fail)
    {
        final int fieldCount = rowType.getFieldCount();
        for (RelCollation collation : collationList) {
            for (
                RelFieldCollation fieldCollation
                : collation.getFieldCollations())
            {
                final int index = fieldCollation.getFieldIndex();
                if ((index < 0) || (index >= fieldCount)) {
                    assert !fail;
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean equal(
        List<RelCollation> collationList1,
        List<RelCollation> collationList2)
    {
        return collationList1.equals(collationList2);
    }
}

// End RelCollationImpl.java
