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

import java.util.List;


/**
 * Type of the cartesian product of two or more sets of records.
 *
 * <p>Its fields are those of its constituent records, but unlike a {@link
 * RelRecordType}, those fields' names are not necessarily distinct.</p>
 *
 * @author jhyde
 * @version $Id$
 */
public class RelCrossType
    extends RelDataTypeImpl
{
    //~ Instance fields --------------------------------------------------------

    public final RelDataType [] types;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a cartesian product type. This should only be called from a
     * factory method.
     *
     * @pre types != null
     * @pre types.length >= 1
     * @pre !(types[i] instanceof CrossType)
     */
    public RelCrossType(
        RelDataType [] types,
        List<RelDataTypeField> fields)
    {
        super(fields);
        this.types = types;
        assert (types != null);
        assert (types.length >= 1);
        for (RelDataType type : types) {
            assert (!(type instanceof RelCrossType));
        }
        computeDigest();
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isStruct()
    {
        return false;
    }

    public RelDataTypeField getField(String fieldName)
    {
        throw new UnsupportedOperationException(
            "not applicable to a join type");
    }

    public int getFieldOrdinal(String fieldName)
    {
        throw new UnsupportedOperationException(
            "not applicable to a join type");
    }

    public List<RelDataTypeField> getFieldList()
    {
        throw new UnsupportedOperationException(
            "not applicable to a join type");
    }

    public RelDataType [] getTypes()
    {
        return types;
    }

    protected void generateTypeString(StringBuilder sb, boolean withDetail)
    {
        sb.append("CrossType(");
        for (int i = 0; i < types.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            RelDataType type = types[i];
            if (withDetail) {
                sb.append(type.getFullTypeString());
            } else {
                sb.append(type.toString());
            }
        }
        sb.append(")");
    }
}

// End RelCrossType.java
