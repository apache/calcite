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
package org.eigenbase.sql.fun;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * Definition of the <code>MIN</code> and <code>MAX</code> aggregate functions,
 * returning the returns the smallest/largest of the values which go into it.
 *
 * <p>There are 3 forms:
 *
 * <dl>
 * <dt>sum(<em>primitive type</em>)</dt>
 * <dd>values are compared using &lt;</dd>
 *
 * <dt>sum({@link java.lang.Comparable})</dt>
 * <dd>values are compared using {@link java.lang.Comparable#compareTo}</dd>
 *
 * <dt>sum({@link java.util.Comparator}, {@link java.lang.Object})</dt>
 * <dd>the {@link java.util.Comparator#compare} method of the comparator is used
 * to compare pairs of objects. The comparator is a startup argument, and must
 * therefore be constant for the duration of the aggregation.</dd>
 * </dl>
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlMinMaxAggFunction
    extends SqlAggFunction
{
    //~ Static fields/initializers ---------------------------------------------

    public static final int MINMAX_INVALID = -1;
    public static final int MINMAX_PRIMITIVE = 0;
    public static final int MINMAX_COMPARABLE = 1;
    public static final int MINMAX_COMPARATOR = 2;

    //~ Instance fields --------------------------------------------------------

    public final RelDataType [] argTypes;
    private final boolean isMin;
    private final int kind;

    //~ Constructors -----------------------------------------------------------

    public SqlMinMaxAggFunction(
        RelDataType [] argTypes,
        boolean isMin,
        int kind)
    {
        super(
            isMin ? "MIN" : "MAX",
            SqlKind.OTHER_FUNCTION,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcComparableOrdered,
            SqlFunctionCategory.System);
        this.argTypes = argTypes;
        this.isMin = isMin;
        this.kind = kind;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isMin()
    {
        return isMin;
    }

    public int getMinMaxKind()
    {
        return kind;
    }

    public RelDataType [] getParameterTypes(RelDataTypeFactory typeFactory)
    {
        switch (kind) {
        case MINMAX_PRIMITIVE:
        case MINMAX_COMPARABLE:
            return argTypes;
        case MINMAX_COMPARATOR:
            return new RelDataType[] { argTypes[1] };
        default:
            throw Util.newInternal("bad kind: " + kind);
        }
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory)
    {
        switch (kind) {
        case MINMAX_PRIMITIVE:
        case MINMAX_COMPARABLE:
            return argTypes[0];
        case MINMAX_COMPARATOR:
            return argTypes[1];
        default:
            throw Util.newInternal("bad kind: " + kind);
        }
    }

}

// End SqlMinMaxAggFunction.java
