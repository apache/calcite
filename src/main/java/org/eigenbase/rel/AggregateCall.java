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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * Call to an aggregation function within an {@link AggregateRel}.
 *
 * @author jhyde
 * @version $Id$
 */
public class AggregateCall
{
    //~ Instance fields --------------------------------------------------------

    private final Aggregation aggregation;

    /**
     * TODO jvs 24-Apr-2006: make this array and its contents immutable
     *
     * @deprecated todo: change all public uses to use {@link #getArgList}, then
     * make private
     */
    public final int [] args;

    private final boolean distinct;
    private final RelDataType type;
    private final String name;
    private final List<Integer> argList;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AggregateCall.
     *
     * @param aggregation Aggregation
     * @param distinct Whether distinct
     * @param argList List of ordinals of arguments
     * @param type Result type
     * @param name Name
     */
    public AggregateCall(
        Aggregation aggregation,
        boolean distinct,
        List<Integer> argList,
        RelDataType type,
        String name)
    {
        this.type = type;
        this.name = name;
        assert aggregation != null;
        assert argList != null;
        assert type != null;
        this.aggregation = aggregation;

        // Conversion from list to array to list is intentional. We want the
        // argList member to point to the same data as the args member.
        this.args = IntList.toArray(argList);
        this.argList = Collections.unmodifiableList(IntList.asList(args));
        this.distinct = distinct;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns whether this AggregateCall is distinct, as in <code>
     * COUNT(DISTINCT empno)</code>.
     *
     * @return whether distinct
     */
    public final boolean isDistinct()
    {
        return distinct;
    }

    /**
     * Returns the Aggregation.
     *
     * @return aggregation
     */
    public final Aggregation getAggregation()
    {
        return aggregation;
    }

    /**
     * Returns the ordinals of the arguments to this call.
     *
     * <p>The list is immutable.
     *
     * @return list of argument ordinals
     */
    public final List<Integer> getArgList()
    {
        return argList;
    }

    /**
     * Returns the result type.
     *
     * @return result type
     */
    public final RelDataType getType()
    {
        return type;
    }

    /**
     * Returns the name.
     *
     * @return name
     */
    public String getName()
    {
        return name;
    }

    public String toString()
    {
        StringBuilder buf = new StringBuilder(aggregation.getName());
        buf.append("(");
        if (distinct) {
            buf.append((argList.size() == 0) ? "DISTINCT" : "DISTINCT ");
        }
        int i = -1;
        for (Integer arg : argList) {
            if (++i > 0) {
                buf.append(", ");
            }
            buf.append("$");
            buf.append(arg);
        }
        buf.append(")");
        return buf.toString();
    }

    // override Object
    public boolean equals(Object o)
    {
        if (!(o instanceof AggregateCall)) {
            return false;
        }
        AggregateCall other = (AggregateCall) o;
        return aggregation.equals(other.aggregation)
            && (distinct == other.distinct)
            && argList.equals(other.argList);
    }

    // override Object
    public int hashCode()
    {
        return aggregation.hashCode() + argList.hashCode();
    }

    /**
     * Creates a binding of this call in the context of an {@link AggregateRel},
     * which can then be used to infer the return type.
     */
    public AggregateRelBase.AggCallBinding createBinding(
        AggregateRelBase aggregateRelBase)
    {
        return new AggregateRelBase.AggCallBinding(
            aggregateRelBase.getCluster().getTypeFactory(),
            (SqlAggFunction) aggregation,
            aggregateRelBase,
            argList);
    }
}

// End AggregateCall.java
