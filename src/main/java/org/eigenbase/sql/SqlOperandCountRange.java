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
package org.eigenbase.sql;

import java.util.*;

import org.eigenbase.util.*;


/**
 * A class that describes how many operands an operator can take.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlOperandCountRange
{
    //~ Static fields/initializers ---------------------------------------------

    // common usage instances
    public static final SqlOperandCountRange Variadic =
        new SqlOperandCountRange();
    public static final SqlOperandCountRange Zero = new SqlOperandCountRange(0);
    public static final SqlOperandCountRange ZeroOrOne =
        new SqlOperandCountRange(0, 1);
    public static final SqlOperandCountRange One = new SqlOperandCountRange(1);
    public static final SqlOperandCountRange OneOrTwo =
        new SqlOperandCountRange(1, 2);
    public static final SqlOperandCountRange Two = new SqlOperandCountRange(2);
    public static final SqlOperandCountRange TwoOrThree =
        new SqlOperandCountRange(2, 3);
    public static final SqlOperandCountRange Three =
        new SqlOperandCountRange(3);
    public static final SqlOperandCountRange ThreeOrFour =
        new SqlOperandCountRange(3, 4);
    public static final SqlOperandCountRange Four = new SqlOperandCountRange(4);

    //~ Instance fields --------------------------------------------------------

    private List<Integer> possibleList;
    private boolean isVariadic;

    //~ Constructors -----------------------------------------------------------

    /**
     * This constructor should only be called internally from this class and
     * only when creating a variadic count descriptor
     */
    private SqlOperandCountRange()
    {
        possibleList = null;
        isVariadic = true;
    }

    private SqlOperandCountRange(Integer [] possibleCounts)
    {
        this(Arrays.asList(possibleCounts));
    }

    public SqlOperandCountRange(int count)
    {
        this(new Integer[] { count });
    }

    public SqlOperandCountRange(List<Integer> list)
    {
        possibleList = Collections.unmodifiableList(list);
        isVariadic = false;
    }

    public SqlOperandCountRange(
        int count1,
        int count2)
    {
        this(new Integer[] { count1, count2 });
    }

    public SqlOperandCountRange(
        int count1,
        int count2,
        int count3)
    {
        this(new Integer[] { count1, count2, count3 });
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns a list of allowed operand counts for a non-variadic operator.
     *
     * @return unmodifiable list of Integer
     *
     * @pre !isVariadic()
     */
    public List<Integer> getAllowedList()
    {
        Util.pre(!isVariadic, "!isVariadic");
        return possibleList;
    }

    /**
     * @return true if any number of operands is allowed
     */
    public boolean isVariadic()
    {
        return isVariadic;
    }
}

// End SqlOperandCountRange.java
