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
package org.eigenbase.rex;

import java.io.*;
import java.util.List;

import org.eigenbase.sql.*;

import com.google.common.collect.ImmutableList;

/**
 * Specification of the window of rows over which a {@link RexOver} windowed
 * aggregate is evaluated.
 *
 * <p>Treat it as immutable!
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 6, 2004
 */
public class RexWindow
{
    //~ Instance fields --------------------------------------------------------

    public final List<RexNode> partitionKeys;
    public final ImmutableList<RexFieldCollation> orderKeys;
    private final SqlNode lowerBound;
    private final SqlNode upperBound;
    private final boolean isRows;
    private final String digest;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a window.
     *
     * <p>If you need to create a window from outside this package, use {@link
     * RexBuilder#makeOver}.
     */
    RexWindow(
        List<RexNode> partitionKeys,
        List<RexFieldCollation> orderKeys,
        SqlNode lowerBound,
        SqlNode upperBound,
        boolean isRows)
    {
        assert partitionKeys != null;
        assert orderKeys != null;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.orderKeys = ImmutableList.copyOf(orderKeys);
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.isRows = isRows;
        this.digest = computeDigest();
        if (!isRows) {
            assert orderKeys.size() > 0 : "logical window requires sort key";
        }
    }

    //~ Methods ----------------------------------------------------------------

    public String toString()
    {
        return digest;
    }

    public int hashCode()
    {
        return digest.hashCode();
    }

    public boolean equals(Object that)
    {
        if (that instanceof RexWindow) {
            RexWindow window = (RexWindow) that;
            return digest.equals(window.digest);
        }
        return false;
    }

    private String computeDigest()
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        int clauseCount = 0;
        if (partitionKeys.size() > 0) {
            if (clauseCount++ > 0) {
                pw.print(' ');
            }
            pw.print("PARTITION BY ");
            for (int i = 0; i < partitionKeys.size(); i++) {
                if (i > 0) {
                    pw.print(", ");
                }
                RexNode partitionKey = partitionKeys.get(i);
                pw.print(partitionKey.toString());
            }
        }
        if (orderKeys.size() > 0) {
            if (clauseCount++ > 0) {
                pw.print(' ');
            }
            pw.print("ORDER BY ");
            for (int i = 0; i < orderKeys.size(); i++) {
                if (i > 0) {
                    pw.print(", ");
                }
                RexFieldCollation orderKey = orderKeys.get(i);
                pw.print(orderKey.toString());
            }
        }
        if (lowerBound == null) {
            // No ROWS or RANGE clause
        } else if (upperBound == null) {
            if (clauseCount++ > 0) {
                pw.print(' ');
            }
            if (isRows) {
                pw.print("ROWS ");
            } else {
                pw.print("RANGE ");
            }
            pw.print(lowerBound.toString());
        } else {
            if (clauseCount++ > 0) {
                pw.print(' ');
            }
            if (isRows) {
                pw.print("ROWS BETWEEN ");
            } else {
                pw.print("RANGE BETWEEN ");
            }
            pw.print(lowerBound.toString());
            pw.print(" AND ");
            pw.print(upperBound.toString());
        }
        return sw.toString();
    }

    public SqlNode getLowerBound()
    {
        return lowerBound;
    }

    public SqlNode getUpperBound()
    {
        return upperBound;
    }

    public boolean isRows()
    {
        return isRows;
    }
}

// End RexWindow.java
