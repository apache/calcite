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
package org.eigenbase.oj.stmt;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;

import net.hydromatic.optiq.prepare.Prepare;

/**
 * Refinement of {@link net.hydromatic.optiq.prepare.Prepare.PreparedExplain} for OJ.
 */
public class PreparedExplanation
    extends Prepare.PreparedExplain
{
    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    public PreparedExplanation(
        RelDataType rowType,
        RelNode rel,
        boolean asXml,
        SqlExplainLevel detailLevel)
    {
        super(rowType, rel, asXml, detailLevel);
    }

    //~ Methods ----------------------------------------------------------------

    public Object execute()
    {
        final String explanation = getCode();
        return executeStatic(explanation);
    }

    public static ResultSet executeStatic(final String explanation)
    {
        final LineNumberReader lineReader =
            new LineNumberReader(new StringReader(explanation));
        Iterator iter =
            new Iterator() {
                private String line;

                public boolean hasNext()
                {
                    if (line != null) {
                        return true;
                    }
                    try {
                        line = lineReader.readLine();
                    } catch (IOException ex) {
                        throw Util.newInternal(ex);
                    }
                    return (line != null);
                }

                public Object next()
                {
                    if (!hasNext()) {
                        return null;
                    }
                    String nextLine = line;
                    line = null;
                    return nextLine;
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }
            };
        return IteratorResultSet.create(
            iter,
            new IteratorResultSet.SingletonColumnGetter());
    }
}

// End PreparedExplanation.java
