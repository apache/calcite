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
package org.eigenbase.sql;

import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.util.*;


/**
 * Defines the keywords which can occur immediately after the "SELECT" keyword.
 */
public class SqlSelectKeyword
    extends EnumeratedValues.BasicValue
    implements SqlLiteral.SqlSymbol
{
    //~ Static fields/initializers ---------------------------------------------

    public static final int Distinct_ordinal = 0;
    public static final SqlSelectKeyword Distinct =
        new SqlSelectKeyword("Distinct", Distinct_ordinal);
    public static final int All_ordinal = 1;
    public static final SqlSelectKeyword All =
        new SqlSelectKeyword("All", All_ordinal);
    public static final EnumeratedValues enumeration =
        new EnumeratedValues(new SqlSelectKeyword[] { Distinct, All });

    //~ Constructors -----------------------------------------------------------

    protected SqlSelectKeyword(String name, int ordinal)
    {
        super(name, ordinal, null);
    }

    //~ Methods ----------------------------------------------------------------

    public String name()
    {
        return getName();
    }

    public int ordinal()
    {
        return getOrdinal();
    }

    /** Creates a parse-tree node representing an occurrence of this keyword
     * at a particular position in the parsed text. */
    public SqlLiteral symbol(SqlParserPos pos) {
        return SqlLiteral.createSymbol(this, pos);
    }
}

// End SqlSelectKeyword.java
