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

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * SqlColumnListConstructor defines the non-standard constructor used to pass a
 * COLUMN_LIST parameter to a UDX.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class SqlColumnListConstructor
    extends SqlSpecialOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlColumnListConstructor()
    {
        super(
            "COLUMN_LIST",
            SqlKind.COLUMN_LIST,
            MaxPrec,
            false,
            SqlTypeStrategies.rtiColumnList,
            null,
            SqlTypeStrategies.otcAny);
    }

    //~ Methods ----------------------------------------------------------------

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        writer.keyword("ROW");
        final SqlWriter.Frame frame = writer.startList("(", ")");
        for (int i = 0; i < operands.length; i++) {
            writer.sep(",");
            operands[0].unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
    }
}

// End SqlColumnListConstructor.java
