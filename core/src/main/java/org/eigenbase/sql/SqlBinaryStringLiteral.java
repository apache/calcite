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

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * A binary (or hexadecimal) string literal.
 *
 * <p>The {@link #value} field is a {@link BitString} and {@link #typeName} is
 * {@link SqlTypeName#BINARY}.
 *
 * @author wael
 * @version $Id$
 */
public class SqlBinaryStringLiteral
    extends SqlAbstractStringLiteral
{
    //~ Constructors -----------------------------------------------------------

    protected SqlBinaryStringLiteral(
        BitString val,
        SqlParserPos pos)
    {
        super(val, SqlTypeName.BINARY, pos);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the underlying BitString
     */
    public BitString getBitString()
    {
        return (BitString) value;
    }

    public SqlNode clone(SqlParserPos pos)
    {
        return new SqlBinaryStringLiteral((BitString) value, pos);
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        assert value instanceof BitString;
        writer.literal("X'" + ((BitString) value).toHexString() + "'");
    }

    protected SqlAbstractStringLiteral concat1(SqlLiteral [] lits)
    {
        BitString [] args = new BitString[lits.length];
        for (int i = 0; i < lits.length; i++) {
            args[i] = ((SqlBinaryStringLiteral) lits[i]).getBitString();
        }
        return new SqlBinaryStringLiteral(
            BitString.concat(args),
            lits[0].getParserPosition());
    }
}

// End SqlBinaryStringLiteral.java
