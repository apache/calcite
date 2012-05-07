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

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * A character string literal.
 *
 * <p>Its {@link #value} field is an {@link NlsString} and {@link #typeName} is
 * {@link SqlTypeName#CHAR}.
 *
 * @author wael
 * @version $Id$
 */
public class SqlCharStringLiteral
    extends SqlAbstractStringLiteral
{
    //~ Constructors -----------------------------------------------------------

    protected SqlCharStringLiteral(
        NlsString val,
        SqlParserPos pos)
    {
        super(val, SqlTypeName.CHAR, pos);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the underlying NlsString
     */
    public NlsString getNlsString()
    {
        return (NlsString) value;
    }

    /**
     * @return the collation
     */
    public SqlCollation getCollation()
    {
        return getNlsString().getCollation();
    }

    public SqlNode clone(SqlParserPos pos)
    {
        return new SqlCharStringLiteral((NlsString) value, pos);
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        if (false) {
            Util.discard(Bug.Frg78Fixed);
            String stringValue = ((NlsString) value).getValue();
            writer.literal(
                writer.getDialect().quoteStringLiteral(stringValue));
        }
        assert value instanceof NlsString;
        writer.literal(value.toString());
    }

    protected SqlAbstractStringLiteral concat1(SqlLiteral [] lits)
    {
        NlsString [] args = new NlsString[lits.length];
        for (int i = 0; i < lits.length; i++) {
            args[i] = ((SqlCharStringLiteral) lits[i]).getNlsString();
        }
        return new SqlCharStringLiteral(
            NlsString.concat(args),
            lits[0].getParserPosition());
    }
}

// End SqlCharStringLiteral.java
