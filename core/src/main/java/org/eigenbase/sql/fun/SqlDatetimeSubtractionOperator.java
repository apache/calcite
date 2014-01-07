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
import org.eigenbase.sql.validate.SqlMonotonicity;
import org.eigenbase.sql.validate.SqlValidatorScope;

/**
 * A special operator for the subtraction of two DATETIMEs. The format of
 * DATETIME substraction is:<br>
 * <code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")" <interval
 * qualifier></code>. This operator is special since it needs to hold the
 * additional interval qualifier specification.
 */
public class SqlDatetimeSubtractionOperator
    extends SqlSpecialOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlDatetimeSubtractionOperator()
    {
        super(
            "-",
            SqlKind.MINUS,
            40,
            true,
            SqlTypeStrategies.rtiNullableThirdArgType,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcMinusDateOperator);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startList("(", ")");
        operands[0].unparse(writer, leftPrec, rightPrec);
        writer.sep("-");
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.endList(frame);
        operands[2].unparse(writer, leftPrec, rightPrec);
    }

    public SqlMonotonicity getMonotonicity(
        SqlCall call,
        SqlValidatorScope scope)
    {
        return SqlStdOperatorTable.minusOperator.getMonotonicity(call, scope);
    }
}

// End SqlDatetimeSubtractionOperator.java
