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
package org.eigenbase.sql.validate;

import java.util.List;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.*;

import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.TableFunction;

/**
* User-defined function.
 *
 * <p>Created by the validator, after resolving a function call to a function
 * (perhaps a table function) defined in an Optiq schema.</p>
*/
public class SqlUserDefinedFunction extends SqlFunction {
  public final TableFunction tableFunction;
  public final Table table;

  public SqlUserDefinedFunction(String name, RelDataType returnType,
      List<RelDataType> argTypes, List<SqlTypeFamily> typeFamilies,
      TableFunction tableFunction, Table table) {
    super(name, null, SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(returnType),
        new ExplicitOperandTypeInference(argTypes),
        OperandTypes.family(typeFamilies),
        null, null);
    this.tableFunction = tableFunction;
    this.table = table;
  }
}

// End SqlUserDefinedFunction.java
