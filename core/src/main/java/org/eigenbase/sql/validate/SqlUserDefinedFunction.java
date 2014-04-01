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

import java.util.ArrayList;
import java.util.List;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlUtil;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Pair;

import net.hydromatic.optiq.*;

/**
* User-defined function.
 *
 * <p>Created by the validator, after resolving a function call to a function
 * defined in an Optiq schema.</p>
*/
public class SqlUserDefinedFunction extends SqlFunction {
  public final Function function;

  public SqlUserDefinedFunction(String name, RelDataType returnType,
      List<RelDataType> argTypes, List<SqlTypeFamily> typeFamilies,
      Function function) {
    super(name, null, SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(returnType),
        new ExplicitOperandTypeInference(argTypes),
        OperandTypes.family(typeFamilies),
        null, null);
    this.function = function;
  }

  /** Returns the table in this UDF, or null if there is no table. */
  public Table getTable(RelDataTypeFactory typeFactory,
      List<SqlNode> operandList) {
    if (!(function instanceof TableMacro)) {
      return null;
    }
    final TableMacro tableMacro = (TableMacro) function;
    // Construct a list of arguments, if they are all constants.
    final List<Object> arguments = new ArrayList<Object>();
    for (Pair<FunctionParameter, SqlNode> pair
        : Pair.zip(tableMacro.getParameters(), operandList)) {
      if (SqlUtil.isNullLiteral(pair.right, true)) {
        arguments.add(null);
      } else if (SqlUtil.isLiteral(pair.right)) {
        final Object o = ((SqlLiteral) pair.right).getValue();
        final Object o2 = coerce(o, pair.left.getType(typeFactory));
        if (o2 == null) {
          return null; // not suitable type
        }
        arguments.add(o2);
      } else {
        // Not all operands are constants. Cannot expand table macro.
        return null;
      }
    }
    return tableMacro.apply(arguments);
  }

  private Object coerce(Object o, RelDataType type) {
    if (!(type instanceof RelDataTypeFactoryImpl.JavaType)) {
      return null;
    }
    final RelDataTypeFactoryImpl.JavaType javaType =
        (RelDataTypeFactoryImpl.JavaType) type;
    final Class clazz = javaType.getJavaClass();
    if (clazz.isInstance(o)) {
      return o;
    }
    if (clazz == String.class && o instanceof NlsString) {
      return ((NlsString) o).getValue();
    }
    return null;
  }
}

// End SqlUserDefinedFunction.java
