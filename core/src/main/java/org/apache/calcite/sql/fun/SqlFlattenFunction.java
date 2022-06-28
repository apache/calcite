/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.Symbolizable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SameOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;

/**
 * Definition of the "TRIM" builtin SQL function.
 */
public class SqlFlattenFunction extends SqlFunction {
  // ~ Constructors -----------------------------------------------------------
  public SqlFlattenFunction() {
    super("FLATTEN",
        SqlKind.FLATTEN, null, null, null,
        SqlFunctionCategory.SYSTEM);
  }

  /**
   * Defines the enumerated values
   * 
   */
  public enum FlattenType implements Symbolizable {
    OUTER("OUTER"), RECURSIVE("RECURSIVE"), MODE("MODE"), PATH("PATH"), INPUT("INPUT");

    private final String type;

    FlattenType(String type) {
      this.type = type;
    }

    public String getType() {
      return type;
    }
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    for (int i = 0; i < call.operandCount(); i += 2) {
      if (i != 0) {
        writer.sep(",");
      }
      call.operand(i).unparse(writer, leftPrec, rightPrec);
      writer.sep("=>");
      call.operand(i + 1).unparse(writer, leftPrec, rightPrec);
    }
    writer.endFunCall(frame);
  }

  @Override
  public String getSignatureTemplate(final int operandsCount) {
    if (operandsCount % 2 == 0) {
      throw new AssertionError();
    }
    StringBuilder template = new StringBuilder();
    template.append("${0}(");
    for (int i = 1; i < operandsCount; i += 2) {
      if (i != 1) {
        template.append(", ");
      }
      template.append("${");
      template.append(i);
      template.append("} => {");
      template.append(i + 1);
      template.append("}");
    }
    template.append(")");
    return template.toString();
  }
}
