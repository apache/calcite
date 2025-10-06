package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Optionality;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Aggregate function that performs a boolean OR across all input rows.
 * Returns TRUE if any input is TRUE, otherwise FALSE.
 */
public class SqlBoolOrAggFunction extends SqlAggFunction {
  /**
   * Creates a SqlBoolOrAggFunction.
   */
  public SqlBoolOrAggFunction(String name,
      SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory category) {
    super(name,
        null,
        kind,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker,
        category,
        false,
        false,
        Optionality.FORBIDDEN);
  }
}
