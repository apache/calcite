package org.apache.calcite.sql;

import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

/**
 * Base class for table-value function windowing operator (TUMBLE, HOP and SESSION).
 * With this class, we are able to re-write argumentMustBeScalar(int) because the first
 * parameter of table-value function windowing is TABLE parameter.
 */
public class SqlTableValueFunctionWindowingOperator extends SqlFunction {

  public SqlTableValueFunctionWindowingOperator(String name, SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory category) {
    super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
  }


  /**
   * The first parameter of table-value function windowing is a TABLE parameter,
   * which is not scalar.
   */
  public boolean argumentMustBeScalar(int ordinal) {
    if (ordinal == 0) {
      return false;
    }
    return true;
  }
}
