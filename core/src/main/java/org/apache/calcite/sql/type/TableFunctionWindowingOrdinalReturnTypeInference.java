package org.apache.calcite.sql.type;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlOperatorBinding;

/**
 * Type-inference strategy whereby the result type of a table function call is a ROW,
 * which is combined from the TABLE parameter's schema (the parameter is specified by ordinal)
 * and two additional fields:
 *  1. wstart. TIMESTAMP type to indicate a window's start.
 *  2. wend. TIMESTAMP type to indicate a window's end.
 */
public class TableFunctionWindowingOrdinalReturnTypeInference implements SqlReturnTypeInference {
  //~ Instance fields --------------------------------------------------------

  private final int ordinal;

  //~ Constructors -----------------------------------------------------------

  public TableFunctionWindowingOrdinalReturnTypeInference(int ordinal) {
    this.ordinal = ordinal;
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    RelDataType inputRowType = opBinding.getOperandType(ordinal);
    List<RelDataTypeField> newFields = new ArrayList<>(inputRowType.getFieldList());
    RelDataType timestampType = opBinding.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP);

    RelDataTypeField windowStartField = new RelDataTypeFieldImpl("wstart", newFields.size(), timestampType);
    newFields.add(windowStartField);
    RelDataTypeField windowEndField = new RelDataTypeFieldImpl("wend", newFields.size(), timestampType);
    newFields.add(windowEndField);

    return new RelRecordType(inputRowType.getStructKind(), newFields);
  }
}
