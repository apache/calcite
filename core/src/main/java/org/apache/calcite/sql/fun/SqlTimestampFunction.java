package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import static org.apache.calcite.util.Static.RESOURCE;

public class SqlTimestampFunction extends SqlAbstractTimeFunction{

    private static final int MAX_TIMESTAMP_PRECISION = 6;
    public final SqlTypeName typeName;

    public SqlTimestampFunction(String name, SqlTypeName typeName) {
        super(name, typeName);
        this.typeName = typeName;
    }

    @Override
    public RelDataType inferReturnType(
            SqlOperatorBinding opBinding) {
        int precision = 0;
        if (opBinding.getOperandCount() == 1) {
            RelDataType type = opBinding.getOperandType(0);
            if (SqlTypeUtil.isNumeric(type)) {
                precision = opBinding.getOperandLiteralValue(0, Integer.class);
            }
        }
        assert precision >= 0;
        if (precision > MAX_TIMESTAMP_PRECISION) {
            throw opBinding.newError(
                    RESOURCE.argumentMustBeValidPrecision(
                            opBinding.getOperator().getName(), 0,
                            MAX_TIMESTAMP_PRECISION));
        }
        return opBinding.getTypeFactory().createSqlType(typeName, precision);
    }
}
