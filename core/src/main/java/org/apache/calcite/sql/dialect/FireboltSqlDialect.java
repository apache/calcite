import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.avatica.util.Quoting;

import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.rel.type.RelDataTypeSystem;

import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlString;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import com.sisense.translation.calcite.adapter.dialects.*;
import java.util.logging.Logger;
import static org.apache.calcite.util.DateTimeStringUtils.getDateFormatter;

import com.sisense.common.infra.Utils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;
import org.apache.calcite.sql.fun.SqlDatePartFunction;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.*;

@Slf4j
public class FireboltSqlDialect extends SisenseDefaultDialect {

  private static final Logger logger = Logger.getLogger(FireboltSqlDialect.class.getName());
  // Create a file object
  File f = new File("program.txt");

  // Get the absolute path of file f
  String absolute = f.getAbsolutePath();

  public FireboltSqlDialect() {
    super(EMPTY_CONTEXT.withDatabaseProduct(DatabaseProduct.POSTGRESQL).withIdentifierQuoteString("\"")
            .withUnquotedCasing(Casing.TO_UPPER));

    System.out.println("=========================Inside Constructor=============================");
    logger.info("=========================Inside Quoting override method=============================");
    // Display the file path of the file object
    // and also the file path of absolute file
    System.out.println("Original  path: "
            + f.getPath());
    System.out.println("Absolute  path: "
            + absolute);
  }

  public static final SqlDialect DEFAULT = new FireboltSqlDialect(EMPTY_CONTEXT);
//  public static final SisenseDefaultDialect SISENSE_DEFAULT_DIALECT = new SisenseDefaultDialect(DEFAULT_CONTEXT);

  /** Creates a FireboltSqlDialect. */
  public FireboltSqlDialect(SqlDialect.Context context) {
    super(context);
  }

//  public static void writeFile(SqlWriter writer) {
//    SisenseDialectUtils.getSubquery(writer, absolute);
//  }

  private static final int IS_ROWS_OPERAND_IDX = 4;
  private static final int UPPER_BOUND_OPERAND_IDX = 6;

  @Override
  public boolean supportsCharSet() {
    return false;
  }

//  @Override
//  public void unparseDateTimeLiteral(SqlWriter writer,
//                                               SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
//    if (literal instanceof SqlTimestampLiteral) {
//      writer.literal("TO_TIMESTAMP('"
//              + literal.toFormattedString() + "', 'YYYY-MM-DD HH24:MI:SS.FF')");
//    } else if (literal instanceof SqlDateLiteral) {
//      writer.literal("TO_DATE('"
//              + literal.toFormattedString() + "', 'YYYY-MM-DD')");
//    } else if (literal instanceof SqlTimeLiteral) {
//      writer.literal("TO_TIME('"
//              + literal.toFormattedString() + "', 'HH24:MI:SS.FF')");
//    } else {
//      super.unparseDateTimeLiteral(writer, literal, leftPrec, rightPrec);
//    }
//  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {

//    throw new Exception("===============Call:"+call.toString()+"====================");
    System.out.println("===============Call:"+call.toString()+"====================");
    logger.info("===============Call:"+call.toString()+"====================");

//    // try-catch block to handle exceptions
//    try {
//
//      String str = call.toString();
//
//      // attach a file to FileWriter
//      FileWriter fw=new FileWriter(absolute);
//
//      // read character wise from string and write
//      // into FileWriter
//      for (int i = 0; i < str.length(); i++)
//        fw.write(str.charAt(i));
//
//      System.out.println("Writing successful");
//      //close the file
//      fw.close();
//    }
//    catch (Exception e) {
//      System.err.println(e.getMessage());
//    }

    if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
      final SqlLiteral timeUnitNode = call.operand(1);
      final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

      SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
              timeUnitNode.getParserPosition());
      SqlFloorFunction.unparseDatetimeFunction(writer, call2, "DATE_TRUNC", false);
    }
//    else if (call.getKind() == SqlKind.CAST && call.toString().contains("AS DATE")) {
//      SisenseDialectUtils.crossDialectsUnparseCall(writer, call, leftPrec, rightPrec);
//    }
//    else if (call.getKind() == SqlKind.TIMESTAMP_DIFF) {
//      writer.print("DATEDIFF(");
//      SisenseDialectUtils.printOperands(writer, call, ",");
//      writer.print(")");
//    }
//    else if (call.getKind() == SqlKind.TRIM) {
//      SISENSE_DEFAULT_DIALECT.generateSqlTrimFunction(writer, call);
//    }
//    else if (call.getKind() == SqlKind.POSITION && call.operandCount() > 2) {
//      SISENSE_DEFAULT_DIALECT.generateSqlIndexOfwRegex(writer, call);
//    }
    else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }

    System.out.println("=========================Inside unparseCall method=============================");
    logger.info("=========================Inside unparseCall override method=============================");
  }

  @Override
  public void unparseSqlDatetimeArithmetic(SqlWriter writer,
                                           SqlCall call, SqlKind sqlKind, int leftPrec, int rightPrec) {

    DatetimeArithmeticToDateAdd.unparseSqlDatetimeArithmetic(writer, call, sqlKind, leftPrec, rightPrec, "DATE_ADD");
  }

  @Override
  public SqlNode getCastSpec(RelDataType type) {
    System.out.println("=========================Inside getCastSpec Method=============================");
    logger.info("=========================Inside getCastSpec override method=============================");

    // try-catch block to handle exceptions
    try {

      String str = "========================= "+ type.getSqlTypeName().toString()+" =============================";

      // attach a file to FileWriter
      FileWriter fw=new FileWriter(absolute);

      // read character wise from string and write
      // into FileWriter
      for (int i = 0; i < str.length(); i++)
        fw.write(str.charAt(i));

      System.out.println("Writing successful");
      //close the file
      fw.close();
    }
    catch (Exception e) {
      System.err.println(e.getMessage());
    }

    String castSpec;
    switch (type.getSqlTypeName()) {
      case TINYINT:
        // Firebolt has no tinyint, so instead cast to INT
        castSpec = "INT";
        break;
      case SMALLINT:
        // Firebolt has no smallint, so instead cast to INT
        castSpec = "INT";
        break;
      case SqlTypeFamily.NUMERIC:
        // Firebolt has no numeric, so instead cast to BIGINT
        castSpec = "BIGINT";
        break;
      case TIME:
        // Firebolt has no TIME, so instead cast to TIMESTAMP
        castSpec = "TIMESTAMP";
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        // Firebolt has no TimeWithTimezone, so instead cast to TIMESTAMP
        castSpec = "TIMESTAMP";
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        // Firebolt has no TimestampWithTimezone, so instead cast to TIMESTAMP
        castSpec = "TIMESTAMP";
        break;
      case CHAR:
        // Firebolt has no CHAR, so instead cast to VARCHAR
        castSpec = "VARCHAR";
        break;
      case DECIMAL:
        // Firebolt has no DECIMAL, so instead cast to FLOAT
        castSpec = "FLOAT";
        break;
      case REAL:
        // Firebolt has no REAL, so instead cast to DOUBLE
        castSpec = "DOUBLE";
        break;
      default:
        return super.getCastSpec(type);
    }
    return new SqlDataTypeSpec(
            new SqlAlienSystemTypeNameSpec(castSpec, type.getSqlTypeName(), SqlParserPos.ZERO),
            SqlParserPos.ZERO);
  }
}
