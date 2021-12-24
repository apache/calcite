package org.apache.calcite.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link SqlDialects}.
 */
public class SqlDialectsTest {
  @Test
  void createContextFromCalciteMetaData() throws SQLException {
    Connection connection = DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    DatabaseMetaData metaData = calciteConnection.getMetaData();

    SqlDialect.Context context = SqlDialects.createContext(metaData);
    assertEquals(context.databaseProductName(), metaData.getDatabaseProductName());
  }
}
