package org.apache.calcite.runtime.inmemory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static java.sql.DriverManager.getConnection;

final class PrimitiveTest {
    @Test
    void testUpdate() throws Exception {
        // set up schema & table
        InMemorySchema schema = new InMemorySchema();
        PrimitiveType table = new PrimitiveType(0, '\u0000');
        List<PrimitiveType> primitivetypesCollection = new ArrayList<>();
        primitivetypesCollection.add(table);
        InMemoryTable<PrimitiveType> primitivetypesTable = new InMemoryTable<>(PrimitiveType.class, primitivetypesCollection);
        schema.addTable("PRIMITIVE", primitivetypesTable);
        // connection
        Connection connection = getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        // add schema
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("DUMMY", schema);
        // set-up statement
        Statement statement = calciteConnection.createStatement();
        try {
            // update query
            statement.executeUpdate("UPDATE DUMMY.PRIMITIVE SET INT_=1");
            // toChar(Object) found
            assertEquals(1,table.INT_);
        } catch (Exception e) {
            e.printStackTrace(); // stack trace in stdout
            // toChar(Object) not found ?
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String sStackTrace = sw.toString(); // stack trace as a string
            assertTrue(sStackTrace.contains("NoSuchMethodException: org.apache.calcite.runtime.SqlFunctions.toChar(java.lang.Object)"));
        } finally {
            // close
            statement.close();
            calciteConnection.close();
        }
    }

    private void assetEquals(int i, int iNT_) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'assetEquals'");
    }
}
