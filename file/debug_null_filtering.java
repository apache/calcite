import java.sql.*;
import java.util.Properties;

public class debug_null_filtering {
    public static void main(String[] args) throws Exception {
        Properties info = new Properties();
        info.put("model", "src/test/resources/bug.json");
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
             Statement statement = connection.createStatement()) {
            
            System.out.println("=== Testing null value filtering ===");
            
            // First, let's see all the raw data
            System.out.println("\n1. All data from DATE table:");
            ResultSet rs1 = statement.executeQuery("select EMPNO, JOINTIME from \"DATE\"");
            while (rs1.next()) {
                Object empno = rs1.getObject(1);
                Object jointime = rs1.getObject(2);
                System.out.println("EMPNO: " + empno + ", JOINTIME: " + jointime + " (null: " + (jointime == null) + ")");
            }
            rs1.close();
            
            // Second, let's see what the WHERE clause filters
            System.out.println("\n2. Data after WHERE JOINTIME is not null:");
            ResultSet rs2 = statement.executeQuery("select EMPNO, JOINTIME from \"DATE\" where JOINTIME is not null");
            while (rs2.next()) {
                Object empno = rs2.getObject(1);
                Object jointime = rs2.getObject(2);
                System.out.println("EMPNO: " + empno + ", JOINTIME: " + jointime + " (null: " + (jointime == null) + ")");
            }
            rs2.close();
            
            // Third, let's try the failing GROUP BY query
            System.out.println("\n3. Attempting GROUP BY query:");
            try {
                ResultSet rs3 = statement.executeQuery("select count(*) as c, JOINTIME as t from \"DATE\" where JOINTIME is not null group by JOINTIME order by JOINTIME");
                while (rs3.next()) {
                    int count = rs3.getInt(1);
                    Object jointime = rs3.getObject(2);
                    System.out.println("COUNT: " + count + ", JOINTIME: " + jointime);
                }
                rs3.close();
            } catch (SQLException e) {
                System.out.println("ERROR: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}