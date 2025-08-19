import java.sql.*;
import java.util.Properties;

public class TestDirectJdbc {
    public static void main(String[] args) throws Exception {
        Properties info = new Properties();
        info.put("model", "src/test/resources/BUG.json");
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                "select * from \"date\" where \"empno\" = 140");
            
            if (resultSet.next()) {
                Time timeVal = resultSet.getTime(3);
                System.out.println("Time object: " + timeVal);
                System.out.println("Time.getTime(): " + timeVal.getTime());
                System.out.println("Time millis % day: " + (timeVal.getTime() % (24L * 60 * 60 * 1000)));
                
                // Also get the raw object
                Object rawObj = resultSet.getObject(3);
                System.out.println("Raw object class: " + rawObj.getClass());
                System.out.println("Raw object value: " + rawObj);
            }
        }
    }
}
