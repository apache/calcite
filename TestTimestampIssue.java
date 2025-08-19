import java.sql.*;
import java.time.*;

public class TestTimestampIssue {
    public static void main(String[] args) {
        // Test what we expect vs what we get
        LocalDateTime ldt = LocalDateTime.of(2015, 12, 30, 7, 15, 56);
        
        // Expected: local time converted to epoch millis
        long expectedLocal = ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        System.out.println("Expected (local): " + expectedLocal);
        
        // What if it's treated as UTC?
        long asUTC = ldt.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        System.out.println("As UTC: " + asUTC);
        
        // What we're actually getting
        long actual = 1451495756000L;
        System.out.println("Actual: " + actual);
        
        // Differences
        System.out.println("Actual - Expected: " + (actual - expectedLocal) + " ms = " + 
                          ((actual - expectedLocal) / 3600000) + " hours");
        System.out.println("Actual - UTC: " + (actual - asUTC) + " ms = " + 
                          ((actual - asUTC) / 3600000) + " hours");
    }
}
