import java.sql.Time;

public class TestTimeValue {
    public static void main(String[] args) {
        // 07:15:56 = 26156000 milliseconds since midnight
        int millisSinceMidnight = 26156000;
        
        // Create Time object from milliseconds
        Time time = new Time(millisSinceMidnight);
        
        // What do we get back?
        System.out.println("Input millis: " + millisSinceMidnight);
        System.out.println("Time.getTime(): " + time.getTime());
        System.out.println("Time.toString(): " + time);
        
        // Try with LocalTime
        java.time.LocalTime localTime = java.time.LocalTime.ofNanoOfDay(millisSinceMidnight * 1_000_000L);
        System.out.println("LocalTime: " + localTime);
        System.out.println("LocalTime millis: " + (localTime.toNanoOfDay() / 1_000_000L));
    }
}
