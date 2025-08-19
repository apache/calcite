import java.sql.Time;
import java.time.LocalTime;

public class TestTimeLocalTime {
  public static void main(String[] args) {
    // Test Time.valueOf().toLocalTime() behavior
    Time t1 = Time.valueOf("07:15:56");
    LocalTime lt1 = t1.toLocalTime();
    
    System.out.println("Time.valueOf(\"07:15:56\"): " + t1);
    System.out.println("getTime(): " + t1.getTime());
    System.out.println("toLocalTime(): " + lt1);
    System.out.println("toLocalTime().toNanoOfDay() / 1_000_000L: " + (lt1.toNanoOfDay() / 1_000_000L));
    
    System.out.println("\nExpected: 26156000");
  }
}
