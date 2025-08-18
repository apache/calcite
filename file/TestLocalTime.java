import java.time.LocalTime;

public class TestLocalTime {
  public static void main(String[] args) {
    LocalTime t1 = LocalTime.parse("07:15:56");
    LocalTime t2 = LocalTime.parse("13:31:21");
    
    System.out.println("Time 1: " + t1);
    System.out.println("Time 1 nanos: " + t1.toNanoOfDay());
    System.out.println("Time 1 millis: " + (t1.toNanoOfDay() / 1_000_000L));
    
    System.out.println("\nTime 2: " + t2);
    System.out.println("Time 2 nanos: " + t2.toNanoOfDay());
    System.out.println("Time 2 millis: " + (t2.toNanoOfDay() / 1_000_000L));
  }
}
