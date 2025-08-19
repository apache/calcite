import java.sql.Time;

public class TestTimeModulo {
  public static void main(String[] args) {
    // Test Time.valueOf behavior
    Time t1 = Time.valueOf("07:15:56");
    Time t2 = Time.valueOf("13:31:21");
    
    System.out.println("Time 1: " + t1);
    System.out.println("Time 1 getTime(): " + t1.getTime());
    System.out.println("Time 1 modulo: " + (t1.getTime() % (24L * 60 * 60 * 1000)));
    
    System.out.println("\nTime 2: " + t2);
    System.out.println("Time 2 getTime(): " + t2.getTime());
    System.out.println("Time 2 modulo: " + (t2.getTime() % (24L * 60 * 60 * 1000)));
    
    // What we expect
    System.out.println("\nExpected milliseconds:");
    System.out.println("07:15:56 = " + (7*3600 + 15*60 + 56) * 1000 + "ms");
    System.out.println("13:31:21 = " + (13*3600 + 31*60 + 21) * 1000 + "ms");
  }
}
