public class TestTime {
  public static void main(String[] args) {
    java.sql.Time t1 = java.sql.Time.valueOf("07:15:56");
    java.sql.Time t2 = java.sql.Time.valueOf("13:31:21");
    
    System.out.println("Time 1: " + t1.getTime());
    System.out.println("Time 2: " + t2.getTime());
    
    // Create Time with raw millis
    java.sql.Time t3 = new java.sql.Time(26156000L);
    java.sql.Time t4 = new java.sql.Time(48681000L);
    
    System.out.println("Time 3: " + t3);
    System.out.println("Time 4: " + t4);
    System.out.println("Time 3 millis: " + t3.getTime());
    System.out.println("Time 4 millis: " + t4.getTime());
    
    // Check toLocalTime
    System.out.println("Time 1 toLocalTime: " + t1.toLocalTime());
    System.out.println("Time 2 toLocalTime: " + t2.toLocalTime());
    System.out.println("Time 3 toLocalTime: " + t3.toLocalTime());
    System.out.println("Time 4 toLocalTime: " + t4.toLocalTime());
  }
}
