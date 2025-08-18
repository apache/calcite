public class TestTime2 {
  public static void main(String[] args) {
    // What we have in Parquet: 26156000 (07:15:56 as millis since midnight)
    int millisSinceMidnight = 26156000;
    
    // Convert millis to HH:mm:ss format
    int totalSeconds = millisSinceMidnight / 1000;
    int hours = totalSeconds / 3600;
    int minutes = (totalSeconds % 3600) / 60;
    int seconds = totalSeconds % 60;
    String timeStr = String.format("%02d:%02d:%02d", hours, minutes, seconds);
    System.out.println("Time string: " + timeStr);
    
    // Create Time using valueOf
    java.sql.Time t = java.sql.Time.valueOf(timeStr);
    System.out.println("Time object: " + t);
    System.out.println("Time millis: " + t.getTime());
    System.out.println("Time millis % day: " + (t.getTime() % (24L * 60 * 60 * 1000)));
  }
}
