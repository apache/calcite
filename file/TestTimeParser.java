import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TestTimeParser {
    public static void main(String[] args) throws Exception {
        String timeStr = "07:15:56";
        
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = sdf.parse(timeStr);
        
        System.out.println("Parsed date: " + date);
        System.out.println("date.getTime(): " + date.getTime());
        System.out.println("Expected: 26156000 (7*3600000 + 15*60000 + 56*1000)");
        
        // What if we parse with local timezone?
        SimpleDateFormat sdf2 = new SimpleDateFormat("HH:mm:ss");
        sdf2.setTimeZone(TimeZone.getDefault());
        Date date2 = sdf2.parse(timeStr);
        
        System.out.println("\nWith local TZ:");
        System.out.println("Parsed date: " + date2);
        System.out.println("date.getTime(): " + date2.getTime());
    }
}
