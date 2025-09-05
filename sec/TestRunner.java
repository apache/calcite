import org.apache.calcite.adapter.sec.DJIComprehensiveTest;

public class TestRunner {
  public static void main(String[] args) throws Exception {
    System.out.println("Loading JDBC Driver...");
    Class.forName("org.apache.calcite.jdbc.Driver");
    
    System.out.println("Starting DJI Comprehensive Test...");
    DJIComprehensiveTest test = new DJIComprehensiveTest();
    DJIComprehensiveTest.setUp();
    test.testComprehensiveDJIAnalysis();
    DJIComprehensiveTest.tearDown();
    System.out.println("Test completed successfully!");
  }
}