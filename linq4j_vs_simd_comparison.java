import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Direct performance comparison between LINQ4J row-wise processing and columnar processing.
 */
public class linq4j_vs_simd_comparison {
    
    private static final int DATA_SIZE = 1_000_000;
    private static final int WARMUP_ITERATIONS = 10;
    private static final int MEASUREMENT_ITERATIONS = 50;
    
    public static void main(String[] args) {
        System.out.println("=== LINQ4J vs Columnar Processing Performance Comparison ===");
        System.out.println("Data size: " + DATA_SIZE + " integers");
        System.out.println("Warmup iterations: " + WARMUP_ITERATIONS);  
        System.out.println("Measurement iterations: " + MEASUREMENT_ITERATIONS);
        System.out.println();
        
        // Generate test data
        Random random = new Random(42);
        List<Integer> rowData = new ArrayList<>(DATA_SIZE);
        int[] columnData = new int[DATA_SIZE];
        
        for (int i = 0; i < DATA_SIZE; i++) {
            int value = random.nextInt(10000);
            rowData.add(value);
            columnData[i] = value;
        }
        
        // Warmup
        System.out.println("Warming up...");
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            linq4jSum(rowData);
            columnarSum(columnData);
        }
        
        // Measure LINQ4J performance
        System.out.println("Measuring LINQ4J row-wise processing...");
        long linq4jTotal = 0;
        long linq4jResult = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            linq4jResult = linq4jSum(rowData);
            long end = System.nanoTime();
            linq4jTotal += (end - start);
        }
        long linq4jAvg = linq4jTotal / MEASUREMENT_ITERATIONS;
        
        // Measure columnar performance
        System.out.println("Measuring columnar processing...");
        long columnarTotal = 0;
        long columnarResult = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            columnarResult = columnarSum(columnData);
            long end = System.nanoTime();
            columnarTotal += (end - start);
        }
        long columnarAvg = columnarTotal / MEASUREMENT_ITERATIONS;
        
        // Results
        System.out.println();
        System.out.println("=== RESULTS ===");
        System.out.println("LINQ4J result: " + linq4jResult);
        System.out.println("Columnar result: " + columnarResult);
        System.out.println("Results match: " + (linq4jResult == columnarResult));
        System.out.println();
        System.out.printf("LINQ4J average time: %,d ns%n", linq4jAvg);
        System.out.printf("Columnar average time: %,d ns%n", columnarAvg);
        System.out.println();
        
        double speedup = (double) linq4jAvg / columnarAvg;
        System.out.printf("Columnar speedup: %.2fx (%.1f%% improvement)%n", speedup, (speedup - 1) * 100);
        
        if (speedup > 1.0) {
            System.out.println("✅ Columnar processing is faster than LINQ4J");
        } else {
            System.out.println("❌ LINQ4J is faster than columnar processing");
        }
    }
    
    /**
     * LINQ4J-style row-wise processing - iterating over objects in a stream-like manner
     */
    private static long linq4jSum(List<Integer> data) {
        return data.stream()
            .mapToLong(Integer::longValue)
            .sum();
    }
    
    /**
     * Columnar processing - operating on primitive arrays
     */
    private static long columnarSum(int[] data) {
        long sum = 0;
        for (int value : data) {
            sum += value;
        }
        return sum;
    }
}