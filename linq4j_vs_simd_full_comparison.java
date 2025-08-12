import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * Complete performance comparison: LINQ4J vs Columnar vs SIMD
 */
public class linq4j_vs_simd_full_comparison {
    
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
    private static final int DATA_SIZE = 1_000_000;
    private static final int WARMUP_ITERATIONS = 10;
    private static final int MEASUREMENT_ITERATIONS = 50;
    
    public static void main(String[] args) {
        System.out.println("=== Complete Performance Comparison: LINQ4J vs Columnar vs SIMD ===");
        System.out.println("Data size: " + DATA_SIZE + " integers");
        System.out.println("SIMD species: " + SPECIES);
        System.out.println("SIMD vector length: " + SPECIES.length());
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
            simdSum(columnData);
        }
        
        // Measure LINQ4J performance (baseline)
        System.out.println("Measuring LINQ4J row-wise processing (baseline)...");
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
        
        // Measure SIMD performance
        System.out.println("Measuring SIMD columnar processing...");
        long simdTotal = 0;
        long simdResult = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            simdResult = simdSum(columnData);
            long end = System.nanoTime();
            simdTotal += (end - start);
        }
        long simdAvg = simdTotal / MEASUREMENT_ITERATIONS;
        
        // Results
        System.out.println();
        System.out.println("=== RESULTS ===");
        System.out.println("LINQ4J result: " + linq4jResult);
        System.out.println("Columnar result: " + columnarResult);
        System.out.println("SIMD result: " + simdResult);
        System.out.println("All results match: " + (linq4jResult == columnarResult && columnarResult == simdResult));
        System.out.println();
        
        System.out.printf("LINQ4J (baseline) time:   %,d ns%n", linq4jAvg);
        System.out.printf("Columnar time:            %,d ns%n", columnarAvg);
        System.out.printf("SIMD time:                %,d ns%n", simdAvg);
        System.out.println();
        
        double columnarSpeedup = (double) linq4jAvg / columnarAvg;
        double simdSpeedup = (double) linq4jAvg / simdAvg;
        
        System.out.printf("Columnar vs LINQ4J: %.2fx speedup (%.1f%% improvement)%n", columnarSpeedup, (columnarSpeedup - 1) * 100);
        System.out.printf("SIMD vs LINQ4J:     %.2fx speedup (%.1f%% improvement)%n", simdSpeedup, (simdSpeedup - 1) * 100);
        
        double simdVsColumnar = (double) columnarAvg / simdAvg;
        System.out.printf("SIMD vs Columnar:   %.2fx speedup (%.1f%% improvement)%n", simdVsColumnar, (simdVsColumnar - 1) * 100);
        
        System.out.println();
        if (simdSpeedup > columnarSpeedup) {
            System.out.println("🏆 SIMD provides the best performance improvement over LINQ4J baseline");
        } else {
            System.out.println("🏆 Basic columnar processing provides better performance than SIMD for this workload");
        }
    }
    
    /**
     * LINQ4J-style row-wise processing - the original baseline we're comparing against
     */
    private static long linq4jSum(List<Integer> data) {
        return data.stream()
            .mapToLong(Integer::longValue)
            .sum();
    }
    
    /**
     * Basic columnar processing - operating on primitive arrays
     */
    private static long columnarSum(int[] data) {
        long sum = 0;
        for (int value : data) {
            sum += value;
        }
        return sum;
    }
    
    /**
     * SIMD columnar processing - using Vector API on primitive arrays
     */
    private static long simdSum(int[] data) {
        long totalSum = 0;
        int vectorLimit = SPECIES.loopBound(data.length);
        
        // Process vectors
        for (int i = 0; i < vectorLimit; i += SPECIES.length()) {
            IntVector chunk = IntVector.fromArray(SPECIES, data, i);
            for (int lane = 0; lane < SPECIES.length(); lane++) {
                totalSum += chunk.lane(lane);
            }
        }
        
        // Process remaining elements
        for (int i = vectorLimit; i < data.length; i++) {
            totalSum += data[i];
        }
        
        return totalSum;
    }
}