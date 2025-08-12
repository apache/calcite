package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;

/**
 * Direct performance comparison between LINQ4J row-wise processing and SIMD columnar processing.
 */
@Tag("temp")
public class Linq4jVsSimdComparisonTest {
    
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
    private static final int DATA_SIZE = 1_000_000;
    private static final int WARMUP_ITERATIONS = 10;
    private static final int MEASUREMENT_ITERATIONS = 50;
    
    @Test
    public void compareLinq4jVsSimd() {
        System.out.println("=== LINQ4J vs SIMD Performance Comparison ===");
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
            simdSum(columnData);
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
        System.out.println("SIMD result: " + simdResult);
        System.out.println("Results match: " + (linq4jResult == simdResult));
        System.out.println();
        System.out.printf("LINQ4J average time: %,d ns%n", linq4jAvg);
        System.out.printf("SIMD average time: %,d ns%n", simdAvg);
        System.out.println();
        
        double speedup = (double) linq4jAvg / simdAvg;
        System.out.printf("SIMD speedup: %.2fx (%.1f%% improvement)%n", speedup, (speedup - 1) * 100);
        
        if (speedup > 1.0) {
            System.out.println("✅ SIMD is faster than LINQ4J");
        } else {
            System.out.println("❌ LINQ4J is faster than SIMD");
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
     * SIMD columnar processing - operating on primitive arrays with vectorization
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