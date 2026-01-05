/*
 * MIT License
 *
 * Copyright (c) 2024 Packt
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package chapter5;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

import java.util.stream.IntStream;

/**
 * Java equivalent of compute_or_not.cc - Performance comparison of different
 * approaches to compute operations on Arrow data
 */
public class ComputeOrNot {

    /**
     * Simple timer utility class
     */
    static class Timer {
        private long startTime;
        private String label;

        Timer(String label) {
            this.label = label;
            this.startTime = System.nanoTime();
        }

        void stop() {
            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            System.out.printf("  %s: %.3f ms%n", label, durationMs);
        }
    }

    /**
     * Method 1: Element-by-element access with null checks
     * Similar to the C++ loop with IsValid/IsNull checks
     */
    public static IntVector method1_ElementByElement(BufferAllocator allocator, IntVector input) {
        IntVector output = new IntVector("result", allocator);
        output.allocateNew(input.getValueCount());

        for (int i = 0; i < input.getValueCount(); i++) {
            if (!input.isNull(i)) {
                output.setSafe(i, input.get(i) + 2);
            } else {
                output.setNull(i);
            }
        }
        output.setValueCount(input.getValueCount());
        return output;
    }

    /**
     * Method 2: Using Java streams for iteration
     */
    public static IntVector method2_Streams(BufferAllocator allocator, IntVector input) {
        IntVector output = new IntVector("result", allocator);
        output.allocateNew(input.getValueCount());

        IntStream.range(0, input.getValueCount())
            .forEach(i -> {
                if (!input.isNull(i)) {
                    output.setSafe(i, input.get(i) + 2);
                } else {
                    output.setNull(i);
                }
            });

        output.setValueCount(input.getValueCount());
        return output;
    }

    /**
     * Method 3: Direct buffer access (most performant)
     * Similar to the C++ raw_values() approach
     */
    public static IntVector method3_DirectBuffer(BufferAllocator allocator, IntVector input) {
        IntVector output = new IntVector("result", allocator);
        output.allocateNew(input.getValueCount());

        ArrowBuf inputBuffer = input.getDataBuffer();
        ArrowBuf outputBuffer = output.getDataBuffer();

        int count = input.getValueCount();
        for (int i = 0; i < count; i++) {
            int value = inputBuffer.getInt((long) i * 4);
            outputBuffer.setInt((long) i * 4, value + 2);
        }

        // Copy validity bitmap
        if (input.getValidityBuffer() != null) {
            output.getValidityBuffer().setBytes(0, input.getValidityBuffer(), 0,
                (int) Math.ceil(count / 8.0));
        }

        output.setValueCount(count);
        return output;
    }

    /**
     * Method 4: Parallel streams for multi-threaded processing
     */
    public static IntVector method4_ParallelStreams(BufferAllocator allocator, IntVector input) {
        IntVector output = new IntVector("result", allocator);
        output.allocateNew(input.getValueCount());

        // Note: Arrow vectors are not thread-safe, so we compute values first
        int[] results = IntStream.range(0, input.getValueCount())
            .parallel()
            .map(i -> input.isNull(i) ? Integer.MIN_VALUE : input.get(i) + 2)
            .toArray();

        for (int i = 0; i < results.length; i++) {
            if (results[i] == Integer.MIN_VALUE) {
                output.setNull(i);
            } else {
                output.setSafe(i, results[i]);
            }
        }

        output.setValueCount(input.getValueCount());
        return output;
    }

    /**
     * Verify two vectors have the same values
     */
    public static boolean verifyEqual(IntVector v1, IntVector v2) {
        if (v1.getValueCount() != v2.getValueCount()) return false;

        for (int i = 0; i < v1.getValueCount(); i++) {
            if (v1.isNull(i) != v2.isNull(i)) return false;
            if (!v1.isNull(i) && v1.get(i) != v2.get(i)) return false;
        }
        return true;
    }

    /**
     * Run benchmark for a specific data size
     */
    public static void runBenchmark(BufferAllocator allocator, int n) {
        System.out.println("\n=== N: " + String.format("%,d", n) + " ===");

        // Create input vector with sequential values
        IntVector input = new IntVector("input", allocator);
        input.allocateNew(n);
        for (int i = 0; i < n; i++) {
            input.setSafe(i, i);
        }
        input.setValueCount(n);

        IntVector result1, result2, result3, result4;

        // Method 1: Element-by-element
        Timer t1 = new Timer("Method 1 (Element-by-element)");
        result1 = method1_ElementByElement(allocator, input);
        t1.stop();

        // Method 2: Streams
        Timer t2 = new Timer("Method 2 (Streams)");
        result2 = method2_Streams(allocator, input);
        t2.stop();

        // Method 3: Direct buffer
        Timer t3 = new Timer("Method 3 (Direct buffer)");
        result3 = method3_DirectBuffer(allocator, input);
        t3.stop();

        // Method 4: Parallel streams
        Timer t4 = new Timer("Method 4 (Parallel streams)");
        result4 = method4_ParallelStreams(allocator, input);
        t4.stop();

        // Verify results match
        boolean match12 = verifyEqual(result1, result2);
        boolean match13 = verifyEqual(result1, result3);
        boolean match14 = verifyEqual(result1, result4);

        System.out.println("  Results match (1==2): " + match12);
        System.out.println("  Results match (1==3): " + match13);
        System.out.println("  Results match (1==4): " + match14);

        // Cleanup
        input.close();
        result1.close();
        result2.close();
        result3.close();
        result4.close();
    }

    public static void main(String[] args) {
        System.out.println("=== Compute Performance Comparison ===");
        System.out.println("Comparing different approaches to add 2 to each element\n");

        System.out.println("Methods:");
        System.out.println("1. Element-by-element with isNull checks");
        System.out.println("2. Java Streams iteration");
        System.out.println("3. Direct ArrowBuf access");
        System.out.println("4. Parallel Streams");

        try (BufferAllocator allocator = new RootAllocator()) {
            // Warm up JIT
            System.out.println("\n--- Warming up JIT ---");
            for (int i = 0; i < 3; i++) {
                runBenchmark(allocator, 10000);
            }

            // Actual benchmarks
            System.out.println("\n--- Benchmark Results ---");
            int[] sizes = {10_000, 100_000, 1_000_000, 10_000_000};

            for (int size : sizes) {
                runBenchmark(allocator, size);
            }
        }

        System.out.println("\n=== Summary ===");
        System.out.println("Direct buffer access is typically fastest for large datasets.");
        System.out.println("Parallel streams can help for very large data on multi-core systems.");
        System.out.println("In C++, the Arrow compute kernels use SIMD and are highly optimized.");
    }
}
