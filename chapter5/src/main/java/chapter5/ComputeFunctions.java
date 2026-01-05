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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;

/**
 * Java equivalent of compute_functions.cc - demonstrates compute operations on Arrow data
 *
 * Note: Arrow Java doesn't have the same compute kernel API as C++.
 * Instead, we implement equivalent operations using standard Java operations
 * on Arrow vectors, or use Gandiva for expression evaluation.
 */
public class ComputeFunctions {

    /**
     * Read Parquet file and perform compute operations
     */
    public static void computeOnParquet(String filepath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String uri = "file://" + Paths.get(filepath).toAbsolutePath();
            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                System.out.println("Schema: " + reader.getVectorSchemaRoot().getSchema());

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();

                    // Get the total_amount column if it exists
                    if (root.getSchema().findField("total_amount") != null) {
                        Float8Vector column = (Float8Vector) root.getVector("total_amount");
                        System.out.println("\nColumn: total_amount");
                        System.out.println("Row count: " + column.getValueCount());

                        // Compute: add 5.5 to each value (equivalent to arrow::compute::Add)
                        double[] incremented = addScalar(column, 5.5);
                        System.out.println("\nAfter adding 5.5 to first 5 values:");
                        for (int i = 0; i < Math.min(5, incremented.length); i++) {
                            System.out.printf("  Original: %.2f -> Incremented: %.2f%n",
                                column.get(i), incremented[i]);
                        }
                    }

                    // Only process first batch for demo
                    break;
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Add a scalar value to all elements in a vector
     * Equivalent to arrow::compute::Add(column, scalar)
     */
    public static double[] addScalar(Float8Vector vector, double scalar) {
        int count = vector.getValueCount();
        double[] result = new double[count];
        for (int i = 0; i < count; i++) {
            if (!vector.isNull(i)) {
                result[i] = vector.get(i) + scalar;
            }
        }
        return result;
    }

    /**
     * Find min and max values
     * Equivalent to arrow::compute::CallFunction("min_max", ...)
     */
    public static void findMinMax(String filepath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String uri = "file://" + Paths.get(filepath).toAbsolutePath();
            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                double globalMin = Double.MAX_VALUE;
                double globalMax = Double.MIN_VALUE;
                long totalCount = 0;

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();

                    if (root.getSchema().findField("total_amount") != null) {
                        Float8Vector column = (Float8Vector) root.getVector("total_amount");

                        for (int i = 0; i < column.getValueCount(); i++) {
                            if (!column.isNull(i)) {
                                double value = column.get(i);
                                if (value < globalMin) globalMin = value;
                                if (value > globalMax) globalMax = value;
                                totalCount++;
                            }
                        }
                    }
                }

                System.out.println("\n=== Min/Max Results ===");
                System.out.println("Total values processed: " + totalCount);
                System.out.println("Min: " + globalMin);
                System.out.println("Max: " + globalMax);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Sort table by a column
     * Equivalent to arrow::compute::CallFunction("sort_indices", ...)
     */
    public static void sortTable(String filepath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String uri = "file://" + Paths.get(filepath).toAbsolutePath();
            ScanOptions options = new ScanOptions(/*batchSize*/ 1024);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                if (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();

                    if (root.getSchema().findField("total_amount") != null) {
                        Float8Vector column = (Float8Vector) root.getVector("total_amount");

                        // Create array of indices and values for sorting
                        int count = column.getValueCount();
                        Integer[] indices = new Integer[count];
                        for (int i = 0; i < count; i++) {
                            indices[i] = i;
                        }

                        // Sort indices by value (descending)
                        Arrays.sort(indices, (a, b) -> {
                            if (column.isNull(a) && column.isNull(b)) return 0;
                            if (column.isNull(a)) return 1;
                            if (column.isNull(b)) return -1;
                            return Double.compare(column.get(b), column.get(a));
                        });

                        System.out.println("\n=== Sorted Results (Top 10 by total_amount DESC) ===");
                        for (int i = 0; i < Math.min(10, count); i++) {
                            int idx = indices[i];
                            System.out.printf("  Index %d: %.2f%n", idx,
                                column.isNull(idx) ? Double.NaN : column.get(idx));
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Demonstrate compute function concepts available in Java Arrow
     */
    public static void demonstrateComputeConcepts() {
        System.out.println("=== Arrow Compute Functions in Java ===\n");

        System.out.println("Arrow C++ has a rich compute kernel API. In Java, equivalent");
        System.out.println("operations can be performed using:\n");

        System.out.println("1. Direct Vector Operations");
        System.out.println("   - Manual iteration over Arrow vectors");
        System.out.println("   - Similar to the examples shown above\n");

        System.out.println("2. Gandiva (Expression Compiler)");
        System.out.println("   - JIT-compiled expressions on Arrow data");
        System.out.println("   - Supports filter, project, and aggregate operations");
        System.out.println("   - Uses arrow-gandiva module\n");

        System.out.println("3. Dataset API Projections");
        System.out.println("   - Can apply expressions during scanning");
        System.out.println("   - Uses native compute kernels\n");

        System.out.println("4. Integration with Other Libraries");
        System.out.println("   - Apache Spark with Arrow");
        System.out.println("   - DuckDB with Arrow");
        System.out.println("   - DataFusion (via FFI)\n");
    }

    public static void main(String[] args) {
        String filepath = "../../sample_data/yellow_tripdata_2015-01.parquet";

        demonstrateComputeConcepts();

        System.out.println("=== Computing on Parquet Data ===");
        System.out.println("File: " + filepath);

        computeOnParquet(filepath);
        findMinMax(filepath);
        sortTable(filepath);
    }
}
