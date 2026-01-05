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
package chapter6;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Java equivalent of streaming_engine.cc - demonstrates streaming execution concepts
 *
 * Note: Java Arrow doesn't have Acero's ExecPlan directly.
 * This shows equivalent streaming patterns using Dataset API and async processing.
 */
public class StreamingEngine {

    /**
     * Timer utility
     */
    static class Timer {
        private long startTime;
        private String label;

        Timer(String label) {
            this.label = label;
            this.startTime = System.nanoTime();
        }

        void stop() {
            double ms = (System.nanoTime() - startTime) / 1_000_000.0;
            System.out.printf("%s: %.2f ms%n", label, ms);
        }
    }

    /**
     * Calculate mean using streaming approach
     * Equivalent to Acero's calc_mean with scan/aggregate/sink nodes
     */
    public static void calcMean(BufferAllocator allocator, String uri) {
        System.out.println("=== Calculate Mean (Streaming) ===\n");

        try {
            String[] columns = {"passenger_count"};
            ScanOptions options = new ScanOptions(1 << 20, java.util.Optional.of(columns));

            Timer timer = new Timer("Mean calculation");

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                DoubleAdder sum = new DoubleAdder();
                AtomicLong count = new AtomicLong(0);

                // Stream through batches
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    FieldVector vector = root.getVector("passenger_count");

                    if (vector != null) {
                        for (int i = 0; i < root.getRowCount(); i++) {
                            if (!vector.isNull(i)) {
                                Object val = vector.getObject(i);
                                if (val instanceof Number) {
                                    sum.add(((Number) val).doubleValue());
                                    count.incrementAndGet();
                                }
                            }
                        }
                    }
                }

                timer.stop();

                double mean = sum.sum() / count.get();
                System.out.printf("Mean: %.4f%n", mean);
                System.out.println("Rows processed: " + count.get());
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Grouped mean - GROUP BY vendor_id
     * Equivalent to Acero's aggregate with hash_mean
     */
    public static void groupedMean(BufferAllocator allocator, String uri) {
        System.out.println("\n=== Grouped Mean (GROUP BY vendor_id) ===\n");

        try {
            String[] columns = {"vendor_id", "passenger_count"};
            ScanOptions options = new ScanOptions(1 << 20, java.util.Optional.of(columns));

            Timer timer = new Timer("Grouped mean calculation");

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                // Accumulators per group
                Map<String, DoubleAdder> sums = new HashMap<>();
                Map<String, AtomicLong> counts = new HashMap<>();

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    FieldVector vendorVector = root.getVector("vendor_id");
                    FieldVector passengerVector = root.getVector("passenger_count");

                    if (vendorVector != null && passengerVector != null) {
                        for (int i = 0; i < root.getRowCount(); i++) {
                            Object vendorObj = vendorVector.getObject(i);
                            String vendor = vendorObj != null ? vendorObj.toString() : "null";

                            if (!passengerVector.isNull(i)) {
                                Object val = passengerVector.getObject(i);
                                if (val instanceof Number) {
                                    sums.computeIfAbsent(vendor, k -> new DoubleAdder())
                                        .add(((Number) val).doubleValue());
                                    counts.computeIfAbsent(vendor, k -> new AtomicLong(0))
                                        .incrementAndGet();
                                }
                            }
                        }
                    }
                }

                timer.stop();

                System.out.println("\nResults:");
                System.out.printf("%-15s %s%n", "vendor_id", "mean(passenger_count)");
                System.out.println("-".repeat(35));

                sums.keySet().stream().sorted().forEach(vendor -> {
                    double mean = sums.get(vendor).sum() / counts.get(vendor).get();
                    System.out.printf("%-15s %.4f%n", vendor, mean);
                });
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Grouped and filtered mean
     * Equivalent to Acero's filter -> project -> aggregate pipeline
     */
    public static void groupedFilteredMean(BufferAllocator allocator, String uri) {
        System.out.println("\n=== Grouped Filtered Mean (year > 2015) ===\n");

        try {
            String[] columns = {"passenger_count", "year"};
            ScanOptions options = new ScanOptions(1 << 20, java.util.Optional.of(columns));

            Timer timer = new Timer("Filtered grouped mean");

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                // Accumulators per year (filtered)
                Map<Integer, DoubleAdder> sums = new HashMap<>();
                Map<Integer, AtomicLong> counts = new HashMap<>();

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    FieldVector yearVector = root.getVector("year");
                    FieldVector passengerVector = root.getVector("passenger_count");

                    if (yearVector != null && passengerVector != null) {
                        for (int i = 0; i < root.getRowCount(); i++) {
                            Object yearObj = yearVector.getObject(i);
                            if (yearObj == null) continue;

                            int year = ((Number) yearObj).intValue();

                            // Filter: year > 2015
                            if (year <= 2015) continue;

                            if (!passengerVector.isNull(i)) {
                                Object val = passengerVector.getObject(i);
                                if (val instanceof Number) {
                                    sums.computeIfAbsent(year, k -> new DoubleAdder())
                                        .add(((Number) val).doubleValue());
                                    counts.computeIfAbsent(year, k -> new AtomicLong(0))
                                        .incrementAndGet();
                                }
                            }
                        }
                    }
                }

                timer.stop();

                System.out.println("\nResults (year > 2015):");
                System.out.printf("%-10s %s%n", "year", "mean(passenger_count)");
                System.out.println("-".repeat(30));

                sums.keySet().stream().sorted().forEach(year -> {
                    double mean = sums.get(year).sum() / counts.get(year).get();
                    System.out.printf("%-10d %.4f%n", year, mean);
                });
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Demonstrate async/parallel streaming
     */
    public static void asyncStreaming(BufferAllocator allocator, String uri) {
        System.out.println("\n=== Async Streaming Demo ===\n");

        ExecutorService executor = Executors.newFixedThreadPool(4);

        try {
            ScanOptions options = new ScanOptions(1 << 18);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                Timer timer = new Timer("Async processing");

                AtomicLong totalRows = new AtomicLong(0);
                int batchNum = 0;

                while (reader.loadNextBatch()) {
                    final int batch = ++batchNum;
                    final int rows = reader.getVectorSchemaRoot().getRowCount();

                    // Process batch asynchronously
                    CompletableFuture.runAsync(() -> {
                        // Simulate processing
                        totalRows.addAndGet(rows);
                        System.out.println("Processed batch " + batch + " with " + rows + " rows");
                    }, executor);

                    if (batchNum >= 5) break; // Demo: only first 5 batches
                }

                // Wait for completion
                Thread.sleep(100);
                timer.stop();

                System.out.println("Total rows processed: " + totalRows.get());
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Explain streaming execution concepts
     */
    public static void explainStreamingConcepts() {
        System.out.println("=== Streaming Execution Concepts ===\n");

        System.out.println("Arrow C++ Acero provides streaming execution with:");
        System.out.println("1. ExecPlan - query execution plan");
        System.out.println("2. ExecNode - processing nodes (scan, filter, aggregate)");
        System.out.println("3. ExecBatch - streaming data batches");
        System.out.println("4. Backpressure - flow control\n");

        System.out.println("Java equivalents demonstrated here:");
        System.out.println("1. Dataset Scanner - streams RecordBatches");
        System.out.println("2. Manual operators - filter/aggregate in code");
        System.out.println("3. CompletableFuture - async batch processing");
        System.out.println("4. AtomicLong/DoubleAdder - thread-safe accumulators\n");

        System.out.println("For full query execution in Java, consider:");
        System.out.println("- DuckDB (embedded SQL with Arrow)");
        System.out.println("- Apache Spark (distributed processing)");
        System.out.println("- Apache Calcite (query optimization)");
        System.out.println("- Arrow Flight SQL (client/server queries)\n");
    }

    public static void main(String[] args) {
        explainStreamingConcepts();

        try (BufferAllocator allocator = new RootAllocator()) {
            // Use local data for demo
            String localUri = "file://" + System.getProperty("user.dir") +
                "/../../sample_data/yellow_tripdata_2015-01.parquet";

            System.out.println("Data source: " + localUri + "\n");

            calcMean(allocator, localUri);
            groupedMean(allocator, localUri);
            groupedFilteredMean(allocator, localUri);
            asyncStreaming(allocator, localUri);
        }
    }
}
