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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Java equivalent of s3_datasets.cc - demonstrates S3-backed dataset operations
 *
 * Note: Java Arrow supports S3 URIs through the Dataset API.
 * Requires AWS SDK for S3 access.
 */
public class S3Datasets {

    /**
     * Simple timer utility
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
            System.out.printf("%s: %.2f ms%n", label, durationMs);
        }

        double elapsed() {
            return (System.nanoTime() - startTime) / 1_000_000.0;
        }
    }

    /**
     * Timing test - create dataset and count rows
     */
    public static void timingTest(BufferAllocator allocator) {
        // S3 URI for the public taxi dataset
        // Note: This requires network access and may take time
        String s3Uri = "s3://ursa-labs-taxi-data/";

        System.out.println("=== Timing Test ===");
        System.out.println("Dataset: " + s3Uri);
        System.out.println("Note: S3 access requires network and may take time\n");

        // In Java, S3 URIs are supported but require proper AWS configuration
        // For anonymous access, environment variables or config may be needed

        try {
            ScanOptions options = new ScanOptions(32768);

            Timer datasetTimer = new Timer("Dataset creation");
            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, s3Uri);
                 Dataset dataset = datasetFactory.finish()) {

                datasetTimer.stop();

                Timer countTimer = new Timer("Row count");
                try (Scanner scanner = dataset.newScan(options);
                     ArrowReader reader = scanner.scanBatches()) {

                    long totalRows = 0;
                    while (reader.loadNextBatch()) {
                        totalRows += reader.getVectorSchemaRoot().getRowCount();
                    }
                    countTimer.stop();

                    System.out.println("Total rows: " + totalRows);
                }
            }
        } catch (Exception e) {
            System.err.println("S3 access error (expected if no network/credentials): " +
                e.getMessage());
            demonstrateS3Concepts();
        }
    }

    /**
     * Compute mean on passenger_count
     */
    public static void computeMean(BufferAllocator allocator) {
        String s3Uri = "s3://ursa-labs-taxi-data/";

        System.out.println("\n=== Compute Mean ===");
        System.out.println("Computing mean of passenger_count column\n");

        try {
            String[] columns = {"passenger_count"};
            ScanOptions options = new ScanOptions(1 << 20, java.util.Optional.of(columns));

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, s3Uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                Timer timer = new Timer("Mean computation");

                AtomicLong totalPassengers = new AtomicLong(0);
                AtomicLong rowCount = new AtomicLong(0);

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    BigIntVector passengerVector = (BigIntVector) root.getVector("passenger_count");

                    if (passengerVector != null) {
                        for (int i = 0; i < root.getRowCount(); i++) {
                            if (!passengerVector.isNull(i)) {
                                totalPassengers.addAndGet(passengerVector.get(i));
                                rowCount.incrementAndGet();
                            }
                        }
                    }
                }

                timer.stop();

                double mean = (double) totalPassengers.get() / rowCount.get();
                System.out.printf("Mean passenger count: %.4f%n", mean);
                System.out.println("Total rows processed: " + rowCount.get());
            }
        } catch (Exception e) {
            System.err.println("S3 access error: " + e.getMessage());
        }
    }

    /**
     * Scan fragments and show partition info
     */
    public static void scanFragments(BufferAllocator allocator) {
        System.out.println("\n=== Scan Fragments ===");
        System.out.println("Discovering fragments and partition expressions\n");

        // In C++, directory partitioning is used to extract year/month from paths
        // Java Dataset API supports similar functionality

        System.out.println("Directory partitioning parses paths like:");
        System.out.println("  s3://bucket/2014/01/data.parquet -> year=2014, month=01");
        System.out.println("  s3://bucket/2015/06/data.parquet -> year=2015, month=06");
        System.out.println("\nThis enables partition pruning for efficient queries.");
    }

    /**
     * Explain S3 Dataset concepts
     */
    public static void demonstrateS3Concepts() {
        System.out.println("\n=== S3 Dataset Concepts in Java ===\n");

        System.out.println("Arrow Java supports S3 through the Dataset API:");
        System.out.println("1. Use 's3://' URIs with FileSystemDatasetFactory");
        System.out.println("2. AWS credentials from environment or config");
        System.out.println("3. Anonymous access for public buckets\n");

        System.out.println("Configuration options:");
        System.out.println("- AWS_REGION environment variable");
        System.out.println("- AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY");
        System.out.println("- ~/.aws/credentials file");
        System.out.println("- IAM roles when running on AWS\n");

        System.out.println("Example S3 URI formats:");
        System.out.println("- s3://bucket-name/path/to/data/");
        System.out.println("- s3://bucket-name/path/to/file.parquet");
        System.out.println("- s3://bucket-name/path/**/*.parquet (with recursive)\n");

        System.out.println("Performance considerations:");
        System.out.println("- Use batch size > 1MB for S3 (network latency)");
        System.out.println("- Enable parallel scanning when available");
        System.out.println("- Use column projection to reduce data transfer");
        System.out.println("- Partition pruning via filter pushdown\n");
    }

    /**
     * Demonstrate local file scanning as fallback
     */
    public static void localFileFallback(BufferAllocator allocator, String localPath) {
        System.out.println("=== Local File Fallback ===");
        System.out.println("Path: " + localPath + "\n");

        try {
            String uri = "file://" + localPath;
            ScanOptions options = new ScanOptions(32768);

            Timer timer = new Timer("Local scan");
            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                long totalRows = 0;
                while (reader.loadNextBatch()) {
                    totalRows += reader.getVectorSchemaRoot().getRowCount();
                }
                timer.stop();

                System.out.println("Total rows: " + totalRows);
            }
        } catch (Exception e) {
            System.err.println("Local scan error: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        System.out.println("=== S3 Dataset Operations ===\n");

        demonstrateS3Concepts();

        try (BufferAllocator allocator = new RootAllocator()) {
            // Attempt S3 access (may fail without credentials)
            System.out.println("\n--- Attempting S3 Operations ---");
            System.out.println("(These may fail without proper AWS configuration)\n");

            // Uncomment to test with actual S3:
            // timingTest(allocator);
            // computeMean(allocator);
            // scanFragments(allocator);

            // Local fallback demo
            String localPath = "../../sample_data/";
            System.out.println("\n--- Local Dataset Demo ---");
            localFileFallback(allocator, localPath);
        }
    }
}
