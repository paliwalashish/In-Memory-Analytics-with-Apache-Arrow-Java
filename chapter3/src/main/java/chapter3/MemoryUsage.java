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
package chapter3;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Java equivalent of chapter3/python/memory_usage.py
 * Compares memory consumption across different data formats and reading methods
 */
public class MemoryUsage {

    private static final String CSV_FILE = "sample_data/yellow_tripdata_2015-01.csv";
    private static final String PARQUET_FILE = "sample_data/yellow_tripdata_2015-01.parquet";
    private static final String ARROW_FILE = "sample_data/yellow_tripdata_2015-01.arrow";

    /**
     * Get current heap memory usage in MB
     */
    private static long getMemoryUsageMB() {
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    }

    /**
     * Force garbage collection and get memory
     */
    private static long getMemoryAfterGC() {
        System.gc();
        try { Thread.sleep(100); } catch (InterruptedException e) {}
        return getMemoryUsageMB();
    }

    /**
     * Read CSV file using basic Java I/O and compute mean
     */
    public static void readCsvBasic(String filepath, String column) {
        System.out.println("\n--- Reading CSV (Basic Java I/O) ---");
        long memBefore = getMemoryAfterGC();

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            String header = reader.readLine();
            String[] columns = header.split(",");

            // Find column index
            int colIndex = -1;
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].trim().equals(column)) {
                    colIndex = i;
                    break;
                }
            }

            if (colIndex < 0) {
                System.out.println("Column not found: " + column);
                return;
            }

            // Read values and compute mean
            double sum = 0;
            long count = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length > colIndex && !values[colIndex].isEmpty()) {
                    try {
                        sum += Double.parseDouble(values[colIndex]);
                        count++;
                    } catch (NumberFormatException e) {
                        // Skip invalid values
                    }
                }
            }

            double mean = count > 0 ? sum / count : 0;
            long memAfter = getMemoryAfterGC();

            System.out.println("Mean: " + mean);
            System.out.println("Rows: " + count);
            System.out.println("Memory overhead: " + (memAfter - memBefore) + " MB");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * Read CSV using Arrow Dataset API with column projection
     */
    public static void readCsvArrow(BufferAllocator allocator, String filepath, String column) {
        System.out.println("\n--- Reading CSV (Arrow Dataset API) ---");
        long memBefore = getMemoryAfterGC();

        try {
            String uri = "file://" + Paths.get(filepath).toAbsolutePath();
            String[] columns = {column};
            ScanOptions options = new ScanOptions(32768, Optional.of(columns));

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.CSV, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                double sum = 0;
                long count = 0;

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    Float8Vector vector = (Float8Vector) root.getVector(column);

                    if (vector != null) {
                        for (int i = 0; i < vector.getValueCount(); i++) {
                            if (!vector.isNull(i)) {
                                sum += vector.get(i);
                                count++;
                            }
                        }
                    }
                }

                double mean = count > 0 ? sum / count : 0;
                long memAfter = getMemoryAfterGC();

                System.out.println("Mean: " + mean);
                System.out.println("Rows: " + count);
                System.out.println("Memory overhead: " + (memAfter - memBefore) + " MB");
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * Read Parquet using Arrow Dataset API with column projection
     */
    public static void readParquetArrow(BufferAllocator allocator, String filepath, String column) {
        System.out.println("\n--- Reading Parquet (Arrow Dataset API) ---");
        long memBefore = getMemoryAfterGC();

        try {
            String uri = "file://" + Paths.get(filepath).toAbsolutePath();
            String[] columns = {column};
            ScanOptions options = new ScanOptions(32768, Optional.of(columns));

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                double sum = 0;
                long count = 0;

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    Float8Vector vector = (Float8Vector) root.getVector(column);

                    if (vector != null) {
                        for (int i = 0; i < vector.getValueCount(); i++) {
                            if (!vector.isNull(i)) {
                                sum += vector.get(i);
                                count++;
                            }
                        }
                    }
                }

                double mean = count > 0 ? sum / count : 0;
                long memAfter = getMemoryAfterGC();

                System.out.println("Mean: " + mean);
                System.out.println("Rows: " + count);
                System.out.println("Memory overhead: " + (memAfter - memBefore) + " MB");
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * Read Arrow IPC file
     */
    public static void readArrowIpc(BufferAllocator allocator, String filepath, String column) {
        System.out.println("\n--- Reading Arrow IPC File ---");
        long memBefore = getMemoryAfterGC();

        try (FileInputStream fis = new FileInputStream(filepath);
             ArrowFileReader reader = new ArrowFileReader(fis.getChannel(), allocator)) {

            double sum = 0;
            long count = 0;

            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                Float8Vector vector = (Float8Vector) root.getVector(column);

                if (vector != null) {
                    for (int i = 0; i < vector.getValueCount(); i++) {
                        if (!vector.isNull(i)) {
                            sum += vector.get(i);
                            count++;
                        }
                    }
                }
            }

            double mean = count > 0 ? sum / count : 0;
            long memAfter = getMemoryAfterGC();

            System.out.println("Mean: " + mean);
            System.out.println("Rows: " + count);
            System.out.println("Memory overhead: " + (memAfter - memBefore) + " MB");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * Read Arrow IPC file using memory mapping (zero-copy potential)
     */
    public static void readArrowMemoryMapped(BufferAllocator allocator, String filepath, String column) {
        System.out.println("\n--- Reading Arrow IPC (Memory Mapped) ---");
        long memBefore = getMemoryAfterGC();

        try (RandomAccessFile raf = new RandomAccessFile(filepath, "r");
             ArrowFileReader reader = new ArrowFileReader(raf.getChannel(), allocator)) {

            double sum = 0;
            long count = 0;

            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                Float8Vector vector = (Float8Vector) root.getVector(column);

                if (vector != null) {
                    for (int i = 0; i < vector.getValueCount(); i++) {
                        if (!vector.isNull(i)) {
                            sum += vector.get(i);
                            count++;
                        }
                    }
                }
            }

            double mean = count > 0 ? sum / count : 0;
            long memAfter = getMemoryAfterGC();

            System.out.println("Mean: " + mean);
            System.out.println("Rows: " + count);
            System.out.println("Memory overhead: " + (memAfter - memBefore) + " MB");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * Explain memory usage comparison
     */
    public static void explainMemoryComparison() {
        System.out.println("=== Memory Usage Comparison ===\n");

        System.out.println("This benchmark compares memory consumption across:");
        System.out.println("1. Basic Java CSV reading (full column in memory)");
        System.out.println("2. Arrow CSV reading (columnar, streaming)");
        System.out.println("3. Parquet reading (compressed, columnar)");
        System.out.println("4. Arrow IPC file (direct access)");
        System.out.println("5. Arrow IPC memory-mapped (zero-copy potential)\n");

        System.out.println("Expected results:");
        System.out.println("- Basic CSV: Highest memory (stores strings + parsing)");
        System.out.println("- Arrow CSV: Lower (streaming batches)");
        System.out.println("- Parquet: Low (compressed, column projection)");
        System.out.println("- Arrow IPC: Low (native format)");
        System.out.println("- Memory-mapped: Lowest (OS manages pages)\n");
    }

    public static void main(String[] args) {
        explainMemoryComparison();

        String column = "total_amount";

        try (BufferAllocator allocator = new RootAllocator()) {
            System.out.println("=== Running Memory Benchmarks ===");
            System.out.println("Column: " + column);

            // Check files exist
            boolean hasCSV = Paths.get(CSV_FILE).toFile().exists();
            boolean hasParquet = Paths.get(PARQUET_FILE).toFile().exists();
            boolean hasArrow = Paths.get(ARROW_FILE).toFile().exists();

            System.out.println("\nFile availability:");
            System.out.println("  CSV: " + (hasCSV ? "Found" : "Not found"));
            System.out.println("  Parquet: " + (hasParquet ? "Found" : "Not found"));
            System.out.println("  Arrow IPC: " + (hasArrow ? "Found" : "Not found"));

            if (hasCSV) {
                readCsvBasic(CSV_FILE, column);
                readCsvArrow(allocator, CSV_FILE, column);
            }

            if (hasParquet) {
                readParquetArrow(allocator, PARQUET_FILE, column);
            }

            if (hasArrow) {
                readArrowIpc(allocator, ARROW_FILE, column);
                readArrowMemoryMapped(allocator, ARROW_FILE, column);
            }

            System.out.println("\n=== Benchmark Complete ===");
        }
    }
}
