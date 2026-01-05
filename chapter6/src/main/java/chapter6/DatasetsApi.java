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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Java equivalent of datasets_api.cc - demonstrates Arrow Dataset API usage
 */
public class DatasetsApi {

    /**
     * Create a sample table with columns a, b, c
     */
    public static VectorSchemaRoot createTable(BufferAllocator allocator) {
        Schema schema = new Schema(Arrays.asList(
            Field.nullable("a", new ArrowType.Int(64, true)),
            Field.nullable("b", new ArrowType.Int(64, true)),
            Field.nullable("c", new ArrowType.Int(64, true))
        ));

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        BigIntVector vectorA = (BigIntVector) root.getVector("a");
        BigIntVector vectorB = (BigIntVector) root.getVector("b");
        BigIntVector vectorC = (BigIntVector) root.getVector("c");

        int numRows = 10;
        vectorA.allocateNew(numRows);
        vectorB.allocateNew(numRows);
        vectorC.allocateNew(numRows);

        for (int i = 0; i < numRows; i++) {
            vectorA.set(i, i);           // 0, 1, 2, ..., 9
            vectorB.set(i, 9 - i);       // 9, 8, 7, ..., 0
            vectorC.set(i, (i % 2) + 1); // 1, 2, 1, 2, ...
        }

        root.setRowCount(numRows);
        return root;
    }

    /**
     * Scan a dataset directory
     */
    public static void scanDataset(BufferAllocator allocator, String basePath) {
        try {
            String uri = "file://" + Paths.get(basePath).toAbsolutePath();
            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                System.out.println("Schema: " + reader.getVectorSchemaRoot().getSchema());

                int totalRows = 0;
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    totalRows += root.getRowCount();
                    System.out.println("Batch with " + root.getRowCount() + " rows");
                }
                System.out.println("Total rows: " + totalRows);
            }
        } catch (Exception e) {
            System.err.println("Error scanning dataset: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Demonstrate filter and select with Dataset API
     * Note: Java Dataset API filtering is done via ScanOptions
     */
    public static void filterAndSelect(BufferAllocator allocator, String basePath) {
        try {
            String uri = "file://" + Paths.get(basePath).toAbsolutePath();

            // ScanOptions with column projection
            // Note: Actual filtering in Java Arrow is more limited than C++
            String[] columns = {"b"};  // Project only column 'b'
            ScanOptions options = new ScanOptions(32768, java.util.Optional.of(columns));

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                System.out.println("\n=== Filter and Select ===");
                System.out.println("Projected columns: b");
                System.out.println("Filter: b < 4 (applied in code)");

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    BigIntVector bVector = (BigIntVector) root.getVector("b");

                    System.out.println("\nFiltered results (b < 4):");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        long value = bVector.get(i);
                        if (value < 4) {
                            System.out.println("  b = " + value);
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
     * Demonstrate derived columns and renaming
     */
    public static void deriveAndRename(BufferAllocator allocator, String basePath) {
        try {
            String uri = "file://" + Paths.get(basePath).toAbsolutePath();
            ScanOptions options = new ScanOptions(32768);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                System.out.println("\n=== Derive and Rename ===");
                System.out.println("Adding derived columns: b_as_float32, b_large (b > 1)");

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();

                    BigIntVector aVector = (BigIntVector) root.getVector("a");
                    BigIntVector bVector = (BigIntVector) root.getVector("b");
                    BigIntVector cVector = (BigIntVector) root.getVector("c");

                    System.out.println("\nResults with derived columns:");
                    System.out.printf("%-5s %-5s %-5s %-15s %-10s%n",
                        "a", "b", "c", "b_as_float32", "b_large");

                    for (int i = 0; i < Math.min(root.getRowCount(), 10); i++) {
                        long a = aVector.get(i);
                        long b = bVector.get(i);
                        long c = cVector.get(i);
                        float bAsFloat = (float) b;
                        boolean bLarge = b > 1;

                        System.out.printf("%-5d %-5d %-5d %-15.1f %-10s%n",
                            a, b, c, bAsFloat, bLarge);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Explain Java Dataset API concepts
     */
    public static void explainDatasetApi() {
        System.out.println("=== Arrow Dataset API in Java ===\n");

        System.out.println("The Arrow Dataset API provides:");
        System.out.println("1. Unified access to various file formats (Parquet, CSV, ORC, IPC)");
        System.out.println("2. Efficient columnar scanning with predicate pushdown");
        System.out.println("3. Multi-file and partitioned dataset support");
        System.out.println("4. Memory-efficient batch processing\n");

        System.out.println("Key classes in Java:");
        System.out.println("- DatasetFactory: Creates datasets from URIs or filesystem");
        System.out.println("- Dataset: Represents a collection of data");
        System.out.println("- Scanner: Configures and executes scans");
        System.out.println("- ScanOptions: Specifies batch size, column projection, etc.");
        System.out.println("- ArrowReader: Reads record batches from scans\n");

        System.out.println("Differences from C++:");
        System.out.println("- Some C++ features like compute expressions in filters");
        System.out.println("  are handled differently in Java");
        System.out.println("- Java uses JNI to call native C++ implementation");
        System.out.println("- Column projection via ScanOptions constructor\n");
    }

    public static void main(String[] args) {
        explainDatasetApi();

        try (BufferAllocator allocator = new RootAllocator()) {
            // Create sample data
            System.out.println("=== Creating Sample Data ===");
            try (VectorSchemaRoot table = createTable(allocator)) {
                System.out.println("Created table:");
                System.out.println(table.contentToTSVString());
            }

            // If you have a parquet dataset directory, scan it
            String samplePath = "/tmp/sample/parquet_dataset";
            Path dataPath = Paths.get(samplePath);

            if (Files.exists(dataPath)) {
                System.out.println("\n=== Scanning Dataset ===");
                scanDataset(allocator, samplePath);
                filterAndSelect(allocator, samplePath);
                deriveAndRename(allocator, samplePath);
            } else {
                System.out.println("\nNote: Create a parquet dataset at " + samplePath);
                System.out.println("to test the scan, filter, and derive operations.");
            }
        }
    }
}
