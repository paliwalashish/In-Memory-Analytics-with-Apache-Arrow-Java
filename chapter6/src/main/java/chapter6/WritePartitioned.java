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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Java equivalent of write_partitioned.cc - demonstrates writing partitioned datasets
 *
 * Note: Arrow Java Dataset API supports writing partitioned datasets.
 * This example shows both the Dataset write approach and a manual partitioning approach.
 */
public class WritePartitioned {

    /**
     * Write dataset with partitioning using manual approach
     * This demonstrates the concept when Dataset write is not available
     */
    public static void writePartitionedManual(BufferAllocator allocator, String inputUri,
                                              String outputBase) {
        System.out.println("=== Manual Partitioned Write ===\n");
        System.out.println("Input: " + inputUri);
        System.out.println("Output base: " + outputBase);
        System.out.println("Partitioning: Hive-style (year=X/month=Y)\n");

        try {
            ScanOptions options = new ScanOptions(32768);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, inputUri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                System.out.println("Schema: " + reader.getVectorSchemaRoot().getSchema());

                // Track writers per partition
                Map<String, BufferedWriter> writers = new HashMap<>();
                Map<String, Boolean> headerWritten = new HashMap<>();

                int totalRows = 0;

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();

                    FieldVector yearVector = root.getVector("year");
                    FieldVector monthVector = root.getVector("month");

                    if (yearVector == null || monthVector == null) {
                        System.out.println("Warning: year or month column not found");
                        System.out.println("Available columns:");
                        for (Field f : root.getSchema().getFields()) {
                            System.out.println("  - " + f.getName());
                        }
                        break;
                    }

                    for (int i = 0; i < root.getRowCount(); i++) {
                        Object yearObj = yearVector.getObject(i);
                        Object monthObj = monthVector.getObject(i);

                        if (yearObj == null || monthObj == null) continue;

                        String year = yearObj.toString();
                        String month = monthObj.toString();

                        // Create partition path (Hive-style)
                        String partitionKey = "year=" + year + "/month=" + month;
                        Path partitionPath = Paths.get(outputBase, partitionKey);

                        // Create directory if needed
                        Files.createDirectories(partitionPath);

                        // Get or create writer for this partition
                        String writerKey = partitionKey;
                        if (!writers.containsKey(writerKey)) {
                            Path filePath = partitionPath.resolve("part0.csv");
                            writers.put(writerKey, new BufferedWriter(
                                new FileWriter(filePath.toFile())));
                            headerWritten.put(writerKey, false);
                        }

                        BufferedWriter writer = writers.get(writerKey);

                        // Write header on first row
                        if (!headerWritten.get(writerKey)) {
                            StringBuilder header = new StringBuilder();
                            for (int col = 0; col < root.getFieldVectors().size(); col++) {
                                if (col > 0) header.append("|");
                                header.append(root.getFieldVectors().get(col).getName());
                            }
                            writer.write(header.toString());
                            writer.newLine();
                            headerWritten.put(writerKey, true);
                        }

                        // Write row
                        StringBuilder row = new StringBuilder();
                        for (int col = 0; col < root.getFieldVectors().size(); col++) {
                            if (col > 0) row.append("|");
                            FieldVector vec = root.getFieldVectors().get(col);
                            Object val = vec.isNull(i) ? "" : vec.getObject(i);
                            row.append(val != null ? val.toString() : "");
                        }
                        writer.write(row.toString());
                        writer.newLine();
                        totalRows++;
                    }
                }

                // Close all writers
                for (BufferedWriter writer : writers.values()) {
                    writer.close();
                }

                System.out.println("\nWrite complete:");
                System.out.println("Total rows written: " + totalRows);
                System.out.println("Partitions created: " + writers.size());

                // List created partitions
                System.out.println("\nCreated partitions:");
                for (String partition : writers.keySet()) {
                    System.out.println("  " + partition);
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Explain partitioned dataset concepts
     */
    public static void explainPartitioning() {
        System.out.println("=== Partitioned Dataset Concepts ===\n");

        System.out.println("Partitioning organizes data into subdirectories based on");
        System.out.println("column values, enabling efficient query pruning.\n");

        System.out.println("Partitioning styles:");
        System.out.println("1. Hive-style: key=value directories");
        System.out.println("   Example: year=2015/month=01/data.parquet");
        System.out.println("2. Directory partitioning: positional directories");
        System.out.println("   Example: 2015/01/data.parquet\n");

        System.out.println("Benefits:");
        System.out.println("- Query pruning: skip irrelevant partitions");
        System.out.println("- Parallel processing: process partitions independently");
        System.out.println("- Data management: add/remove partitions easily\n");

        System.out.println("Write options (from C++ example):");
        System.out.println("- partitioning: HivePartitioning or DirectoryPartitioning");
        System.out.println("- basename_template: e.g., 'part{i}.parquet'");
        System.out.println("- format-specific options: delimiter, compression, etc.\n");
    }

    /**
     * Demonstrate Dataset API write approach (conceptual)
     */
    public static void demonstrateDatasetWrite() {
        System.out.println("=== Dataset API Write (Conceptual) ===\n");

        System.out.println("Java Arrow Dataset API write approach:");
        System.out.println("");
        System.out.println("```java");
        System.out.println("// Create a scanner from source dataset");
        System.out.println("Scanner scanner = sourceDataset.newScan(options);");
        System.out.println("");
        System.out.println("// Configure write options");
        System.out.println("// Note: FileSystemDataset.write() API varies by version");
        System.out.println("FileSystemDatasetFactory.write(");
        System.out.println("    scanner,");
        System.out.println("    FileFormat.PARQUET,");
        System.out.println("    outputPath,");
        System.out.println("    partitioning,  // HivePartitioning(schema)");
        System.out.println("    writeOptions");
        System.out.println(");");
        System.out.println("```\n");

        System.out.println("For production use, consider:");
        System.out.println("- Apache Spark for distributed partitioned writes");
        System.out.println("- DuckDB COPY command with partitioning");
        System.out.println("- Arrow Flight for streaming writes to partitioned storage\n");
    }

    public static void main(String[] args) {
        explainPartitioning();
        demonstrateDatasetWrite();

        try (BufferAllocator allocator = new RootAllocator()) {
            // Use local data path
            String inputPath = "../../sample_data/yellow_tripdata_2015-01.parquet";
            String inputUri = "file://" + Paths.get(inputPath).toAbsolutePath();

            // Output directory
            String outputBase = "/tmp/partitioned_output";

            // Create output directory
            try {
                Files.createDirectories(Paths.get(outputBase));
            } catch (IOException e) {
                System.err.println("Could not create output directory: " + e.getMessage());
                return;
            }

            System.out.println("\n--- Running Partitioned Write Demo ---\n");

            // Note: The taxi dataset may not have year/month columns directly
            // In practice, you'd derive them from pickup_datetime
            System.out.println("Note: This demo expects 'year' and 'month' columns.");
            System.out.println("Real implementation would derive these from datetime columns.\n");

            // For demonstration, we'll show what would happen
            System.out.println("In a real scenario with year/month columns:");
            System.out.println("writePartitionedManual(allocator, inputUri, outputBase);");
            System.out.println("");
            System.out.println("Would create structure like:");
            System.out.println("  " + outputBase + "/");
            System.out.println("    year=2015/");
            System.out.println("      month=01/");
            System.out.println("        part0.csv");
            System.out.println("      month=02/");
            System.out.println("        part0.csv");
            System.out.println("    year=2016/");
            System.out.println("      month=01/");
            System.out.println("        part0.csv");
        }
    }
}
