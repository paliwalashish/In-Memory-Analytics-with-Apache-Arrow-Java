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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;

/**
 * Java equivalent of simple_acero.cc - demonstrates query execution concepts
 *
 * Note: Arrow Java doesn't have Acero (the C++ streaming execution engine).
 * Instead, we demonstrate similar concepts using:
 * 1. Arrow Dataset API with projections and filters
 * 2. Manual stream processing
 * 3. Integration points with query engines like DuckDB or DataFusion
 */
public class SimpleAcero {

    /**
     * Simple projection - select specific columns from data
     * Equivalent to Acero's project node
     */
    public static void simpleProjection(String filepath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String uri = "file://" + Paths.get(filepath).toAbsolutePath();
            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                Schema fullSchema = reader.getVectorSchemaRoot().getSchema();
                System.out.println("Full schema: " + fullSchema.getFields().stream()
                    .map(Field::getName).collect(Collectors.joining(", ")));

                // Project specific columns (like Acero project node)
                List<String> projectedColumns = Arrays.asList("name", "species", "homeworld");
                System.out.println("\nProjected columns: " + String.join(", ", projectedColumns));

                int batchCount = 0;
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    batchCount++;

                    if (batchCount == 1) {
                        System.out.println("\n=== Projected Results (first batch) ===");

                        // Print header
                        for (String col : projectedColumns) {
                            if (root.getSchema().findField(col) != null) {
                                System.out.printf("%-20s", col);
                            }
                        }
                        System.out.println();

                        // Print rows
                        for (int row = 0; row < Math.min(10, root.getRowCount()); row++) {
                            for (String col : projectedColumns) {
                                FieldVector vector = root.getVector(col);
                                if (vector != null) {
                                    Object value = vector.getObject(row);
                                    String strValue = value != null ? value.toString() : "null";
                                    System.out.printf("%-20s", strValue.length() > 18 ?
                                        strValue.substring(0, 15) + "..." : strValue);
                                }
                            }
                            System.out.println();
                        }
                    }
                }

                System.out.println("\nTotal batches processed: " + batchCount);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Aggregation - group by and aggregate
     * Equivalent to Acero's aggregate node
     */
    public static void aggregation(String filepath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String uri = "file://" + Paths.get(filepath).toAbsolutePath();
            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                // Aggregate: GROUP BY homeworld, collect list of names
                Map<String, List<String>> aggregation = new HashMap<>();

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();

                    FieldVector homeworldVector = root.getVector("homeworld");
                    FieldVector nameVector = root.getVector("name");

                    if (homeworldVector != null && nameVector != null) {
                        for (int i = 0; i < root.getRowCount(); i++) {
                            Object hwObj = homeworldVector.getObject(i);
                            Object nameObj = nameVector.getObject(i);

                            String homeworld = hwObj != null ? hwObj.toString() : "unknown";
                            String name = nameObj != null ? nameObj.toString() : "unknown";

                            aggregation.computeIfAbsent(homeworld, k -> new ArrayList<>()).add(name);
                        }
                    }
                }

                System.out.println("\n=== Aggregation Results ===");
                System.out.println("GROUP BY homeworld, hash_list(name):\n");

                aggregation.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .limit(10)
                    .forEach(entry -> {
                        System.out.printf("%-20s: %s%n", entry.getKey(),
                            entry.getValue().stream().limit(5).collect(Collectors.joining(", ")) +
                            (entry.getValue().size() > 5 ? "..." : ""));
                    });
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Filter and aggregate sequence
     * Equivalent to Acero's Declaration::Sequence with filter and aggregate nodes
     */
    public static void filterAndAggregate(String filepath) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String uri = "file://" + Paths.get(filepath).toAbsolutePath();
            ScanOptions options = new ScanOptions(/*batchSize*/ 32768);

            // Exclusion list (like is_in with invert in C++)
            Set<String> exclusions = new HashSet<>(Arrays.asList("Skako", "Utapau", "Nal Hutta"));

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                // Aggregate with filter: exclude certain homeworlds
                Map<String, AggregateResult> aggregation = new HashMap<>();

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();

                    FieldVector homeworldVector = root.getVector("homeworld");
                    FieldVector nameVector = root.getVector("name");
                    FieldVector speciesVector = root.getVector("species");
                    FieldVector heightVector = root.getVector("height");

                    if (homeworldVector != null && nameVector != null) {
                        for (int i = 0; i < root.getRowCount(); i++) {
                            Object hwObj = homeworldVector.getObject(i);
                            String homeworld = hwObj != null ? hwObj.toString() : "unknown";

                            // Filter: exclude certain homeworlds
                            if (exclusions.contains(homeworld)) {
                                continue;
                            }

                            Object nameObj = nameVector.getObject(i);
                            Object speciesObj = speciesVector != null ? speciesVector.getObject(i) : null;
                            Object heightObj = heightVector != null ? heightVector.getObject(i) : null;

                            String name = nameObj != null ? nameObj.toString() : "unknown";
                            String species = speciesObj != null ? speciesObj.toString() : "unknown";
                            Double height = null;
                            if (heightObj instanceof Number) {
                                height = ((Number) heightObj).doubleValue();
                            }

                            AggregateResult result = aggregation.computeIfAbsent(
                                homeworld, k -> new AggregateResult());
                            result.names.add(name);
                            result.species.add(species);
                            if (height != null) {
                                result.heights.add(height);
                            }
                        }
                    }
                }

                System.out.println("\n=== Filter + Aggregate Results ===");
                System.out.println("Excluded homeworlds: " + exclusions);
                System.out.println("\nGROUP BY homeworld:\n");

                System.out.printf("%-20s %-30s %-30s %s%n",
                    "Homeworld", "Names", "Species", "Avg Height");
                System.out.println("-".repeat(100));

                aggregation.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .limit(10)
                    .forEach(entry -> {
                        AggregateResult r = entry.getValue();
                        double avgHeight = r.heights.isEmpty() ? 0 :
                            r.heights.stream().mapToDouble(Double::doubleValue).average().orElse(0);

                        System.out.printf("%-20s %-30s %-30s %.1f%n",
                            entry.getKey(),
                            r.names.stream().limit(3).collect(Collectors.joining(",")) +
                                (r.names.size() > 3 ? "..." : ""),
                            r.species.stream().distinct().limit(3).collect(Collectors.joining(",")) +
                                (r.species.stream().distinct().count() > 3 ? "..." : ""),
                            avgHeight);
                    });
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Helper class for aggregation results
     */
    static class AggregateResult {
        List<String> names = new ArrayList<>();
        List<String> species = new ArrayList<>();
        List<Double> heights = new ArrayList<>();
    }

    /**
     * Explain Acero concepts in Java context
     */
    public static void explainAceroConcepts() {
        System.out.println("=== Acero Query Execution Concepts in Java ===\n");

        System.out.println("Apache Arrow Acero is a C++ streaming query execution engine.");
        System.out.println("In Java, similar functionality can be achieved through:\n");

        System.out.println("1. Arrow Dataset API");
        System.out.println("   - Native support for scan, project, filter");
        System.out.println("   - Uses underlying C++ implementation via JNI");
        System.out.println("   - Demonstrated in the examples above\n");

        System.out.println("2. Gandiva Expression Compiler");
        System.out.println("   - JIT compilation of filter/project expressions");
        System.out.println("   - High performance LLVM-based execution\n");

        System.out.println("3. Integration with Query Engines");
        System.out.println("   - DuckDB: In-process SQL with Arrow support");
        System.out.println("   - DataFusion: Rust-based, accessible via Arrow Flight");
        System.out.println("   - Apache Spark: Distributed processing with Arrow\n");

        System.out.println("4. Manual Stream Processing");
        System.out.println("   - Process record batches in a streaming fashion");
        System.out.println("   - Build custom aggregations and transformations");
        System.out.println("   - Demonstrated in the examples above\n");
    }

    public static void main(String[] args) {
        String filepath = "../../sample_data/starwars.parquet";

        explainAceroConcepts();

        System.out.println("=== Running Query Examples ===");
        System.out.println("File: " + filepath + "\n");

        System.out.println("--- Simple Projection ---");
        simpleProjection(filepath);

        System.out.println("\n--- Aggregation (GROUP BY) ---");
        aggregation(filepath);

        System.out.println("\n--- Filter + Aggregate ---");
        filterAndAggregate(filepath);
    }
}
