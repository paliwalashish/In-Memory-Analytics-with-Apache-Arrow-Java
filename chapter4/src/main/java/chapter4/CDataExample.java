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
package chapter4;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;
import java.util.Random;

/**
 * Java equivalent of example_cdata.cc - demonstrates Arrow C Data Interface
 *
 * The Arrow C Data Interface provides a stable ABI for sharing Arrow data
 * across different language runtimes without copying data.
 */
public class CDataExample {

    /**
     * Generate random int32 data
     */
    public static int[] generateData(int size) {
        Random random = new Random();
        int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = random.nextInt();
        }
        return data;
    }

    /**
     * Export Arrow data using C Data Interface
     *
     * This demonstrates how to export Arrow vectors for use by other languages/libraries
     */
    public static void exportInt32Data(BufferAllocator allocator) {
        final int length = 1000;
        int[] data = generateData(length);

        // Create Arrow vector with the data
        try (IntVector vector = new IntVector("int32_data", allocator)) {
            vector.allocateNew(length);
            for (int i = 0; i < length; i++) {
                vector.set(i, data[i]);
            }
            vector.setValueCount(length);

            System.out.println("Created IntVector with " + length + " values");
            System.out.println("First 5 values: " +
                vector.get(0) + ", " + vector.get(1) + ", " + vector.get(2) + ", " +
                vector.get(3) + ", " + vector.get(4));

            // Export using C Data Interface
            try (ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                 ArrowArray arrowArray = ArrowArray.allocateNew(allocator)) {

                // Export the vector to C Data Interface structures
                Data.exportVector(allocator, vector, null, arrowArray, arrowSchema);

                System.out.println("\nExported to C Data Interface:");
                System.out.println("ArrowSchema address: " + arrowSchema.memoryAddress());
                System.out.println("ArrowArray address: " + arrowArray.memoryAddress());

                // Import back from C Data Interface (demonstrating round-trip)
                try (IntVector importedVector = (IntVector) Data.importVector(
                        allocator, arrowArray, arrowSchema, null)) {

                    System.out.println("\nImported back from C Data Interface:");
                    System.out.println("Imported vector length: " + importedVector.getValueCount());
                    System.out.println("First 5 imported values: " +
                        importedVector.get(0) + ", " + importedVector.get(1) + ", " +
                        importedVector.get(2) + ", " + importedVector.get(3) + ", " +
                        importedVector.get(4));
                }
            }
        }
    }

    /**
     * Export a VectorSchemaRoot (record batch) using C Data Interface
     */
    public static void exportRecordBatch(BufferAllocator allocator) {
        // Create schema
        Schema schema = new Schema(Arrays.asList(
            Field.nullable("id", new ArrowType.Int(32, true)),
            Field.nullable("value", new ArrowType.Int(32, true))
        ));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector idVector = (IntVector) root.getVector("id");
            IntVector valueVector = (IntVector) root.getVector("value");

            // Populate data
            int numRows = 100;
            idVector.allocateNew(numRows);
            valueVector.allocateNew(numRows);

            for (int i = 0; i < numRows; i++) {
                idVector.set(i, i);
                valueVector.set(i, i * 10);
            }
            root.setRowCount(numRows);

            System.out.println("\n=== Record Batch Export ===");
            System.out.println("Created VectorSchemaRoot with " + numRows + " rows");

            // Export the record batch
            try (ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                 ArrowArray arrowArray = ArrowArray.allocateNew(allocator)) {

                Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema);

                System.out.println("Exported VectorSchemaRoot to C Data Interface");
                System.out.println("Schema address: " + arrowSchema.memoryAddress());
                System.out.println("Array address: " + arrowArray.memoryAddress());

                // Import back
                try (VectorSchemaRoot imported = Data.importVectorSchemaRoot(
                        allocator, arrowArray, arrowSchema, null)) {

                    System.out.println("\nImported VectorSchemaRoot:");
                    System.out.println("Row count: " + imported.getRowCount());
                    System.out.println("Schema: " + imported.getSchema());
                }
            }
        }
    }

    /**
     * Demonstrate interoperability concepts
     */
    public static void demonstrateInteroperability() {
        System.out.println("\n=== C Data Interface Interoperability ===");
        System.out.println("The Arrow C Data Interface enables:");
        System.out.println("1. Zero-copy data sharing between languages (Java <-> C++ <-> Python <-> R)");
        System.out.println("2. Stable ABI that doesn't change between Arrow versions");
        System.out.println("3. Integration with native libraries without serialization overhead");
        System.out.println("\nCommon use cases:");
        System.out.println("- Calling C/C++ libraries from Java via JNI");
        System.out.println("- Sharing data with Python (via PyArrow) through shared memory");
        System.out.println("- Integrating with database drivers that support Arrow");
        System.out.println("- GPU data transfer (through CUDA Arrow extensions)");
    }

    public static void main(String[] args) {
        try (BufferAllocator allocator = new RootAllocator()) {
            System.out.println("=== Arrow C Data Interface Example ===\n");

            exportInt32Data(allocator);
            exportRecordBatch(allocator);
            demonstrateInteroperability();
        }
    }
}
