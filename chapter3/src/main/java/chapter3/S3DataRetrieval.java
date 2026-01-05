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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

/**
 * Java equivalent of chapter3/go/main.go
 * Demonstrates S3 data retrieval and Arrow conversion
 */
public class S3DataRetrieval {

    private static final String BUCKET = "dataforgood-fb-data";
    private static final String KEY = "csv/month=2019-06/country=ZWE/type=children_under_five/ZWE_children_under_five.csv.gz";
    private static final String OUTPUT_FILE = "tripdata.arrow";

    /**
     * Define the Arrow schema for the data
     */
    private static Schema createSchema() {
        return new Schema(Arrays.asList(
            Field.nullable("latitude", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Field.nullable("longitude", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Field.nullable("population", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
        ));
    }

    /**
     * Download gzip-compressed CSV from S3 and convert to Arrow IPC format
     */
    public static void downloadAndConvert() {
        System.out.println("=== S3 Data Retrieval and Arrow Conversion ===\n");
        System.out.println("Bucket: " + BUCKET);
        System.out.println("Key: " + KEY);
        System.out.println("Output: " + OUTPUT_FILE + "\n");

        try (S3Client s3Client = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build()) {

            System.out.println("Downloading from S3...");

            GetObjectRequest request = GetObjectRequest.builder()
                .bucket(BUCKET)
                .key(KEY)
                .build();

            try (ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(request);
                 GZIPInputStream gzipStream = new GZIPInputStream(s3Object);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream));
                 BufferAllocator allocator = new RootAllocator()) {

                Schema schema = createSchema();

                // Skip header line
                String header = reader.readLine();
                System.out.println("Header: " + header);

                // Create Arrow file for writing
                try (FileOutputStream fos = new FileOutputStream(OUTPUT_FILE);
                     FileChannel channel = fos.getChannel();
                     VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                     ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {

                    Float8Vector latVector = (Float8Vector) root.getVector("latitude");
                    Float8Vector lonVector = (Float8Vector) root.getVector("longitude");
                    Float8Vector popVector = (Float8Vector) root.getVector("population");

                    writer.start();

                    int batchSize = 500000;
                    int rowIndex = 0;
                    int totalRows = 0;
                    String line;

                    latVector.allocateNew(batchSize);
                    lonVector.allocateNew(batchSize);
                    popVector.allocateNew(batchSize);

                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length >= 3) {
                            try {
                                double lat = Double.parseDouble(parts[0]);
                                double lon = Double.parseDouble(parts[1]);
                                double pop = Double.parseDouble(parts[2]);

                                latVector.setSafe(rowIndex, lat);
                                lonVector.setSafe(rowIndex, lon);
                                popVector.setSafe(rowIndex, pop);
                                rowIndex++;
                                totalRows++;

                                // Write batch when full
                                if (rowIndex >= batchSize) {
                                    root.setRowCount(rowIndex);
                                    writer.writeBatch();
                                    System.out.println("Wrote batch of " + rowIndex + " rows");
                                    rowIndex = 0;
                                }
                            } catch (NumberFormatException e) {
                                // Skip malformed rows
                            }
                        }
                    }

                    // Write remaining rows
                    if (rowIndex > 0) {
                        root.setRowCount(rowIndex);
                        writer.writeBatch();
                        System.out.println("Wrote final batch of " + rowIndex + " rows");
                    }

                    writer.end();
                    System.out.println("\nTotal rows written: " + totalRows);
                    System.out.println("Output file: " + OUTPUT_FILE);
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            demonstrateS3Concepts();
        }
    }

    /**
     * Demonstrate concepts when S3 access fails
     */
    private static void demonstrateS3Concepts() {
        System.out.println("\n=== S3 Data Retrieval Concepts ===\n");

        System.out.println("This example demonstrates:");
        System.out.println("1. Downloading gzip-compressed CSV from AWS S3");
        System.out.println("2. Using anonymous credentials for public buckets");
        System.out.println("3. Converting CSV data to Apache Arrow IPC format");
        System.out.println("4. Writing Arrow data in batches for memory efficiency\n");

        System.out.println("Key components:");
        System.out.println("- S3Client: AWS SDK for Java S3 client");
        System.out.println("- GZIPInputStream: Decompress gzip data on the fly");
        System.out.println("- VectorSchemaRoot: Arrow record batch container");
        System.out.println("- ArrowFileWriter: Write Arrow IPC file format\n");

        System.out.println("Schema definition:");
        System.out.println("  - latitude: float64 (nullable)");
        System.out.println("  - longitude: float64 (nullable)");
        System.out.println("  - population: float64 (nullable)\n");
    }

    /**
     * Alternative: Use Arrow Dataset API for S3 (if supported)
     */
    public static void demonstrateDatasetS3() {
        System.out.println("\n=== Arrow Dataset S3 Access ===\n");

        System.out.println("Arrow Dataset API can also read from S3:");
        System.out.println("```java");
        System.out.println("String uri = \"s3://bucket-name/path/to/data.parquet\";");
        System.out.println("DatasetFactory factory = new FileSystemDatasetFactory(");
        System.out.println("    allocator, NativeMemoryPool.getDefault(),");
        System.out.println("    FileFormat.PARQUET, uri);");
        System.out.println("Dataset dataset = factory.finish();");
        System.out.println("Scanner scanner = dataset.newScan(options);");
        System.out.println("```\n");
    }

    public static void main(String[] args) {
        downloadAndConvert();
        demonstrateDatasetS3();
    }
}
