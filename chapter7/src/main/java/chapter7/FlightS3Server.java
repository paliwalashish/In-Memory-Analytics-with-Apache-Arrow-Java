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
package chapter7;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Collections;

/**
 * Flight server that serves data from S3 (equivalent to Go/Python examples)
 * This demonstrates accessing S3-backed Parquet files through Flight
 */
public class FlightS3Server {

    private static final String BUCKET = "ursa-labs-taxi-data";
    private static final Region REGION = Region.US_EAST_2;

    /**
     * S3-backed Flight producer
     */
    static class S3FlightProducer extends NoOpFlightProducer {
        private final BufferAllocator allocator;
        private final S3Client s3Client;
        private final String bucket;

        public S3FlightProducer(BufferAllocator allocator) {
            this.allocator = allocator;
            this.s3Client = S3Client.builder()
                .region(REGION)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build();
            this.bucket = BUCKET;
        }

        @Override
        public void listFlights(CallContext context, Criteria criteria,
                               StreamListener<FlightInfo> listener) {
            try {
                String prefix = criteria.getExpression().length > 0
                    ? new String(criteria.getExpression())
                    : "";

                ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix)
                    .build();

                ListObjectsV2Response response = s3Client.listObjectsV2(request);

                for (S3Object obj : response.contents()) {
                    String key = obj.key();
                    if (!key.endsWith(".parquet")) {
                        continue;
                    }

                    try {
                        FlightInfo info = getFlightInfoForS3Key(key, obj.size());
                        listener.onNext(info);
                    } catch (Exception e) {
                        System.err.println("Error processing: " + key + " - " + e.getMessage());
                    }
                }

                listener.onCompleted();
            } catch (Exception e) {
                listener.onError(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
            }
        }

        private FlightInfo getFlightInfoForS3Key(String key, long size) throws Exception {
            String uri = "s3://" + bucket + "/" + key;

            // For S3 files, we'd normally read the schema from the file
            // This is a simplified version
            FlightDescriptor descriptor = FlightDescriptor.path(key);
            Ticket ticket = new Ticket(key.getBytes());
            FlightEndpoint endpoint = new FlightEndpoint(ticket);

            // Return with unknown row count (-1) since we don't read the file
            return new FlightInfo(
                null, // Schema would need to be read from file
                descriptor,
                Collections.singletonList(endpoint),
                -1, // Unknown row count
                size
            );
        }

        @Override
        public void getStream(CallContext context, Ticket ticket,
                             ServerStreamListener listener) {
            String key = new String(ticket.getBytes());
            String uri = "s3://" + bucket + "/" + key;

            try {
                ScanOptions options = new ScanOptions(100000);

                try (DatasetFactory factory = new FileSystemDatasetFactory(
                        allocator, NativeMemoryPool.getDefault(),
                        FileFormat.PARQUET, uri);
                     Dataset dataset = factory.finish();
                     Scanner scanner = dataset.newScan(options);
                     ArrowReader reader = scanner.scanBatches()) {

                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    listener.start(root);

                    int batchCount = 0;
                    while (reader.loadNextBatch()) {
                        listener.putNext();
                        batchCount++;
                    }

                    listener.completed();
                    System.out.println("Sent " + batchCount + " batches for: " + key);
                }
            } catch (Exception e) {
                listener.error(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
            }
        }

        public void close() {
            s3Client.close();
        }
    }

    /**
     * Demonstrate S3 Flight server concepts
     */
    public static void demonstrateS3Flight() {
        System.out.println("=== S3-Backed Arrow Flight Server ===\n");

        System.out.println("This example shows how to create a Flight server that:");
        System.out.println("1. Lists Parquet files from an S3 bucket");
        System.out.println("2. Serves file metadata as FlightInfo");
        System.out.println("3. Streams Parquet data to Flight clients\n");

        System.out.println("S3 Configuration:");
        System.out.println("  Bucket: " + BUCKET);
        System.out.println("  Region: " + REGION);
        System.out.println("  Auth: Anonymous (public bucket)\n");

        System.out.println("Client usage:");
        System.out.println("  1. ListFlights('2009') - List files from 2009");
        System.out.println("  2. GetFlightInfo(path) - Get file metadata");
        System.out.println("  3. DoGet(ticket) - Stream file data\n");

        System.out.println("Benefits of Flight + S3:");
        System.out.println("  - Centralized access control");
        System.out.println("  - Query pushdown potential");
        System.out.println("  - Caching layer");
        System.out.println("  - Cross-region optimization\n");
    }

    /**
     * Run client demo against the S3 server
     */
    public static void runS3ClientDemo(int port) {
        try (BufferAllocator allocator = new RootAllocator()) {
            Location location = Location.forGrpcInsecure("localhost", port);

            try (FlightClient client = FlightClient.builder(allocator, location).build()) {
                System.out.println("Listing flights for '2009'...");

                Criteria criteria = new Criteria("2009".getBytes());
                int count = 0;

                for (FlightInfo info : client.listFlights(criteria)) {
                    System.out.println("  " + info.getDescriptor().getPath());
                    count++;
                    if (count >= 5) {
                        System.out.println("  ... and more");
                        break;
                    }
                }

                System.out.println("Found " + count + "+ flights\n");
            }
        } catch (Exception e) {
            System.err.println("Client error: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        demonstrateS3Flight();

        System.out.println("=== Running S3 Flight Server Demo ===\n");
        System.out.println("Note: This requires network access to S3\n");

        try (BufferAllocator allocator = new RootAllocator()) {
            S3FlightProducer producer = new S3FlightProducer(allocator);

            Location location = Location.forGrpcInsecure("localhost", 0);

            try (FlightServer server = FlightServer.builder(allocator, location, producer).build()) {
                server.start();
                System.out.println("S3 Flight server started on port: " + server.getPort());

                // Run client demo
                runS3ClientDemo(server.getPort());

                System.out.println("Server shutting down...");
            } finally {
                producer.close();
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.out.println("\nNote: S3 access requires network connectivity.");
            System.out.println("The FlightServer example can be used with local files instead.");
        }
    }
}
