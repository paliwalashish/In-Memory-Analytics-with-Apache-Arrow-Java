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
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Java equivalent of chapter7/go/main.go and chapter7/python/flight_server.py
 * Implements an Apache Arrow Flight server and client for distributed data access
 */
public class DataFlightServer {

    private static final String DATA_DIR = "sample_data";

    /**
     * Custom Flight producer that serves Parquet files
     */
    static class ParquetFlightProducer extends NoOpFlightProducer {
        private final BufferAllocator allocator;
        private final String dataDir;

        public ParquetFlightProducer(BufferAllocator allocator, String dataDir) {
            this.allocator = allocator;
            this.dataDir = dataDir;
        }

        @Override
        public void listFlights(CallContext context, Criteria criteria,
                               StreamListener<FlightInfo> listener) {
            try {
                String prefix = criteria.getExpression().length > 0
                    ? new String(criteria.getExpression())
                    : "";

                Path dataPath = Paths.get(dataDir);
                if (!Files.exists(dataPath)) {
                    listener.onCompleted();
                    return;
                }

                // List all parquet files matching criteria
                List<Path> parquetFiles = Files.walk(dataPath)
                    .filter(p -> p.toString().endsWith(".parquet"))
                    .filter(p -> p.getFileName().toString().contains(prefix))
                    .collect(Collectors.toList());

                for (Path file : parquetFiles) {
                    try {
                        FlightInfo info = getFlightInfo(file);
                        listener.onNext(info);
                    } catch (Exception e) {
                        System.err.println("Error processing: " + file + " - " + e.getMessage());
                    }
                }

                listener.onCompleted();
            } catch (Exception e) {
                listener.onError(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
            }
        }

        private FlightInfo getFlightInfo(Path file) throws Exception {
            String uri = "file://" + file.toAbsolutePath();

            try (DatasetFactory factory = new FileSystemDatasetFactory(
                    allocator, NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET, uri);
                 Dataset dataset = factory.finish()) {
                ScanOptions scanOptions = new ScanOptions(100);
                Schema schema = dataset.newScan(scanOptions).schema();

                // Count rows
                ScanOptions options = new ScanOptions(32768);
                long totalRows = 0;
                try (Scanner scanner = dataset.newScan(options);
                     ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        totalRows += reader.getVectorSchemaRoot().getRowCount();
                    }
                }

                FlightDescriptor descriptor = FlightDescriptor.path(file.toString());
                Ticket ticket = new Ticket(file.toString().getBytes());
                FlightEndpoint endpoint = new FlightEndpoint(ticket);

                return new FlightInfo(schema, descriptor, Collections.singletonList(endpoint),
                    totalRows, -1);
            }
        }

        @Override
        public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
            try {
                String path = descriptor.getPath().get(0);
                return getFlightInfo(Paths.get(path));
            } catch (Exception e) {
                throw CallStatus.NOT_FOUND.withDescription(e.getMessage()).toRuntimeException();
            }
        }

        @Override
        public void getStream(CallContext context, Ticket ticket,
                             ServerStreamListener listener) {
            String path = new String(ticket.getBytes());

            try {
                String uri = "file://" + Paths.get(path).toAbsolutePath();
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
                    System.out.println("Sent " + batchCount + " batches for: " + path);
                }
            } catch (Exception e) {
                listener.error(CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException());
            }
        }
    }

    /**
     * Start the Flight server
     */
    public static FlightServer startServer(BufferAllocator allocator, Location location,
                                               String dataDir) throws IOException {
        ParquetFlightProducer producer = new ParquetFlightProducer(allocator, dataDir);
        return FlightServer.builder(allocator, location, producer).build();
    }

    /**
     * Run Flight client demo
     */
    public static void runClientDemo(Location serverLocation) {
        System.out.println("\n=== Flight Client Demo ===\n");

        try (BufferAllocator allocator = new RootAllocator();
             FlightClient client = FlightClient.builder(allocator, serverLocation).build()) {

            // List all flights (with criteria "2015" to filter)
            System.out.println("Listing flights matching '2015':");
            Criteria criteria = new Criteria("2015".getBytes());

            Iterable<FlightInfo> flights = client.listFlights(criteria);
            FlightInfo firstFlight = null;

            for (FlightInfo info : flights) {
                System.out.println("  Path: " + info.getDescriptor().getPath());
                System.out.println("  Rows: " + info.getRecords());
                System.out.println("  Schema: " + info.getSchema().getFields().size() + " fields");
                if (firstFlight == null) {
                    firstFlight = info;
                }
            }

            // Get data from first flight
            if (firstFlight != null && !firstFlight.getEndpoints().isEmpty()) {
                System.out.println("\nRetrieving data from first flight...");
                Ticket ticket = firstFlight.getEndpoints().get(0).getTicket();

                try (FlightStream stream = client.getStream(ticket)) {
                    VectorSchemaRoot root = stream.getRoot();
                    long totalRows = 0;

                    while (stream.next()) {
                        totalRows += root.getRowCount();
                    }

                    System.out.println("Total rows retrieved: " + totalRows);
                    System.out.println("Schema: " + root.getSchema());
                }
            }

        } catch (Exception e) {
            System.err.println("Client error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Explain Flight concepts
     */
    public static void explainFlightConcepts() {
        System.out.println("=== Apache Arrow Flight ===\n");

        System.out.println("Arrow Flight is a high-performance RPC framework for");
        System.out.println("transporting Arrow data over the network.\n");

        System.out.println("Key concepts:");
        System.out.println("1. FlightServer - Hosts data and serves requests");
        System.out.println("2. FlightClient - Connects and retrieves data");
        System.out.println("3. FlightInfo - Metadata about available datasets");
        System.out.println("4. FlightEndpoint - Where to retrieve the data");
        System.out.println("5. Ticket - Token to request specific data\n");

        System.out.println("Main operations:");
        System.out.println("- ListFlights: Enumerate available datasets");
        System.out.println("- GetFlightInfo: Get metadata for a dataset");
        System.out.println("- DoGet: Stream data from server to client");
        System.out.println("- DoPut: Stream data from client to server");
        System.out.println("- DoExchange: Bidirectional streaming\n");

        System.out.println("Benefits:");
        System.out.println("- Native Arrow format (no serialization overhead)");
        System.out.println("- gRPC-based (HTTP/2, streaming, auth)");
        System.out.println("- Parallel data retrieval");
        System.out.println("- Language-agnostic\n");
    }

    public static void main(String[] args) {
        explainFlightConcepts();

        try (BufferAllocator allocator = new RootAllocator()) {
            Location location = Location.forGrpcInsecure("localhost", 0);

            System.out.println("Starting Flight server...");
            try (org.apache.arrow.flight.FlightServer server =
                     FlightServer.builder(allocator, location,
                         new ParquetFlightProducer(allocator, DATA_DIR)).build()) {

                server.start();
                System.out.println("Server started on port: " + server.getPort());

                // Run client demo
                Location serverLocation = Location.forGrpcInsecure("localhost", server.getPort());
                runClientDemo(serverLocation);

                System.out.println("\nServer shutting down...");
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
