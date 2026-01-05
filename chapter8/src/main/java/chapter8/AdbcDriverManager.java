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
package chapter8;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.HashMap;
import java.util.Map;

/**
 * Java equivalent of adbc_driver_manager.cc - demonstrates ADBC driver management
 *
 * ADBC (Arrow Database Connectivity) provides a standard API for database access
 * that returns Arrow-formatted data directly.
 */
public class AdbcDriverManager {

    /**
     * Demonstrate ADBC driver manager concepts
     */
    public static void demonstrateDriverManager() {
        System.out.println("=== ADBC Driver Manager ===\n");

        System.out.println("ADBC (Arrow Database Connectivity) provides:");
        System.out.println("1. Standard API across databases");
        System.out.println("2. Direct Arrow data return (zero-copy potential)");
        System.out.println("3. Driver-based architecture\n");

        System.out.println("Available Java ADBC Drivers:");
        System.out.println("- arrow-adbc-driver-jdbc: JDBC wrapper for any JDBC driver");
        System.out.println("- arrow-adbc-driver-flight-sql: Flight SQL client");
        System.out.println("- Native drivers via JNI (PostgreSQL, SQLite, etc.)\n");

        System.out.println("ADBC Architecture:");
        System.out.println("  Application");
        System.out.println("      |");
        System.out.println("  ADBC API");
        System.out.println("      |");
        System.out.println("  Driver Manager");
        System.out.println("      |");
        System.out.println("  +---+---+---+");
        System.out.println("  |   |   |   |");
        System.out.println(" SQLite PG JDBC Flight");
        System.out.println("\n");
    }

    /**
     * Load and use JDBC-based ADBC driver
     */
    public static void jdbcDriverExample() {
        System.out.println("=== JDBC Driver Example ===\n");

        try (BufferAllocator allocator = new RootAllocator()) {
            // Create JDBC driver
            AdbcDriver driver = new JdbcDriver(allocator);

            // Configure database connection
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("uri", "jdbc:sqlite::memory:");
            // For other databases:
            // parameters.put("uri", "jdbc:postgresql://localhost:5432/mydb");
            // parameters.put("username", "user");
            // parameters.put("password", "pass");

            try (AdbcDatabase database = driver.open(parameters);
                 AdbcConnection connection = database.connect()) {

                System.out.println("Connected to database via JDBC ADBC driver");

                // Create a table
                try (AdbcStatement stmt = connection.createStatement()) {
                    stmt.setSqlQuery("CREATE TABLE test (id INTEGER, name TEXT)");
                    stmt.executeUpdate();
                    System.out.println("Created table 'test'");
                }

                // Insert data
                try (AdbcStatement stmt = connection.createStatement()) {
                    stmt.setSqlQuery("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')");
                    stmt.executeUpdate();
                    System.out.println("Inserted 2 rows");
                }

                // Query and get Arrow results
                try (AdbcStatement stmt = connection.createStatement()) {
                    stmt.setSqlQuery("SELECT * FROM test");
                    AdbcStatement.QueryResult result = stmt.executeQuery();

                    try (ArrowReader reader = result.getReader()) {
                        System.out.println("\nQuery results (Arrow format):");
                        System.out.println("Schema: " + reader.getVectorSchemaRoot().getSchema());

                        while (reader.loadNextBatch()) {
                            VectorSchemaRoot root = reader.getVectorSchemaRoot();
                            System.out.println(root.contentToTSVString());
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
     * Show how to list available drivers
     */
    public static void listDrivers() {
        System.out.println("=== Available ADBC Drivers ===\n");

        System.out.println("Java ADBC includes:");
        System.out.println("1. JdbcDriver (org.apache.arrow.adbc.driver.jdbc)");
        System.out.println("   - Wraps any JDBC driver");
        System.out.println("   - Works with MySQL, PostgreSQL, SQLite, Oracle, etc.");
        System.out.println("   - URI format: jdbc:<subprotocol>:<subname>\n");

        System.out.println("2. FlightSqlDriver (org.apache.arrow.adbc.driver.flightsql)");
        System.out.println("   - Connects to Flight SQL servers");
        System.out.println("   - Native Arrow data transfer");
        System.out.println("   - URI format: grpc://<host>:<port>\n");

        System.out.println("Driver loading in Java:");
        System.out.println("```java");
        System.out.println("// JDBC Driver");
        System.out.println("AdbcDriver driver = new JdbcDriver(allocator);");
        System.out.println("");
        System.out.println("// Flight SQL Driver");
        System.out.println("AdbcDriver driver = new FlightSqlDriver(allocator);");
        System.out.println("```\n");
    }

    /**
     * Compare ADBC vs JDBC
     */
    public static void compareAdbcJdbc() {
        System.out.println("=== ADBC vs JDBC ===\n");

        System.out.println("JDBC (Java Database Connectivity):");
        System.out.println("- Row-oriented ResultSet");
        System.out.println("- Type conversion at application level");
        System.out.println("- Widespread adoption\n");

        System.out.println("ADBC (Arrow Database Connectivity):");
        System.out.println("- Column-oriented Arrow batches");
        System.out.println("- Native Arrow types");
        System.out.println("- Potential zero-copy with native drivers");
        System.out.println("- Better for analytics workloads\n");

        System.out.println("Use ADBC when:");
        System.out.println("- Processing large result sets");
        System.out.println("- Building analytics pipelines");
        System.out.println("- Integrating with Arrow-native systems");
        System.out.println("- Need columnar data for ML/AI\n");
    }

    public static void main(String[] args) {
        demonstrateDriverManager();
        listDrivers();
        compareAdbcJdbc();

        System.out.println("=== Running JDBC ADBC Example ===\n");
        jdbcDriverExample();
    }
}
