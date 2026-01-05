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
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.HashMap;
import java.util.Map;

/**
 * Java equivalent of adbc_postgres.cc - demonstrates PostgreSQL connectivity via ADBC
 */
public class AdbcPostgres {

    // Connection parameters - adjust for your PostgreSQL setup
    private static final String POSTGRES_URI =
        "jdbc:postgresql://localhost:5432/postgres";
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PASSWORD = "mysecretpassword";

    /**
     * Connect to PostgreSQL and perform basic operations
     */
    public static void postgresExample() {
        System.out.println("=== PostgreSQL ADBC Example ===\n");
        System.out.println("Connecting to: " + POSTGRES_URI);

        try (BufferAllocator allocator = new RootAllocator()) {
            // Create JDBC-based driver for PostgreSQL
            AdbcDriver driver = new JdbcDriver(allocator);

            Map<String, Object> parameters = new HashMap<>();
            parameters.put("uri", POSTGRES_URI);
            parameters.put("username", POSTGRES_USER);
            parameters.put("password", POSTGRES_PASSWORD);

            try (AdbcDatabase database = driver.open(parameters);
                 AdbcConnection connection = database.connect()) {

                System.out.println("Connected to PostgreSQL\n");

                // Enable autocommit
                connection.setAutoCommit(true);

                // Create table
                executeUpdate(connection,
                    "CREATE TABLE IF NOT EXISTS foo (col VARCHAR(80))");
                System.out.println("Created table 'foo'");

                // Insert data
                executeUpdate(connection, "INSERT INTO foo VALUES ('bar')");
                System.out.println("Inserted row");

                // Query data
                System.out.println("\nQuerying data:");
                executeQuery(connection, "SELECT * FROM foo");

                // More complex example
                executeUpdate(connection,
                    "CREATE TABLE IF NOT EXISTS users (" +
                    "  id SERIAL PRIMARY KEY," +
                    "  name VARCHAR(100)," +
                    "  age INTEGER," +
                    "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                    ")");
                System.out.println("\nCreated table 'users'");

                // Insert multiple rows
                executeUpdate(connection,
                    "INSERT INTO users (name, age) VALUES " +
                    "('Alice', 30), ('Bob', 25), ('Charlie', 35)");
                System.out.println("Inserted 3 users");

                // Query with filter
                System.out.println("\nUsers over 27:");
                executeQuery(connection, "SELECT * FROM users WHERE age > 27");

                // Aggregation query
                System.out.println("\nAge statistics:");
                executeQuery(connection,
                    "SELECT COUNT(*) as count, AVG(age) as avg_age, " +
                    "MIN(age) as min_age, MAX(age) as max_age FROM users");

                // Cleanup (optional)
                // executeUpdate(connection, "DROP TABLE IF EXISTS foo");
                // executeUpdate(connection, "DROP TABLE IF EXISTS users");

            }
        } catch (Exception e) {
            System.err.println("PostgreSQL Error: " + e.getMessage());
            e.printStackTrace();
            demonstratePostgresConcepts();
        }
    }

    /**
     * Execute an update statement (CREATE, INSERT, UPDATE, DELETE)
     */
    private static void executeUpdate(AdbcConnection connection, String sql)
            throws Exception {
        try (AdbcStatement stmt = connection.createStatement()) {
            stmt.setSqlQuery(sql);
            stmt.executeUpdate();
        }
    }

    /**
     * Execute a query and print Arrow results
     */
    private static void executeQuery(AdbcConnection connection, String sql)
            throws Exception {
        try (AdbcStatement stmt = connection.createStatement()) {
            stmt.setSqlQuery(sql);
            AdbcStatement.QueryResult result = stmt.executeQuery();

            try (ArrowReader reader = result.getReader()) {
                System.out.println("Schema: " + reader.getVectorSchemaRoot().getSchema());

                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    System.out.println(root.contentToTSVString());
                }
            }
        }
    }

    /**
     * Explain PostgreSQL ADBC concepts when connection fails
     */
    private static void demonstratePostgresConcepts() {
        System.out.println("\n=== PostgreSQL ADBC Concepts ===\n");

        System.out.println("To use ADBC with PostgreSQL:");
        System.out.println("1. Ensure PostgreSQL is running");
        System.out.println("2. Update connection parameters in the code");
        System.out.println("3. Include PostgreSQL JDBC driver in classpath\n");

        System.out.println("Connection string format:");
        System.out.println("  jdbc:postgresql://<host>:<port>/<database>\n");

        System.out.println("Example Docker command to start PostgreSQL:");
        System.out.println("  docker run -d \\");
        System.out.println("    --name postgres \\");
        System.out.println("    -e POSTGRES_PASSWORD=mysecretpassword \\");
        System.out.println("    -p 5432:5432 \\");
        System.out.println("    postgres\n");

        System.out.println("Maven dependency for PostgreSQL JDBC:");
        System.out.println("  <dependency>");
        System.out.println("    <groupId>org.postgresql</groupId>");
        System.out.println("    <artifactId>postgresql</artifactId>");
        System.out.println("    <version>42.6.0</version>");
        System.out.println("  </dependency>\n");

        System.out.println("Native PostgreSQL ADBC driver:");
        System.out.println("- C/C++ uses adbc_driver_postgresql");
        System.out.println("- Java uses JDBC wrapper or JNI binding");
        System.out.println("- Native driver offers better performance\n");
    }

    /**
     * Show prepared statement usage
     */
    public static void preparedStatementExample() {
        System.out.println("=== Prepared Statements ===\n");

        System.out.println("ADBC supports prepared statements for:");
        System.out.println("- Parameter binding");
        System.out.println("- Query plan reuse");
        System.out.println("- SQL injection prevention\n");

        System.out.println("Example (conceptual):");
        System.out.println("```java");
        System.out.println("try (AdbcStatement stmt = connection.createStatement()) {");
        System.out.println("    stmt.setSqlQuery(\"SELECT * FROM users WHERE age > ?\");");
        System.out.println("    stmt.bind(ageVector);  // Bind Arrow vector");
        System.out.println("    QueryResult result = stmt.executeQuery();");
        System.out.println("    // Process results...");
        System.out.println("}");
        System.out.println("```\n");
    }

    /**
     * Demonstrate bulk operations
     */
    public static void bulkOperationsExample() {
        System.out.println("=== Bulk Operations ===\n");

        System.out.println("ADBC enables efficient bulk data transfer:");
        System.out.println("1. Bind Arrow RecordBatch for bulk inserts");
        System.out.println("2. Stream large result sets as batches");
        System.out.println("3. Minimal serialization overhead\n");

        System.out.println("Bulk insert example (conceptual):");
        System.out.println("```java");
        System.out.println("// Create VectorSchemaRoot with data");
        System.out.println("VectorSchemaRoot batch = createDataBatch();");
        System.out.println("");
        System.out.println("try (AdbcStatement stmt = connection.createStatement()) {");
        System.out.println("    stmt.setSqlQuery(\"INSERT INTO table\");");
        System.out.println("    stmt.bind(batch);");
        System.out.println("    stmt.executeUpdate();");
        System.out.println("}");
        System.out.println("```\n");
    }

    public static void main(String[] args) {
        postgresExample();
        preparedStatementExample();
        bulkOperationsExample();
    }
}
