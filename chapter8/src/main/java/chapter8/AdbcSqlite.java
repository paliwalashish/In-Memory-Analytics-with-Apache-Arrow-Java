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
 * Java equivalent of adbc_sqlite.cc - demonstrates SQLite connectivity via ADBC
 */
public class AdbcSqlite {

    /**
     * Basic SQLite example using ADBC
     */
    public static void sqliteExample() {
        System.out.println("=== SQLite ADBC Example ===\n");

        try (BufferAllocator allocator = new RootAllocator()) {
            // Create JDBC-based driver for SQLite
            AdbcDriver driver = new JdbcDriver(allocator);

            // SQLite URI - use file path or :memory: for in-memory database
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("uri", "jdbc:sqlite:data.db");
            // For in-memory: parameters.put("uri", "jdbc:sqlite::memory:");

            try (AdbcDatabase database = driver.open(parameters);
                 AdbcConnection connection = database.connect()) {

                System.out.println("Connected to SQLite database\n");

                // Enable autocommit
                connection.setAutoCommit(true);

                // Create table
                try (AdbcStatement stmt = connection.createStatement()) {
                    stmt.setSqlQuery(
                        "CREATE TABLE IF NOT EXISTS foo (col VARCHAR(80))");
                    stmt.executeUpdate();
                    System.out.println("Created table 'foo'");
                }

                // Insert data
                try (AdbcStatement stmt = connection.createStatement()) {
                    stmt.setSqlQuery("INSERT INTO foo VALUES ('bar')");
                    long rowsAffected = stmt.executeUpdate().getAffectedRows();
                    System.out.println("Rows Inserted: " + rowsAffected);
                }

                // Query data
                try (AdbcStatement stmt = connection.createStatement()) {
                    stmt.setSqlQuery("SELECT * FROM foo");
                    AdbcStatement.QueryResult result = stmt.executeQuery();

                    try (ArrowReader reader = result.getReader()) {
                        System.out.println("\nQuery Results:");
                        System.out.println("Schema: " + reader.getVectorSchemaRoot().getSchema());

                        while (reader.loadNextBatch()) {
                            VectorSchemaRoot root = reader.getVectorSchemaRoot();
                            System.out.println(root.contentToTSVString());
                        }
                    }
                }

            }
        } catch (Exception e) {
            System.err.println("SQLite Error: " + e.getMessage());
            e.printStackTrace();
            demonstrateSqliteConcepts();
        }
    }

    /**
     * More comprehensive SQLite example with multiple operations
     */
    public static void comprehensiveSqliteExample() {
        System.out.println("\n=== Comprehensive SQLite Example ===\n");

        try (BufferAllocator allocator = new RootAllocator()) {
            AdbcDriver driver = new JdbcDriver(allocator);

            Map<String, Object> parameters = new HashMap<>();
            parameters.put("uri", "jdbc:sqlite::memory:");

            try (AdbcDatabase database = driver.open(parameters);
                 AdbcConnection connection = database.connect()) {

                connection.setAutoCommit(true);

                // Create products table
                executeStatement(connection,
                    "CREATE TABLE products (" +
                    "  id INTEGER PRIMARY KEY," +
                    "  name TEXT NOT NULL," +
                    "  price REAL," +
                    "  quantity INTEGER" +
                    ")");
                System.out.println("Created 'products' table");

                // Insert products
                executeStatement(connection,
                    "INSERT INTO products (name, price, quantity) VALUES " +
                    "('Widget', 9.99, 100)," +
                    "('Gadget', 19.99, 50)," +
                    "('Gizmo', 14.99, 75)," +
                    "('Doohickey', 24.99, 30)");
                System.out.println("Inserted 4 products\n");

                // Query all products
                System.out.println("All Products:");
                executeQuery(connection, "SELECT * FROM products");

                // Query with filter
                System.out.println("\nProducts under $20:");
                executeQuery(connection, "SELECT * FROM products WHERE price < 20");

                // Aggregation
                System.out.println("\nInventory Summary:");
                executeQuery(connection,
                    "SELECT " +
                    "  COUNT(*) as total_products," +
                    "  SUM(quantity) as total_quantity," +
                    "  AVG(price) as avg_price," +
                    "  SUM(price * quantity) as inventory_value " +
                    "FROM products");

                // Update
                executeStatement(connection,
                    "UPDATE products SET quantity = quantity + 10 WHERE name = 'Widget'");
                System.out.println("\nUpdated Widget quantity");

                // Verify update
                System.out.println("\nWidget after update:");
                executeQuery(connection,
                    "SELECT * FROM products WHERE name = 'Widget'");

                // Delete
                executeStatement(connection,
                    "DELETE FROM products WHERE price > 20");
                System.out.println("\nDeleted expensive products");

                // Final state
                System.out.println("\nFinal Products:");
                executeQuery(connection, "SELECT * FROM products");
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Helper to execute statements
     */
    private static void executeStatement(AdbcConnection connection, String sql)
            throws Exception {
        try (AdbcStatement stmt = connection.createStatement()) {
            stmt.setSqlQuery(sql);
            stmt.executeUpdate();
        }
    }

    /**
     * Helper to execute queries and print results
     */
    private static void executeQuery(AdbcConnection connection, String sql)
            throws Exception {
        try (AdbcStatement stmt = connection.createStatement()) {
            stmt.setSqlQuery(sql);
            AdbcStatement.QueryResult result = stmt.executeQuery();

            try (ArrowReader reader = result.getReader()) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    System.out.println(root.contentToTSVString());
                }
            }
        }
    }

    /**
     * Explain SQLite ADBC concepts
     */
    private static void demonstrateSqliteConcepts() {
        System.out.println("\n=== SQLite ADBC Concepts ===\n");

        System.out.println("SQLite with ADBC:");
        System.out.println("- Embedded database (no server required)");
        System.out.println("- File-based or in-memory");
        System.out.println("- Great for development and testing\n");

        System.out.println("URI formats:");
        System.out.println("  jdbc:sqlite:path/to/database.db  (file-based)");
        System.out.println("  jdbc:sqlite::memory:             (in-memory)\n");

        System.out.println("Maven dependency for SQLite JDBC:");
        System.out.println("  <dependency>");
        System.out.println("    <groupId>org.xerial</groupId>");
        System.out.println("    <artifactId>sqlite-jdbc</artifactId>");
        System.out.println("    <version>3.43.0.0</version>");
        System.out.println("  </dependency>\n");
    }

    /**
     * Compare C++ ADBC SQLite vs Java ADBC SQLite
     */
    public static void compareCppJava() {
        System.out.println("=== C++ vs Java ADBC Comparison ===\n");

        System.out.println("C++ ADBC SQLite (from adbc_sqlite.cc):");
        System.out.println("```cpp");
        System.out.println("AdbcDatabase database = {};");
        System.out.println("AdbcDatabaseNew(&database, &error);");
        System.out.println("AdbcDatabaseSetOption(&database, \"uri\", \"file:data.db\", &error);");
        System.out.println("AdbcDatabaseInit(&database, &error);");
        System.out.println("```\n");

        System.out.println("Java ADBC SQLite:");
        System.out.println("```java");
        System.out.println("AdbcDriver driver = new JdbcDriver(allocator);");
        System.out.println("Map<String, Object> params = Map.of(\"uri\", \"jdbc:sqlite:data.db\");");
        System.out.println("AdbcDatabase database = driver.open(params);");
        System.out.println("```\n");

        System.out.println("Key differences:");
        System.out.println("- Java uses JdbcDriver wrapper (or native JNI driver)");
        System.out.println("- C++ uses native adbc_driver_sqlite");
        System.out.println("- Java has automatic resource management (try-with-resources)");
        System.out.println("- C++ requires manual cleanup with Release() calls\n");
    }

    public static void main(String[] args) {
        sqliteExample();
        comprehensiveSqliteExample();
        compareCppJava();
    }
}
