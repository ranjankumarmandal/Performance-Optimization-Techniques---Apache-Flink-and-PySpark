import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * ================================================================================
 * APACHE FLINK DATASTREAM API VS TABLE API/SQL - CODE EXAMPLES
 * ================================================================================
 *
 * This file demonstrates the differences between DataStream API and Table API/SQL
 * with practical examples for staff engineer preparation.
 *
 * Topics Covered:
 * 1. DataStream API Basics
 * 2. Table API Basics
 * 3. SQL Basics
 * 4. Converting Between APIs
 * 5. Performance Comparison
 * 6. Choosing Between APIs
 * ================================================================================
 */

public class DataStreamVsTableApi {

    /**
     * Section 1: DataStream API Basics
     * Demonstrates low-level, fine-grained control with DataStream API
     */
    public static void section1_dataStreamApi() throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("SECTION 1: DATASTREAM API BASICS");
        System.out.println("=".repeat(80));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("\n--- DataStream API Characteristics ---");
        System.out.println("Low-level API with fine-grained control");
        System.out.println("Process function for custom logic");
        System.out.println("Stateful operations");
        System.out.println("Event-time processing");
        System.out.println("Use for: complex event processing, custom state logic");

        // Create sample data
        List<Tuple3<Integer, String, Double>> data = new ArrayList<>();
        data.add(new Tuple3<>(1, "Alice", 70000.0));
        data.add(new Tuple3<>(2, "Bob", 80000.0));
        data.add(new Tuple3<>(3, "Charlie", 90000.0));
        data.add(new Tuple3<>(4, "David", 75000.0));
        data.add(new Tuple3<>(5, "Eve", 85000.0));

        DataStream<Tuple3<Integer, String, Double>> stream = env.fromCollection(data);

        System.out.println("\n--- DataStream Transformations ---");

        // Map transformation
        DataStream<Tuple3<Integer, String, Double>> mapped = stream.map(
                new MapFunction<Tuple3<Integer, String, Double>, Tuple3<Integer, String, Double>>() {
                    @Override
                    public Tuple3<Integer, String, Double> map(Tuple3<Integer, String, Double> value) {
                        // 10% salary increase
                        return new Tuple3<>(value.f0, value.f1, value.f2 * 1.1);
                    }
                }
        );

        System.out.println("Map: 10% salary increase");

        // Filter transformation
        DataStream<Tuple3<Integer, String, Double>> filtered = mapped.filter(
                new FilterFunction<Tuple3<Integer, String, Double>>() {
                    @Override
                    public boolean filter(Tuple3<Integer, String, Double> value) {
                        return value.f2 > 80000;
                    }
                }
        );

        System.out.println("Filter: salary > 80000");

        // KeyBy and aggregation
        DataStream<Tuple2<String, Double>> aggregated = filtered
                .keyBy(value -> "high_salary")
                .sum(2);

        System.out.println("KeyBy and Sum: aggregate salaries");

        // Print results
        aggregated.print();

        System.out.println("\n--- DataStream API Advantages ---");
        System.out.println("Fine-grained control over execution");
        System.out.println("Custom state management");
        System.out.println("Complex event processing");
        System.out.println("Low-level optimizations");

        env.execute("DataStreamApiDemo");
    }

    /**
     * Section 2: Table API Basics
     * Demonstrates relational, declarative API with Table API
     */
    public static void section2_tableApi() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("SECTION 2: TABLE API BASICS");
        System.out.println("=".repeat(80));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println("\n--- Table API Characteristics ---");
        System.out.println("Relational API, declarative");
        System.out.println("Optimized by Calcite optimizer");
        System.out.println("Built-in functions, windowing");
        System.out.println("Easier to write and maintain");
        System.out.println("Use for: standard analytics, ETL, SQL users");

        // Create sample data
        List<Tuple3<Integer, String, Double>> data = new ArrayList<>();
        data.add(new Tuple3<>(1, "Alice", 70000.0));
        data.add(new Tuple3<>(2, "Bob", 80000.0));
        data.add(new Tuple3<>(3, "Charlie", 90000.0));
        data.add(new Tuple3<>(4, "David", 75000.0));
        data.add(new Tuple3<>(5, "Eve", 85000.0));

        DataStream<Tuple3<Integer, String, Double>> stream = env.fromCollection(data);

        // Convert DataStream to Table
        Table table = tableEnv.fromDataStream(
                stream,
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.DOUBLE())
                        .build()
        );

        tableEnv.createTemporaryView("employees", table);

        System.out.println("\n--- Table API Operations ---");

        // Select with expression
        Table result1 = tableEnv.from("employees")
                .select($("f1").as("name"), $("f2").as("salary"));

        System.out.println("Select: name and salary columns");

        // Filter
        Table result2 = result1.filter($("salary").greaterThan(80000));

        System.out.println("Filter: salary > 80000");

        // Add column
        Table result3 = result2.addColumns(
                $("salary").multiply(1.1).as("new_salary")
        );

        System.out.println("Add column: 10% salary increase");

        // Execute and print
        tableEnv.toDataStream(result3).print();

        System.out.println("\n--- Table API Advantages ---");
        System.out.println("Declarative - describe what, not how");
        System.out.println("Calcite optimizer optimizes queries");
        System.out.println("Rich built-in functions");
        System.out.println("Type-safe operations");
        System.out.println("Better code maintainability");

        env.execute("TableApiDemo");
    }

    /**
     * Section 3: SQL Basics
     * Demonstrates SQL queries on streaming data
     */
    public static void section3_sql() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("SECTION 3: SQL BASICS");
        System.out.println("=".repeat(80));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println("\n--- SQL Characteristics ---");
        System.out.println("Standard SQL syntax");
        System.out.println("Familiar to data engineers");
        System.out.println("Same optimizer as Table API");
        System.out.println("Supports complex queries");

        // Create sample data
        List<Tuple3<Integer, String, Double>> data = new ArrayList<>();
        data.add(new Tuple3<>(1, "Alice", 70000.0));
        data.add(new Tuple3<>(2, "Bob", 80000.0));
        data.add(new Tuple3<>(3, "Charlie", 90000.0));
        data.add(new Tuple3<>(4, "David", 75000.0));
        data.add(new Tuple3<>(5, "Eve", 85000.0));

        DataStream<Tuple3<Integer, String, Double>> stream = env.fromCollection(data);

        // Convert to Table
        Table table = tableEnv.fromDataStream(
                stream,
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.DOUBLE())
                        .build()
        );

        tableEnv.createTemporaryView("employees", table);

        System.out.println("\n--- SQL Queries ---");

        // Simple SELECT
        Table result1 = tableEnv.sqlQuery(
                "SELECT f1 as name, f2 as salary FROM employees"
        );

        System.out.println("Query: SELECT name, salary FROM employees");

        // WHERE clause
        Table result2 = tableEnv.sqlQuery(
                "SELECT f1 as name, f2 as salary FROM employees WHERE f2 > 80000"
        );

        System.out.println("Query: SELECT ... WHERE salary > 80000");

        // Aggregation
        Table result3 = tableEnv.sqlQuery(
                "SELECT COUNT(*) as count, AVG(f2) as avg_salary FROM employees"
        );

        System.out.println("Query: SELECT COUNT(*), AVG(salary) FROM employees");

        // Execute and print
        System.out.println("\n--- Aggregation Result ---");
        tableEnv.toDataStream(result3).print();

        System.out.println("\n--- SQL Advantages ---");
        System.out.println("Standard SQL - easy to learn");
        System.out.println("Portable skills");
        System.out.println("Complex queries (joins, subqueries)");
        System.out.println("Integration with BI tools");

        env.execute("SqlDemo");
    }

    /**
     * Section 4: Converting Between APIs
     * Demonstrates seamless integration between DataStream and Table APIs
     */
    public static void section4_convertingApis() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("SECTION 4: CONVERTING BETWEEN APIS");
        System.out.println("=".repeat(80));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println("\n--- DataStream to Table ---");

        // Create DataStream
        List<Tuple3<Integer, String, Double>> data = new ArrayList<>();
        data.add(new Tuple3<>(1, "Alice", 70000.0));
        data.add(new Tuple3<>(2, "Bob", 80000.0));
        data.add(new Tuple3<>(3, "Charlie", 90000.0));

        DataStream<Tuple3<Integer, String, Double>> dataStream = env.fromCollection(data);

        // Convert to Table with schema
        Table table = tableEnv.fromDataStream(
                dataStream,
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.DOUBLE())
                        .build()
        );

        System.out.println("DataStream → Table: tableEnv.fromDataStream()");

        // Register as view
        tableEnv.createTemporaryView("employees", table);

        System.out.println("\n--- Table to DataStream ---");

        // Table to DataStream (append mode)
        DataStream<Row> rowStream = tableEnv.toDataStream(table);
        System.out.println("Table → DataStream (Row): tableEnv.toDataStream()");

        // Table to DataStream (changelog mode)
        DataStream<Tuple2<String, Double>> tupleStream = tableEnv
                .toDataStream(table, DataTypes.TUPLE(DataTypes.STRING(), DataTypes.DOUBLE()));

        System.out.println("Table → DataStream (Typed): tableEnv.toDataStream() with type");

        System.out.println("\n--- Mixed API Usage ---");

        // DataStream → Table → SQL → DataStream
        Table filteredTable = tableEnv.sqlQuery(
                "SELECT f1 as name, f2 as salary FROM employees WHERE f2 > 75000"
        );

        DataStream<Row> finalStream = tableEnv.toDataStream(filteredTable);

        System.out.println("Pattern: DataStream → Table → SQL → DataStream");
        System.out.println("This allows using the best API for each operation");

        finalStream.print();

        env.execute("ConvertingApisDemo");
    }

    /**
     * Section 5: Performance Comparison
     * Compares DataStream API vs Table API/SQL performance
     */
    public static void section5_performanceComparison() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("SECTION 5: PERFORMANCE COMPARISON");
        System.out.println("=".repeat(80));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Create larger dataset
        List<Tuple3<Integer, String, Double>> data = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            data.add(new Tuple3<>(i, "User" + i, 50000.0 + i * 100));
        }

        System.out.println("\n--- DataStream API Performance ---");

        DataStream<Tuple3<Integer, String, Double>> stream = env.fromCollection(data);

        long startTime = System.currentTimeMillis();

        DataStream<Tuple3<Integer, String, Double>> resultStream = stream
                .filter(value -> value.f2 > 60000)
                .map(value -> new Tuple3<>(value.f0, value.f1, value.f2 * 1.1));

        resultStream.executeAndCollect().close();

        long dataStreamTime = System.currentTimeMillis() - startTime;

        System.out.println("DataStream API time: " + dataStreamTime + " ms");

        System.out.println("\n--- Table API Performance ---");

        Table table = tableEnv.fromDataStream(
                env.fromCollection(data),
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.DOUBLE())
                        .build()
        );

        tableEnv.createTemporaryView("employees", table);

        startTime = System.currentTimeMillis();

        Table resultTable = tableEnv.from("employees")
                .filter($("f2").greaterThan(60000))
                .select($("f0"), $("f1"), $("f2").multiply(1.1));

        tableEnv.toDataStream(resultTable).executeAndCollect().close();

        long tableApiTime = System.currentTimeMillis() - startTime;

        System.out.println("Table API time: " + tableApiTime + " ms");

        System.out.println("\n--- Performance Analysis ---");
        System.out.println("Table/SQL: optimized by Calcite");
        System.out.println("  - Rule-based optimization");
        System.out.println("  - Cost-based optimization");
        System.out.println("  - Predicate pushdown");
        System.out.println("  - Column pruning");
        System.out.println("  - Often faster for standard operations");

        System.out.println("\nDataStream: more control");
        System.out.println("  - Custom optimizations");
        System.out.println("  - Fine-grained control");
        System.out.println("  - Can be faster for custom logic");

        System.out.println("\n--- When Each is Faster ---");
        System.out.println("Table/SQL faster:");
        System.out.println("  - Standard aggregations");
        System.out.println("  - Joins");
        System.out.println("  - Window operations");
        System.out.println("  - Complex queries");

        System.out.println("\nDataStream faster:");
        System.out.println("  - Custom state logic");
        System.out.println("  - Complex event processing");
        System.out.println("  - Low-level optimizations");
        System.out.println("  - Custom serialization");
    }

    /**
     * Section 6: Choosing Between APIs
     * Guidelines for selecting the appropriate API
     */
    public static void section6_choosingApis() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("SECTION 6: CHOOSING BETWEEN APIS");
        System.out.println("=".repeat(80));

        System.out.println("\n--- Use DataStream API When ---");
        System.out.println("✓ Need fine-grained control over execution");
        System.out.println("✓ Implementing complex stateful processing");
        System.out.println("✓ Custom state management required");
        System.out.println("✓ Complex event processing (CEP)");
        System.out.println("✓ Custom event-time handling");
        System.out.println("✓ Low-level optimizations needed");
        System.out.println("✓ Team has strong Java/Scala skills");

        System.out.println("\n--- Use Table API/SQL When ---");
        System.out.println("✓ Standard analytics and aggregations");
        System.out.println("✓ ETL pipelines");
        System.out.println("✓ Team has SQL background");
        System.out.println("✓ Code maintainability is priority");
        System.out.println("✓ Need built-in functions");
        System.out.println("✓ Complex joins and subqueries");
        System.out.println("✓ Integration with BI tools");

        System.out.println("\n--- Can Mix Both APIs ---");
        System.out.println("✓ Use DataStream for custom logic");
        System.out.println("✓ Use Table/SQL for standard operations");
        System.out.println("✓ Convert back and forth as needed");
        System.out.println("✓ Leverage strengths of each");

        System.out.println("\n--- Example: Mixed Approach ---");
        System.out.println("1. Ingest with DataStream (custom parsing)");
        System.out.println("2. Convert to Table (schema enforcement)");
        System.out.println("3. Process with SQL (joins, aggregations)");
        System.out.println("4. Convert back to DataStream (custom output)");

        System.out.println("\n--- Decision Matrix ---");
        System.out.println("Requirement | DataStream | Table/SQL");
        System.out.println("-------------|-----------|----------");
        System.out.println("Simple filter | ✓ | ✓");
        System.out.println("Complex aggregation | ✓ | ✓");
        System.out.println("Custom state | ✓ | ✗");
        System.out.println("Standard joins | ✓ | ✓");
        System.out.println("Complex event patterns | ✓ | ✗");
        System.out.println("SQL-like queries | ✗ | ✓");
        System.out.println("Code maintainability | - | ✓");
        System.out.println("Performance control | ✓ | -");

        System.out.println("\n--- Team Considerations ---");
        System.out.println("Data engineers: often prefer SQL");
        System.out.println("Software engineers: often prefer DataStream");
        System.out.println("Consider team expertise");
        System.out.println("Consider code maintainability");
    }

    /**
     * Section 7: Advanced Table API Features
     * Demonstrates advanced Table API capabilities
     */
    public static void section7_advancedTableApi() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("SECTION 7: ADVANCED TABLE API FEATURES");
        System.out.println("=".repeat(80));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Create sample data
        List<Tuple3<Integer, String, String>> data = new ArrayList<>();
        data.add(new Tuple3<>(1, "Alice", "Engineering"));
        data.add(new Tuple3<>(2, "Bob", "Marketing"));
        data.add(new Tuple3<>(3, "Charlie", "Engineering"));
        data.add(new Tuple3<>(4, "David", "Sales"));
        data.add(new Tuple3<>(5, "Eve", "Engineering"));

        DataStream<Tuple3<Integer, String, String>> stream = env.fromCollection(data);

        Table table = tableEnv.fromDataStream(
                stream,
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.STRING())
                        .build()
        );

        tableEnv.createTemporaryView("employees", table);

        System.out.println("\n--- Window Operations ---");

        // Tumbling window (requires event time, simplified here)
        System.out.println("Table API supports:");
        System.out.println("  - TUMBLE windows");
        System.out.println("  - HOP windows");
        System.out.println("  - SESSION windows");
        System.out.println("  - CUMULATE windows");

        System.out.println("\n--- Group Window (Legacy) ---");
        System.out.println("GROUP BY TUMBLE(rowtime, INTERVAL '1' HOUR)");

        System.out.println("\n--- Window TVF (Table-valued Function) ---");
        System.out.println("TABLE(TUMBLE(TABLE employees, DESCRIPTOR(rowtime), INTERVAL '1' HOUR))");

        System.out.println("\n--- Joins ---");

        // Create second table
        List<Tuple2<String, Integer>> deptData = new ArrayList<>();
        deptData.add(new Tuple2<>("Engineering", 100));
        deptData.add(new Tuple2<>("Marketing", 50));
        deptData.add(new Tuple2<>("Sales", 30));

        Table deptTable = tableEnv.fromDataStream(
                env.fromCollection(deptData),
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.INT())
                        .build()
        );

        tableEnv.createTemporaryView("departments", deptTable);

        // Inner join
        Table joined = tableEnv.sqlQuery(
                "SELECT e.f1 as name, e.f2 as dept, d.f1 as budget " +
                "FROM employees e " +
                "JOIN departments d ON e.f2 = d.f0"
        );

        System.out.println("Inner join: employees JOIN departments ON department");

        System.out.println("\n--- User-Defined Functions ---");

        // Register scalar function
        tableEnv.createTemporaryFunction("uppercase", new UppercaseFunction());

        Table withUdf = tableEnv.sqlQuery(
                "SELECT f1 as name, uppercase(f1) as upper_name FROM employees"
        );

        System.out.println("UDF: SELECT name, uppercase(name) FROM employees");

        System.out.println("\n--- Advanced Features ---");
        System.out.println("✓ User-defined functions (scalar, aggregate, table)");
        System.out.println("✓ Temporal joins");
        System.out.println("✓ Interval joins");
        System.out.println("✓ Pattern matching (CEP via MATCH_RECOGNIZE)");
        System.out.println("✓ Over aggregation");
        System.out.println("✓ Top-N queries");

        env.execute("AdvancedTableApiDemo");
    }

    // ==================== Helper Classes ====================

    /**
     * User-defined scalar function
     */
    public static class UppercaseFunction extends ScalarFunction {
        public String eval(String str) {
            return str.toUpperCase();
        }
    }

    /**
     * Main method to run all demonstrations
     */
    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("FLINK DATASTREAM API VS TABLE API/SQL DEMONSTRATIONS");
        System.out.println("=".repeat(80));

        try {
            section1_dataStreamApi();
            section2_tableApi();
            section3_sql();
            section4_convertingApis();
            section5_performanceComparison();
            section6_choosingApis();
            section7_advancedTableApi();

            System.out.println("\n" + "=".repeat(80));
            System.out.println("ALL DEMONSTRATIONS COMPLETED");
            System.out.println("=".repeat(80));
            System.out.println("\nKey Takeaways:");
            System.out.println("1. DataStream API: Low-level, fine-grained control");
            System.out.println("2. Table API: Relational, declarative, optimized");
            System.out.println("3. SQL: Standard syntax, familiar to data engineers");
            System.out.println("4. Can convert between APIs seamlessly");
            System.out.println("5. Table/SQL often faster for standard operations");
            System.out.println("6. DataStream faster for custom logic");
            System.out.println("7. Choose based on requirements and team expertise");

        } catch (Exception e) {
            System.err.println("Error executing demonstrations: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
