"""
================================================================================
PYSPARK CORE SPARK ARCHITECTURE FUNDAMENTALS - CODE EXAMPLES
================================================================================

This file demonstrates the core Spark architecture concepts with practical
examples for staff engineer preparation.

Topics Covered:
1. Spark Application Architecture
2. RDD vs DataFrame vs Dataset
3. Lazy Evaluation
4. Lineage and Fault Tolerance
5. Execution Model (Narrow vs Wide Dependencies)
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time


def create_spark_session():
    """
    Create and return a SparkSession with appropriate configuration.
    Demonstrates Spark Application Architecture.
    """
    spark = SparkSession.builder \
        .appName("CoreSparkArchitectureDemo") \
        .master("local[4]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    print("=" * 80)
    print("SPARK SESSION CREATED")
    print("=" * 80)
    print(f"Spark Version: {spark.version}")
    print(f"Master: {spark.conf.get('spark.master')}")
    print(f"App Name: {spark.conf.get('spark.app.name')}")
    print(f"Executor Memory: {spark.conf.get('spark.executor.memory')}")
    print(f"Driver Memory: {spark.conf.get('spark.driver.memory')}")
    print(f"Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
    print("=" * 80)
    
    return spark


def section_1_spark_application_architecture(spark):
    """
    Section 1: Spark Application Architecture
    Demonstrates the components: Driver, Executor, Cluster Manager, Task, Stage, Job
    """
    print("\n" + "=" * 80)
    print("SECTION 1: SPARK APPLICATION ARCHITECTURE")
    print("=" * 80)
    
    # Create sample data
    data = [
        ("Alice", 25, 70000),
        ("Bob", 30, 80000),
        ("Charlie", 35, 90000),
        ("David", 28, 75000),
        ("Eve", 32, 85000)
    ]
    
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", IntegerType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    print("\nSample DataFrame:")
    df.show()
    
    # Demonstrate Job, Stage, Task concepts
    print("\n--- Understanding Jobs, Stages, and Tasks ---")
    print("A Job is triggered by an ACTION (e.g., count, collect, show)")
    print("A Stage is a set of tasks that can be executed in parallel")
    print("A Task is the smallest unit of work, operating on a single partition")
    
    # This triggers a job
    start_time = time.time()
    count_result = df.count()
    elapsed = time.time() - start_time
    
    print(f"\nAction: df.count()")
    print(f"Result: {count_result} rows")
    print(f"Time elapsed: {elapsed:.4f} seconds")
    
    # Another action - triggers another job
    start_time = time.time()
    avg_salary = df.agg({"salary": "avg"}).collect()[0][0]
    elapsed = time.time() - start_time
    
    print(f"\nAction: df.agg({{'salary': 'avg'}})")
    print(f"Result: Average salary = ${avg_salary:.2f}")
    print(f"Time elapsed: {elapsed:.4f} seconds")
    
    print("\n--- Accessing Spark UI ---")
    print("Spark UI: http://localhost:4040 (when running locally)")
    print("Check the Jobs tab to see the jobs triggered by actions")
    print("Check the Stages tab to see stages and tasks")
    print("Check the Executors tab to see executor information")


def section_2_rdd_vs_dataframe(spark):
    """
    Section 2: RDD vs DataFrame vs Dataset
    Demonstrates the differences and performance characteristics
    """
    print("\n" + "=" * 80)
    print("SECTION 2: RDD VS DATAFRAME VS DATASET")
    print("=" * 80)
    
    # Create sample data
    data = [
        ("Alice", 25, 70000),
        ("Bob", 30, 80000),
        ("Charlie", 35, 90000),
        ("David", 28, 75000),
        ("Eve", 32, 85000)
    ]
    
    # RDD Approach (Low-level, untyped, less optimized)
    print("\n--- RDD Approach ---")
    rdd = spark.sparkContext.parallelize(data)
    
    print("RDD Type:", type(rdd))
    print("RDD is untyped - no schema information")
    
    # RDD transformation (map)
    rdd_mapped = rdd.map(lambda x: (x[0], x[1], x[2] * 1.1))  # 10% salary increase
    
    print("RDD Transformation: map for 10% salary increase")
    print("RDD Action: collect")
    rdd_result = rdd_mapped.collect()
    for row in rdd_result:
        print(f"  {row}")
    
    # DataFrame Approach (Typed with schema, Catalyst optimizer)
    print("\n--- DataFrame Approach ---")
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", IntegerType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    print("DataFrame Type:", type(df))
    print("DataFrame Schema:")
    df.printSchema()
    
    # DataFrame transformation (withColumn)
    df_transformed = df.withColumn("new_salary", col("salary") * 1.1)
    
    print("DataFrame Transformation: withColumn for 10% salary increase")
    print("DataFrame Action: show")
    df_transformed.show()
    
    # Compare performance
    print("\n--- Performance Comparison ---")
    
    # RDD timing
    start = time.time()
    rdd_result = rdd.map(lambda x: (x[0], x[1], x[2] * 1.1)).collect()
    rdd_time = time.time() - start
    
    # DataFrame timing
    start = time.time()
    df_result = df.withColumn("new_salary", col("salary") * 1.1).collect()
    df_time = time.time() - start
    
    print(f"RDD Time: {rdd_time:.4f} seconds")
    print(f"DataFrame Time: {df_time:.4f} seconds")
    print(f"DataFrame is typically faster due to Catalyst optimizer and Tungsten")
    
    print("\n--- Key Differences ---")
    print("RDD:")
    print("  - Low-level API")
    print("  - No schema, untyped")
    print("  - No Catalyst optimization")
    print("  - More control, less optimized")
    print("\nDataFrame:")
    print("  - High-level API")
    print("  - Schema-based, typed")
    print("  - Catalyst optimizer optimizes queries")
    print("  - Tungsten execution engine for performance")
    print("  - Preferred for most use cases")
    print("\nDataset:")
    print("  - Type-safe DataFrame (Scala/Java only)")
    print("  - Not available in PySpark")


def section_3_lazy_evaluation(spark):
    """
    Section 3: Lazy Evaluation
    Demonstrates how transformations are lazy and actions trigger execution
    """
    print("\n" + "=" * 80)
    print("SECTION 3: LAZY EVALUATION")
    print("=" * 80)
    
    # Create sample data
    data = [(i, i * 2) for i in range(1, 101)]
    df = spark.createDataFrame(data, ["number", "doubled"])
    
    print("\n--- Understanding Lazy Evaluation ---")
    print("Transformations are LAZY - they are not executed immediately")
    print("Actions are EAGER - they trigger execution of the DAG")
    
    print("\nOriginal DataFrame:")
    df.show(5)
    
    # Chain multiple transformations (lazy)
    print("\n--- Chaining Transformations (Lazy) ---")
    print("These transformations are NOT executed yet:")
    print("  1. Filter: number > 50")
    print("  2. WithColumn: tripled = number * 3")
    print("  3. Select: number, tripled")
    
    df_transformed = df.filter(col("number") > 50) \
                       .withColumn("tripled", col("number") * 3) \
                       .select("number", "tripled")
    
    print("\nNo output yet - transformations are lazy")
    print("The DAG (Directed Acyclic Graph) is built but not executed")
    
    # Trigger execution with an action
    print("\n--- Triggering Execution with Action ---")
    print("Action: show()")
    df_transformed.show(5)
    
    print("\nNow the DAG was executed!")
    
    # Demonstrate with explain()
    print("\n--- Viewing the Execution Plan ---")
    print("df_transformed.explain() shows the logical and physical plan")
    df_transformed.explain(extended=False)
    
    print("\n--- Common Transformations (Lazy) ---")
    print("  - select(), filter(), withColumn(), groupBy()")
    print("  - orderBy(), join(), distinct(), drop()")
    print("  - map(), flatMap(), reduceByKey() (RDD)")
    
    print("\n--- Common Actions (Eager) ---")
    print("  - collect(), count(), take(), first()")
    print("  - show(), write(), head(), foreach()")
    
    # Demonstrate that transformations are only computed when needed
    print("\n--- Demonstration: Selective Computation ---")
    df_filtered = df.filter(col("number") > 90)
    df_limited = df_filtered.limit(1)
    
    print("Created: df.filter(number > 90).limit(1)")
    print("Only 1 row will be processed, not all 100 rows")
    result = df_limited.collect()
    print(f"Result: {result}")


def section_4_lineage_and_fault_tolerance(spark):
    """
    Section 4: Lineage and Fault Tolerance
    Demonstrates how Spark tracks lineage and handles failures
    """
    print("\n" + "=" * 80)
    print("SECTION 4: LINEAGE AND FAULT TOLERANCE")
    print("=" * 80)
    
    # Create sample data
    data = [(i, i * 2) for i in range(1, 21)]
    df = spark.createDataFrame(data, ["number", "doubled"])
    
    print("\n--- Understanding Lineage ---")
    print("Lineage tracks the transformation history of an RDD/DataFrame")
    print("If a partition is lost, Spark can recompute it using lineage")
    
    # Build a lineage chain
    print("\n--- Building a Lineage Chain ---")
    df_step1 = df.filter(col("number") > 5)
    df_step2 = df_step1.withColumn("tripled", col("number") * 3)
    df_step3 = df_step2.filter(col("tripled") < 50)
    df_step4 = df_step3.select("number", "tripled")
    
    print("Step 1: filter(number > 5)")
    print("Step 2: withColumn(tripled = number * 3)")
    print("Step 3: filter(tripled < 50)")
    print("Step 4: select(number, tripled)")
    
    print("\nResult:")
    df_step4.show()
    
    # View lineage (for RDD)
    print("\n--- Viewing RDD Lineage ---")
    rdd = df.rdd
    rdd_filtered = rdd.filter(lambda x: x[0] > 5)
    rdd_mapped = rdd_filtered.map(lambda x: (x[0], x[1], x[0] * 3))
    
    print("RDD Lineage:")
    print(rdd_mapped.toDebugString())
    
    print("\n--- Fault Tolerance ---")
    print("If an executor fails, Spark recomputes lost partitions using lineage")
    print("No need to replicate data like in HDFS")
    
    print("\n--- Checkpointing ---")
    print("For very long lineage chains, checkpointing can be used")
    print("Checkpointing breaks lineage by saving to reliable storage")
    print("Trade-off: faster recovery vs. storage cost")
    
    # Demonstrate checkpointing
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    
    df_checkpointed = df_step4.checkpoint()
    print("\nDataFrame checkpointed to: /tmp/spark-checkpoints")
    print("Lineage is now truncated at checkpoint")
    
    print("\n--- When to Use Checkpointing ---")
    print("  - Very long lineage chains (100+ transformations)")
    print("  - Iterative algorithms (MLlib)")
    print("  - When recomputation is expensive")
    print("  - Stateful streaming operations")


def section_5_execution_model(spark):
    """
    Section 5: Execution Model
    Demonstrates narrow vs wide dependencies and pipelining
    """
    print("\n" + "=" * 80)
    print("SECTION 5: EXECUTION MODEL")
    print("=" * 80)
    
    # Create sample data
    data1 = [(i, f"name_{i}") for i in range(1, 11)]
    data2 = [(i, i * 10) for i in range(1, 11)]
    
    df1 = spark.createDataFrame(data1, ["id", "name"])
    df2 = spark.createDataFrame(data2, ["id", "value"])
    
    print("\n--- Narrow Dependencies ---")
    print("Narrow dependencies: data stays on the same partition")
    print("No shuffle required")
    print("Can be pipelined (executed in a single stage)")
    print("Examples: map, filter, union, mapPartitions")
    
    print("\nExample: Narrow Dependency Operations")
    df_narrow = df1.filter(col("id") > 5) \
                  .withColumn("upper_name", col("name").substr(1, 5)) \
                  .select("id", "upper_name")
    
    print("Operations: filter -> withColumn -> select")
    print("All are narrow dependencies - no shuffle")
    print("\nResult:")
    df_narrow.show()
    
    print("\nView execution plan (single stage expected):")
    df_narrow.explain()
    
    print("\n--- Wide Dependencies ---")
    print("Wide dependencies: data must be shuffled across partitions")
    print("Shuffle required (network I/O, disk I/O)")
    print("Creates stage boundaries")
    print("Examples: groupBy, join, reduceByKey, distinct, reorder")
    
    print("\nExample: Wide Dependency Operation (groupBy)")
    df_wide = df1.groupBy("id").count()
    
    print("Operation: groupBy('id').count()")
    print("This is a wide dependency - requires shuffle")
    print("\nResult:")
    df_wide.show()
    
    print("\nView execution plan (multiple stages expected):")
    df_wide.explain()
    
    print("\n--- Join Operation (Wide Dependency) ---")
    df_joined = df1.join(df2, "id")
    
    print("Operation: df1.join(df2, 'id')")
    print("This is a wide dependency - requires shuffle")
    print("\nResult:")
    df_joined.show()
    
    print("\nView execution plan:")
    df_joined.explain()
    
    print("\n--- Pipelining ---")
    print("Narrow dependencies can be pipelined together")
    print("Multiple operations executed in a single task")
    print("Reduces overhead of scheduling multiple tasks")
    
    print("\nExample of Pipelining:")
    df_pipelined = df1.filter(col("id") > 3) \
                      .withColumn("doubled_id", col("id") * 2) \
                      .filter(col("doubled_id") < 15) \
                      .select("id", "doubled_id")
    
    print("filter -> withColumn -> filter -> select")
    print("All pipelined into a single stage")
    print("\nResult:")
    df_pipelined.show()
    
    print("\n--- Minimizing Wide Dependencies ---")
    print("Wide dependencies (shuffles) are expensive")
    print("Tips to minimize:")
    print("  1. Filter before groupBy/join to reduce data volume")
    print("  2. Use reduceByKey instead of groupByKey (map-side combine)")
    print("  3. Use broadcast joins for small tables")
    print("  4. Combine multiple shuffles into one when possible")
    
    # Demonstrate filtering before shuffle
    print("\n--- Good Practice: Filter Before Shuffle ---")
    
    # Bad: Shuffle all data then filter
    print("Bad: df.groupBy().agg().filter()")
    start = time.time()
    df_bad = df1.groupBy("id").count().filter(col("count") > 0)
    df_bad.collect()
    bad_time = time.time() - start
    print(f"Time: {bad_time:.4f} seconds")
    
    # Good: Filter before shuffle
    print("\nGood: df.filter().groupBy().agg()")
    start = time.time()
    df_good = df1.filter(col("id") > 0).groupBy("id").count()
    df_good.collect()
    good_time = time.time() - start
    print(f"Time: {good_time:.4f} seconds")
    
    print("\nFiltering before shuffle reduces the amount of data shuffled")


def main():
    """
    Main function to run all demonstrations
    """
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run all sections
        section_1_spark_application_architecture(spark)
        section_2_rdd_vs_dataframe(spark)
        section_3_lazy_evaluation(spark)
        section_4_lineage_and_fault_tolerance(spark)
        section_5_execution_model(spark)
        
        print("\n" + "=" * 80)
        print("ALL DEMONSTRATIONS COMPLETED")
        print("=" * 80)
        print("\nKey Takeaways:")
        print("1. Spark Application: Driver + Executors + Cluster Manager")
        print("2. Prefer DataFrames over RDDs for performance")
        print("3. Transformations are lazy, actions trigger execution")
        print("4. Lineage enables fault tolerance through recomputation")
        print("5. Minimize wide dependencies (shuffles) for better performance")
        print("\nCheck Spark UI at http://localhost:4040 for detailed metrics")
        
    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()
