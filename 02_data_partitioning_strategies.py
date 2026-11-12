"""
================================================================================
PYSPARK DATA PARTITIONING STRATEGIES - CODE EXAMPLES
================================================================================

This file demonstrates data partitioning strategies with practical
examples for staff engineer preparation.

Topics Covered:
1. Partitioning Basics
2. Default Partitioning
3. Repartition vs Coalesce
4. Partitioning Strategies
5. Bucketing
6. Partition Pruning
7. Salting for Skewed Data
8. Dynamic Partition Pruning
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when, floor, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import time


def create_spark_session():
    """
    Create and return a SparkSession with appropriate configuration.
    """
    spark = SparkSession.builder \
        .appName("DataPartitioningStrategiesDemo") \
        .master("local[4]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    print("=" * 80)
    print("SPARK SESSION CREATED")
    print("=" * 80)
    print(f"Spark Version: {spark.version}")
    print(f"Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
    print("=" * 80)
    
    return spark


def section_1_partitioning_basics(spark):
    """
    Section 1: Partitioning Basics
    Demonstrates partition concepts and partition count
    """
    print("\n" + "=" * 80)
    print("SECTION 1: PARTITIONING BASICS")
    print("=" * 80)
    
    # Create sample data
    data = [(i, f"user_{i}", i % 100) for i in range(1, 1001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id"])
    
    print("\n--- Understanding Partitions ---")
    print("Partition = chunk of data processed by one task")
    print("Too few partitions: underutilized cluster, OOM")
    print("Too many partitions: overhead, small files problem")
    print("Target: 128MB-256MB per partition (HDFS block size)")
    
    print(f"\nDataFrame created with {df.count()} rows")
    print(f"Number of partitions: {df.rdd.getNumPartitions()}")
    
    # Show partition distribution
    print("\n--- Partition Distribution ---")
    def partition_func(iterator):
        for x in iterator:
            yield (x[0],)
    
    partition_counts = df.rdd.mapPartitionsWithIndex(
        lambda idx, iter: [(idx, len(list(iter)))], 
        preservesPartitioning=True
    ).collect()
    
    print("Partition sizes:")
    for part_id, count in partition_counts:
        print(f"  Partition {part_id}: {count} rows")
    
    print("\n--- Partition Size Guidelines ---")
    print("Small data (< 1GB): 2-4 partitions")
    print("Medium data (1-10GB): 10-50 partitions")
    print("Large data (10-100GB): 50-200 partitions")
    print("Very large data (> 100GB): 200-1000 partitions")


def section_2_default_partitioning(spark):
    """
    Section 2: Default Partitioning
    Demonstrates default partition settings
    """
    print("\n" + "=" * 80)
    print("SECTION 2: DEFAULT PARTITIONING")
    print("=" * 80)
    
    print("\n--- Default Shuffle Partitions ---")
    print(f"spark.sql.shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
    print("This is the default number of partitions for shuffle operations")
    
    # Create sample data
    data = [(i, f"user_{i}", i % 50) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id"])
    
    print(f"\nDataFrame created with {df.count()} rows")
    print(f"Initial partitions: {df.rdd.getNumPartitions()}")
    
    # GroupBy triggers shuffle
    df_grouped = df.groupBy("group_id").count()
    print(f"After groupBy (shuffle): {df_grouped.rdd.getNumPartitions()} partitions")
    
    print("\n--- Tuning Default Partitions ---")
    print("Formula: total_data_size / target_partition_size (128MB)")
    print("Example: 10GB data / 128MB = ~80 partitions")
    
    # Demonstrate setting custom shuffle partitions
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    print(f"\nSet spark.sql.shuffle.partitions to 4")
    
    df_grouped2 = df.groupBy("group_id").count()
    print(f"After groupBy with new setting: {df_grouped2.rdd.getNumPartitions()} partitions")
    
    # Reset to default
    spark.conf.set("spark.sql.shuffle.partitions", "8")


def section_3_repartition_vs_coalesce(spark):
    """
    Section 3: Repartition vs Coalesce
    Demonstrates the difference between repartition and coalesce
    """
    print("\n" + "=" * 80)
    print("SECTION 3: REPARTITION VS COALESCE")
    print("=" * 80)
    
    # Create sample data with few partitions
    data = [(i, f"user_{i}", i % 10) for i in range(1, 1001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id"])
    df = df.coalesce(2)  # Start with 2 partitions
    
    print(f"\nStarting with {df.rdd.getNumPartitions()} partitions")
    
    print("\n--- Repartition ---")
    print("repartition(num): full shuffle, can increase or decrease partitions")
    print("More expensive but ensures even distribution")
    
    start = time.time()
    df_repartitioned = df.repartition(8)
    repartition_time = time.time() - start
    
    print(f"repartition(8): {df_repartitioned.rdd.getNumPartitions()} partitions")
    print(f"Time: {repartition_time:.4f} seconds")
    
    print("\n--- Coalesce ---")
    print("coalesce(num): no shuffle, only decreases partitions")
    print("Faster but may result in uneven distribution")
    
    start = time.time()
    df_coalesced = df.coalesce(1)
    coalesce_time = time.time() - start
    
    print(f"coalesce(1): {df_coalesced.rdd.getNumPartitions()} partitions")
    print(f"Time: {coalesce_time:.4f} seconds")
    
    print(f"\nCoalesce is {repartition_time/coalesce_time:.1f}x faster than repartition")
    
    print("\n--- When to Use Each ---")
    print("Use repartition when:")
    print("  - You need to increase partitions")
    print("  - You need even distribution after filtering")
    print("  - You need to redistribute data for joins")
    print("\nUse coalesce when:")
    print("  - You only need to decrease partitions")
    print("  - Performance is critical")
    print("  - After filtering to reduce partitions")
    
    # Demonstrate filtering + coalesce pattern
    print("\n--- Best Practice: Filter + Coalesce ---")
    df_large = spark.range(10000).withColumn("value", col("id") % 100)
    df_filtered = df_large.filter(col("value") > 90)
    
    print(f"Before filter: {df_large.rdd.getNumPartitions()} partitions")
    print(f"After filter: {df_filtered.rdd.getNumPartitions()} partitions")
    print(f"After coalesce: {df_filtered.coalesce(2).rdd.getNumPartitions()} partitions")
    print("Coalesce after filtering reduces overhead for small remaining dataset")


def section_4_partitioning_strategies(spark):
    """
    Section 4: Partitioning Strategies
    Demonstrates hash, range, and custom partitioning
    """
    print("\n" + "=" * 80)
    print("SECTION 4: PARTITIONING STRATEGIES")
    print("=" * 80)
    
    # Create sample data
    data = [(i, f"user_{i}", i % 10, 2020 + (i % 5)) for i in range(1, 1001)]
    df = spark.createDataFrame(data, ["id", "name", "group_id", "year"])
    
    print("\n--- Hash Partitioning (Default) ---")
    print("Default for most operations")
    print("Uses hashCode of partition key to distribute data")
    
    df_hash = df.repartition(4, "group_id")
    print(f"repartition(4, 'group_id'): {df_hash.rdd.getNumPartitions()} partitions")
    
    # Show distribution
    def count_by_partition(iterator):
        yield (len(list(iterator)),)
    
    partition_counts = df_hash.rdd.mapPartitions(count_by_partition).collect()
    print("Partition distribution:")
    for i, count in enumerate(partition_counts):
        print(f"  Partition {i}: {count[0]} rows")
    
    print("\n--- Range Partitioning ---")
    print("For ordered data")
    print("Ensures data in range is in same partition")
    
    df_range = df.repartitionByRange(4, "id")
    print(f"repartitionByRange(4, 'id'): {df_range.rdd.getNumPartitions()} partitions")
    print("Data with similar IDs will be in same partition")
    
    print("\n--- Partitioning for Joins/Aggregations ---")
    print("Partition by columns used in joins/aggregations")
    print("Reduces shuffle for subsequent operations")
    
    # Create two DataFrames with same partitioning
    df1 = df.repartition(4, "group_id")
    df2 = df.select("group_id", "year").repartition(4, "group_id")
    
    print("\nBoth DataFrames partitioned by 'group_id'")
    print("Join on 'group_id' will be more efficient")
    
    joined = df1.join(df2, "group_id")
    print(f"Joined DataFrame: {joined.rdd.getNumPartitions()} partitions")
    
    print("\n--- Choosing Partition Key ---")
    print("Choose partition key based on:")
    print("  - Columns used in joins")
    print("  - Columns used in groupBy")
    print("  - Data distribution (avoid skew)")
    print("  - Cardinality (high cardinality is better)")


def section_5_bucketing(spark):
    """
    Section 5: Bucketing
    Demonstrates bucketing for frequent joins
    """
    print("\n" + "=" * 80)
    print("SECTION 5: BUCKETING")
    print("=" * 80)
    
    print("\n--- Understanding Bucketing ---")
    print("Bucketing pre-shuffles data for frequent joins")
    print("Reduces shuffle for subsequent joins on bucketed columns")
    print("Works best for equality joins on bucketed columns")
    
    # Create sample data
    users_data = [(i, f"user_{i}", i % 100) for i in range(1, 1001)]
    orders_data = [(i, i % 100, i * 10) for i in range(1, 2001)]
    
    users_df = spark.createDataFrame(users_data, ["user_id", "name", "region"])
    orders_df = spark.createDataFrame(orders_data, ["order_id", "user_id", "amount"])
    
    print("\n--- Without Bucketing ---")
    print("Join requires shuffle")
    
    start = time.time()
    joined = users_df.join(orders_df, "user_id")
    joined_count = joined.count()
    no_bucket_time = time.time() - start
    
    print(f"Join result: {joined_count} rows")
    print(f"Time: {no_bucket_time:.4f} seconds")
    
    print("\n--- With Bucketing ---")
    print("Save tables with bucketing")
    
    # Note: In practice, you would save these as managed tables
    # For demo, we'll show the pattern
    
    print("\nPattern for bucketing:")
    print("users_df.write.bucketBy(10, 'user_id').sortBy('user_id').saveAsTable('users')")
    print("orders_df.write.bucketBy(10, 'user_id').sortBy('user_id').saveAsTable('orders')")
    print("\nSubsequent joins on user_id will not require shuffle")
    
    print("\n--- Bucketing Best Practices ---")
    print("  - Use same number of buckets for joined tables")
    print("  - Bucket on join columns")
    print("  - Sort by join columns for additional optimization")
    print("  - Best for frequently joined tables")
    print("  - Not needed for one-time joins")
    
    print("\n--- Bucketing vs Partitioning ---")
    print("Partitioning:")
    print("  - Creates directory structure")
    print("  - Good for filtering on partition columns")
    print("  - Can have many partitions")
    print("\nBucketing:")
    print("  - Creates fixed number of files")
    print("  - Good for joins on bucketed columns")
    print("  - Fixed number of buckets")
    print("  - Can be combined with partitioning")


def section_6_partition_pruning(spark):
    """
    Section 6: Partition Pruning
    Demonstrates partition pruning for efficient reads
    """
    print("\n" + "=" * 80)
    print("SECTION 6: PARTITION PRUNING")
    print("=" * 80)
    
    print("\n--- Understanding Partition Pruning ---")
    print("Partition pruning: Spark reads only relevant partitions")
    print("Reduces I/O significantly")
    print("Works with Parquet, ORC, and other file formats")
    
    # Create sample data with dates
    data = []
    for year in [2020, 2021, 2022]:
        for month in range(1, 13):
            for day in range(1, 11):
                data.append((f"{year}-{month:02d}-{day:02d}", year, month, day, year * 10000 + month * 100 + day))
    
    df = spark.createDataFrame(data, ["date", "year", "month", "day", "value"])
    
    print(f"\nCreated DataFrame with {df.count()} rows")
    print("Data spans 3 years (2020-2022)")
    
    print("\n--- Writing with Partitioning ---")
    print("Save data partitioned by year and month")
    
    # For demo, we'll just show the pattern
    print("\nPattern:")
    print("df.write.partitionBy('year', 'month').parquet('/path/to/data')")
    
    print("\nThis creates directory structure:")
    print("/path/to/data/")
    print("  year=2020/")
    print("    month=01/")
    print("    month=02/")
    print("    ...")
    print("  year=2021/")
    print("    ...")
    
    print("\n--- Reading with Partition Pruning ---")
    print("When filtering on partition columns, Spark only reads relevant partitions")
    
    print("\nExample queries:")
    print("df.filter(col('year') == 2021)  # Only reads year=2021 partition")
    print("df.filter(col('year') == 2021).filter(col('month') == 6)  # Only reads year=2021/month=06")
    
    print("\n--- Partition Pruning Best Practices ---")
    print("  - Partition by frequently filtered columns")
    print("  - Avoid too many partitions (1000s of folders)")
    print("  - Target: ~1GB per partition")
    print("  - Use low cardinality columns for partitioning")
    print("  - Common: date, region, category")
    
    print("\n--- Over-Partitioning Issues ---")
    print("Too many small partitions cause:")
    print("  - NameNode pressure (HDFS)")
    print("  - Small file problem")
    print("  - Slow listing operations")
    print("  - Increased metadata overhead")
    
    print("\n--- Under-Partitioning Issues ---")
    print("Too few partitions cause:")
    print("  - Reading unnecessary data")
    print("  - Longer query times")
    print("  - Less parallelism")


def section_7_salting_for_skewed_data(spark):
    """
    Section 7: Salting for Skewed Data
    Demonstrates salting technique for handling skewed joins
    """
    print("\n" + "=" * 80)
    print("SECTION 7: SALTING FOR SKEWED DATA")
    print("=" * 80)
    
    print("\n--- Understanding Data Skew ---")
    print("Data skew: some partitions have much more data than others")
    print("Causes stragglers (slow tasks)")
    print("Common in joins on skewed keys")
    
    # Create skewed data
    skewed_data = []
    # Most data for key 1
    for i in range(900):
        skewed_data.append((1, f"user_{i}", i))
    # Few data for other keys
    for i in range(100):
        skewed_data.append((i + 2, f"user_{i+900}", i + 900))
    
    df_skewed = spark.createDataFrame(skewed_data, ["key", "name", "value"])
    
    print(f"\nCreated skewed DataFrame with {df_skewed.count()} rows")
    
    # Show distribution
    df_distribution = df_skewed.groupBy("key").count().orderBy("count", ascending=False)
    print("\nKey distribution:")
    df_distribution.show(5)
    
    print("\n--- Problem: Skewed Join ---")
    print("Join on skewed key causes one partition to be very large")
    
    other_data = [(i, f"other_{i}") for i in range(1, 12)]
    df_other = spark.createDataFrame(other_data, ["key", "other_name"])
    
    print("\nWithout salting, join on 'key' will be slow")
    
    print("\n--- Solution: Salting ---")
    print("Add random salt to skewed keys to distribute data")
    
    # Add salt to skewed DataFrame
    from pyspark.sql.functions import floor, rand
    
    # Add salt (0-9) to all rows
    df_salted = df_skewed.withColumn("salt", floor(rand() * 10))
    
    # Explode key with salt
    df_salted = df_salted.withColumn("salted_key", 
                                     when(col("key") == 1, 
                                         concat(col("key"), lit("_"), col("salt")))
                                     .otherwise(col("key")))
    
    print("\nAdded salt to create salted_key")
    print("Key 1 becomes: 1_0, 1_1, 1_2, ..., 1_9")
    
    # Add salt to other DataFrame (explode for key 1)
    from pyspark.sql.functions import explode, array, lit
    
    # Create salted version of other DataFrame
    other_salted_list = []
    for i in range(1, 12):
        if i == 1:
            for salt in range(10):
                other_salted_list.append((i, f"other_{i}", salt, f"{i}_{salt}"))
        else:
            other_salted_list.append((i, f"other_{i}", 0, str(i)))
    
    df_other_salted = spark.createDataFrame(
        other_salted_list, 
        ["key", "other_name", "salt", "salted_key"]
    )
    
    print("\nExploded other DataFrame for key 1")
    
    # Join on salted key
    joined_salted = df_salted.join(df_other_salted, "salted_key")
    
    print(f"\nJoined on salted_key: {joined_salted.count()} rows")
    print("Data is now distributed across 10 partitions instead of 1")
    
    print("\n--- Salting Best Practices ---")
    print("  - Use when you have skewed keys")
    print("  - Choose salt size based on skew degree")
    print("  - Remove salt after join if needed")
    print("  - Monitor for even distribution")
    
    print("\n--- Alternative: AQE Skew Join (Spark 3.0+) ---")
    print("spark.sql.adaptive.skewJoin.enabled: true")
    print("Automatically handles skewed joins")
    print("No manual salting needed")


def section_8_dynamic_partition_pruning(spark):
    """
    Section 8: Dynamic Partition Pruning
    Demonstrates dynamic partition pruning (Spark 3.0+)
    """
    print("\n" + "=" * 80)
    print("SECTION 8: DYNAMIC PARTITION PRUNING")
    print("=" * 80)
    
    print("\n--- Understanding Dynamic Partition Pruning ---")
    print("Spark 3.0+ feature")
    print("Prunes partitions based on join conditions")
    print("Especially useful for star schema queries")
    
    # Create fact table (large)
    fact_data = []
    for year in [2020, 2021, 2022]:
        for month in range(1, 13):
            for i in range(100):
                fact_data.append((year, month, i, year * 10000 + month * 100 + i))
    
    fact_df = spark.createDataFrame(fact_data, ["year", "month", "day", "value"])
    
    # Create dimension table (small)
    dim_data = [(2021, "2021 Data"), (2022, "2022 Data")]
    dim_df = spark.createDataFrame(dim_data, ["year", "description"])
    
    print(f"\nFact table: {fact_df.count()} rows (spanning 3 years)")
    print(f"Dimension table: {dim_df.count()} rows (filtering to 2 years)")
    
    print("\n--- Without Dynamic Partition Pruning ---")
    print("Would read all partitions from fact table")
    
    print("\n--- With Dynamic Partition Pruning ---")
    print("Spark infers from dimension table which partitions to read")
    print("Only reads year=2021 and year=2022 partitions")
    
    # Enable dynamic partition pruning
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning", "true")
    print(f"\nDynamic partition pruning enabled: {spark.conf.get('spark.sql.optimizer.dynamicPartitionPruning')}")
    
    # Join with dynamic partition pruning
    joined = fact_df.join(dim_df, "year")
    
    print("\nQuery plan will show dynamic partition pruning")
    print("Check with: joined.explain()")
    
    print("\n--- When Dynamic Partition Pruning Helps ---")
    print("  - Star schema queries (fact joins dimension)")
    print("  - Dimension table filters fact table partitions")
    print("  - Large fact tables with many partitions")
    print("  - Dimension table is small or filtered")
    
    print("\n--- Configuration ---")
    print("spark.sql.optimizer.dynamicPartitionPruning: enable/disable")
    print("spark.sql.optimizer.dynamicPartitionPruningFallbackFilterRatio: fallback ratio")
    print("spark.sql.optimizer.dynamicPartitionPruningReuseSubquery: reuse subquery")
    
    print("\n--- Monitoring ---")
    print("Check query plan for 'DynamicPruning' expressions")
    print("Look for reduced scan metrics in Spark UI")


def main():
    """
    Main function to run all demonstrations
    """
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run all sections
        section_1_partitioning_basics(spark)
        section_2_default_partitioning(spark)
        section_3_repartition_vs_coalesce(spark)
        section_4_partitioning_strategies(spark)
        section_5_bucketing(spark)
        section_6_partition_pruning(spark)
        section_7_salting_for_skewed_data(spark)
        section_8_dynamic_partition_pruning(spark)
        
        print("\n" + "=" * 80)
        print("ALL DEMONSTRATIONS COMPLETED")
        print("=" * 80)
        print("\nKey Takeaways:")
        print("1. Target 128MB-256MB per partition")
        print("2. Use coalesce to decrease, repartition to increase")
        print("3. Partition by frequently filtered columns")
        print("4. Use bucketing for frequent joins")
        print("5. Use salting for skewed data")
        print("6. Enable dynamic partition pruning for star schemas")
        print("\nCheck Spark UI at http://localhost:4040 for partition metrics")
        
    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()
