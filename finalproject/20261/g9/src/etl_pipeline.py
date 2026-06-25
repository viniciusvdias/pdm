import sys
import os
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def main():
    # Generate a timestamp to uniquely identify the output folder
    start_datetime_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Validate command line arguments for input and output paths
    if len(sys.argv) != 3:
        print("Usage: spark-submit etl_pipeline.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_dir = sys.argv[2]

    # Initialize the SparkSession with MinIO integration
    spark = SparkSession.builder \
        .appName("Subgraph Optimization Log ETL") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "pdm_minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "pdm_minio") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Set the number of shuffle partitions based on the available cores to optimize memory allocation
    active_cores = spark.sparkContext.defaultParallelism
    max_shuffle_partitions = max(active_cores * 30, 200)
    spark.conf.set("spark.sql.shuffle.partitions", str(max_shuffle_partitions))

    # Read raw text logs from object storage and attach the source filename to preserve data lineage
    repetition_regex = r"-(\d+)\.txt(?:\.gz)?$"
    df_raw = spark.read.text(input_path) \
        .withColumn("file_path", F.input_file_name()) \
        .withColumn("repetition", F.regexp_extract("file_path", repetition_regex, 1).cast("int"))

    # Filter and parse the initialization lines to extract graph parameters and execution configurations
    args_regex = r"args is set to '.*?/([^/ ]+)\s+(\d+)\s+(\d+)\s+(?:-?\d+)\s+(\d+)\s+([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_]+)"
    threads_regex = r"--executor-cores\s+(\d+)"

    df_args = df_raw.filter(F.col("value").like("%args is set to '%")).select(
        "file_path", "repetition",
        F.regexp_extract("value", args_regex, 1).alias("graph_name"),
        F.regexp_extract("value", args_regex, 6).alias("metaheuristic"),
        F.regexp_extract("value", args_regex, 2).cast("int").alias("initial_vertices"),
        F.regexp_extract("value", args_regex, 3).cast("int").alias("num_initial_solutions"),
        F.regexp_extract("value", args_regex, 4).cast("int").alias("timeout_ms"),
        F.regexp_extract("value", args_regex, 5).alias("objective_function")
    )

    # Filter and parse the lines containing the number of allocated executor cores
    df_threads = df_raw.filter(F.col("value").like("%--executor-cores%")).select(
        "file_path",
        F.regexp_extract("value", threads_regex, 1).cast("int").alias("num_threads")
    )

    # Extract lines representing the final best subgraph solutions found during the run
    time_regex = r"ElapsedTimeMs=(\d+)"
    cost_regex = r"BestSubgraph=.*cost=([0-9.]+)"
    v_regex = r"BestSubgraph=.*nvertices=(\d+)"
    e_regex = r"BestSubgraph=.*nedges=(\d+)"
    run_regex = r"SubgraphOptimization\s+(\d+)"

    df_best_raw = df_raw.filter(F.col("value").like("%BestSubgraph=%")).select(
        "file_path",
        F.regexp_extract("value", time_regex, 1).cast("int").alias("total_time_ms"),
        F.regexp_extract("value", cost_regex, 1).cast("double").alias("cost_best_solution"),
        F.regexp_extract("value", v_regex, 1).cast("int").alias("vertices_best_solution"),
        F.regexp_extract("value", e_regex, 1).cast("int").alias("edges_best_solution"),
        F.regexp_extract("value", run_regex, 1).cast("int").alias("run_number"),
        F.col("value").alias("best_solution")
    )

    # Aggregate the best solution data by file to find the ultimate cost and total runs
    df_best = df_best_raw.groupBy("file_path").agg(
        F.max("total_time_ms").alias("total_time_ms"),
        F.max("cost_best_solution").alias("cost_best_solution"),
        F.max("vertices_best_solution").alias("vertices_best_solution"),
        F.max("edges_best_solution").alias("edges_best_solution"),
        F.max("best_solution").alias("best_solution"),
        (F.max("run_number") + 1).alias("effective_runs")
    )

    # Filter lines containing temporal intermediate costs to build the execution timeline
    timestamp_regex = r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"
    inter_regex = r"^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\s+\S+\s+\d+\s+(\d+)\s+(\d+).*\s([0-9.]+)$"

    df_timeline = df_raw.filter(F.col("value").rlike(r"^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}")).select(
        "file_path",
        F.to_timestamp(F.regexp_extract("value", timestamp_regex, 1), "yy/MM/dd HH:mm:ss").alias("log_timestamp"),
        F.regexp_extract("value", inter_regex, 1).cast("int").alias("inter_v"),
        F.regexp_extract("value", inter_regex, 2).cast("int").alias("inter_e"),
        F.regexp_extract("value", inter_regex, 3).cast("double").alias("inter_cost")
    ).filter(F.col("inter_cost").isNotNull())


    # Broadcast and join the small parameter dataframes to prevent expensive network shuffles
    df_params = df_args.join(F.broadcast(df_threads), on="file_path", how="left")
    df_final_parameters = df_params.join(F.broadcast(df_best), on="file_path", how="left")

    # Identify the exact starting timestamp for each specific log file
    df_start_time = df_timeline.groupBy("file_path").agg(F.min("log_timestamp").alias("start_timestamp"))
    df_final_parameters = df_final_parameters.join(F.broadcast(df_start_time), on="file_path", how="left")

    # Match the intermediate timeline states against the known best solution to locate its exact completion timestamp
    df_join = df_timeline.join(F.broadcast(df_final_parameters), on="file_path", how="inner")

    df_matched = df_join.filter(
        (F.col("inter_v") == F.col("vertices_best_solution")) &
        (F.col("inter_e") == F.col("edges_best_solution")) &
        (F.abs(F.col("inter_cost") - F.col("cost_best_solution")) < 1e-6)
    )

    df_best_time = df_matched.groupBy("file_path").agg(
        F.min("log_timestamp").alias("best_solution_timestamp")
    )

    df_final_table = df_final_parameters.join(df_best_time, on="file_path", how="left")

    # Calculate the time the algorithm spent to reach its best solution
    df_final_table = df_final_table.withColumn(
        "time_to_best_solution_ms",
        (F.unix_timestamp("best_solution_timestamp") - F.unix_timestamp("start_timestamp")) * 1000
    )

    # Reorder the dataframe columns
    expected_columns = [
        "graph_name", "metaheuristic", "initial_vertices", "num_initial_solutions",
        "timeout_ms", "objective_function", "num_threads", "repetition",
        "total_time_ms", "time_to_best_solution_ms", "effective_runs",
        "cost_best_solution", "vertices_best_solution", "edges_best_solution",
        "best_solution"
    ]
    df_final_table = df_final_table.select(*expected_columns)

    # Write the final dataframe as a CSV
    csv_foldername = os.path.join(output_dir, f"fractal_metrics_{start_datetime_str}")
    df_final_table.write.csv(csv_foldername, header=True, mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    main()