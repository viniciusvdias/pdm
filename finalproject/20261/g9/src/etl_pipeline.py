import sys
import time
import os
import csv
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def main():
    start_time = time.time()
    start_datetime_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if len(sys.argv) != 3:
        print("Usage: spark-submit etl_pipeline.py <input_s3_path> <output_local_dir>")
        sys.exit(1)
        
    input_path = sys.argv[1]
    output_dir = sys.argv[2]
    
    print(f"--- Starting ETL Pipeline ---")
    print(f"Input Path: {input_path}")
    print(f"Output Directory: {output_dir}")
    
    spark = SparkSession.builder \
        .appName("Subgraph Optimization Log ETL") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "pdm_minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "pdm_minio") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    active_cores = spark.sparkContext.defaultParallelism

    # --- PHASE 1: Data Ingestion & Optimization ---
    # FIX: We MUST capture the file_name BEFORE we repartition, otherwise Spark loses the file lineage!
    df_raw = spark.read.text(input_path) \
        .withColumn("file_path", F.input_file_name()) \
        .repartition(active_cores)

    # --- PHASE 2: Regex Extraction ---
    repetition_regex = r"-(\d+)\.txt(?:\.gz)?$"
    args_regex = r"args is set to '.*?/([^/ ]+)\s+(\d+)\s+(\d+)\s+(?:-?\d+)\s+(\d+)\s+([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_]+)"
    threads_regex = r"--executor-cores\s+(\d+)"
    time_regex = r"ElapsedTimeMs=(\d+)"
    cost_regex = r"BestSubgraph=.*cost=([0-9.]+)"
    v_regex = r"BestSubgraph=.*nvertices=(\d+)"
    e_regex = r"BestSubgraph=.*nedges=(\d+)"
    run_regex = r"SubgraphOptimization\s+(\d+)"
    timestamp_regex = r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})"
    inter_regex = r"^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\s+\S+\s+\d+\s+(\d+)\s+(\d+).*\s([0-9.]+)$"

    df_extracted = df_raw.select(
        "file_path", "value",
        F.regexp_extract("file_path", repetition_regex, 1).cast("int").alias("repetition"),
        F.regexp_extract("value", args_regex, 1).alias("graph_name"),
        F.regexp_extract("value", args_regex, 6).alias("metaheuristic"),
        F.regexp_extract("value", args_regex, 2).cast("int").alias("initial_vertices"),
        F.regexp_extract("value", args_regex, 3).cast("int").alias("num_initial_solutions"),
        F.regexp_extract("value", args_regex, 4).cast("int").alias("timeout_ms"),
        F.regexp_extract("value", args_regex, 5).alias("objective_function"),
        F.regexp_extract("value", threads_regex, 1).cast("int").alias("num_threads"),
        F.regexp_extract("value", time_regex, 1).cast("int").alias("total_time_ms"),
        F.regexp_extract("value", cost_regex, 1).cast("double").alias("cost_best_solution"),
        F.regexp_extract("value", v_regex, 1).cast("int").alias("vertices_best_solution"),
        F.regexp_extract("value", e_regex, 1).cast("int").alias("edges_best_solution"),
        F.regexp_extract("value", run_regex, 1).cast("int").alias("run_number"),
        F.to_timestamp(F.regexp_extract("value", timestamp_regex, 1), "yy/MM/dd HH:mm:ss").alias("log_timestamp"),
        F.regexp_extract("value", inter_regex, 1).cast("int").alias("inter_v"),
        F.regexp_extract("value", inter_regex, 2).cast("int").alias("inter_e"),
        F.regexp_extract("value", inter_regex, 3).cast("double").alias("inter_cost"),
        
        # NEW: Extract the raw line if it contains the BestSubgraph string
        F.when(F.col("value").like("%BestSubgraph=%"), F.col("value")).otherwise(F.lit(None)).alias("best_solution_raw")
    )

    df_extracted.cache()
    total_lines = df_extracted.count()

    # --- PHASE 3: Aggregation ---
    df_final_parameters = df_extracted.groupBy("file_path", "repetition").agg(
        F.max("graph_name").alias("graph_name"),
        F.max("metaheuristic").alias("metaheuristic"),
        F.max("initial_vertices").alias("initial_vertices"),
        F.max("num_initial_solutions").alias("num_initial_solutions"),
        F.max("timeout_ms").alias("timeout_ms"),
        F.max("objective_function").alias("objective_function"),
        F.max("num_threads").alias("num_threads"),
        F.max("total_time_ms").alias("total_time_ms"),
        F.max("cost_best_solution").alias("cost_best_solution"),
        F.max("vertices_best_solution").alias("vertices_best_solution"),
        F.max("edges_best_solution").alias("edges_best_solution"),
        F.max("best_solution_raw").alias("best_solution"), # NEW: Grab the best raw string
        (F.max("run_number") + 1).alias("effective_runs"),
        F.min("log_timestamp").alias("start_timestamp")
    )

    # --- PHASE 4: The Temporal Join ---
    df_timeline = df_extracted.filter(F.col("inter_cost").isNotNull()).select(
        "file_path", "log_timestamp", "inter_v", "inter_e", "inter_cost"
    )

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

    df_final_table = df_final_table.withColumn(
        "time_to_best_solution_ms",
        (F.unix_timestamp("best_solution_timestamp") - F.unix_timestamp("start_timestamp")) * 1000
    )
    
    # FORMATTING: Order the columns exactly as requested in the Google Sheets snippet
    expected_columns = [
        "graph_name", "metaheuristic", "initial_vertices", "num_initial_solutions",
        "timeout_ms", "objective_function", "num_threads", "repetition", 
        "total_time_ms", "time_to_best_solution_ms", "effective_runs", 
        "cost_best_solution", "vertices_best_solution", "edges_best_solution", 
        "best_solution"
    ]
    df_final_table = df_final_table.select(*expected_columns)

    # --- PHASE 5: Save Output as dynamically named CSV (Zero Dependencies) ---
    csv_filename = os.path.join(output_dir, f"fractal_metrics_{start_datetime_str}.csv")
    final_rows = df_final_table.collect()
    
    if final_rows:
        with open(csv_filename, mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(df_final_table.columns)
            for row in final_rows:
                writer.writerow(row)
                
    print(f"Results successfully saved to {csv_filename}")

    # --- PHASE 6: Benchmark & Telemetry ---
    end_time = time.time()
    execution_time = end_time - start_time
    throughput = total_lines / execution_time if execution_time > 0 else 0

    benchmark_file = os.path.join(output_dir, "benchmarks.csv")
    file_exists = os.path.isfile(benchmark_file)

    with open(benchmark_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["Timestamp", "Total_Files_Processed", "Total_Lines", "Active_Spark_Cores", "Execution_Time_Sec", "Throughput_Lines_Per_Sec"])
        # Update: We use len(final_rows) to accurately count the number of files processed
        writer.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), len(final_rows), total_lines, active_cores, round(execution_time, 2), round(throughput, 2)])

    print(f"--- Benchmark Logged: {round(execution_time, 2)}s | {round(throughput, 2)} lines/sec ---")
    spark.stop()

if __name__ == "__main__":
    main()