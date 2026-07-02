import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def execute_workload_once(fn):
    result = fn()
    result.cache()
    result.count()  # force full execution
    return result


def run_workload(name, fn):
    start = time.time()
    result = execute_workload_once(fn)
    elapsed = time.time() - start
    print(f"[TIMING] {name}: {elapsed:.3f}s")
    return result, elapsed


def run_workload_async(name, fn, repeat_count=100):
    if repeat_count < 1:
        raise ValueError("repeat_count must be at least 1")

    start = time.time()
    with ThreadPoolExecutor(max_workers=repeat_count) as executor:
        futures = [executor.submit(execute_workload_once, fn) for _ in range(repeat_count)]
        results = [future.result() for future in as_completed(futures)]
    elapsed = time.time() - start
    print(f"[TIMING] {name}: {elapsed:.3f}s ({repeat_count} async repetitions)")
    return results[0], elapsed


def workload1_weapon_effectiveness(df):
    return (
        df.groupBy("wp", "wp_type")
        .agg(
            F.count("*").alias("events"),
            F.avg("hp_dmg").alias("avg_hp_dmg"),
            F.avg("arm_dmg").alias("avg_arm_dmg"),
            (F.sum(F.when(F.col("hitbox") == "Head", 1).otherwise(0)) / F.count("*") * 100)
            .alias("headshot_pct"),
        )
        .orderBy(F.desc("avg_hp_dmg"))
    )


def workload2_rank_vs_performance(df):
    per_player = (
        df.groupBy("att_id", "att_rank")
        .agg(
            F.sum("hp_dmg").alias("total_hp_dmg"),
            F.count("*").alias("events"),
            F.avg("hp_dmg").alias("avg_hp_dmg"),
        )
    )
    return (
        per_player.groupBy("att_rank")
        .agg(
            F.count("att_id").alias("players"),
            F.avg("avg_hp_dmg").alias("avg_dmg_per_event"),
            F.avg("total_hp_dmg").alias("avg_total_dmg"),
        )
        .orderBy("att_rank")
    )


def workload3_side_advantage(df):
    side_stats = (
        df.groupBy("att_side", "vic_side", "is_bomb_planted")
        .agg(
            F.count("*").alias("events"),
            F.avg("hp_dmg").alias("avg_hp_dmg"),
            F.sum("hp_dmg").alias("total_hp_dmg"),
        )
        .orderBy("att_side", "vic_side", "is_bomb_planted")
    )
    return side_stats


def parse_workloads(raw_value):
    if not raw_value:
        return [1, 2, 3]

    workloads = []
    for token in raw_value.split(","):
        value = token.strip()
        if not value:
            continue
        try:
            workload_id = int(value)
        except ValueError as exc:
            raise ValueError(f"Invalid workload '{value}'") from exc
        if workload_id not in {1, 2, 3}:
            raise ValueError(f"Workload '{value}' is not supported")
        workloads.append(workload_id)

    if not workloads:
        raise ValueError("No workloads selected")
    return workloads


def read_input_dataframe(spark, input_path):
    if os.path.isdir(input_path):
        csv_files = sorted(
            os.path.join(input_path, name)
            for name in os.listdir(input_path)
            if name.lower().endswith(".csv")
        )
        if not csv_files:
            raise FileNotFoundError(f"No CSV files were found in directory: {input_path}")

        print(f"[INFO] Reading {len(csv_files)} CSV file(s) from directory: {input_path}")
        reader = spark.read.option("header", True).option("inferSchema", True)
        if len(csv_files) == 1:
            return reader.csv(csv_files[0]).cache()
        return reader.csv(csv_files).cache()

    if os.path.isfile(input_path):
        print(f"[INFO] Reading CSV file: {input_path}")
        return spark.read.option("header", True).option("inferSchema", True).csv(input_path).cache()

    raise FileNotFoundError(f"Input path not found: {input_path}")


def main():
    parser = argparse.ArgumentParser(description="Run ESEA damage analysis workloads")
    parser.add_argument("input_path")
    parser.add_argument("output_dir")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--workloads", default="1,2,3")
    parser.add_argument("--repeat-count", type=int, default=100)
    args = parser.parse_args()

    try:
        selected_workloads = parse_workloads(args.workloads)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)

    if args.repeat_count < 1:
        print("[ERROR] --repeat-count must be greater than 0")
        sys.exit(1)

    spark = (
        SparkSession.builder
        .appName("ESEA-CS:GO-Damage-Analysis")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Workers declared: {args.workers}")
    print(f"[INFO] Runs declared: {args.runs}")
    print(f"[INFO] Workloads declared: {','.join(map(str, selected_workloads))}")
    print(f"[INFO] Repeat count: {args.repeat_count}")
    print(f"[INFO] Reading from: {args.input_path}")

    read_start = time.time()
    df = read_input_dataframe(spark, args.input_path)
    total = df.count()
    read_elapsed = time.time() - read_start

    print(f"[INFO] Rows loaded: {total}")
    print(f"[TIMING] DATA_READ: {read_elapsed:.3f}s")

    timings = {}

    if 1 in selected_workloads:
        result1, timings["WORKLOAD-1"] = run_workload_async(
            "WORKLOAD-1 (Weapon Effectiveness)",
            lambda: workload1_weapon_effectiveness(df),
            repeat_count=args.repeat_count,
        )
        result1.write.mode("overwrite").csv(f"{args.output_dir}/workload1", header=True)

    if 2 in selected_workloads:
        result2, timings["WORKLOAD-2"] = run_workload_async(
            "WORKLOAD-2 (Rank vs Performance)",
            lambda: workload2_rank_vs_performance(df),
            repeat_count=args.repeat_count,
        )
        result2.write.mode("overwrite").csv(f"{args.output_dir}/workload2", header=True)

    if 3 in selected_workloads:
        result3, timings["WORKLOAD-3"] = run_workload_async(
            "WORKLOAD-3 (Side Advantage CT vs T)",
            lambda: workload3_side_advantage(df),
            repeat_count=args.repeat_count,
        )
        result3.write.mode("overwrite").csv(f"{args.output_dir}/workload3", header=True)

    print("\n[SUMMARY] Execution times:")
    print(f"  DATA_READ: {read_elapsed:.3f}s")
    for name, t in timings.items():
        print(f"  {name}: {t:.3f}s")

    spark.stop()


if __name__ == "__main__":
    main()
