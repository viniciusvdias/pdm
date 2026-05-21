import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_workload(name, fn):
    start = time.time()
    result = fn()
    result.cache()
    result.count()  # force full execution
    elapsed = time.time() - start
    print(f"[TIMING] {name}: {elapsed:.3f}s")
    return result, elapsed

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

def main():
    if len(sys.argv) < 3:
        print("Usage: main.py <input_csv> <output_dir>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_dir = sys.argv[2]

    spark = (
        SparkSession.builder
        .appName("ESEA-CS:GO-Damage-Analysis")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Reading: {input_path}")
    df = (
        spark.read.csv(input_path, header=True, inferSchema=True)
        .cache()
    )
    total = df.count()
    print(f"[INFO] Rows loaded: {total}")

    timings = {}

    result1, timings["WORKLOAD-1"] = run_workload(
        "WORKLOAD-1 (Weapon Effectiveness)",
        lambda: workload1_weapon_effectiveness(df),
    )
    result1.write.mode("overwrite").csv(f"{output_dir}/workload1", header=True)

    result2, timings["WORKLOAD-2"] = run_workload(
        "WORKLOAD-2 (Rank vs Performance)",
        lambda: workload2_rank_vs_performance(df),
    )
    result2.write.mode("overwrite").csv(f"{output_dir}/workload2", header=True)

    result3, timings["WORKLOAD-3"] = run_workload(
        "WORKLOAD-3 (Side Advantage CT vs T)",
        lambda: workload3_side_advantage(df),
    )
    result3.write.mode("overwrite").csv(f"{output_dir}/workload3", header=True)

    print("\n[SUMMARY] Execution times:")
    for name, t in timings.items():
        print(f"  {name}: {t:.3f}s")

    spark.stop()

if __name__ == "__main__":
    main()
