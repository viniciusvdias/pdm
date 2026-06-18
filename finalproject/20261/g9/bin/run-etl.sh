#!/bin/bash

# Define the input and output paths
INPUT_PATH="s3a://datasample/"
OUTPUT_DIR="/opt/spark/work-dir/results"

# Get number of threads and memory parameters
CORES=${1:-""}
MEMORY=${2:-"1g"}

# If the user provided a core count, build the Spark flag. 
# If empty, Spark automatically uses all available cores on the Worker.
CORE_FLAG=""
if [ -n "$CORES" ]; then
    CORE_FLAG="--total-executor-cores $CORES"
fi

echo "Submitting ETL job from the Spark Master"

# Execute the job
docker exec -it pdm-spark-master bash -c "cd /opt/spark/work-dir/src && /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    $CORE_FLAG \
    --executor-memory $MEMORY \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    etl_pipeline.py \
    '$INPUT_PATH' \
    '$OUTPUT_DIR'"

echo "Job finished!"
