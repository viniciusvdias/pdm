#!/bin/bash
echo "-------> Running run-etl.sh <--------"
# Get the number of CPU cores and RAM allocation parameters
CORES=${1:-"2"}
MEMORY=${2:-"2g"}

# Define internal container paths for the S3 input data and result outputs
INPUT_PATH=${3:-"s3a://datasample/"}
OUTPUT_DIR="/opt/spark/work-dir/results/fractal_results"
SPARK_LOG_DIR="/tmp/spark-logs"
SPILL_DIR="/tmp/spark-spill"

docker exec pdm-spark-master mkdir -p "$SPARK_LOG_DIR"
docker exec pdm-spark-master mkdir -p "$SPILL_DIR"

# Execute the Spark job natively inside the master container
# Override the MinIO endpoint configuration to use the Docker internal network name
docker exec -i pdm-spark-master bash -c "/opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --total-executor-cores $CORES \
    --executor-memory $MEMORY \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.driver.host=pdm-spark-master \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=file://$SPARK_LOG_DIR \
    --conf spark.local.dir=$SPILL_DIR \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=4g \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=pdm_minio \
    --conf spark.hadoop.fs.s3a.secret.key=pdm_minio \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    /opt/spark/work-dir/src/etl_pipeline.py \
    $INPUT_PATH \
    $OUTPUT_DIR"

EXIT_CODE=$?

echo "Spark exit code: $EXIT_CODE"
echo "-------> Finished run-etl.sh <--------"

exit $EXIT_CODE