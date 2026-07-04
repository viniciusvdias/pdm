#!/bin/bash

# Ensure the Docker environment is actively running before starting the benchmark
#docker compose up -d

# Pause execution to allow MinIO and the setup container to initialize and upload data
sleep 15

# Define paths for local size measurement and output logging
INPUT_PATH="s3a://datasample/"
LOCAL_DATA_DIR="./datasample"
BASH_LOG_DIR="./results/execution_metrics"
mkdir -p "$BASH_LOG_DIR"
BASH_LOG="$BASH_LOG_DIR/execution_times.csv"

# Calculate exact input size in bytes using standard Linux disk usage to measure throughput context
DATA_SIZE_BYTES=$(du -sb "$LOCAL_DATA_DIR" | cut -f1)

# Initialize the benchmark tracking file with the CSV headers
if [ ! -f "$BASH_LOG" ]; then
    echo "Timestamp,Input_Size_Bytes,Cores,Memory,Repetition,Execution_Time_Sec" > "$BASH_LOG"
fi

# Configure hardware allocation and repetition limits for the experiment
MEMORY="8g"
MIN_CORES=1
MAX_CORES=1
REPETITIONS=1

# Iterate through the defined range of CPU cores
for (( CORES=$MIN_CORES; CORES<=$MAX_CORES; CORES++ )); do

    # Execute multiple passes of the same configuration
    for (( REP=1; REP<=$REPETITIONS; REP++ )); do
        # Capture the start time to calculate wall clock execution duration
        START_TIME=$SECONDS

        # Run the etl script
        ./bin/run-etl.sh $CORES $MEMORY

        # Capture the exit code of the Spark job to detect fatal crashes
        SPARK_EXIT_CODE=$?

        # Calculate the total elapsed time in seconds
        ELAPSED_TIME=$(( SECONDS - START_TIME ))

        # Validate job success or abort in case of crashes
        if [ $SPARK_EXIT_CODE -eq 0 ]; then
            echo "$(date '+%Y-%m-%d %H:%M:%S'),$DATA_SIZE_BYTES,$CORES,$MEMORY,$REP,$ELAPSED_TIME" >> "$BASH_LOG"
            sleep 5
        else
            echo "Fatal crash detected on Cores $CORES and Repetition $REP"
            echo "Stopping benchmark to prevent false data"
            exit 1
        fi
    done
done

echo "Experiments completed"
