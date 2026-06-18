#!/bin/bash

echo " Starting Experiments"

# Experiment Parameters
MEMORY="32g"
MIN_CORES=32
MAX_CORES=32
REPETITIONS=5

echo "Configuration:"
echo "- Executor cores: $MIN_CORES to $MAX_CORES"
echo "- Executor memory: $MEMORY"
echo "- Runs: $REPETITIONS"

# Outer Loop: Iterate through the number of cores (from MIN to MAX)
for (( CORES=$MIN_CORES; CORES<=$MAX_CORES; CORES++ )); do
    # Run the same configuration multiple times
    for (( REP=1; REP<=$REPETITIONS; REP++ )); do
        echo "Starting run: [Cores: $CORES] - Repetition $REP of $REPETITIONS"
        
        # Run the ETL script
        ./bin/run-etl.sh $CORES $RAM_LIMIT
        
        echo "Run finished"
        sleep 5
    done
done

echo "Experiments completed!  "
