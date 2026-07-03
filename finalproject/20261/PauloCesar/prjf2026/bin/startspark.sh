#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPARK_DIR="$PROJECT_ROOT/src/spark"
BOOTSTRAP_SCRIPT="$PROJECT_ROOT/bin/bootstrap_python_env.sh"

if [[ ! -x "$BOOTSTRAP_SCRIPT" ]]; then
  echo "Script de bootstrap nao encontrado: $BOOTSTRAP_SCRIPT" >&2
  exit 1
fi

VENV_DIR="$("$BOOTSTRAP_SCRIPT")"
PYTHON_BIN="$VENV_DIR/bin/python3"
SPARK_SUBMIT_BIN="$($PYTHON_BIN - <<'PY'
import os
import pyspark
print(os.path.join(os.path.dirname(pyspark.__file__), 'bin', 'spark-submit'))
PY
)"

exec "$SPARK_SUBMIT_BIN" --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.3 "$SPARK_DIR/spark_consumer.py"
