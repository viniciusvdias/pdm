#!/usr/bin/env bash
# Run all QA tools; never abort — results go to reports/qa_<timestamp>.md
set -uo pipefail
cd "$(dirname "$0")/.."

STAMP=$(date +%Y%m%dT%H%M%S)
REPORT="reports/qa_${STAMP}.md"
mkdir -p reports

if [[ -d .venv ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
PYTHON_VER=$(python --version 2>&1)
RUFF_VER=$(ruff --version 2>&1 || echo "n/a")
PYLINT_VER=$(pylint --version 2>&1 | head -1 || echo "n/a")
BANDIT_VER=$(bandit --version 2>&1 | head -1 || echo "n/a")
PYTEST_VER=$(pytest --version 2>&1 || echo "n/a")

run_step() {
  local name=$1
  shift
  echo "==> $name"
  "$@" > "/tmp/qa_${STAMP}_${name}.out" 2>&1
  echo $? > "/tmp/qa_${STAMP}_${name}.code"
}

run_step ruff ruff check src/
run_step pylint pylint src/ --score=y
run_step bandit bandit -r src/ -q
run_step pytest pytest --cov=src --cov-report=term-missing tests/unit/ -q
run_step mutmut bash -c "mutmut run && mutmut results"

{
  echo "# Relatório QA — ${STAMP}"
  echo
  echo "**Git commit**: ${GIT_COMMIT}"
  echo "**Python**: ${PYTHON_VER}"
  echo "**Ferramentas**: ruff ${RUFF_VER}; ${PYLINT_VER}; ${BANDIT_VER}; ${PYTEST_VER}"
  echo

  for tool in ruff pylint bandit pytest mutmut; do
    code=$(cat "/tmp/qa_${STAMP}_${tool}.code" 2>/dev/null || echo "?")
    echo "## ${tool} (exit ${code})"
    echo
    echo '```'
    cat "/tmp/qa_${STAMP}_${tool}.out" 2>/dev/null || echo "(no output)"
    echo '```'
    echo
  done

  echo "## Code Review (python-code-review)"
  echo
  echo "_Run \`/python-code-review\` and paste results here._"
} > "$REPORT"

echo "Wrote $REPORT"
exit 0
