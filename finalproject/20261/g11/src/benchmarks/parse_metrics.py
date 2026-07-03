#!/usr/bin/env python3
"""
Parser de metricas do benchmark (cenarios A e B)
================================================

Le os arquivos JSONL de StreamingQueryProgress produzidos pelo stream_job.py
(um JSON por micro-batch, gravado pelo ProgressFileListener) e:

  1.  Por RUN: agrega os micro-batches num resumo (throughput e latencia).
  2.  Por CONFIGURACAO (varias runs): calcula MEDIA e DESVIO PADRAO.

Definicoes das metricas
-----------------------
- THROUGHPUT (registros/s): total de linhas de entrada processadas no run
  dividido pelo tempo total gasto em micro-batches que tiveram entrada
  (sum(numInputRows) / sum(triggerExecutionMs com numInputRows>0) * 1000).
  Esta e a vazao "de regime" (ignora batches vazios de aquecimento/idle), que e
  o que interessa comparar entre configuracoes.
- MICRO-BATCH LATENCY (ms): media do triggerExecutionMs entre os micro-batches
  NAO-vazios do run (numInputRows>0). E o tempo de processamento de um
  micro-batch — a latencia de processamento do Structured Streaming.

So contam micro-batches com numInputRows>0 (descarta os triggers vazios que o
Spark emite enquanto espera dados ou depois de drenar o backlog), porque eles
distorceriam latencia/vazao.

Modos de uso
------------
  # 1) Resumo de UM run (1 ou mais arquivos JSONL -> 1 linha por (run)):
  python parse_metrics.py run --run-id A_cores1_rep1 \
      --label scenario=A,param=cores,value=1,workers=1,rep=1 \
      out1.jsonl out2.jsonl

  Isso ANEXA uma linha ao CSV de runs (--runs-csv, default runs.csv).

  # 2) Agregacao final por configuracao (le o runs.csv e gera summary.csv):
  python parse_metrics.py aggregate --runs-csv runs.csv --out summary.csv
"""

import argparse
import csv
import json
import math
import os
import sys
from collections import defaultdict


# --------------------------------------------------------------------------- #
# Leitura dos JSONL de micro-batch
# --------------------------------------------------------------------------- #
def load_batches(paths):
    """Le todos os JSONL e retorna a lista de registros de micro-batch."""
    batches = []
    for path in paths:
        if not os.path.exists(path):
            print(f"[parse] aviso: arquivo inexistente, ignorado: {path}",
                  file=sys.stderr)
            continue
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    batches.append(json.loads(line))
                except json.JSONDecodeError:
                    print(f"[parse] aviso: linha JSON invalida em {path}",
                          file=sys.stderr)
    return batches


def summarize_run(batches):
    """Agrega os micro-batches de UM run em (throughput, latencia, contagens).

    Retorna dict com:
      total_input_rows, n_nonempty_batches, sum_trigger_ms_nonempty,
      throughput_rps, avg_latency_ms.
    """
    total_rows = 0
    n_nonempty = 0
    sum_ms = 0.0
    latencies = []
    for b in batches:
        rows = b.get("numInputRows") or 0
        if rows <= 0:
            continue
        ms = b.get("triggerExecutionMs")
        if ms is None:
            continue
        total_rows += rows
        n_nonempty += 1
        sum_ms += ms
        latencies.append(ms)

    throughput = (total_rows / (sum_ms / 1000.0)) if sum_ms > 0 else 0.0
    avg_latency = (sum(latencies) / len(latencies)) if latencies else 0.0
    return {
        "total_input_rows": total_rows,
        "n_nonempty_batches": n_nonempty,
        "sum_trigger_ms_nonempty": round(sum_ms, 1),
        "throughput_rps": round(throughput, 1),
        "avg_latency_ms": round(avg_latency, 1),
    }


# --------------------------------------------------------------------------- #
# Estatistica
# --------------------------------------------------------------------------- #
def mean_std(values):
    """Media e desvio padrao AMOSTRAL (n-1). Com 1 valor, std=0."""
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mu = sum(values) / n
    if n == 1:
        return mu, 0.0
    var = sum((v - mu) ** 2 for v in values) / (n - 1)
    return mu, math.sqrt(var)


# --------------------------------------------------------------------------- #
# Subcomando: run  (anexa 1 linha de resumo de run ao runs.csv)
# --------------------------------------------------------------------------- #
RUN_FIELDS = [
    "run_id", "scenario", "param", "value", "workers", "rep",
    "throughput_rps", "avg_latency_ms",
    "total_input_rows", "n_nonempty_batches", "sum_trigger_ms_nonempty",
]


def parse_label(label):
    """Converte 'k1=v1,k2=v2' num dict."""
    out = {}
    if not label:
        return out
    for pair in label.split(","):
        pair = pair.strip()
        if not pair:
            continue
        if "=" in pair:
            k, v = pair.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def cmd_run(args):
    batches = load_batches(args.jsonl)
    summary = summarize_run(batches)
    label = parse_label(args.label)

    row = {
        "run_id": args.run_id,
        "scenario": label.get("scenario", ""),
        "param": label.get("param", ""),
        "value": label.get("value", ""),
        "workers": label.get("workers", ""),
        "rep": label.get("rep", ""),
        "throughput_rps": summary["throughput_rps"],
        "avg_latency_ms": summary["avg_latency_ms"],
        "total_input_rows": summary["total_input_rows"],
        "n_nonempty_batches": summary["n_nonempty_batches"],
        "sum_trigger_ms_nonempty": summary["sum_trigger_ms_nonempty"],
    }

    write_header = not os.path.exists(args.runs_csv) or os.path.getsize(args.runs_csv) == 0
    with open(args.runs_csv, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RUN_FIELDS)
        if write_header:
            w.writeheader()
        w.writerow(row)

    print(f"[parse] run {args.run_id}: "
          f"throughput={row['throughput_rps']} reg/s, "
          f"latencia={row['avg_latency_ms']} ms "
          f"({summary['n_nonempty_batches']} micro-batches, "
          f"{summary['total_input_rows']:,} linhas) -> {args.runs_csv}")
    if summary["n_nonempty_batches"] == 0:
        print("[parse] AVISO: nenhum micro-batch com entrada — verifique se o "
              "producer rodou e se o metrics-file foi gravado.", file=sys.stderr)


# --------------------------------------------------------------------------- #
# Subcomando: aggregate  (runs.csv -> summary.csv com media+desvio)
# --------------------------------------------------------------------------- #
SUMMARY_FIELDS = [
    "scenario", "param", "value", "workers", "n_runs",
    "throughput_rps_mean", "throughput_rps_std",
    "latency_ms_mean", "latency_ms_std",
]


def cmd_aggregate(args):
    if not os.path.exists(args.runs_csv):
        print(f"[parse] erro: {args.runs_csv} nao existe — rode os 'run' antes.",
              file=sys.stderr)
        sys.exit(1)

    groups = defaultdict(list)
    with open(args.runs_csv, newline="") as fh:
        reader = csv.DictReader(fh)
        # Normaliza os nomes das colunas (tolera espacos de alinhamento no header,
        # ex.: " scenario" -> "scenario").
        if reader.fieldnames:
            reader.fieldnames = [(name or "").strip() for name in reader.fieldnames]
        for r in reader:
            # Tira espaco das chaves E dos valores (CSV pode vir alinhado).
            r = {(k or "").strip(): (v.strip() if isinstance(v, str) else v)
                 for k, v in r.items()}
            # Agrupa por (scenario, param, value, workers): a "configuracao".
            try:
                key = (r["scenario"], r["param"], r["value"], r["workers"])
                tp = float(r["throughput_rps"])
                lat = float(r["avg_latency_ms"])
            except (ValueError, KeyError):
                continue
            groups[key].append((tp, lat))

    rows = []
    for (scenario, param, value, workers), vals in sorted(groups.items()):
        tps = [v[0] for v in vals]
        lats = [v[1] for v in vals]
        tp_mu, tp_sd = mean_std(tps)
        lat_mu, lat_sd = mean_std(lats)
        rows.append({
            "scenario": scenario,
            "param": param,
            "value": value,
            "workers": workers,
            "n_runs": len(vals),
            "throughput_rps_mean": round(tp_mu, 1),
            "throughput_rps_std": round(tp_sd, 1),
            "latency_ms_mean": round(lat_mu, 1),
            "latency_ms_std": round(lat_sd, 1),
        })

    with open(args.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=SUMMARY_FIELDS)
        w.writeheader()
        w.writerows(rows)

    print(f"[parse] resumo por configuracao -> {args.out}")
    # Eco legivel no terminal.
    for r in rows:
        print(f"  [{r['scenario']}] {r['param']}={r['value']} workers={r['workers']} "
              f"(n={r['n_runs']}): "
              f"throughput {r['throughput_rps_mean']}±{r['throughput_rps_std']} reg/s | "
              f"latencia {r['latency_ms_mean']}±{r['latency_ms_std']} ms")


# --------------------------------------------------------------------------- #
def main():
    p = argparse.ArgumentParser(description="Parser de metricas do benchmark")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("run", help="resume 1 run e anexa ao runs.csv")
    pr.add_argument("jsonl", nargs="+", help="arquivo(s) JSONL de micro-batch")
    pr.add_argument("--run-id", required=True)
    pr.add_argument("--label", default="",
                    help="rotulos k=v separados por virgula: "
                         "scenario,param,value,workers,rep")
    pr.add_argument("--runs-csv", default="runs.csv")
    pr.set_defaults(func=cmd_run)

    pa = sub.add_parser("aggregate", help="agrega runs.csv -> summary.csv (media+desvio)")
    pa.add_argument("--runs-csv", default="runs.csv")
    pa.add_argument("--out", default="summary.csv")
    pa.set_defaults(func=cmd_aggregate)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
