"""Gera ``presentation/presentation.pdf`` de forma reprodutível (matplotlib).

Cada slide é uma página A4 paisagem com título e bullets. Mantém o conteúdo
versionado em código, evitando binário "mágico". Roda no container app
(matplotlib já instalado): ver ``bin/make_slides.sh``.
"""

from __future__ import annotations

import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

SLIDES = [
    ("Motor de Liquidação estilo Pix com Apache Flink (PyFlink)", [
        "Grupo G3 — PDM 2026/1",
        "Stream processing · Event-time · Exactly-once",
        "Dataset: PaySim (~6,36M transações, amplificado para >=1GB)",
    ]),
    ("Problema", [
        "Pix: dezenas de milhões de transações/dia, liquidação em segundos",
        "1) Latência baixíssima  -> stream processing",
        "2) Correção financeira  -> exactly-once (diferença de 1 centavo = falha)",
        "3) Resiliência a falhas -> event-time + checkpointing",
    ]),
    ("Arquitetura", [
        "producer -> Kafka tx.raw -> Flink (settlement + AML)",
        "settlement: ledger keyed por conta, ordem de event-time, RocksDB+MinIO",
        "sinks exactly-once -> tx.outcomes / aml.alerts",
        "consumer -> Postgres (saldo final) ; Prometheus+Grafana",
        "tudo em Docker Compose (roda out-of-the-box)",
    ]),
    ("Exactly-once: a prova", [
        "Baseline batch determinístico = saldo final por conta",
        "Stream em EXACTLY_ONCE deve bater AO CENTAVO com o baseline",
        "Demo de falha: matar um TaskManager no meio do processamento",
        "Recovery do último checkpoint -> reconciliação = ZERO",
        "AT_LEAST_ONCE + falha -> divergência (double-count)",
    ]),
    ("Detecção de lavagem (AML)", [
        "Ciclo A->B->C->A em janela de 10 min (arestas TRANSFER)",
        "Structuring: transferências fragmentadas just-below-limit",
        "Velocidade: muitas transações da mesma conta em janela curta",
        "Avaliação precision/recall vs rótulo isFraud",
    ]),
    ("Carga com bursts aleatórios", [
        "Picos aleatórios de volume (10–80× a taxa base)",
        "Injeção de transações sintéticas de ALTO VALOR",
        "Estressa watermarks, backpressure, checkpoint e recovery",
        "Modo reprodutível (seed) p/ experimentos; aleatório p/ demo",
    ]),
    ("Experimentos (média ± desvio, >=3 reps)", [
        "W1 throughput × paralelismo ; W2 latência P50/95/99",
        "W3 custo do exactly-once ; W4 tempo de recovery",
        "W5 reconciliação financeira ; W6 custo de checkpoint",
        "W7 precision/recall AML ; W8 estado ; W9 bursts",
    ]),
    ("Conclusão", [
        "Demonstra visualmente o valor do exactly-once",
        "TaskManager morre ao vivo -> recupera -> saldo bate ao centavo",
        "Stack 100% Docker, reprodutível, em Python (PyFlink)",
    ]),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default="presentation/presentation.pdf")
    args = ap.parse_args()

    with PdfPages(args.output) as pdf:
        for title, bullets in SLIDES:
            fig = plt.figure(figsize=(11.69, 8.27))  # A4 paisagem
            fig.patch.set_facecolor("white")
            ax = fig.add_axes([0, 0, 1, 1]); ax.axis("off")
            ax.add_patch(plt.Rectangle((0, 0.86), 1, 0.14, color="#1e3a8a"))
            ax.text(0.05, 0.90, title, fontsize=24, color="white",
                    weight="bold", va="center")
            y = 0.74
            for b in bullets:
                ax.text(0.07, y, "•  " + b, fontsize=16, color="#111827", va="top")
                y -= 0.09
            ax.text(0.95, 0.03, "PDM 2026/1 · G3", fontsize=9,
                    color="#6b7280", ha="right")
            pdf.savefig(fig); plt.close(fig)
    print(f"Slides -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
