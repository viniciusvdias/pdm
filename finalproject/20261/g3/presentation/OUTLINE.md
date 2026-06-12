# Slides — roteiro

O PDF da apresentação é **gerado de forma reprodutível** (conteúdo versionado em
`src/presentation/make_slides.py`), garantindo o requisito "roda via Docker":

```bash
./bin/make_slides.sh        # cria presentation/presentation.pdf
```

Sequência de slides:

1. **Capa** — Motor de Liquidação estilo Pix (PyFlink) · G3 · 3 pilares.
2. **Problema** — Pix: latência, correção financeira, resiliência.
3. **Arquitetura** — producer → Kafka → Flink (settlement+AML) → outcomes →
   Postgres/Grafana; tudo em Docker Compose.
4. **Exactly-once: a prova** — baseline batch determinístico; demo de falha
   (matar TaskManager) → reconciliação ao centavo; ALO diverge.
5. **AML** — ciclo A→B→C→A, structuring, velocidade; precision/recall vs isFraud.
6. **Bursts** — picos aleatórios de volume e alto valor; stress de checkpoint/recovery.
7. **Experimentos** — W1–W9, média ± desvio (≥3 reps), gráficos com barras de erro.
8. **Conclusão** — valor visual do exactly-once; stack 100% Docker em Python.

> Recomenda-se gravar a **demo de falha** como vídeo de backup para a apresentação.
