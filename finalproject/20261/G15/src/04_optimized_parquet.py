"""
Pipeline B (otimizado) - ingestao dos blocos .parquet (uint8) gerados pelo 03_spark_etl.

Sem decode de JPEG no caminho quente: o parquet ja entrega pixel cru uint8. A CPU so le o byte
e transfere; o cast + escala ([-1,1]) do MobileNet roda na GPU. A leitura sequencial de poucos
arquivos grandes elimina o Small File Problem -> a ingestao acompanha a GPU e o starvation cai.

Leitura via PYARROW (nao tensorflow-io, que arrastaria um TF que sobrescreveria o TF/NGC da NVIDIA):
from_generator + interleave (varios arquivos em paralelo) + prefetch.

Saidas em RESULTS_DIR: parquet_forward_{per_round,stats,raw_samples}.csv + comparacao_jpg_vs_parquet.*
"""

import glob
import time
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")                 # headless
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import tensorflow as tf

tf.keras.mixed_precision.set_global_policy("mixed_float16")   # mesma config do 02, antes do modelo

import bench_common as bc

PARQUET_DIR = bc.PARQUET_DIR
RESULTS_DIR = Path(bc.RESULTS_DIR)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
IMG_SIZE    = bc.IMG_SIZE
CYCLE       = 2                       # arquivos parquet lidos em paralelo (casa com os 2 vCPU)

for gpu in tf.config.list_physical_devices("GPU"):
    try:
        tf.config.experimental.set_memory_growth(gpu, True)
    except Exception as e:  # noqa: BLE001
        print("memory_growth:", e)

print("TensorFlow:", tf.__version__, "| policy:", tf.keras.mixed_precision.global_policy().name)
print(f"vCPUs (cgroup): {bc.N_CONTAINER_CPUS} | RAM limite (cgroup): {bc.CONTAINER_MEM_LIMIT_GB:.1f} GB")

GPU_HANDLE = bc.init_gpu_handle()

# ---------------------------------------------------------------------------
# Ingestao do parquet -> tf.data via pyarrow.
# ---------------------------------------------------------------------------
PARQUET_FILES = sorted(glob.glob(f"{PARQUET_DIR}/*.parquet"))
if not PARQUET_FILES:
    raise FileNotFoundError(f"Nenhum .parquet em {PARQUET_DIR}. Rode o 03_spark_etl.py antes.")
print(f"Arquivos parquet: {len(PARQUET_FILES)}")


def _parquet_gen(path):
    # Roda numa thread do tf.data; le 1 arquivo em lotes (o Arrow libera o GIL no I/O/deserialize,
    # entao o interleave paraleliza de verdade).
    if isinstance(path, bytes):
        path = path.decode()
    pf = pq.ParquetFile(path)
    for b in pf.iter_batches(batch_size=1024, columns=["label", "data"]):
        labels  = b.column("label").to_numpy(zero_copy_only=False)
        buffers = b.column("data").to_pylist()
        for lbl, buf in zip(labels, buffers):
            # bytes uint8 -> (224,224,3). SEM decode de JPEG -> esse e o ganho do Pipeline B.
            yield (np.frombuffer(buf, dtype=np.uint8).reshape(IMG_SIZE[0], IMG_SIZE[1], 3),
                   np.float32(lbl))


def build_parquet_dataset(batch_size, n_images=None):
    ds = tf.data.Dataset.from_tensor_slices(PARQUET_FILES)
    ds = ds.interleave(
        lambda p: tf.data.Dataset.from_generator(
            _parquet_gen, args=(p,),
            output_signature=(
                tf.TensorSpec(shape=(IMG_SIZE[0], IMG_SIZE[1], 3), dtype=tf.uint8),
                tf.TensorSpec(shape=(), dtype=tf.float32),
            ),
        ),
        cycle_length=CYCLE,                    # arquivos lidos em paralelo
        num_parallel_calls=tf.data.AUTOTUNE,
        deterministic=False,
    )
    if n_images is not None:
        ds = ds.take(n_images)
    ds = ds.batch(batch_size)
    ds = ds.prefetch(tf.data.AUTOTUNE)         # sobrepoe leitura (CPU) e forward (GPU)
    return ds


model = bc.build_model()


@tf.function
def infer(x_uint8):
    # PREPROCESS NA GPU: o parquet entrega pixel cru uint8; o cast + escala ([-1,1]) roda aqui.
    x = tf.cast(x_uint8, tf.float16) / 127.5 - 1.0
    return model(x, training=False)


print("Modelo pronto (MobileNetV2 + FP16, preprocess na GPU). Params:", f"{model.count_params():,}")

# ---------------------------------------------------------------------------
# Rodadas do pipeline real.
# ---------------------------------------------------------------------------
round_summaries, raw_samples = [], []

_ds_warm = build_parquet_dataset(bc.BATCH_SIZE, bc.N_IMAGES)   # warmup fora do cronometro
for x, _ in _ds_warm.take(5):
    infer(x)

for r in range(1, bc.N_ROUNDS + 1):
    print(f"\n===== Rodada {r}/{bc.N_ROUNDS} =====")
    ds = build_parquet_dataset(bc.BATCH_SIZE, bc.N_IMAGES)

    monitor = bc.ResourceMonitor(GPU_HANDLE, interval=bc.SAMPLE_EVERY)
    monitor.start()
    n_images = 0
    t0 = time.time()
    for _ in range(bc.N_PASSES):
        for x, y in ds:
            infer(x)                                   # forward (preprocess na GPU)
            n_images += int(x.shape[0])
    elapsed = time.time() - t0
    df_s = monitor.stop(); df_s["round"] = r; raw_samples.append(df_s)

    summary = bc.summarize_round(r, n_images, elapsed, df_s)
    round_summaries.append(summary)
    print(f"  imagens={n_images:,} | tempo={elapsed:.1f}s | "
          f"throughput={summary['throughput_img_s']:.1f} img/s | "
          f"CPU={summary['cpu_mean_pct']:.0f}% | GPU={summary['gpu_util_mean_pct']:.0f}%")

summary_df = pd.DataFrame(round_summaries)
raw_df = pd.concat(raw_samples, ignore_index=True)

# ---------------------------------------------------------------------------
# Tetos isolados (mesma logica do 02) -> quantifica o starvation deste pipeline.
# ---------------------------------------------------------------------------
x_fixed, _ = next(iter(build_parquet_dataset(bc.BATCH_SIZE, bc.N_IMAGES)))
infer(x_fixed)                                   # warmup do teste de GPU
N_STEPS = max(30, 15000 // bc.BATCH_SIZE)

cpu_ceils, gpu_ceils = [], []
for r in range(1, bc.N_ROUNDS + 1):
    # TETO CPU: leitura+deserializacao do parquet, SEM modelo (produtor)
    ds_cpu = build_parquet_dataset(bc.BATCH_SIZE, bc.N_IMAGES)
    n, t0 = 0, time.time()
    for _ in range(bc.N_PASSES):
        for x, y in ds_cpu:
            n += int(x.shape[0])
    cpu_ceils.append(n / (time.time() - t0))
    # TETO GPU: forward sobre 1 batch fixo uint8 (consumidor)
    n, t0 = 0, time.time()
    for _ in range(N_STEPS):
        infer(x_fixed)
        n += int(x_fixed.shape[0])
    gpu_ceils.append(n / (time.time() - t0))
    print(f"  rodada {r}: teto CPU(leitura)={cpu_ceils[-1]:.0f} | teto GPU={gpu_ceils[-1]:.0f} img/s")

cpu_ceiling     = float(np.mean(cpu_ceils))
cpu_ceiling_std = float(np.std(cpu_ceils, ddof=1)) if bc.N_ROUNDS > 1 else 0.0
gpu_ceiling     = float(np.mean(gpu_ceils))
gpu_ceiling_std = float(np.std(gpu_ceils, ddof=1)) if bc.N_ROUNDS > 1 else 0.0

# ---------------------------------------------------------------------------
# Estatisticas + CSVs.
# ---------------------------------------------------------------------------
stats = bc.compute_stats("parquet_uint8_forward", summary_df,
                         cpu_ceiling, cpu_ceiling_std, gpu_ceiling, gpu_ceiling_std)
stats_df = pd.DataFrame([stats])
summary_df.to_csv(RESULTS_DIR / "parquet_forward_per_round.csv", index=False)
stats_df.to_csv(RESULTS_DIR / "parquet_forward_stats.csv", index=False)
raw_df.to_csv(RESULTS_DIR / "parquet_forward_raw_samples.csv", index=False)

tp_mean, tp_std = stats["throughput_mean_img_s"], stats["throughput_std_img_s"]
gpu_waste_pct = stats["gpu_waste_pct"]
print(f"\nTeto CPU (leitura)  : {cpu_ceiling:.0f} +/- {cpu_ceiling_std:.0f} img/s   (produtor)")
print(f"Teto GPU (forward)  : {gpu_ceiling:.0f} +/- {gpu_ceiling_std:.0f} img/s   (consumidor)")
print(f"Pipeline parquet    : {tp_mean:.0f} +/- {tp_std:.0f} img/s")
print(f">> GPU desperdicada : {gpu_waste_pct:.0f}%  (starvation)")
print(f"GPU util {stats['gpu_util_mean_pct']:.0f}% | CPU {stats['cpu_mean_pct']:.0f}% | "
      f"RAM {stats['cont_mem_peak_gb']:.2f}/{bc.CONTAINER_MEM_LIMIT_GB:.1f} GB")

# ---------------------------------------------------------------------------
# Comparacao com o Pipeline A (.jpg), se o 02 ja rodou.
# ---------------------------------------------------------------------------
jpg_csv = RESULTS_DIR / "jpg_forward_stats.csv"
if not jpg_csv.exists():
    print(f"[!] {jpg_csv} nao encontrado. Rode o 02 (jpg_forward) antes p/ a comparacao.")
else:
    jpg = pd.read_csv(jpg_csv).iloc[0]
    speedup = tp_mean / jpg["throughput_mean_img_s"]
    cmp = pd.DataFrame({
        "metrica": ["throughput (img/s)", "GPU util (%)", "CPU util (%)",
                    "teto CPU/leitura (img/s)", "GPU desperdicada (%)"],
        "jpg (A)": [jpg["throughput_mean_img_s"], jpg["gpu_util_mean_pct"], jpg["cpu_mean_pct"],
                    jpg["cpu_ceiling_img_s"], jpg["gpu_waste_pct"]],
        "parquet (B)": [tp_mean, stats["gpu_util_mean_pct"], stats["cpu_mean_pct"],
                        cpu_ceiling, gpu_waste_pct],
        "delta": [f"{speedup:.2f}x",
                  f"{stats['gpu_util_mean_pct'] - jpg['gpu_util_mean_pct']:+.0f} pp",
                  f"{stats['cpu_mean_pct'] - jpg['cpu_mean_pct']:+.0f} pp",
                  f"{cpu_ceiling / jpg['cpu_ceiling_img_s']:.2f}x",
                  f"{gpu_waste_pct - jpg['gpu_waste_pct']:+.0f} pp"],
    })
    print("\n" + cmp.to_string(index=False))
    cmp.to_csv(RESULTS_DIR / "comparacao_jpg_vs_parquet.csv", index=False)

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    ax = axes[0]
    ax.bar(["jpg (A)", "parquet (B)"], [jpg["throughput_mean_img_s"], tp_mean],
           yerr=[jpg["throughput_std_img_s"], tp_std], capsize=6,
           color=["#c0392b", "#27ae60"], alpha=0.85)
    ax.set_title(f"Throughput de ingestao  ({speedup:.2f}x)")
    ax.set_ylabel("imagens / segundo")
    for i, v in enumerate([jpg["throughput_mean_img_s"], tp_mean]):
        ax.text(i, v, f"{v:.0f}", ha="center", va="bottom")

    ax = axes[1]
    x = np.arange(2); w = 0.35
    ax.bar(x - w/2, [jpg["cpu_mean_pct"], stats["cpu_mean_pct"]], w, label="CPU", color="#2980b9", alpha=0.85)
    ax.bar(x + w/2, [jpg["gpu_util_mean_pct"], stats["gpu_util_mean_pct"]], w, label="GPU", color="#27ae60", alpha=0.85)
    ax.set_xticks(x); ax.set_xticklabels(["jpg (A)", "parquet (B)"]); ax.set_ylim(0, 100)
    ax.set_title("Utilizacao CPU vs GPU"); ax.set_ylabel("%"); ax.legend()
    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "comparacao_jpg_vs_parquet.png", dpi=120, bbox_inches="tight")
    plt.close(fig)

print("\nPipeline B (parquet) concluido.")
