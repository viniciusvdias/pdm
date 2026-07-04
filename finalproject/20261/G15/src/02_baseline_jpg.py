"""
Pipeline A (baseline) - ingestao de milhares de .jpg soltos com tf.data.

Small File Problem: cada imagem e lida e DECODIFICADA na CPU a cada passada. Com os 2 vCPU do
conteiner, o decode vira o gargalo (produtor lento) e a GPU (consumidor rapido) fica faminta.
Mede throughput (img/s) por rodada + tetos isolados de CPU (decode) e GPU (forward) para
quantificar o starvation. Roda N_ROUNDS vezes -> media +/- desvio padrao.

Saidas em RESULTS_DIR: jpg_forward_{per_round,stats,raw_samples}.csv + jpg_forward_*.png
"""

import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")                 # headless: salva PNG sem display
import matplotlib.pyplot as plt
import tensorflow as tf

# FP16 corta a VRAM de ativacao pela metade e usa os tensor cores. Antes de construir o modelo.
tf.keras.mixed_precision.set_global_policy("mixed_float16")

import bench_common as bc

DATA_GLOB   = os.path.join(bc.RAW_DIR, "*.jpg")
RESULTS_DIR = Path(bc.RESULTS_DIR)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
IMG_SIZE    = bc.IMG_SIZE

# Evita que o TF reserve 100% da VRAM de uma vez (ajuda o pynvml a medir a ocupacao real).
for gpu in tf.config.list_physical_devices("GPU"):
    try:
        tf.config.experimental.set_memory_growth(gpu, True)
    except Exception as e:  # noqa: BLE001
        print("memory_growth:", e)

print("TensorFlow:", tf.__version__, "| policy:", tf.keras.mixed_precision.global_policy().name)
print("GPUs visiveis:", tf.config.list_physical_devices("GPU"))
print(f"vCPUs (cgroup): {bc.N_CONTAINER_CPUS} | RAM limite (cgroup): {bc.CONTAINER_MEM_LIMIT_GB:.1f} GB")
print("Resultados em:", RESULTS_DIR)

GPU_HANDLE = bc.init_gpu_handle()

# ---------------------------------------------------------------------------
# tf.data sobre os .jpg soltos: le bytes -> decode (CPU) -> resize -> preprocess.
# ---------------------------------------------------------------------------
ALL_FILES = tf.io.gfile.glob(DATA_GLOB)
if bc.N_IMAGES is not None:
    ALL_FILES = ALL_FILES[:bc.N_IMAGES]
if len(ALL_FILES) == 0:
    raise FileNotFoundError(f"Nenhum .jpg em {DATA_GLOB}. Rode o 01_download.py antes.")
print(f"Imagens no dataset: {len(ALL_FILES):,}  (N_IMAGES={bc.N_IMAGES})")


def get_label(file_path):
    # tf.strings pq dentro do tf.data o processamento roda em modo grafo
    fname = tf.strings.split(file_path, os.sep)[-1]
    is_cat = tf.strings.regex_full_match(tf.strings.lower(fname), "cat.*")
    return tf.cast(tf.where(is_cat, 0, 1), tf.float32)   # 0.0 gato, 1.0 cachorro


def process_path(file_path):
    label = get_label(file_path)
    img = tf.io.read_file(file_path)                                 # le os bytes do disco
    img = tf.io.decode_jpeg(img, channels=3)                         # DECODE na CPU -> o gargalo
    img = tf.image.resize(img, IMG_SIZE)                             # resize na CPU
    img = tf.keras.applications.mobilenet_v2.preprocess_input(img)   # normaliza p/ MobileNetV2 ([-1,1])
    return img, label


def build_dataset(batch_size):
    ds = tf.data.Dataset.from_tensor_slices(ALL_FILES)
    ds = ds.shuffle(len(ALL_FILES), reshuffle_each_iteration=True)   # espalha a leitura pelo disco
    ds = ds.map(process_path, num_parallel_calls=tf.data.AUTOTUNE)
    ds = ds.batch(batch_size)
    ds = ds.prefetch(tf.data.AUTOTUNE)                               # sobrepoe decode (CPU) e forward (GPU)
    return ds


model = bc.build_model()


@tf.function
def infer(x):
    return model(x, training=False)      # forward-only, compilado em grafo


print("Modelo pronto (MobileNetV2, inferencia + FP16). Parametros:", f"{model.count_params():,}")

# ---------------------------------------------------------------------------
# Rodadas do pipeline real (throughput + serie temporal de recursos).
# ---------------------------------------------------------------------------
round_summaries, raw_samples = [], []

# Warmup FORA do cronometro: traca o @tf.function + esquenta pipeline/disco, pra a rodada 1
# nao carregar o custo do 1o tracado + leitura fria e inflar o desvio padrao.
_ds_warm = build_dataset(bc.BATCH_SIZE)
for x, _ in _ds_warm.take(5):
    infer(x)

for r in range(1, bc.N_ROUNDS + 1):
    print(f"\n===== Rodada {r}/{bc.N_ROUNDS} =====")
    ds = build_dataset(bc.BATCH_SIZE)      # shuffle -> re-decode a cada passada

    monitor = bc.ResourceMonitor(GPU_HANDLE, interval=bc.SAMPLE_EVERY)
    monitor.start()

    n_images = 0
    t0 = time.time()
    for _ in range(bc.N_PASSES):
        for x, y in ds:                    # y (label) nao e usado na inferencia
            infer(x)                       # forward-only na GPU
            n_images += int(x.shape[0])
    elapsed = time.time() - t0

    df_s = monitor.stop()
    df_s["round"] = r
    raw_samples.append(df_s)

    summary = bc.summarize_round(r, n_images, elapsed, df_s)
    round_summaries.append(summary)
    print(f"  imagens={n_images:,} | tempo={elapsed:.1f}s | "
          f"throughput={summary['throughput_img_s']:.1f} img/s | "
          f"CPU medio={summary['cpu_mean_pct']:.0f}% | GPU media={summary['gpu_util_mean_pct']:.0f}%")

summary_df = pd.DataFrame(round_summaries)
raw_df = pd.concat(raw_samples, ignore_index=True)

# ---------------------------------------------------------------------------
# Tetos isolados (img/s): quanto CPU (decode) e GPU (forward) aguentam sozinhas.
# O pipeline .jpg fica preso perto do teto da CPU, bem abaixo do teto da GPU:
# a diferenca e a capacidade de GPU desperdicada (starvation).
# ---------------------------------------------------------------------------
x_fixed, _ = next(iter(build_dataset(bc.BATCH_SIZE)))   # batch ja decodificado p/ o teto da GPU
infer(x_fixed)                                          # warmup: traca o grafo
N_STEPS = max(30, 15000 // bc.BATCH_SIZE)               # ~15k imgs por rodada no teste da GPU

cpu_ceils, gpu_ceils = [], []
for r in range(1, bc.N_ROUNDS + 1):
    # (a) TETO DA CPU: decode puro, sem GPU (produtor)
    ds_cpu = build_dataset(bc.BATCH_SIZE)
    n, t0 = 0, time.time()
    for _ in range(bc.N_PASSES):
        for x, y in ds_cpu:
            n += int(x.shape[0])
    cpu_ceils.append(n / (time.time() - t0))

    # (b) TETO DA GPU: forward sobre 1 batch fixo (consumidor), sem I/O
    n, t0 = 0, time.time()
    for _ in range(N_STEPS):
        infer(x_fixed)
        n += int(x_fixed.shape[0])
    gpu_ceils.append(n / (time.time() - t0))
    print(f"  rodada {r}: teto CPU={cpu_ceils[-1]:.0f} | teto GPU={gpu_ceils[-1]:.0f} img/s")

cpu_ceiling     = float(np.mean(cpu_ceils))
cpu_ceiling_std = float(np.std(cpu_ceils, ddof=1)) if bc.N_ROUNDS > 1 else 0.0
gpu_ceiling     = float(np.mean(gpu_ceils))
gpu_ceiling_std = float(np.std(gpu_ceils, ddof=1)) if bc.N_ROUNDS > 1 else 0.0

# ---------------------------------------------------------------------------
# Estatisticas + CSVs.
# ---------------------------------------------------------------------------
stats = bc.compute_stats("jpg_forward_fp16", summary_df,
                         cpu_ceiling, cpu_ceiling_std, gpu_ceiling, gpu_ceiling_std)
stats_df = pd.DataFrame([stats])

summary_df.to_csv(RESULTS_DIR / "jpg_forward_per_round.csv", index=False)
stats_df.to_csv(RESULTS_DIR / "jpg_forward_stats.csv", index=False)
raw_df.to_csv(RESULTS_DIR / "jpg_forward_raw_samples.csv", index=False)

tp_mean, tp_std = stats["throughput_mean_img_s"], stats["throughput_std_img_s"]
gpu_waste_pct = stats["gpu_waste_pct"]
print(f"\nTeto CPU (decode)    : {cpu_ceiling:.0f} +/- {cpu_ceiling_std:.0f} img/s   (produtor)")
print(f"Teto GPU (forward)   : {gpu_ceiling:.0f} +/- {gpu_ceiling_std:.0f} img/s   (consumidor)")
print(f"Pipeline .jpg (real) : {tp_mean:.0f} +/- {tp_std:.0f} img/s")
print(f">> GPU DESPERDICADA  : {gpu_waste_pct:.0f}%  (starvation)")
print(f"GPU util media {stats['gpu_util_mean_pct']:.0f}% | CPU media {stats['cpu_mean_pct']:.0f}% | "
      f"RAM pico {stats['cont_mem_peak_gb']:.2f}/{bc.CONTAINER_MEM_LIMIT_GB:.1f} GB")
print(f"CSVs salvos em {RESULTS_DIR}")

# ---------------------------------------------------------------------------
# Graficos.
# ---------------------------------------------------------------------------
fig, axes = plt.subplots(1, 3, figsize=(18, 5))

ax = axes[0]
ax.bar(summary_df["round"].astype(str), summary_df["throughput_img_s"], color="#c0392b", alpha=0.85)
ax.axhline(tp_mean, color="black", ls="--", label=f"media = {tp_mean:.0f} img/s")
ax.axhspan(tp_mean - tp_std, tp_mean + tp_std, color="gray", alpha=0.2, label=f"+/- desvio = {tp_std:.0f}")
ax.set_title("Throughput por rodada (.jpg forward)")
ax.set_xlabel("Rodada"); ax.set_ylabel("Imagens / segundo"); ax.legend()

ax = axes[1]
for r, g in raw_df.groupby("round"):
    ax.plot(g["t_rel"], g["gpu_util_pct"], label=f"rodada {r}", alpha=0.8)
ax.set_title("Ocupacao da GPU ao longo do tempo")
ax.set_xlabel("Tempo na rodada (s)"); ax.set_ylabel("GPU (%)")
ax.set_ylim(0, 100); ax.legend()

ax = axes[2]
ax.bar(["CPU medio", "GPU media"], [stats["cpu_mean_pct"], stats["gpu_util_mean_pct"]],
       color=["#2980b9", "#27ae60"], alpha=0.85)
ax.set_title("CPU vs GPU (medias)")
ax.set_ylabel("Utilizacao (%)"); ax.set_ylim(0, 100)
for i, v in enumerate([stats["cpu_mean_pct"], stats["gpu_util_mean_pct"]]):
    ax.text(i, v + 1, f"{v:.0f}%", ha="center")

plt.tight_layout()
plt.savefig(RESULTS_DIR / "jpg_forward_plots.png", dpi=120, bbox_inches="tight")
plt.close(fig)

# Grafico-sintese: tetos isolados vs pipeline real -> tamanho do starvation
fig, ax = plt.subplots(figsize=(7, 5))
labels = ["CPU\n(decode)", "GPU\n(forward)", "Pipeline\n(.jpg real)"]
vals   = [cpu_ceiling, gpu_ceiling, tp_mean]
errs   = [cpu_ceiling_std, gpu_ceiling_std, tp_std]
bars = ax.bar(labels, vals, yerr=errs, capsize=6,
              color=["#2980b9", "#27ae60", "#c0392b"], alpha=0.85)
ax.set_ylabel("Throughput (img/s)")
ax.set_title("Tetos isolados vs pipeline real (.jpg) - evidencia do starvation")
for b, v in zip(bars, vals):
    ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.0f}", ha="center", va="bottom")
ax.axhline(gpu_ceiling, color="#27ae60", ls="--", alpha=0.5)
ax.annotate(f"{gpu_waste_pct:.0f}% da GPU\nociosa (starvation)",
            xy=(2, tp_mean), xytext=(2, (tp_mean + gpu_ceiling) / 2),
            ha="center", color="#7f8c8d", fontsize=10,
            arrowprops=dict(arrowstyle="<->", color="#7f8c8d"))
plt.tight_layout()
plt.savefig(RESULTS_DIR / "jpg_forward_ceilings.png", dpi=120, bbox_inches="tight")
plt.close(fig)

print("Graficos salvos. Pipeline A (jpg) concluido.")
