"""
Utilidades compartilhadas pelos benchmarks (02_baseline_jpg / 04_optimized_parquet).

Reune o que era duplicado entre os dois notebooks originais:
  - configuracao via variaveis de ambiente (paths e parametros do experimento);
  - leitura de CPU/RAM do CONTEINER via cgroup (o psutil sozinho enxerga o HOST);
  - thread de monitoramento de recursos (CPU / RAM / GPU);
  - modelo consumidor de GPU (MobileNetV2 forward-only + FP16);
  - calculo do resumo estatistico de cada pipeline.

Todos os scripts leem os mesmos parametros daqui, entao `bin/run.sh` (smoke test com o
datasample) e `bin/run_full.sh` (dataset completo) so precisam exportar env vars diferentes.
"""

import os
import time
import threading

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Configuracao (via variaveis de ambiente, com defaults do experimento original).
# ---------------------------------------------------------------------------
def _env_int(name, default):
    v = os.environ.get(name)
    return int(v) if v not in (None, "") else default


def _env_float(name, default):
    v = os.environ.get(name)
    return float(v) if v not in (None, "") else default


def _env_opt_int(name):
    """None quando ausente/vazio; senao int. Usado por N_IMAGES (None = tudo)."""
    v = os.environ.get(name)
    return int(v) if v not in (None, "") else None


DATA_DIR     = os.environ.get("DATA_DIR", "/tf/data")
RAW_DIR      = os.environ.get("RAW_DIR", os.path.join(DATA_DIR, "raw_jpg"))
PARQUET_DIR  = os.environ.get("PARQUET_DIR", os.path.join(DATA_DIR, "optimized"))
RESULTS_DIR  = os.environ.get("RESULTS_DIR", os.path.join(DATA_DIR, "results"))

IMG_SIZE     = (224, 224)                       # entrada da MobileNetV2 (fixo)
BATCH_SIZE   = _env_int("BATCH_SIZE", 256)      # forward usa pouca VRAM -> batch grande amortiza o passo
N_IMAGES     = _env_opt_int("N_IMAGES")         # None = TODAS as imagens; int = subset (teste rapido)
N_PASSES     = _env_int("N_PASSES", 1)          # passadas por rodada (suba se o subset for pequeno)
N_ROUNDS     = _env_int("N_ROUNDS", 3)          # rodadas -> media +/- desvio padrao (regra do projeto)
SAMPLE_EVERY = _env_float("SAMPLE_EVERY", 1.0)  # intervalo de amostragem do monitor (s)


# ---------------------------------------------------------------------------
# Limites do CONTEINER via cgroup.
# Dentro do Docker o psutil le /proc do HOST e ignora o cgroup. Corrigimos duas metricas:
#   - CPU: normalizada pelos vCPUs do conteiner (senao 2 vCPUs a 100% viram ~17% num host de 12).
#   - RAM: memory.current do conteiner (senao reporta a RAM do HOST inteiro).
# ---------------------------------------------------------------------------
def detect_container_cpu_quota():
    """Numero de vCPUs disponiveis ao conteiner (float)."""
    try:  # cgroup v2
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota_str, period_str = f.read().strip().split()
        if quota_str != "max":
            return float(quota_str) / float(period_str)
    except (FileNotFoundError, OSError, ValueError):
        pass
    try:  # cgroup v1
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as f:
            quota = int(f.read().strip())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as f:
            period = int(f.read().strip())
        if quota > 0:
            return quota / period
    except (FileNotFoundError, OSError, ValueError):
        pass
    try:  # fallback: CPUs visiveis ao processo
        return float(len(os.sched_getaffinity(0)))
    except (AttributeError, OSError):
        return float(os.cpu_count() or 1)


def read_container_mem():
    """(usado_gb, limite_gb) da memoria do CONTEINER via cgroup (inclui page cache)."""
    try:  # cgroup v2
        with open("/sys/fs/cgroup/memory.current") as f:
            used = int(f.read().strip())
        with open("/sys/fs/cgroup/memory.max") as f:
            lim = f.read().strip()
        limit = None if lim == "max" else int(lim)
        return used / 1024 ** 3, (limit / 1024 ** 3 if limit else float("nan"))
    except (FileNotFoundError, OSError, ValueError):
        pass
    try:  # cgroup v1
        with open("/sys/fs/cgroup/memory/memory.usage_in_bytes") as f:
            used = int(f.read().strip())
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            limit = int(f.read().strip())
        limit = limit if limit < (1 << 62) else None
        return used / 1024 ** 3, (limit / 1024 ** 3 if limit else float("nan"))
    except (FileNotFoundError, OSError, ValueError):
        return float("nan"), float("nan")


N_CONTAINER_CPUS = detect_container_cpu_quota()
_, CONTAINER_MEM_LIMIT_GB = read_container_mem()


# ---------------------------------------------------------------------------
# GPU (pynvml). Sem GPU/pynvml -> handle None e o monitor apenas ignora a GPU.
# ---------------------------------------------------------------------------
def init_gpu_handle():
    try:
        import pynvml
        pynvml.nvmlInit()
        handle = pynvml.nvmlDeviceGetHandleByIndex(0)
        name = pynvml.nvmlDeviceGetName(handle)
        print("GPU detectada:", name.decode() if isinstance(name, bytes) else name)
        return handle
    except Exception as e:  # noqa: BLE001 - qualquer falha => segue sem monitorar GPU
        print("pynvml indisponivel, GPU nao sera monitorada:", e)
        return None


class ResourceMonitor(threading.Thread):
    """
    Amostra a cada `interval` s (metricas principais em MAIUSCULA):

      cpu_pct           % dos vCPUs do CONTEINER (0-100). PRINCIPAL de CPU: ~100% durante o
                        pipeline .jpg = decode saturando os 2 vCPU (o gargalo).
      cpu_pct_host      % medio do HOST. So referencia/debug.
      proc_cpu_raw      % do processo sobre 1 vCPU (pode passar de 100).
      cont_mem_used_gb  RAM do CONTEINER via cgroup. PRINCIPAL de RAM (inclui page cache).
      proc_rss_gb       RSS do processo Python (working set).
      ram_used_host_gb  RAM do HOST inteiro. So referencia.
      gpu_util_pct      Utilizacao da GPU (pynvml). BAIXA no pipeline .jpg = GPU faminta.
      gpu_mem_gb        VRAM ocupada.
    """

    def __init__(self, gpu_handle, interval=1.0, n_container_cpus=N_CONTAINER_CPUS):
        super().__init__(daemon=True)
        import psutil
        self.handle = gpu_handle
        self.interval = interval
        self.n_container_cpus = max(float(n_container_cpus), 1e-6)  # evita divisao por zero
        self._stop_event = threading.Event()
        self.samples = []
        self._psutil = psutil
        self.proc = psutil.Process()

    def run(self):
        psutil = self._psutil
        psutil.cpu_percent(None)          # prime: a 1a leitura e descartavel
        self.proc.cpu_percent(None)
        t_start = time.time()
        while not self._stop_event.is_set():
            cpu_host      = psutil.cpu_percent(None)
            proc_cpu_raw  = self.proc.cpu_percent(None)
            cpu_container = min(proc_cpu_raw / self.n_container_cpus, 100.0)

            cont_used_gb, _  = read_container_mem()
            proc_rss_gb      = self.proc.memory_info().rss / (1024 ** 3)
            ram_used_host_gb = psutil.virtual_memory().used / (1024 ** 3)

            gpu_util, gpu_mem_gb = np.nan, np.nan
            if self.handle is not None:
                try:
                    import pynvml
                    gpu_util   = float(pynvml.nvmlDeviceGetUtilizationRates(self.handle).gpu)
                    gpu_mem_gb = pynvml.nvmlDeviceGetMemoryInfo(self.handle).used / (1024 ** 3)
                except Exception:  # noqa: BLE001
                    pass

            self.samples.append({
                "t_rel":            time.time() - t_start,
                "cpu_pct":          cpu_container,
                "cpu_pct_host":     cpu_host,
                "proc_cpu_raw":     proc_cpu_raw,
                "cont_mem_used_gb": cont_used_gb,
                "proc_rss_gb":      proc_rss_gb,
                "ram_used_host_gb": ram_used_host_gb,
                "gpu_util_pct":     gpu_util,
                "gpu_mem_gb":       gpu_mem_gb,
            })
            self._stop_event.wait(self.interval)

    def stop(self):
        self._stop_event.set()
        self.join()
        return pd.DataFrame(self.samples)


def build_model(img_size=IMG_SIZE):
    """
    Consumidor de GPU: MobileNetV2 (ImageNet) forward-only + FP16.
    Modelo leve: consome imagens muito mais rapido que a CPU as entrega -> quando a ingestao
    e o gargalo (pipeline .jpg), a GPU aparece faminta (starvation).
    """
    import tensorflow as tf
    base = tf.keras.applications.MobileNetV2(
        weights="imagenet", include_top=False,
        input_shape=(*img_size, 3), pooling="avg",
    )
    # dtype="float32" na saida: boa pratica de mixed precision (estabilidade numerica)
    out = tf.keras.layers.Dense(1, activation="sigmoid", dtype="float32")(base.output)
    return tf.keras.Model(base.input, out)


def summarize_round(r, n_images, elapsed, df_s):
    """Metricas agregadas de UMA rodada (1 linha do CSV per_round)."""
    return {
        "round": r,
        "n_images": n_images,
        "round_time_s": elapsed,
        "throughput_img_s": n_images / elapsed,
        "cpu_mean_pct": df_s["cpu_pct"].mean(),
        "cpu_max_pct": df_s["cpu_pct"].max(),
        "cont_mem_peak_gb": df_s["cont_mem_used_gb"].max(),
        "proc_rss_peak_gb": df_s["proc_rss_gb"].max(),
        "ram_host_peak_gb": df_s["ram_used_host_gb"].max(),
        "gpu_util_mean_pct": df_s["gpu_util_pct"].mean(),
        "gpu_util_max_pct": df_s["gpu_util_pct"].max(),
        "gpu_mem_peak_gb": df_s["gpu_mem_gb"].max(),
    }


def compute_stats(pipeline_name, summary_df, cpu_ceiling, cpu_ceiling_std,
                  gpu_ceiling, gpu_ceiling_std):
    """
    Linha unica de estatisticas do pipeline (media +/- desvio + tetos isolados).
    `gpu_waste_pct` = starvation: % do teto da GPU que o pipeline real NAO aproveita.
    """
    tp = summary_df["throughput_img_s"].values
    tp_mean = float(tp.mean())
    tp_std = float(tp.std(ddof=1)) if len(tp) > 1 else 0.0
    gpu_waste_pct = 100.0 * (1 - tp_mean / gpu_ceiling) if gpu_ceiling else float("nan")
    return {
        "pipeline": pipeline_name,
        "n_rounds": int(len(summary_df)),
        "batch_size": BATCH_SIZE,
        "n_images_round": int(summary_df["n_images"].iloc[0]),
        "throughput_mean_img_s": tp_mean,
        "throughput_std_img_s": tp_std,
        "cpu_ceiling_img_s": cpu_ceiling,          # teto do produtor (leitura/decode)
        "cpu_ceiling_std_img_s": cpu_ceiling_std,
        "gpu_ceiling_img_s": gpu_ceiling,          # teto do consumidor (forward)
        "gpu_ceiling_std_img_s": gpu_ceiling_std,
        "gpu_waste_pct": gpu_waste_pct,            # starvation
        "round_time_mean_s": float(summary_df["round_time_s"].mean()),
        "cpu_mean_pct": float(summary_df["cpu_mean_pct"].mean()),
        "gpu_util_mean_pct": float(summary_df["gpu_util_mean_pct"].mean()),
        "cont_mem_peak_gb": float(summary_df["cont_mem_peak_gb"].max()),
        "gpu_mem_peak_gb": float(summary_df["gpu_mem_peak_gb"].max()),
    }
