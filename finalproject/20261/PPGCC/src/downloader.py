"""
downloader.py — baixa os registros de corridas de táxi amarelo de NYC (parquet) e converte para CSV.

O NYC TLC publica os dados de corridas como arquivos parquet no CDN oficial CloudFront.
Este script baixa um ou mais arquivos mensais, converte cada um para CSV e
os concatena em arquivos-alvo de ~1 GB e ~5 GB.

Variáveis de ambiente:
  DATASETS_DIR   onde armazenar os arquivos CSV (padrão: /datasets)
  TARGET_1GB     nome do arquivo com ~1 GB (padrão: taxi_1gb.csv)
  TARGET_5GB     nome do arquivo com ~5 GB (padrão: taxi_5gb.csv)
  SKIP_5GB       definir como "true" para pular a construção do dataset de 5 GB

Uso:
  python downloader.py
"""

import os
import sys
import requests
from pathlib import Path

import pandas as pd

# Padrão de URL dos arquivos parquet do NYC TLC (CDN oficial CloudFront)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Arquivos mensais — do mais recente para o mais antigo (experimentos usam dados recentes)
# ~380 MB CSV por mês → ~3 arquivos para 1 GB, ~14 para 5 GB, ~27 para 10 GB
MONTHS = [
    "2024-06", "2024-05", "2024-04",
    "2024-03", "2024-02", "2024-01",
    "2023-12", "2023-11", "2023-10",
    "2023-09", "2023-08", "2023-07",
    "2023-06", "2023-05", "2023-04",
    "2023-03", "2023-02", "2023-01",
    "2022-12", "2022-11", "2022-10",
    "2022-09", "2022-08", "2022-07",
    "2022-06", "2022-05", "2022-04",
    "2022-03", "2022-02", "2022-01",
]

# Tamanho aproximado do CSV por arquivo mensal (parquet comprimido → CSV expandido)
# Táxi amarelo 2023-2024: ~47 MB parquet → ~380 MB CSV (19 colunas, ~3,5 M linhas/mês)
APPROX_CSV_MB_PER_FILE = 380

DATASETS_DIR = os.environ.get("DATASETS_DIR", "/datasets")
TARGET_1GB  = os.environ.get("TARGET_1GB",  "taxi_1gb.csv")
TARGET_5GB  = os.environ.get("TARGET_5GB",  "taxi_5gb.csv")
TARGET_10GB = os.environ.get("TARGET_10GB", "taxi_10gb.csv")
SKIP_5GB    = os.environ.get("SKIP_5GB",  "false").lower() == "true"
SKIP_10GB   = os.environ.get("SKIP_10GB", "false").lower() == "true"


def download_parquet(year_month: str, dest_dir: Path) -> Path:
    """Baixa um arquivo parquet mensal do CDN do NYC TLC. Pula se já existir."""
    filename = f"yellow_tripdata_{year_month}.parquet"
    dest_path = dest_dir / filename
    if dest_path.exists():
        print(f"  [pular] {filename} já foi baixado")
        return dest_path

    url = f"{BASE_URL}/{filename}"
    print(f"  [download] {url}", flush=True)
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                print(f"\r    {pct:.1f}%  ({downloaded // 1024 // 1024} MB)", end="", flush=True)
    print()
    return dest_path


def parquet_to_csv(parquet_path: Path, csv_path: Path) -> Path:
    """Converte um arquivo parquet para CSV. Pula se o CSV já existir."""
    if csv_path.exists():
        print(f"  [pular] {csv_path.name} já foi convertido")
        return csv_path
    print(f"  [converter] {parquet_path.name} → {csv_path.name}", flush=True)
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path, index=False)
    size_mb = csv_path.stat().st_size / 1024 / 1024
    print(f"    → {size_mb:.1f} MB", flush=True)
    return csv_path


def build_target(csv_files: list[Path], target_path: Path, target_gb: float):
    """Concatena arquivos CSV até atingir o tamanho-alvo em GB."""
    if target_path.exists():
        actual_gb = target_path.stat().st_size / 1024 / 1024 / 1024
        print(f"  [pular] {target_path.name} já existe ({actual_gb:.2f} GB)")
        return

    target_bytes = target_gb * 1024 * 1024 * 1024
    written = 0
    header_written = False

    print(f"  [construir] {target_path.name} (alvo ≥ {target_gb} GB) ...", flush=True)

    with open(target_path, "w", newline="", encoding="utf-8") as out:
        for csv_file in csv_files:
            if written >= target_bytes:
                break
            df = pd.read_csv(csv_file)
            if not header_written:
                # Escreve com cabeçalho apenas no primeiro arquivo
                df.to_csv(out, index=False)
                header_written = True
            else:
                # Arquivos subsequentes sem cabeçalho para evitar duplicação
                df.to_csv(out, index=False, header=False)
            written += csv_file.stat().st_size
            current_gb = target_path.stat().st_size / 1024 / 1024 / 1024
            print(f"    adicionado {csv_file.name}  (total até agora: {current_gb:.2f} GB)", flush=True)

    final_gb = target_path.stat().st_size / 1024 / 1024 / 1024
    print(f"  Concluído: {target_path.name}  ({final_gb:.2f} GB)", flush=True)


def main():
    datasets_dir = Path(DATASETS_DIR)
    datasets_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir = datasets_dir / "parquet"
    parquet_dir.mkdir(exist_ok=True)
    csv_dir = datasets_dir / "monthly_csv"
    csv_dir.mkdir(exist_ok=True)

    # Calcula quantos arquivos mensais são necessários para cada tamanho-alvo
    files_needed_1gb  = max(3,  int(1024  / APPROX_CSV_MB_PER_FILE) + 1)
    files_needed_5gb  = max(files_needed_1gb,  int(5120  / APPROX_CSV_MB_PER_FILE) + 1)
    files_needed_10gb = max(files_needed_5gb,  int(10240 / APPROX_CSV_MB_PER_FILE) + 1)

    if not SKIP_10GB:
        files_needed = files_needed_10gb
    elif not SKIP_5GB:
        files_needed = files_needed_5gb
    else:
        files_needed = files_needed_1gb

    months_to_use = MONTHS[:files_needed]

    print(f"Baixando {len(months_to_use)} arquivos parquet mensais...", flush=True)
    csv_files = []
    for ym in months_to_use:
        parquet_path = download_parquet(ym, parquet_dir)
        csv_path = csv_dir / f"yellow_tripdata_{ym}.csv"
        csv_files.append(parquet_to_csv(parquet_path, csv_path))

    print("\nConstruindo datasets-alvo...", flush=True)
    build_target(csv_files, datasets_dir / TARGET_1GB, target_gb=1.0)

    if not SKIP_5GB:
        build_target(csv_files, datasets_dir / TARGET_5GB, target_gb=5.0)

    if not SKIP_10GB:
        build_target(csv_files, datasets_dir / TARGET_10GB, target_gb=10.0)

    print("\nTodos os datasets prontos.", flush=True)


if __name__ == "__main__":
    main()
