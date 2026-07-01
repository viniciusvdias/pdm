"""
Preparo do dataset completo (Pipeline A de entrada).

Pre-requisito: o train.zip do Dogs vs. Cats ja baixado e descompactado em RAW_DIR
(o bin/download_data.sh cuida disso; ver README, secao 2).

Faz duas coisas:
  1. Move as imagens com label (cat.* / dog.*) de RAW_DIR/train/ para RAW_DIR/.
  2. DUPLICA as imagens ate atingir TARGET_GB. E o passo que estoura o limite de RAM do
     conteiner e evita o OS cache -> forca o gargalo de I/O que o experimento quer medir.

NAO usar no smoke test (datasample): a duplicacao mataria o proposito de ser pequeno/rapido.
Config via env: RAW_DIR, TARGET_GB.
"""

import os
import shutil
import time
from pathlib import Path

RAW_DIR   = Path(os.environ.get("RAW_DIR", "/tf/data/raw_jpg"))
TRAIN_DIR = RAW_DIR / "train"                              # subpasta criada pelo unzip do train.zip
TARGET_GB = float(os.environ.get("TARGET_GB", "1.5"))     # meta de tamanho (>= 1 GB exigido)

RAW_DIR.mkdir(parents=True, exist_ok=True)
print(f"RAW_DIR = {RAW_DIR} | Meta = {TARGET_GB} GB")

# 1. Mover imagens com label de RAW_DIR/train/ para RAW_DIR/ (se ainda estiverem la).
if TRAIN_DIR.exists():
    labeled = [img for img in TRAIN_DIR.rglob("*.jpg")
               if img.name.lower().startswith(("cat", "dog"))]
    print(f"Encontradas {len(labeled)} imagens com label em {TRAIN_DIR}")

    moved = 0
    for img in labeled:
        destiny = RAW_DIR / img.name
        if not destiny.exists():
            shutil.move(str(img), str(destiny))
            moved += 1
    print(f"{moved} imagens movidas para {RAW_DIR}")
    shutil.rmtree(TRAIN_DIR, ignore_errors=True)
    print(f"Subpasta {TRAIN_DIR.name}/ removida.")
else:
    base = [p for p in RAW_DIR.glob("*.jpg") if p.name.lower().startswith(("cat", "dog"))]
    if base:
        print(f"Sem subpasta train/ - {len(base)} imagens ja estao em {RAW_DIR}.")
    else:
        raise FileNotFoundError(
            f"Nenhuma imagem encontrada. Baixe o train.zip para {RAW_DIR} e descompacte "
            "(ver bin/download_data.sh e o README)."
        )


# 2. Duplicar ate TARGET_GB (estoura a RAM e evita o OS cache).
def inflate_directory(path, target_gb):
    base_files = [p for p in path.glob("*.jpg") if "__dup" not in p.name]
    if not base_files:
        raise RuntimeError("Nenhuma imagem-base encontrada.")

    current_bytes = sum(f.stat().st_size for f in path.glob("*.jpg"))
    target_bytes  = target_gb * (1024 ** 3)
    print(f"Tamanho inicial: {current_bytes / (1024**3):.2f} GB | Base: {len(base_files):,} imagens")

    if current_bytes >= target_bytes:
        print("Meta ja atingida.")
        return

    copies, dup_pass, t0 = 0, 1, time.time()
    while current_bytes < target_bytes:
        for arq in base_files:
            nome_final = path / f"{arq.stem}__dup{dup_pass}{arq.suffix}"
            if not nome_final.exists():
                shutil.copy(arq, nome_final)
                current_bytes += arq.stat().st_size   # mesmo tamanho do original: evita stat no disco
                copies += 1
            if current_bytes >= target_bytes:
                break
        print(f"  Passe {dup_pass:02d} | {copies:,} copias | "
              f"{current_bytes/(1024**3):.2f} GB | {time.time()-t0:.0f}s")
        dup_pass += 1

    print(f"Inflate concluido: {current_bytes/(1024**3):.2f} GB em {copies:,} copias "
          f"({dup_pass-1} passes) - {time.time()-t0:.0f}s")


inflate_directory(RAW_DIR, TARGET_GB)

# Relatorio final.
files = list(RAW_DIR.glob("*.jpg"))
n_cat = sum(1 for f in files if f.name.lower().startswith("cat"))
n_dog = sum(1 for f in files if f.name.lower().startswith("dog"))
total_gb = sum(f.stat().st_size for f in files) / (1024 ** 3)
print(f"\nDir: {RAW_DIR} | Total: {len(files):,} imagens ({n_cat:,} gatos, {n_dog:,} cachorros) | {total_gb:.2f} GB")
