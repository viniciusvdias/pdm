from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass, asdict
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Iterable, Iterator, Optional

CSV_COLUMNS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]

BASE_EPOCH_MS = 1_672_531_200_000
HOUR_MS = 3_600_000
EVENT_SPACING_MS = 10
MAX_OOO_MS = 300
ACCOUNT_TO_ACCOUNT_TYPES = {"TRANSFER"}


def _to_cents(value: str) -> int:
    """Converte string monetária em centavos inteiros (exato).
    Usa Decimal para lidar com casas decimais e notação científica
    (o PaySim ocasionalmente traz valores como 0E-8) sem erro de float.
    """
    value = (value or "0").strip()
    if not value:
        return 0
    try:
        cents = (Decimal(value) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return int(cents)
    except InvalidOperation:
        return 0


def _jitter_ms(transaction_id: str) -> int:
    """Jitter determinístico [0, MAX_OOO_MS) derivado do id (out-of-orderness)."""
    h = int(transaction_id[:8], 16)
    return h % MAX_OOO_MS


def make_transaction_id(row_index: int, name_orig: str, name_dest: str,
                        step: str, amount: str) -> str:
    raw = f"{row_index}|{name_orig}|{name_dest}|{step}|{amount}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


@dataclass
class Transaction:
    transaction_id: str
    step: int
    event_time: int          
    type: str
    amount_cents: int
    name_orig: str
    old_balance_orig_cents: int
    name_dest: str
    old_balance_dest_cents: int
    is_fraud: int
    is_flagged_fraud: int

    def to_dict(self) -> dict:
        return asdict(self)


def parse_row(row: dict, row_index: int) -> Optional[Transaction]:
    """Converte uma linha (dict do csv.DictReader) em Transaction normalizada.

    Retorna ``None`` para linhas claramente inválidas (sem origem ou sem tipo).
    """
    name_orig = (row.get("nameOrig") or "").strip()
    tx_type = (row.get("type") or "").strip()
    if not name_orig or not tx_type:
        return None

    step_raw = (row.get("step") or "0").strip()
    amount_raw = (row.get("amount") or "0").strip()
    name_dest = (row.get("nameDest") or "").strip()

    tid = make_transaction_id(row_index, name_orig, name_dest, step_raw, amount_raw)
    try:
        step = int(step_raw)
    except ValueError:
        step = 0
    # event_time monotônico com a ordem do arquivo + jitter determinístico.
    event_time = BASE_EPOCH_MS + row_index * EVENT_SPACING_MS + _jitter_ms(tid)

    return Transaction(
        transaction_id=tid,
        step=step,
        event_time=event_time,
        type=tx_type,
        amount_cents=_to_cents(amount_raw),
        name_orig=name_orig,
        old_balance_orig_cents=_to_cents(row.get("oldbalanceOrg", "0")),
        name_dest=name_dest,
        old_balance_dest_cents=_to_cents(row.get("oldbalanceDest", "0")),
        is_fraud=int(float(row.get("isFraud", "0") or 0)),
        is_flagged_fraud=int(float(row.get("isFlaggedFraud", "0") or 0)),
    )


def open_text(path: str):
    """Abre CSV texto detectando gzip pelos magic bytes (1f 8b).

    Robusto ao nome do arquivo: o Docker monta a amostra .gz como
    ``/data/input.csv`` (sem extensão), então não dá para confiar no sufixo.
    """
    import gzip
    with open(path, "rb") as fh:
        magic = fh.read(2)
    if magic == b"\x1f\x8b":
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def iter_transactions(fileobj: Iterable[str],
                      start_index: int = 0) -> Iterator[Transaction]:
    """Itera Transactions a partir de um arquivo CSV (com header)."""
    reader = csv.DictReader(fileobj)
    idx = start_index
    for raw in reader:
        tx = parse_row(raw, idx)
        idx += 1
        if tx is not None:
            yield tx


def transaction_to_csv_line(tx: Transaction) -> str:
    """Serializa em linha CSV no formato original do PaySim (p/ amostra/amplificação)."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    amount = f"{tx.amount_cents / 100:.2f}"
    old_orig = f"{tx.old_balance_orig_cents / 100:.2f}"
    old_dest = f"{tx.old_balance_dest_cents / 100:.2f}"
    writer.writerow([
        tx.step, tx.type, amount, tx.name_orig, old_orig, "0.0",
        tx.name_dest, old_dest, "0.0", tx.is_fraud, tx.is_flagged_fraud,
    ])
    return buf.getvalue().rstrip("\r\n")
