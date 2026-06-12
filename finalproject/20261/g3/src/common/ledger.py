"""Semântica de liquidação (ledger) compartilhada entre stream e batch.

A MESMA lógica é aplicada pelo operador PyFlink (por conta, em ordem de
event-time) e pelo baseline batch (varrendo o input ordenado por
``(event_time, transaction_id)``). Como a função é determinística e a ordem por
conta é idêntica nos dois mundos, o saldo final de cada conta é **idêntico** —
é isso que o experimento de reconciliação exactly-once comprova.

Modelo (simplificações documentadas no README):
- Toda conta tem saldo inicial constante ``OPENING_BALANCE_CENTS`` (determinístico,
  igual em stream e batch — evita a ambiguidade dos saldos por linha do PaySim,
  que são inconsistentes entre linhas).
- Cada transação expande em 1–2 *ops* keyed por conta:
    * CASH_IN              -> [CREDIT origem]
    * PAYMENT, DEBIT       -> [DEBIT  origem]
    * TRANSFER, CASH_OUT   -> [DEBIT  origem, CREDIT destino]
- **Aceitação do DEBIT por LIMITE POR TRANSAÇÃO** (``TX_LIMIT_CENTS``), e não pelo
  saldo corrente. O Pix real impõe limite por transação; além disso, um critério
  que depende só do valor da transação é **independente da ordem** de processamento.
  Como créditos são comutativos e o conjunto de débitos aceitos não depende da
  ordem, o saldo final = OPENING + Σcréditos − Σ(débitos aceitos) é o MESMO no
  stream (exactly-once, qualquer ordem) e no baseline batch -> reconciliação exata
  ao centavo, **sem necessidade de buffering por event-time** (que sofreria com a
  razão event-time/ingestão e com o flush da cauda).
- DEBIT: se ``valor <= TX_LIMIT_CENTS`` debita e marca SETTLED; senão REJECTED.
- CREDIT: credita e marca SETTLED.
- O saldo corrente é mantido/reportado como ledger de auditoria, mas NÃO decide a
  aceitação (que é por limite). Um DEBIT rejeitado não cancela o CREDIT pareado
  (simplificação; ambos os mundos usam a MESMA regra).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List

from .schema import Transaction

# Saldo inicial de toda conta (centavos).
OPENING_BALANCE_CENTS = int(os.environ.get("OPENING_BALANCE_CENTS", 500_000_000))

# Limite por transação (centavos). Débitos acima são REJECTED. Independente de
# ordem -> mantém a reconciliação exata. Default 200.000,00 (rejeita a cauda alta
# de TRANSFER/CASH_OUT do PaySim, gerando uma taxa de REJECTED visível).
TX_LIMIT_CENTS = int(os.environ.get("TX_LIMIT_CENTS", 20_000_000))

# Tipos cuja origem é debitada.
_DEBIT_TYPES = {"PAYMENT", "DEBIT", "TRANSFER", "CASH_OUT"}
# Tipos que também creditam uma conta destino (destino "C...").
_CREDIT_DEST_TYPES = {"TRANSFER", "CASH_OUT"}

DEBIT = "DEBIT"
CREDIT = "CREDIT"


@dataclass
class LedgerOp:
    transaction_id: str
    event_time: int
    account: str
    delta_cents: int      # >0 crédito, <0 débito (valor absoluto em amount)
    kind: str             # DEBIT | CREDIT
    type: str             # tipo PaySim original
    is_fraud: int


def expand_to_ops(tx: Transaction) -> List[LedgerOp]:
    """Expande uma transação em ops de ledger keyed por conta."""
    ops: List[LedgerOp] = []
    amt = tx.amount_cents
    if tx.type == "CASH_IN":
        ops.append(LedgerOp(tx.transaction_id, tx.event_time, tx.name_orig,
                            amt, CREDIT, tx.type, tx.is_fraud))
        return ops
    if tx.type in _DEBIT_TYPES:
        ops.append(LedgerOp(tx.transaction_id, tx.event_time, tx.name_orig,
                            -amt, DEBIT, tx.type, tx.is_fraud))
    if tx.type in _CREDIT_DEST_TYPES and tx.name_dest:
        ops.append(LedgerOp(tx.transaction_id, tx.event_time, tx.name_dest,
                            amt, CREDIT, tx.type, tx.is_fraud))
    return ops


def apply_op(balance: int, op: LedgerOp) -> tuple[int, str]:
    """Aplica uma op a um saldo. Retorna (novo_saldo, status).

    Aceitação do DEBIT por LIMITE POR TRANSAÇÃO (independente de ordem).
    """
    if op.kind == DEBIT:
        amount = -op.delta_cents
        if amount <= TX_LIMIT_CENTS:
            return balance - amount, "SETTLED"
        return balance, "REJECTED"
    # CREDIT
    return balance + op.delta_cents, "SETTLED"


def opening_for(_account: str) -> int:
    return OPENING_BALANCE_CENTS
