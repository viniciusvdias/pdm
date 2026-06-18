from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List

from .schema import Transaction

#Saldo inicial de toda conta (centavos).
OPENING_BALANCE_CENTS = int(os.environ.get("OPENING_BALANCE_CENTS", 500_000_000))

#Tipos cuja origem é debitada
_DEBIT_TYPES = {"PAYMENT", "DEBIT", "TRANSFER", "CASH_OUT"}
#Tipos que também creditam uma conta destino (destino "C...").
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

    Aceitação do DEBIT por SALDO SUFICIENTE (depende da ordem -> stream e batch
    aplicam as ops de cada conta na mesma ordem determinística por event-time).
    """
    if op.kind == DEBIT:
        amount = -op.delta_cents
        if amount <= balance:
            return balance - amount, "SETTLED"
        return balance, "REJECTED"
    # CREDIT
    return balance + op.delta_cents, "SETTLED"


def opening_for(_account: str) -> int:
    return OPENING_BALANCE_CENTS
