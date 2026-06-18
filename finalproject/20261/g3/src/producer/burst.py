from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass
from typing import Optional


def _getenv_float(name: str, default: float) -> float:
    v = os.environ.get(name, "")
    return float(v) if v != "" else default


def _getenv_int(name: str, default: int) -> int:
    v = os.environ.get(name, "")
    return int(v) if v != "" else default


@dataclass
class BurstState:
    multiplier: float
    active: bool
    until: float


class BurstController:
    def __init__(self) -> None:
        self.mode = os.environ.get("BURST_MODE", "rate")  # none|rate|synthetic
        self.prob = _getenv_float("BURST_PROB", 0.02)
        self.min_s = _getenv_float("BURST_MIN_S", 3)
        self.max_s = _getenv_float("BURST_MAX_S", 10)
        self.mult_min = _getenv_float("BURST_MULT_MIN", 10)
        self.mult_max = _getenv_float("BURST_MULT_MAX", 80)
        self.value_factor = _getenv_float("BURST_VALUE_FACTOR", 50)
        seed_env = os.environ.get("BURST_SEED", "")
        self.rng = random.Random(int(seed_env)) if seed_env != "" else random.Random()
        self._next_tick = 0.0
        self._state = BurstState(multiplier=1.0, active=False, until=0.0)
        self._synth_seq = 0

    def update(self, now: float) -> BurstState:
        """Avalia/avança o estado do burst. Chamar frequentemente."""
        if self.mode == "none":
            return self._state
        if self._state.active and now >= self._state.until:
            self._state = BurstState(1.0, False, 0.0)
        if now >= self._next_tick:
            self._next_tick = now + 1.0
            if not self._state.active and self.rng.random() < self.prob:
                dur = self.rng.uniform(self.min_s, self.max_s)
                mult = self.rng.uniform(self.mult_min, self.mult_max)
                self._state = BurstState(mult, True, now + dur)
        return self._state

    @property
    def synthetic_enabled(self) -> bool:
        return self.mode == "synthetic"

    def make_synthetic(self, base_event_time: int) -> dict:
        """Cria uma transação sintética de alto valor (conta BURST*)."""
        self._synth_seq += 1
        i = self._synth_seq
        acct = f"BURST{self.rng.randint(0, 999)}"
        dest = f"BURST{self.rng.randint(0, 999)}"
        # valor alto: base 100k..1M inflada pelo fator.
        base = self.rng.uniform(100_000, 1_000_000)
        amount_cents = int(base * self.value_factor * 100)
        tid = f"synth-{i}-{self.rng.getrandbits(32):08x}"
        return {
            "transaction_id": tid,
            "step": 0,
            "event_time": base_event_time,
            "type": "TRANSFER",
            "amount_cents": amount_cents,
            "name_orig": acct,
            "old_balance_orig_cents": 0,
            "name_dest": dest,
            "old_balance_dest_cents": 0,
            "is_fraud": 0,
            "is_flagged_fraud": 0,
            "synthetic": True,
        }
