-- Esquema de auditoria/métricas alimentado pelo consumer.

-- Saldo final autoritativo por conta (upsert com o último update visto no stream).
-- Base da reconciliação stream × baseline batch.
CREATE TABLE IF NOT EXISTS account_balance (
    account        TEXT PRIMARY KEY,
    balance_cents  BIGINT      NOT NULL,
    -- sequência de aplicação por conta (apply_seq, zero-padded) do op que produziu
    -- este saldo. Mantemos sempre o de MAIOR chave => o ÚLTIMO op aplicado ao
    -- estado da conta no operador => saldo final, robusto a reordenação/eventos
    -- atrasados (a ordenação por event_time não basta sob out-of-orderness).
    order_key      TEXT        NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Trilha de resultados de liquidação (SETTLED / REJECTED) por op.
CREATE TABLE IF NOT EXISTS outcomes (
    transaction_id TEXT        NOT NULL,
    account        TEXT        NOT NULL,
    op_kind        TEXT        NOT NULL,
    status         TEXT        NOT NULL,
    balance_cents  BIGINT      NOT NULL,
    event_time     BIGINT      NOT NULL,
    tx_type        TEXT        NOT NULL,
    is_fraud       SMALLINT    NOT NULL DEFAULT 0,
    ingest_time_ms BIGINT,
    settle_time_ms BIGINT,
    PRIMARY KEY (transaction_id, account, op_kind)
);

-- Alertas AML emitidos pelo job de detecção.
CREATE TABLE IF NOT EXISTS aml_alerts (
    alert_id    TEXT PRIMARY KEY,
    pattern     TEXT        NOT NULL,   -- CYCLE | VELOCITY | STRUCTURING
    accounts    TEXT        NOT NULL,   -- contas envolvidas (CSV)
    amount_cents BIGINT,
    event_time  BIGINT      NOT NULL,
    detail      TEXT
);

-- Métricas agregadas (TPS, latência) para conferência fora do Prometheus.
CREATE TABLE IF NOT EXISTS metrics (
    id         BIGSERIAL PRIMARY KEY,
    metric     TEXT        NOT NULL,
    value      DOUBLE PRECISION NOT NULL,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outcomes_status ON outcomes(status);
CREATE INDEX IF NOT EXISTS idx_outcomes_fraud  ON outcomes(is_fraud);
CREATE INDEX IF NOT EXISTS idx_alerts_pattern  ON aml_alerts(pattern);
