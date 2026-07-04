"""Utilitários compartilhados pelos jobs PyFlink (settlement e AML).

Centraliza a leitura de configuração por ambiente e a construção do ambiente de
execução, da fonte Kafka, do sink Kafka e do extrator de event-time, para que os
dois jobs fiquem consistentes (mesmas garantias, mesmos watermarks).
"""

from __future__ import annotations

import json
import os

from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema, DeliveryGuarantee,
)


def env_int(name: str, default: int) -> int:
    v = os.environ.get(name, "")
    return int(v) if v != "" else default


def env_str(name: str, default: str) -> str:
    return os.environ.get(name, default) or default


def bootstrap() -> str:
    return env_str("KAFKA_BOOTSTRAP", "kafka:9092")


def build_env() -> StreamExecutionEnvironment:
    """Cria o ambiente com checkpointing e garantia configuráveis."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(env_int("PARALLELISM", 2))

    guarantee = env_str("GUARANTEE", "EXACTLY_ONCE").upper()
    checkpoint_ms = env_int("CHECKPOINT_MS", 5000)
    env.enable_checkpointing(checkpoint_ms)
    cc = env.get_checkpoint_config()
    if guarantee == "AT_LEAST_ONCE":
        cc.set_checkpointing_mode(CheckpointingMode.AT_LEAST_ONCE)
    else:
        cc.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    cc.set_min_pause_between_checkpoints(max(500, checkpoint_ms // 2))
    cc.set_checkpoint_timeout(600_000)
    cc.set_tolerable_checkpoint_failure_number(3)
    return env


class _EventTimeAssigner(TimestampAssigner):
    """Extrai ``event_time`` (epoch ms) do JSON de cada registro."""

    def extract_timestamp(self, value, record_timestamp) -> int:
        try:
            return int(json.loads(value)["event_time"])
        except Exception:
            return record_timestamp if record_timestamp and record_timestamp > 0 else 0


def watermark_strategy() -> WatermarkStrategy:
    ooo = env_int("WATERMARK_OOO_MS", 2000)
    return (WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_millis(ooo))
            .with_timestamp_assigner(_EventTimeAssigner()))


def kafka_source(topics: str, group_id: str) -> KafkaSource:
    return (KafkaSource.builder()
            .set_bootstrap_servers(bootstrap())
            .set_topics(topics)
            .set_group_id(group_id)
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build())


def kafka_sink(topic: str, tx_prefix: str) -> KafkaSink:
    """Sink Kafka com garantia conforme env (EXACTLY_ONCE | AT_LEAST_ONCE)."""
    guarantee = env_str("GUARANTEE", "EXACTLY_ONCE").upper()
    serializer = (KafkaRecordSerializationSchema.builder()
                  .set_topic(topic)
                  .set_value_serialization_schema(SimpleStringSchema())
                  .build())
    builder = (KafkaSink.builder()
               .set_bootstrap_servers(bootstrap())
               .set_record_serializer(serializer))
    if guarantee == "AT_LEAST_ONCE":
        builder = builder.set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    else:
        builder = (builder
                   .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
                   .set_transactional_id_prefix(tx_prefix)
                   .set_property("transaction.timeout.ms", "900000"))
    return builder.build()
