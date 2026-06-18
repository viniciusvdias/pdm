"""Configuração central do pipeline.

Todo serviço (coletor, consumidor, processador) importa daqui, de modo que
broker, tópico e símbolos ficam definidos num único lugar. Os valores vêm do
.env (não versionado); os defaults servem pro ambiente local do docker-compose.
"""
import os

from dotenv import load_dotenv

load_dotenv()

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TRADES_TOPIC = os.getenv("KAFKA_TRADES_TOPIC", "trades")

# --- Coletor Binance ---
# Aceita um ou vários pares separados por vírgula, ex: "BTCUSDT,ETHUSDT".
BINANCE_SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("BINANCE_SYMBOLS", "BTCUSDT").split(",")
    if s.strip()
]

# --- Processador Spark ---
WINDOW_SIZE_SECONDS = int(os.getenv("WINDOW_SIZE_SECONDS", "10"))
WATERMARK_SECONDS = int(os.getenv("WATERMARK_SECONDS", "10"))
TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "5"))

# --- Métricas ---
METRICS_DIR = os.getenv("METRICS_DIR", "metrics")
EXPERIMENT_LABEL = os.getenv("EXPERIMENT_LABEL", "default")

# Endpoint de streams combinados da Binance (funciona com 1 ou N símbolos).
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream?streams="


def binance_trade_stream_url(symbols=None) -> str:
    """Monta a URL do WebSocket para o(s) stream(s) de trade dos símbolos."""
    symbols = symbols or BINANCE_SYMBOLS
    streams = "/".join(f"{s.lower()}@trade" for s in symbols)
    return BINANCE_WS_BASE + streams
