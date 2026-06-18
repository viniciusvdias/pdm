"""Coletor (producer): Binance WebSocket -> Kafka tópico `trades`.

Lê o stream de trades da Binance, normaliza cada negócio para o schema acordado
pelo grupo e publica no tópico `trades`. A chave da mensagem é o symbol, então
trades do mesmo par caem sempre na mesma partição (preserva ordem por ativo).

Inclui reconexão automática: o WebSocket da Binance cai depois de alguns minutos,
então o loop reabre a conexão com backoff em vez de morrer.

Schema publicado (JSON):
    {trade_id, ts_trade, ts_ingest, price, qty, side, symbol}

Uso:
    python binance.py
"""
import asyncio
import json
import signal
import time

import websockets
from confluent_kafka import Producer

import config


def make_producer() -> Producer:
    """Cria o producer Kafka. linger.ms agrupa mensagens em lotes (mais vazão)."""
    return Producer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "linger.ms": 5,
            "client.id": "coletor-binance",
        }
    )


def normalize(raw: dict) -> dict:
    """Converte o evento bruto de trade da Binance para o schema do contrato.

    Formato do evento `@trade` da Binance:
        t = trade id, T = trade time (ms), s = symbol, p = price, q = qty,
        m = "buyer is market maker" (True => agressor foi o vendedor).
    """
    return {
        "trade_id": raw["t"],
        "ts_trade": raw["T"],
        "ts_ingest": int(time.time() * 1000),  # carimbo de ingestão (ms)
        "price": raw["p"],
        "qty": raw["q"],
        "side": "sell" if raw["m"] else "buy",
        "symbol": raw["s"],
    }


def delivery_report(err, msg):
    """Callback de entrega. Só barulho em caso de falha (não loga sucesso)."""
    if err is not None:
        print(f"[ERRO] falha ao entregar mensagem: {err}")


async def stream_to_kafka(producer: Producer, stop: asyncio.Event):
    """Loop principal com reconexão: WebSocket -> normaliza -> produz no Kafka."""
    url = config.binance_trade_stream_url()
    backoff = 1  # segundos; cresce até um teto a cada queda

    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                print(f"Conectado a {url}")
                print(f"Publicando em '{config.TRADES_TOPIC}' "
                      f"@ {config.KAFKA_BOOTSTRAP_SERVERS}")
                backoff = 1  # reconectou com sucesso -> zera o backoff

                while not stop.is_set():
                    resposta = await asyncio.wait_for(ws.recv(), timeout=30)
                    envelope = json.loads(resposta)
                    # streams combinados embrulham o evento em {"stream":..,"data":..}
                    raw = envelope.get("data", envelope)
                    trade = normalize(raw)

                    producer.produce(
                        config.TRADES_TOPIC,
                        key=trade["symbol"],
                        value=json.dumps(trade),
                        on_delivery=delivery_report,
                    )
                    # serve os callbacks de entrega sem bloquear o loop
                    producer.poll(0)

        except asyncio.CancelledError:
            raise
        except Exception as e:  # queda de WS, timeout, erro de rede
            if stop.is_set():
                break
            print(f"[AVISO] conexão caiu ({type(e).__name__}: {e}). "
                  f"Reconectando em {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def main():
    producer = make_producer()
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        await stream_to_kafka(producer, stop)
    finally:
        print("Encerrando: dando flush nas mensagens pendentes...")
        producer.flush(10)
        print("Coletor finalizado.")


if __name__ == "__main__":
    asyncio.run(main())
