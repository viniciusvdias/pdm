"""Consumidor burro: lê do tópico `trades` e imprime.

Não faz janelamento nem métrica — só prova que o grupo de consumidores está
assinando o tópico certo e enxergando o que o coletor publica. É o passo anterior
a plugar a lógica de candle (que se desenvolve offline, num arquivo).

Uso:
    python consumer.py
"""
import json
import signal

from confluent_kafka import Consumer

import config


def make_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "consumidor-burro",
            "auto.offset.reset": "earliest",  # começa do início se não houver offset
        }
    )


def main():
    consumer = make_consumer()
    consumer.subscribe([config.TRADES_TOPIC])
    print(f"Consumindo de '{config.TRADES_TOPIC}' "
          f"@ {config.KAFKA_BOOTSTRAP_SERVERS} (Ctrl+C para sair)")

    rodando = True

    def parar(*_):
        nonlocal rodando
        rodando = False

    signal.signal(signal.SIGINT, parar)
    signal.signal(signal.SIGTERM, parar)

    total = 0
    try:
        while rodando:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[ERRO] {msg.error()}")
                continue
            trade = json.loads(msg.value())
            total += 1
            print(f"part={msg.partition()} offset={msg.offset()} "
                  f"key={msg.key().decode() if msg.key() else None} -> {trade}")
    finally:
        print(f"\nEncerrando. Total consumido nesta sessão: {total}")
        consumer.close()


if __name__ == "__main__":
    main()
