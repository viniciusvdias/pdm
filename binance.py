import asyncio
import websockets
import json

async def stream_binance_trades():
    # URL do WebSocket apontando para o stream de trades do BTC/USDT
    url = "wss://stream.binance.com:9443/ws/btcusdt@depth5"

    async with websockets.connect(url) as websocket:
        print("Conectado! Aguardando o fluxo de dados...")
        
        while True:
            # Fica escutando a porta aguardando a Binance empurrar o dado
            resposta = await websocket.recv()
            dados = json.loads(resposta)
            
            print(dados)

# Roda o loop assíncrono
asyncio.run(stream_binance_trades())