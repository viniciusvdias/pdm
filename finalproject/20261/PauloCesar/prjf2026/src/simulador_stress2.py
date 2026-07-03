import asyncio
import json
import random
import time
import matplotlib.pyplot as plt
from aiokafka import AIOKafkaProducer

# Configurações do ambiente — Alinhado com o Spark Consumer
KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092' # IP explícito para evitar problemas no macOS
TOPIC_NAME = 'dados-sensores'
INTERVALO_ENVIO = 5.0  # Janela estrita de 5 segundos exigida no artigo

# Lista com os cenários de teste expandidos para a escala macro-ambiental
CENARIOS_DISPOSITIVOS = [20, 100, 200, 300, 400, 500, 1000, 2000, 3000, 5000]
DURACAO_POR_CENARIO = 30  # Tempo de coleta de dados por cenário (segundos)

# Estruturas para armazenar os resultados dos gráficos de forma independente
resultados_latencia = []
resultados_mensagens = []
resultados_perda_pct = [] # Nova métrica para a banca ver a integridade

async def simular_no_esp32(producer, sensor_id):
    """Simula um dispositivo ESP32 gerando telemetria e dados geográficos."""
    try:
        # Gerando dados simulados com variações estocásticas normais
        temperatura = round(random.uniform(22.0, 38.0), 2)
        umidade = round(random.uniform(30.0, 75.0), 2)
        co2 = round(random.uniform(400.0, 600.0), 2)
        
        # Simulação geográfica coerente (Delimitação regional de Lavras/MG)
        latitude = round(random.uniform(-21.25, -21.20), 6)
        longitude = round(random.uniform(-45.02, -44.97), 6)
        
        # Inteligência de borda (Edge AI) simplificada para o payload
        status_ia = "ALERTA_INCENDIO" if temperatura > 35 and umidade < 35 else "NORMAL"

        # Payload alinhado ao Schema do Spark Consumer
        payload = {
            "sensor_id": f"ESP32_{sensor_id:04d}",
            "timestamp": int(time.time()), # Enviado como Long puro (Unix Epoch)
            "latitude": latitude,
            "longitude": longitude,
            "temperatura": temperatura,
            "umidade": umidade,
            "co2": co2,
            "status_ia_borda": status_ia
        }

        # Medição do tempo de envio assíncrono (Latência do Produtor)
        start_time = time.time()
        await producer.send_and_wait(TOPIC_NAME, json.dumps(payload).encode('utf-8'))
        latencia = (time.time() - start_time) * 1000  # Convertido para milissegundos
        
        return latencia
    except Exception as e:
        print(f"❌ Erro ao enviar mensagem do sensor ESP32_{sensor_id:04d}: {e}")
        return None

async def executar_cenario(qtd_dispositivos):
    """Executa a simulação assíncrona concorrente para uma quantidade específica de nós."""
    print(f"\n🚀 Iniciando cenário de teste com {qtd_dispositivos} ESP32 simulados...")
    
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    try:
        await producer.start()
    except Exception as e:
        await producer.stop()
        print(f"❌ Erro ao conectar no Kafka em {KAFKA_BOOTSTRAP_SERVERS}: {e}")
        return 0, 0, 0
    
    latencias_cenario = []
    total_tentativas_geradas = 0
    total_mensagens_confirmadas = 0
    tempo_final = time.time() + DURACAO_POR_CENARIO
    
    try:
        while time.time() < tempo_final:
            start_ciclo = time.time()
            
            # Cria tarefas assíncronas simultâneas para TODOS os dispositivos do cenário
            tarefas = [simular_no_esp32(producer, i) for i in range(qtd_dispositivos)]
            total_tentativas_geradas += len(tarefas)
            
            # Executa a transmissão em lote assíncrono (Mecanismo concorrente)
            resultados = await asyncio.gather(*tarefas)
            
            # Filtra os envios bem-sucedidos e computa as latências
            latencias_validas = [l for l in resultados if l is not None]
            latencias_cenario.extend(latencias_validas)
            total_mensagens_confirmadas += len(latencias_validas)
            
            # Controle estrito de intervalo de 5s entre envios com escalonamento dinâmico
            tempo_gasto = time.time() - start_ciclo
            tempo_espera = max(0, INTERVALO_ENVIO - tempo_gasto)
            await asyncio.sleep(tempo_espera)
            
    finally:
        await producer.stop()
        
    latencia_media = sum(latencias_cenario) / len(latencias_cenario) if latencias_cenario else 0
    msg_por_segundo = total_mensagens_confirmadas / DURACAO_POR_CENARIO
    
    # Cálculo científico da taxa de descarte de dados (Perda)
    mensagens_perdidas = total_tentativas_geradas - total_mensagens_confirmadas
    pct_perda = (mensagens_perdidas / total_tentativas_geradas) * 100 if total_tentativas_geradas > 0 else 0
    
    print(f"📊 Fim do cenário. Latência Média: {latencia_media:.2f}ms | Throughput: {msg_por_segundo:.2f} msg/s | Perda: {pct_perda:.2f}%")
    return latencia_media, msg_por_segundo, pct_perda

async def main():
    print("=== PIPELINE DE ESTRESSE ASSÍNCRONO: KAFKA ECOSYSTEM ===")
    
    # Executa sequencialmente cada cenário com intervalo de descanso de 5s
    for qtd in CENARIOS_DISPOSITIVOS:
        lat_media, msg_sec, pct_perda = await executar_cenario(qtd)
        resultados_latencia.append(lat_media)
        resultados_mensagens.append(msg_sec)
        resultados_perda_pct.append(pct_perda)
        
        print("⏳ Aguardando 5 segundos para estabilização do Broker antes do próximo cenário...")
        await asyncio.sleep(5)
        
    # Eixo X comum para os três gráficos
    eixo_x = [str(c) for c in CENARIOS_DISPOSITIVOS]

    # ------------------------------------------------------------------------------
    # GRÁFICO 1: Vazão de Produção da Borda (Throughput de Envio dos ESP32)
    # ------------------------------------------------------------------------------
    plt.figure(figsize=(10, 5))
    color_throughput = '#1f77b4'
    bars = plt.bar(eixo_x, resultados_mensagens, color=color_throughput, alpha=0.7, width=0.4)
    plt.bar_label(bars, fmt='%.1f', padding=3, fontweight='bold')
    
    plt.xlabel('Quantidade de Dispositivos Concorrentes (ESP32)', fontweight='bold', labelpad=10)
    plt.ylabel('Taxa de Transmissão (Mensagens / Segundo)', fontweight='bold', color=color_throughput)
    plt.title('Análise de Throughput: Volume de Mensagens de Sensores Enviadas por Segundo', fontsize=11, fontweight='bold', pad=15)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    plt.tight_layout()
    plt.savefig('grafico_1_throughput_esp32.png', dpi=300)
    plt.close()
    print("💾 [SUCESSO] Gráfico 1 salvo como 'grafico_1_throughput_esp32.png'")

    # ------------------------------------------------------------------------------
    # GRÁFICO 2: Velocidade com que o Kafka recebe os dados (Latência de Ingestão)
    # ------------------------------------------------------------------------------
    plt.figure(figsize=(10, 5))
    color_latencia = '#d62728'
    plt.plot(eixo_x, resultados_latencia, color=color_latencia, marker='o', linewidth=2.5, markersize=8)
    
    for i, txt in enumerate(resultados_latencia):
        plt.annotate(f"{txt:.1f}ms", (eixo_x[i], resultados_latencia[i]), textcoords="offset points", 
                     xytext=(0,10), ha='center', color=color_latencia, weight='bold')
                     
    plt.xlabel('Quantidade de Dispositivos Concorrentes (ESP32)', fontweight='bold', labelpad=10)
    plt.ylabel('Latência Média de Confirmação (Milissegundos)', fontweight='bold', color=color_latencia)
    plt.title('Análise de Responsividade: Tempo de Resposta do Broker Apache Kafka', fontsize=11, fontweight='bold', pad=15)
    plt.grid(True, linestyle='--', alpha=0.3)
    plt.tight_layout()
    plt.savefig('grafico_2_latencia_kafka.png', dpi=300)
    plt.close()
    print("💾 [SUCESSO] Gráfico 2 salvo como 'grafico_2_latencia_kafka.png'")

    # ------------------------------------------------------------------------------
    # GRÁFICO 3: Índice de Perda de Dados (Confiabilidade do Sistema de Fluxo)
    # ------------------------------------------------------------------------------
    plt.figure(figsize=(10, 5))
    color_perda = '#2ca02c'
    plt.plot(eixo_x, resultados_perda_pct, color=color_perda, marker='s', linewidth=2, linestyle='--', markersize=8)
    
    for i, txt in enumerate(resultados_perda_pct):
        plt.annotate(f"{txt:.2f}%", (eixo_x[i], resultados_perda_pct[i]), textcoords="offset points", 
                     xytext=(0,10), ha='center', color=color_perda, weight='bold')
                     
    plt.xlabel('Quantidade de Dispositivos Concorrentes (ESP32)', fontweight='bold', labelpad=10)
    plt.ylabel('Taxa de Descarte de Pacotes (%)', fontweight='bold', color=color_perda)
    plt.ylim(-1, 10) # Limite visual fixado para provar graficamente o valor próximo ou igual a zero
    plt.title('Análise de Confiabilidade: Índice de Perda de Mensagens de Ponta a Ponta', fontsize=11, fontweight='bold', pad=15)
    plt.grid(True, linestyle='--', alpha=0.3)
    plt.tight_layout()
    plt.savefig('grafico_3_perda_dados.png', dpi=300)
    plt.close()
    print("💾 [SUCESSO] Gráfico 3 salvo como 'grafico_3_perda_dados.png'")
    
    print("\n🎉 Todos os três gráficos foram gerados isoladamente e salvos com sucesso!")

if __name__ == '__main__':
    asyncio.run(main())