# Configuração do Cluster Apache Spark no Docker Swarm

## Visão Geral do Cluster

Este projeto foi otimizado para usar **todas as 4 máquinas virtuais** do laboratório PDM de forma distribuída. A configuração atual maximiza o uso de recursos disponíveis:

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                             CLUSTER DOCKER SWARM (4 NÓDOS)                             │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐                │
│  │   VM01      │   │   VM02      │   │   VM03      │   │   VM04      │                │
│  │  (Manager)  │   │  (Worker)   │   │  (Worker)   │   │  (Worker)   │                │
│  │             │   │             │   │             │   │             │                │
│  │ Master      │   │ Workers     │   │ Workers     │   │ Workers     │                │
│  │ + 2 Workers │   │ (2x)        │   │ (2x)        │   │ (2x)        │                │
│  └─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘                │
│                                                                                         │
│                      TOTAL: 8 WORKERS DISTRIBUÍDOS                                     │
│                   Cada Worker: 4 cores, 12GB RAM                                       │
│                   Capacidade total: 32 cores, 96GB RAM                                 │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Recursos Configurados

### **Por Worker:**

- **CPU**: 4 cores
- **RAM**: 12GB
- **Rede**: Otimizada para Docker Swarm
- **Armazenamento**: Volumes compartilhados

### **Driver (Master):**

- **CPU**: 2 cores
- **RAM**: 8GB
- **Localização**: VM01 (Manager Node)

### **Capacidade Total:**

- **Workers**: 8 instâncias distribuídas
- **CPU Total**: 32 cores
- **RAM Total**: 96GB para executores + 8GB para driver
- **Paralelismo**: Otimizado para processamento distribuído

## Instruções de Configuração

### Pré-requisitos

1. **Docker Swarm já está configurado** no laboratório com 4 nós
2. **Você está logado na VM02** (node atual)
3. **O cluster já está ativo** conforme mostrado pelo `docker node ls`

### Passos para Deployment

#### 1. Clonar o Projeto

```bash
# Na VM02 (ou qualquer nó manager)
git clone <url-do-seu-repo>
cd g2
```

#### 2. Construir a Imagem

```bash
# Construir a imagem do projeto
./bin/run_project.sh build

# Ou diretamente:
docker build -t ru-ufla-analytics:latest .
```

#### 3. Fazer Deploy no Cluster

```bash
# Deploy completo no cluster Swarm
./bin/run_project.sh swarm-deploy
```

Este comando irá:

- Verificar se o Swarm está ativo
- Construir a imagem se necessário
- Fazer deploy da stack com 8 workers distribuídos
- Configurar rede overlay para comunicação entre nós

#### 4. Verificar o Status

```bash
# Verificar status dos serviços
./bin/run_project.sh swarm-status

# Verificar nós do cluster
docker node ls

# Verificar serviços rodando
docker service ls
```

### Executando Análises

#### Opção 1: Análise Automática (Recomendada)

```bash
# Execução completa automática
./bin/run_project.sh swarm-analyze complete

# Análise apenas com dados de amostra
./bin/run_project.sh swarm-analyze sample

# Análise de períodos específicos
./bin/run_project.sh swarm-analyze complete 2024/1,2024/2
```

#### Opção 2: Análise Manual

```bash
# Criar job de análise manualmente
docker service create --name ru-analytics-job \
    --network ru-analytics_spark-network \
    --mount type=bind,source=$(pwd)/misc,destination=/app/misc \
    --mount type=bind,source=$(pwd)/datasample,destination=/app/datasample \
    --env SPARK_MASTER_URL=spark://spark-master:7077 \
    --env DOCKER_SWARM_MODE=true \
    --constraint 'node.role == manager' \
    --restart-condition none \
    ru-ufla-analytics:latest \
    /app/.venv/bin/python -m src.main analyze --master-url spark://spark-master:7077 --mode complete
```

## Monitoramento e Controle

### Interfaces Web

Após o deploy, você pode acessar as interfaces web:

- **Spark Master UI**: http://localhost:8080
- **Spark Application UI**: http://localhost:4040 (durante execução)
- **Workers UIs**: http://localhost:8081-8084

### Comandos de Monitoramento

```bash
# Ver logs do master
docker service logs -f ru-analytics_spark-master

# Ver logs dos workers
docker service logs -f ru-analytics_spark-worker

# Ver logs de um job específico
docker service logs -f ru-analytics-job

# Status detalhado
./bin/run_project.sh swarm-status

# Escalar workers (se necessário)
./bin/run_project.sh swarm-scale 16  # Duplicar workers
```

### Escalabilidade

```bash
# Escalar para mais workers (máximo recomendado: 16)
./bin/run_project.sh swarm-scale 12

# Voltar para configuração padrão
./bin/run_project.sh swarm-scale 8
```

## Otimizações Implementadas

### 1. **Configurações de Rede**

- Timeouts estendidos para cluster distribuído
- Heartbeat otimizado para latência de rede
- Configurações RPC ajustadas

### 2. **Gestão de Recursos**

- Memória otimizada por executor (12GB)
- Cores balanceados (4 por worker)
- Garbage collection otimizado (G1GC)

### 3. **Paralelismo e Particionamento**

- Adaptive Query Execution habilitado
- Particionamento automático otimizado
- Skew join detection ativado

### 4. **Tolerância a Falhas**

- Retry configurado para clusters distribuídos
- Blacklist de nós problemáticos
- Restart policies otimizadas

### 5. **Serialização e Shuffle**

- Kryo serializer com configurações otimizadas
- Compressão de shuffle habilitada
- Buffers aumentados para cluster

## Troubleshooting

### Problema: Workers não conectam ao Master

```bash
# Verificar conectividade de rede
docker service ls
docker service ps ru-analytics_spark-worker --no-trunc

# Recriar serviços se necessário
./bin/run_project.sh swarm-remove
./bin/run_project.sh swarm-deploy
```

### Problema: Performance baixa

```bash
# Verificar distribuição de workers
docker service ps ru-analytics_spark-worker

# Verificar logs para gargalos
docker service logs ru-analytics_spark-master

# Ajustar recursos se necessário
./bin/run_project.sh swarm-scale 12
```

### Problema: Memória insuficiente

```bash
# Verificar uso de recursos
docker stats

# Ajustar configurações de memória no docker-compose.swarm.yml
# Reduzir SPARK_EXECUTOR_MEMORY se necessário
```

## Limpeza e Remoção

```bash
# Remover stack completa
./bin/run_project.sh swarm-remove

# Remover imagens não utilizadas
docker image prune -f

# Remover volumes não utilizados
docker volume prune -f
```

## Configurações Avançadas

### Ajuste de Performance

Para ajustar a performance, edite o arquivo `docker-compose.swarm.yml`:

```yaml
environment:
  - SPARK_EXECUTOR_MEMORY=16g # Aumentar se há RAM disponível
  - SPARK_EXECUTOR_CORES=6 # Aumentar se há CPU disponível
  - SPARK_EXECUTOR_INSTANCES=12 # Aumentar número de workers
```

### Monitoramento Detalhado

```bash
# CPU e memória por container
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"

# Logs específicos por serviço
docker service logs --tail 100 ru-analytics_spark-worker

# Inspect de serviços
docker service inspect ru-analytics_spark-worker
```

## Resumo dos Comandos Principais

| Comando                                       | Descrição                  |
| --------------------------------------------- | -------------------------- |
| `./bin/run_project.sh swarm-deploy`           | Deploy completo no cluster |
| `./bin/run_project.sh swarm-analyze complete` | Executar análise completa  |
| `./bin/run_project.sh swarm-status`           | Ver status do cluster      |
| `./bin/run_project.sh swarm-scale 12`         | Escalar workers            |
| `./bin/run_project.sh swarm-remove`           | Remover stack              |
| `./bin/run_project.sh swarm-logs`             | Ver logs                   |

---

**🚀 Agora você tem um cluster Apache Spark distribuído usando todos os recursos das 4 VMs do laboratório!**

**💡 Capacidade total: 32 cores, 96GB RAM para processamento distribuído de Big Data**
