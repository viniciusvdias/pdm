# 🎓 RU-UFLA Analytics - Setup Inicial

> **Status:** Estrutura inicial configurada ✅  
> **Próximos passos:** Implementação das análises específicas

## 📋 Sobre o Projeto

Análise de dados do Restaurante Universitário (RU) da Universidade Federal de Lavras (UFLA) utilizando processamento distribuído com Apache Spark.

### 🎯 Objetivos

- **Análise de Séries Temporais:** Detecção de picos e tendências de consumo
- **Análise de Grafos:** Co-ocorrência e detecção de grupos sociais
- **Métricas de Performance:** Monitoramento detalhado com sparkMeasure

### 🏗️ Arquitetura

- **Cluster Docker Swarm** com Apache Spark
- **Driver + 2 Workers** para processamento distribuído
- **Jupyter Notebooks** para desenvolvimento interativo
- **sparkMeasure** para coleta de métricas de performance

## 🚀 Como Executar

### Pré-requisitos

- Docker
- Docker Compose

### Comandos Principais

```bash
# Tornar script executável
chmod +x bin/run_project.sh

# Construir e iniciar o cluster (primeiro uso)
./bin/run_project.sh build
./bin/run_project.sh up

# Ou simplesmente (já constrói automaticamente)
./bin/run_project.sh
```

### 🌐 Interfaces Web

Após inicializar o cluster:

| Serviço                  | URL                   | Descrição                   |
| ------------------------ | --------------------- | --------------------------- |
| **Jupyter Notebook**     | http://localhost:8888 | Desenvolvimento interativo  |
| **Spark Master UI**      | http://localhost:8080 | Monitoramento do cluster    |
| **Spark Application UI** | http://localhost:4040 | Métricas da aplicação atual |
| **Worker 1 UI**          | http://localhost:8081 | Status do Worker 1          |
| **Worker 2 UI**          | http://localhost:8082 | Status do Worker 2          |

### 📁 Estrutura do Projeto

```
g2/
├── bin/                    # Scripts executáveis
│   └── run_project.sh     # Script principal
├── src/                    # Código fonte Python
│   ├── __init__.py
│   ├── config.py          # Configurações Spark
│   └── spark_measure_utils.py  # Utilitários sparkMeasure
├── notebooks/              # Jupyter Notebooks
│   └── 01_exploracao_inicial.ipynb
├── misc/                   # Arquivos auxiliares
│   ├── results/           # Resultados das análises
│   └── metrics/           # Métricas de performance
├── datasample/            # Dados de amostra (< 1MB)
│   └── ru_sample.csv
├── presentation/          # Slides da apresentação
├── docker-compose.yml     # Configuração do cluster
├── Dockerfile            # Imagem Docker personalizada
├── pyproject.toml        # Dependências Python (UV)
└── README-provisorio.md  # Esta documentação
```

## 🛠️ Stack Tecnológica

- **🐍 Python 3.12** com UV para gerenciamento de dependências
- **⚡ Apache Spark 3.5.0** para processamento distribuído
- **📊 sparkMeasure 0.25** - API oficial para métricas de performance
- **📝 loguru** - Sistema de logging estruturado e colorido
- **🐳 Docker & Docker Compose** para containerização
- **📓 Jupyter Notebook** para análise interativa
- **🔥 PySpark** para interface Python do Spark

## 📊 Dados

### Fonte

- **Dataset:** Registros públicos do RU-UFLA (2009-presente)
- **Volume:** Milhões de registros estruturados
- **Atributos:** cidade de nascimento, data de consumo, curso, etc.

### Amostra Disponível

- Arquivo: `datasample/ru_sample.csv`
- Tamanho: < 1MB (para testes rápidos)

## 🎯 Próximas Implementações

### Análises Planejadas

1. **Séries Temporais**

   - Tendências de consumo por período
   - Detecção de sazonalidade
   - Picos de demanda

2. **Análise de Grafos**

   - Redes de co-ocorrência entre estudantes
   - Detecção de comunidades
   - Métricas de centralidade

3. **Visualizações**
   - Dashboards interativos
   - Relatórios automáticos
   - Gráficos de performance

### Métricas de Performance

- Tempo de execução por estágio
- Uso de CPU, memória e I/O
- Métricas de shuffle e garbage collection
- Armazenamento em CSV/JSON para análise posterior

## 🔧 Comandos Úteis

```bash
# Ver status do cluster
./bin/run_project.sh status

# Ver logs em tempo real
./bin/run_project.sh logs

# Reiniciar cluster
./bin/run_project.sh restart

# Parar cluster
./bin/run_project.sh down

# Limpeza completa
./bin/run_project.sh clean

# Abrir interfaces no navegador
./bin/run_project.sh jupyter
./bin/run_project.sh spark-ui

# Ver ajuda
./bin/run_project.sh help
```

## 📝 Desenvolvimento

### Workflow Recomendado

1. **Desenvolvimento:** Use Jupyter Notebook (http://localhost:8888)
2. **Código modular:** Organize funções em `src/`
3. **Testes:** Execute no cluster distribuído
4. **Métricas:** Use sparkMeasure para performance
5. **Resultados:** Salve em `misc/results/`

### Boas Práticas

- Sempre use o `SparkConfig.get_spark_session()` para sessões Spark
- Use `@measure_spark_operation` para decorar funções importantes
- Para análises rápidas, use `QuickMeasure.time_spark_sql()`
- Use `SparkMeasureWrapper` para controle fino das medições
- Salve métricas regularmente para análise de performance
- Mantenha dados grandes fora do repositório

### 🧪 Testando a Instalação

```bash
# Testar integração completa
./bin/run_project.sh test

# Ver logs em tempo real
./bin/run_project.sh logs

# Demonstrar sistema de logging
docker exec -it spark-master python /app/src/example_logging.py
```

### 📝 Sistema de Logging

O projeto usa **loguru** para logging estruturado e profissional:

#### **Configuração via Variáveis de Ambiente:**

```bash
# Configurar nível de log
export LOG_LEVEL=DEBUG    # DEBUG, INFO, WARNING, ERROR

# Habilitar/desabilitar arquivo de log
export LOG_TO_FILE=true   # true, false

# Habilitar/desabilitar cores no terminal
export LOG_COLORS=true    # true, false

# Executar com configuração personalizada
LOG_LEVEL=DEBUG ./bin/run_project.sh up
```

#### **Arquivos de Log Gerados:**

- **`misc/logs/ru_ufla_analytics.log`** - Log geral com rotação (10MB)
- **`misc/logs/errors.log`** - Apenas erros (5MB)
- **`misc/logs/performance.log`** - Métricas de performance (20MB)

#### **Recursos do Logging:**

- ✅ **Logs coloridos** no terminal para melhor visualização
- ✅ **Rotação automática** de arquivos (evita arquivos gigantes)
- ✅ **Compressão** automática dos logs antigos
- ✅ **Thread-safe** para ambientes multi-threaded
- ✅ **Structured logging** com contexto de módulos
- ✅ **Stack traces** detalhados em caso de erro
- ✅ **Interceptação** de logs do Python padrão e PySpark

#### **Exemplo de Uso no Código:**

```python
from loguru import logger
from logging_config import get_module_logger

# Logger básico
logger.info("Mensagem informativa")
logger.warning("Aviso importante")
logger.error("Erro ocorreu")

# Logger específico para módulo
data_logger = get_module_logger("data_processing")
data_logger.success("Dados carregados: {} registros", count)

# Logger de performance
perf_logger = get_performance_logger()
perf_logger.info("Tempo de execução: {}s", execution_time)
```

---

**📧 Contato:** Grupo 2 - Big Data  
**📅 Versão:** 0.1.0 (Setup Inicial)  
**🔄 Última atualização:** Janeiro 2025
