"""
Configurações do projeto para Spark e sparkMeasure
Suporte para execução local e em cluster Docker Swarm
"""

import os
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


# Configurar loguru para o módulo de configuração
logger = logger.bind(module="config")


class SparkConfig:
    """Configurações para o Apache Spark"""

    @staticmethod
    def get_spark_session(app_name: str = "RU-UFLA-Analytics") -> SparkSession:
        """
        Cria e retorna uma sessão Spark configurada

        Args:
            app_name: Nome da aplicação Spark

        Returns:
            SparkSession configurada
        """
        logger.info("Configurando sessão Spark para aplicação: {}", app_name)

        conf = SparkConf()

        # Configurações básicas
        conf.set("spark.app.name", app_name)
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

        # Configurações de memória 
        conf.set("spark.executor.memory", "4g")  # Aumentado de 2g para 4g
        conf.set("spark.driver.memory", "2g")   # Aumentado de 1g para 2g
        conf.set("spark.driver.maxResultSize", "2g")  # Novo: para results grandes
        conf.set("spark.executor.memoryFraction", "0.8")
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        
        # Configurações de serialização para performance
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.kryo.unsafe", "true")
        conf.set("spark.kryoserializer.buffer.max", "128m")  # Aumentado para objetos grandes
        
        # Configurações para JSON multiline
        conf.set("spark.sql.json.multiLine", "true")
        conf.set("spark.sql.streaming.multipleWatermarkPolicy", "min")
        
        # Configurações de garbage collection
        conf.set("spark.executor.extraJavaOptions", 
                "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1PrintRegionRememberedSetInfo")
        conf.set("spark.driver.extraJavaOptions", 
                "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1PrintRegionRememberedSetInfo")

        # Configurações do sparkMeasure para Spark
        conf.set("spark.jars", "/opt/spark/jars/spark-measure_2.12-0.25.jar")

        # Configurações específicas do Spark
        conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

        # Configuração para cluster ou local
        master_url = os.getenv("SPARK_MASTER_URL", "local[*]")

        # Detectar ambiente de execução
        execution_mode = SparkConfig._detect_execution_mode(master_url)
        logger.info("Modo de execução detectado: {}", execution_mode)

        # Configurações específicas por ambiente
        if execution_mode == "local":
            SparkConfig._configure_local_mode(conf)
        elif execution_mode == "docker_swarm":
            SparkConfig._configure_swarm_mode(conf)

        logger.debug("Usando Spark master URL: {}", master_url)

        try:
            spark = (
                SparkSession.builder.config(conf=conf).master(master_url).getOrCreate()
            )

            # Configurar nível de log do Spark
            spark.sparkContext.setLogLevel("WARN")

            logger.success("Sessão Spark criada com sucesso")
            logger.info("Versão do Spark: {}", spark.version)
            logger.info("Master URL: {}", spark.conf.get("spark.master"))
            logger.info("Modo de execução: {}", execution_mode)
            logger.debug(
                "Paralelismo padrão: {}", spark.sparkContext.defaultParallelism
            )

            return spark

        except Exception as e:
            logger.error("Erro ao criar sessão Spark: {}", e)
            raise

    @staticmethod
    def _detect_execution_mode(master_url: str) -> str:
        """Detecta o modo de execução baseado na URL do master e variáveis de ambiente"""

        # Verificar se está em ambiente Docker (Compose ou Swarm)
        if (os.getenv("DOCKER_SWARM_MODE") == "true" or 
            "spark-master" in master_url or
            os.path.exists("/.dockerenv")):  # Arquivo presente em containers Docker
            return "docker_swarm"

        # Verificar se é cluster remoto
        if (
            master_url.startswith("spark://")
            and "localhost" not in master_url
            and "127.0.0.1" not in master_url
            and "local" not in master_url
        ):
            return "cluster"

        # Modo local por padrão
        return "local"

    @staticmethod
    def _configure_local_mode(conf: SparkConf):
        """Configurações otimizadas para execução local"""
        logger.debug("Configurando modo local")

        # Otimizações para desenvolvimento local
        conf.set("spark.ui.enabled", "true")
        conf.set("spark.ui.port", "4040")
        conf.set("spark.eventLog.enabled", "false")

        # Memória reduzida para desenvolvimento
        conf.set("spark.executor.instances", "1")
        conf.set("spark.executor.cores", "2")

    @staticmethod
    def _configure_swarm_mode(conf: SparkConf):
        """Configurações otimizadas para Docker Swarm"""
        logger.debug("Configurando modo Docker Swarm")

        # Configurações de rede para Docker - ESSENCIAL para funcionamento
        # O driver precisa ser acessível pelos executors na rede Docker
        import socket
        hostname = socket.gethostname()
        logger.debug(f"Configurando driver host como: {hostname}")
        
        conf.set("spark.driver.host", hostname)
        conf.set("spark.driver.bindAddress", "0.0.0.0")
        
        # Configurações específicas para rede Docker
        conf.set("spark.driver.port", "0")  # Porta dinâmica
        conf.set("spark.blockManager.port", "0")  # Porta dinâmica
        conf.set("spark.executor.heartbeatInterval", "20s")
        conf.set("spark.network.timeout", "600s")
        conf.set("spark.executor.heartbeat.maxFailures", "20")

        # Configurações de recursos para containers
        conf.set("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "4g"))  # Aumentado de 1g para 4g
        conf.set("spark.executor.cores", os.getenv("SPARK_EXECUTOR_CORES", "2"))     # Aumentado de 1 para 2
        conf.set("spark.executor.instances", os.getenv("SPARK_EXECUTOR_INSTANCES", "2"))
        
        # Configurações adicionais para datasets grandes em Docker
        conf.set("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        conf.set("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "2g"))
        
        # Configurações para melhor estabilidade em Docker
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # UI e monitoramento
        conf.set("spark.ui.enabled", "true")
        conf.set("spark.ui.port", "4040")
        conf.set("spark.eventLog.enabled", "false")  # Desabilitado por enquanto
        
        # Configurações de tolerância a falhas para containers
        conf.set("spark.task.maxAttempts", "3")
        conf.set("spark.stage.maxConsecutiveAttempts", "8")
        
        # Configurações específicas do PySpark para datasets grandes
        conf.set("spark.python.worker.reuse", "true")
        conf.set("spark.python.worker.memory", os.getenv("SPARK_PYTHON_WORKER_MEMORY", "1g"))  # Aumentado de 512m para 1g


class DataPaths:
    """Caminhos para os dados e resultados com suporte a múltiplos ambientes"""

    @classmethod
    def _get_base_dir(cls):
        """Determina o diretório base baseado no ambiente de execução"""
        # Se está em container Docker, usar /app
        if os.path.exists("/.dockerenv") or os.getenv("DOCKER_SWARM_MODE") == "true":
            return "/app"
        else:
            # Ambiente local: usar diretório atual do projeto
            return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    @classmethod
    def _initialize_paths(cls):
        """Inicializa os caminhos baseado no ambiente"""
        cls.BASE_DIR = cls._get_base_dir()
        cls.DATA_SAMPLE_DIR = f"{cls.BASE_DIR}/datasample"
        cls.MISC_DIR = f"{cls.BASE_DIR}/misc"
        cls.RESULTS_DIR = f"{cls.MISC_DIR}/results"
        cls.METRICS_DIR = f"{cls.MISC_DIR}/metrics"
        cls.LOGS_DIR = f"{cls.MISC_DIR}/logs"
        
        # Arquivos de dados - suporte para múltiplos formatos
        cls.RU_DATA_SAMPLE = f"{cls.DATA_SAMPLE_DIR}/ru_sample.json"
        cls.RU_DATA_CSV = f"{cls.DATA_SAMPLE_DIR}/ru_sample.csv"
        
        # Dataset completo
        cls.DATA_DIR = f"{cls.MISC_DIR}/data"
        cls.RU_DATA_COMPLETE = f"{cls.DATA_DIR}/dataset.json"

        # Diretório temporário para comunicação PySpark-GraphX
        cls.TEMP_DIR = f"{cls.MISC_DIR}/temp"
        
        # Diretório para bibliotecas externas (JAR files)
        cls.LIBS_DIR = f"{cls.MISC_DIR}/libs"

    @classmethod
    def ensure_directories(cls):
        """Garante que os diretórios necessários existam"""
        import os

        logger.debug("Verificando e criando diretórios necessários")

        directories_created = 0
        for attr_name in dir(cls):
            if attr_name.endswith("_DIR"):
                path = getattr(cls, attr_name)
                if not os.path.exists(path):
                    os.makedirs(path, exist_ok=True)
                    directories_created += 1
                    logger.debug("Diretório criado: {}", path)

        if directories_created > 0:
            logger.info("Criados {} diretórios", directories_created)
        else:
            logger.debug("Todos os diretórios já existem")

    @classmethod
    def get_sample_file(cls) -> str:
        """Retorna o caminho do arquivo de amostra disponível"""

        # Preferir JSON, depois CSV
        if os.path.exists(cls.RU_DATA_SAMPLE):
            return cls.RU_DATA_SAMPLE
        elif os.path.exists(cls.RU_DATA_CSV):
            return cls.RU_DATA_CSV
        else:
            # Retornar o padrão mesmo se não existir (para erro informativo)
            return cls.RU_DATA_SAMPLE


# Inicializar caminhos automaticamente após definição da classe
DataPaths._initialize_paths()


class DockerSwarmConfig:
    """Configurações específicas para Docker Swarm"""

    @staticmethod
    def is_swarm_mode() -> bool:
        """Verifica se está executando em modo Docker Swarm"""
        return os.getenv("DOCKER_SWARM_MODE", "false").lower() == "true"

    @staticmethod
    def get_service_name() -> str:
        """Retorna o nome do serviço no Swarm"""
        return os.getenv("DOCKER_SERVICE_NAME", "ru-analytics")

    @staticmethod
    def get_replicas() -> int:
        """Retorna o número de réplicas configuradas"""
        return int(os.getenv("DOCKER_REPLICAS", "1"))

    @staticmethod
    def get_network_name() -> str:
        """Retorna o nome da rede do Swarm"""
        return os.getenv("DOCKER_NETWORK_NAME", "ru-analytics-network")
