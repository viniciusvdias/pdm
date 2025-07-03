"""
Utilitários para métricas de performance com sparkMeasure
Usando a API oficial da biblioteca sparkmeasure
"""

import json
import csv
from datetime import datetime
from typing import Dict, Any, Optional
from loguru import logger
from pyspark.sql import SparkSession


# Configurar logger para este módulo
logger = logger.bind(module="sparkmeasure")


class SparkMeasureWrapper:
    """Wrapper simplificado usando a API oficial da sparkMeasure"""

    def __init__(self, spark: SparkSession, mode: str = "stage"):
        """
        Inicializa o wrapper sparkMeasure

        Args:
            spark: Sessão Spark
            mode: "stage" ou "task" - nível de coleta de métricas
        """
        self.spark = spark
        self.mode = mode.lower()
        self._metrics_collector = None
        self._start_time = None
        self._end_time = None

        logger.info("Inicializando sparkMeasure em modo: {}", self.mode)

        # Importar a API oficial da sparkMeasure
        try:
            if self.mode == "stage":
                from sparkmeasure import StageMetrics

                self._metrics_collector = StageMetrics(spark)
                logger.debug("StageMetrics configurado")
            elif self.mode == "task":
                from sparkmeasure import TaskMetrics

                self._metrics_collector = TaskMetrics(spark)
                logger.debug("TaskMetrics configurado")
            else:
                error_msg = f"Mode deve ser 'stage' ou 'task', recebido: {mode}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            logger.success(
                "sparkMeasure inicializado com sucesso em modo: {}", self.mode
            )

        except ImportError as e:
            logger.error("Erro ao importar sparkMeasure: {}", e)
            logger.warning(
                "Certifique-se de que sparkmeasure está instalado: pip install sparkmeasure"
            )
            logger.warning(
                "E adicione --packages ch.cern.sparkmeasure:spark-measure_2.12:0.25"
            )
            raise
        except Exception as e:
            logger.error("Erro inesperado ao inicializar sparkMeasure: {}", e)
            raise

    def start_measurement(self):
        """Inicia a medição de métricas"""
        if not self._metrics_collector:
            logger.error("Metrics collector não foi inicializado")
            return

        self._start_time = datetime.now()

        try:
            self._metrics_collector.begin()
            logger.info(
                "Medição iniciada em: {} (modo: {})",
                self._start_time.strftime("%H:%M:%S"),
                self.mode,
            )
        except Exception as e:
            logger.error("Erro ao iniciar medição: {}", e)
            raise

    def stop_measurement(self) -> Dict[str, Any]:
        """
        Para a medição e retorna as métricas coletadas

        Returns:
            Dicionário com as métricas coletadas
        """
        if not self._metrics_collector:
            logger.error("Metrics collector não foi inicializado")
            return {}

        self._end_time = datetime.now()
        execution_time = (self._end_time - self._start_time).total_seconds()

        try:
            # Parar coleta
            self._metrics_collector.end()
            logger.debug("Coleta de métricas finalizada")

            # Coletar métricas agregadas
            if self.mode == "stage":
                raw_metrics = self._metrics_collector.aggregate_stagemetrics()
                logger.debug(
                    "Métricas de stage coletadas: {} métricas", len(raw_metrics) if raw_metrics else 0
                )
            else:  # task mode
                raw_metrics = self._metrics_collector.aggregate_taskmetrics()
                logger.debug(
                    "Métricas de task coletadas: {} métricas", len(raw_metrics) if raw_metrics else 0
                )

            # Converter JavaMap para dict Python para serialização JSON
            if raw_metrics:
                raw_metrics = self._convert_java_map_to_dict(raw_metrics)
            else:
                raw_metrics = {}

            # Estruturar métricas no nosso formato
            metrics = {
                "execution_info": {
                    "start_time": self._start_time.isoformat(),
                    "end_time": self._end_time.isoformat(),
                    "execution_time_seconds": execution_time,
                    "mode": self.mode,
                },
                "spark_metrics": raw_metrics,
            }

            logger.success("Medição finalizada. Tempo total: {:.2f}s", execution_time)

            # Log métricas principais
            if raw_metrics:
                num_stages = raw_metrics.get("numStages", "N/A")
                num_tasks = raw_metrics.get("numTasks", "N/A")
                elapsed_time = raw_metrics.get("elapsedTime", "N/A")
                
                # Tratar valores None
                if elapsed_time is None:
                    elapsed_time = "N/A"
                elif isinstance(elapsed_time, (int, float)):
                    elapsed_time = f"{elapsed_time}ms"
                
                logger.info(
                    "Métricas principais - Stages: {}, Tasks: {}, Elapsed: {}",
                    num_stages if num_stages is not None else "N/A",
                    num_tasks if num_tasks is not None else "N/A",
                    elapsed_time,
                )

            return metrics

        except Exception as e:
            logger.error("Erro ao coletar métricas: {}", e)
            return {
                "execution_info": {
                    "start_time": self._start_time.isoformat(),
                    "end_time": self._end_time.isoformat(),
                    "execution_time_seconds": execution_time,
                    "mode": self.mode,
                },
                "error": str(e),
            }

    def _convert_java_map_to_dict(self, java_obj):
        """
        Converte objetos Java (JavaMap, etc.) para tipos Python serializáveis

        Args:
            java_obj: Objeto Java retornado pela sparkMeasure

        Returns:
            Dicionário Python serializável
        """
        try:
            # Se for um JavaMap ou similar, converter para dict
            if hasattr(java_obj, "items"):
                result = {}
                for key, value in java_obj.items():
                    # Converter chaves e valores recursivamente
                    py_key = str(key) if key is not None else "null"
                    py_value = self._convert_java_value(value)
                    result[py_key] = py_value
                return result
            else:
                # Se for outro tipo de objeto Java, converter valor diretamente
                return self._convert_java_value(java_obj)

        except Exception as e:
            logger.warning("Erro ao converter objeto Java: {}", e)
            return {"conversion_error": str(e), "original_type": str(type(java_obj))}

    def _convert_java_value(self, value):
        """
        Converte um valor Java individual para tipo Python

        Args:
            value: Valor a ser convertido

        Returns:
            Valor Python serializável
        """
        if value is None:
            return None
        elif isinstance(value, (int, float, str, bool)):
            return value
        elif hasattr(value, "items"):  # JavaMap aninhado
            return self._convert_java_map_to_dict(value)
        elif hasattr(value, "__iter__") and not isinstance(value, str):  # Lista/array
            try:
                return [self._convert_java_value(item) for item in value]
            except:
                return str(value)
        else:
            # Para outros tipos, converter para string
            return str(value)

    def print_report(self):
        """Imprime relatório de métricas usando a API oficial"""
        if not self._metrics_collector:
            logger.error("Metrics collector não foi inicializado")
            return

        logger.info("Gerando relatório de métricas Spark...")

        try:
            self._metrics_collector.print_report()
            logger.success("Relatório de métricas exibido")
        except Exception as e:
            logger.error("Erro ao gerar relatório: {}", e)

    def print_memory_report(self):
        """Imprime relatório de memória usando a API oficial"""
        if not self._metrics_collector:
            logger.error("Metrics collector não foi inicializado")
            return

        logger.info("Gerando relatório de memória...")

        try:
            self._metrics_collector.print_memory_report()
            logger.success("Relatório de memória exibido")
        except Exception as e:
            logger.error("Erro ao gerar relatório de memória: {}", e)

    def run_and_measure(self, spark_code: str, globals_dict: dict = None):
        """
        Executa código Spark e mede automaticamente

        Args:
            spark_code: Código Spark para executar
            globals_dict: Dicionário de variáveis globais (para eval)
        """
        if not self._metrics_collector:
            logger.error("Metrics collector não foi inicializado")
            return

        logger.info(
            "Executando e medindo código: {}",
            spark_code[:100] + "..." if len(spark_code) > 100 else spark_code,
        )

        try:
            if globals_dict:
                self._metrics_collector.runandmeasure(globals_dict, spark_code)
                logger.success("Código executado e medido com sucesso")
            else:
                # Para código mais simples
                self.start_measurement()
                result = eval(spark_code)
                self.stop_measurement()
                logger.success("Código executado manualmente com medição")
                return result

        except Exception as e:
            logger.error("Erro na execução do código: {}", e)
            raise

    def save_metrics(
        self,
        metrics: Dict[str, Any],
        filename_prefix: str = "spark_metrics",
        output_dir: str = None,
    ) -> None:
        """
        Salva as métricas em arquivos JSON e CSV

        Args:
            metrics: Métricas coletadas
            filename_prefix: Prefixo para o nome dos arquivos
            output_dir: Diretório de saída (usa /app/misc/metrics se não especificado)
        """
        import os

        # Usar diretório padrão se não especificado
        if output_dir is None:
            output_dir = "/app/misc/metrics"

        logger.debug(
            "Salvando métricas com prefixo: {} em: {}", filename_prefix, output_dir
        )

        try:
            os.makedirs(output_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Salvar como JSON
            json_filename = f"{output_dir}/{filename_prefix}_{timestamp}.json"
            with open(json_filename, "w") as f:
                json.dump(metrics, f, indent=2)
            logger.debug("Métricas JSON salvas: {}", json_filename)

            # Salvar métricas principais como CSV
            csv_filename = f"{output_dir}/{filename_prefix}_{timestamp}.csv"
            self._save_metrics_csv(metrics, csv_filename)

            logger.success("Métricas salvas com sucesso")
            logger.info("JSON: {}", json_filename)
            logger.info("CSV: {}", csv_filename)

        except Exception as e:
            logger.error("Erro ao salvar métricas: {}", e)
            raise

    def _save_metrics_csv(self, metrics: Dict[str, Any], csv_filename: str):
        """Salva métricas em formato CSV"""
        try:
            with open(csv_filename, "w", newline="") as f:
                writer = csv.writer(f)

                # Cabeçalho
                headers = ["timestamp", "execution_time_seconds", "mode"]

                # Adicionar headers das métricas Spark
                spark_metrics = metrics.get("spark_metrics", {})
                headers.extend(spark_metrics.keys())

                writer.writerow(headers)

                # Dados
                exec_info = metrics.get("execution_info", {})
                row = [
                    exec_info.get("start_time", ""),
                    exec_info.get("execution_time_seconds", 0),
                    exec_info.get("mode", ""),
                ]

                # Adicionar valores das métricas Spark
                row.extend(spark_metrics.values())

                writer.writerow(row)

            logger.debug("Métricas CSV salvas: {}", csv_filename)

        except Exception as e:
            logger.error("Erro ao salvar métricas CSV: {}", e)
            raise


def measure_spark_operation(
    operation_name: str = "spark_operation", mode: str = "stage"
):
    """
    Decorator para medir automaticamente operações Spark usando a API oficial

    Args:
        operation_name: Nome da operação para identificação
        mode: "stage" ou "task" - nível de coleta de métricas
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            operation_logger = logger.bind(operation=operation_name)
            operation_logger.info("Iniciando operação: {}", operation_name)

            # Tentar encontrar a sessão Spark nos argumentos
            spark = None

            # Procurar nos argumentos posicionais
            for arg in args:
                if hasattr(arg, "sparkContext"):
                    spark = arg
                    break
                # Verificar se é um objeto com atributo spark (como self.spark)
                elif hasattr(arg, "spark") and hasattr(arg.spark, "sparkContext"):
                    spark = arg.spark
                    break

            # Procurar nos argumentos nomeados
            if not spark:
                spark = kwargs.get("spark")

            if spark:
                operation_logger.debug("Sessão Spark encontrada, iniciando medição")
                measurer = SparkMeasureWrapper(spark, mode)
                measurer.start_measurement()

                try:
                    result = func(*args, **kwargs)
                    metrics = measurer.stop_measurement()
                    measurer.save_metrics(metrics, operation_name)
                    measurer.print_report()
                    operation_logger.success(
                        "Operação {} concluída com sucesso", operation_name
                    )
                    return result
                except Exception as e:
                    operation_logger.error(
                        "Erro na execução da operação {}: {}", operation_name, e
                    )
                    measurer.stop_measurement()
                    raise
            else:
                operation_logger.warning(
                    "Sessão Spark não encontrada, executando sem medição"
                )
                return func(*args, **kwargs)

        return wrapper

    return decorator
