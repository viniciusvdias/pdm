"""
Módulo de experimentos simples para implementar os TODOs da seção 6
"""

import json
import csv
import time
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from logging_config import get_module_logger
from config import DataPaths
import unicodedata
from spark_measure_utils import SparkMeasureWrapper


def normalize_fieldnames(data):
    """Remove acentos dos nomes dos campos de dicionários ou listas de dicionários."""
    def normalize(s):
        if not isinstance(s, str):
            return s
        return unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')
    if isinstance(data, dict):
        return {normalize(k): v for k, v in data.items()}
    elif isinstance(data, list):
        return [normalize_fieldnames(item) for item in data]
    return data


def run_basic_experiments(spark: SparkSession, df: DataFrame, dataset_size: str, periods: Optional[list] = None):
    """Executa experimentos com métricas reais"""
    logger = get_module_logger("experiments")
    logger.info("🧪 Executando experimentos com métricas REAIS")
    try:
        performance_results = run_performance_experiments_fixed(spark, df, dataset_size, logger)
        save_performance_results(performance_results, logger)
        scalability_results = run_scalability_experiments_fixed(spark, df, dataset_size, logger)
        save_scalability_results(scalability_results, logger)
        run_period_experiments(df, dataset_size, logger)
        logger.success("✅ Experimentos com métricas reais concluídos!")
    except Exception as e:
        logger.error(f"❌ Erro nos experimentos: {e}")
        raise


def run_performance_experiments_fixed(spark: SparkSession, df: DataFrame, dataset_size: str, logger):
    """Executa experimentos de performance com métricas reais"""
    # Configurações reais que podemos ajustar
    configs = [
        {"parallelism": 2, "partitions": 10},
        {"parallelism": 4, "partitions": 20},
        {"parallelism": 8, "partitions": 40},
    ]
    performance_results = []
    for config in configs:
        try:
            spark.conf.set("spark.default.parallelism", config["parallelism"])
            spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", config["partitions"])
            measurer = SparkMeasureWrapper(spark, mode="stage")
            # WORKLOAD-1: Análise de Estatísticas Básicas
            measurer.start_measurement()
            total_records = df.count()
            distinct_users = df.select("documento").distinct().count()
            metrics = measurer.stop_measurement()
            spark_metrics = metrics.get("spark_metrics", {})
            execution_time = metrics["execution_info"]["execution_time_seconds"]
            memory_used = spark_metrics.get("peakExecutionMemory", 0) / (1024**3)
            performance_results.append({
                "Workload": "WORKLOAD-1",
                "Dataset": dataset_size.capitalize(),
                "Parallelism": config["parallelism"],
                "Partitions": config["partitions"],
                "Tempo (s)": round(execution_time, 2),
                "Memoria Real (GB)": round(memory_used, 2),
                "Throughput (rec/s)": round(total_records / execution_time, 0),
                "Registros": total_records,
                "Usuarios Distintos": distinct_users
            })
            # WORKLOAD-2: Análise de Grafo (real)
            measurer.start_measurement()
            graph_result = df.groupBy("documento", "periodo_letivo").count().collect()
            metrics = measurer.stop_measurement()
            spark_metrics = metrics.get("spark_metrics", {})
            execution_time = metrics["execution_info"]["execution_time_seconds"]
            memory_used = spark_metrics.get("peakExecutionMemory", 0) / (1024**3)
            performance_results.append({
                "Workload": "WORKLOAD-2",
                "Dataset": dataset_size.capitalize(),
                "Parallelism": config["parallelism"],
                "Partitions": config["partitions"],
                "Tempo (s)": round(execution_time, 2),
                "Memoria Real (GB)": round(memory_used, 2),
                "Throughput (rec/s)": round(len(graph_result) / execution_time, 0),
                "Registros": len(graph_result),
                "Usuarios Distintos": distinct_users
            })
        except Exception as e:
            logger.warning(f"Erro na configuração {config}: {e}")
    return performance_results


def run_scalability_experiments_fixed(spark: SparkSession, df: DataFrame, dataset_size: str, logger):
    """Executa experimentos de escalabilidade com diferentes níveis de paralelismo"""
    parallelism_configs = [
        {"parallelism": 1, "partitions": 4},
        {"parallelism": 2, "partitions": 8},
        {"parallelism": 4, "partitions": 16},
        {"parallelism": 8, "partitions": 32}
    ]
    scalability_results = []
    baseline_time = None
    for config in parallelism_configs:
        try:
            spark.conf.set("spark.default.parallelism", config["parallelism"])
            df_partitioned = df.repartition(config["partitions"])
            measurer = SparkMeasureWrapper(spark, mode="stage")
            measurer.start_measurement()
            result = df_partitioned.groupBy("periodo_letivo", "tipo_usuario").agg(
                {"documento": "count", "tipo_consumo": "count"}
            ).collect()
            metrics = measurer.stop_measurement()
            execution_time = metrics["execution_info"]["execution_time_seconds"]
            spark_metrics = metrics.get("spark_metrics", {})
            if baseline_time is None:
                baseline_time = execution_time
            speedup = baseline_time / execution_time if execution_time > 0 else 0
            efficiency = (speedup / config["parallelism"]) * 100
            scalability_results.append({
                "Parallelism": config["parallelism"],
                "Partitions": config["partitions"],
                "Tempo Total (s)": round(execution_time, 2),
                "Speedup": round(speedup, 2),
                "Eficiencia (%)": round(efficiency, 1),
                "Memoria (GB)": round(spark_metrics.get("peakExecutionMemory", 0) / (1024**3), 2),
                "Stages": spark_metrics.get("numStages", 0),
                "Tasks": spark_metrics.get("numTasks", 0)
            })
        except Exception as e:
            logger.warning(f"Erro com configuração {config}: {e}")
    return scalability_results


def run_period_experiments(df: DataFrame, dataset_size: str, logger):
    """Executa experimentos por períodos letivos"""
    logger.info("📅 Executando experimentos por períodos...")
    
    # Períodos disponíveis
    available_periods = [
        ["2023/1"], ["2023/2"], ["2024/1"], ["2024/2"],
        ["2023/1", "2023/2"], ["2024/1", "2024/2"]
    ]
    
    period_results = {}
    
    for periods in available_periods:
        try:
            start_time = time.time()
            
            # Aplicar filtro de período
            from pyspark.sql.functions import col
            df_filtered = df.filter(col("periodo_letivo").isin(periods))
            total_records = df_filtered.count()
            
            execution_time = time.time() - start_time
            throughput = total_records / execution_time if execution_time > 0 else 0
            
            period_key = "_".join(periods)
            period_results[period_key] = {
                "periods": periods,
                "execution_time": execution_time,
                "throughput": throughput,
                "success": True
            }
            
        except Exception as e:
            logger.warning(f"Erro nos períodos {periods}: {e}")
            period_key = "_".join(periods)
            period_results[period_key] = {
                "periods": periods,
                "execution_time": 0,
                "throughput": 0,
                "success": False
            }
    
    # Salvar resultados
    if period_results:
        save_period_results(period_results, logger)


def save_performance_results(results: list, logger):
    """Salva resultados de performance"""
    # Normalizar campos
    results_norm = normalize_fieldnames(results)
    # Salvar em JSON
    json_file = f"{DataPaths.RESULTS_DIR}/performance_analysis.json"
    with open(json_file, 'w') as f:
        json.dump(results_norm, f, indent=2)
    # Salvar em CSV
    csv_file = f"{DataPaths.RESULTS_DIR}/performance_table.csv"
    with open(csv_file, 'w', newline='') as f:
        if results_norm:
            writer = csv.DictWriter(f, fieldnames=results_norm[0].keys())
            writer.writeheader()
            writer.writerows(results_norm)
    logger.info(f"📊 Resultados de performance salvos: {csv_file}")


def save_scalability_results(results: list, logger):
    """Salva resultados de escalabilidade"""
    # Normalizar campos
    results_norm = normalize_fieldnames(results)
    # Salvar em JSON
    json_file = f"{DataPaths.RESULTS_DIR}/scalability_analysis.json"
    with open(json_file, 'w') as f:
        json.dump(results_norm, f, indent=2)
    # Salvar em CSV
    csv_file = f"{DataPaths.RESULTS_DIR}/scalability_table.csv"
    with open(csv_file, 'w', newline='') as f:
        if results_norm:
            writer = csv.DictWriter(f, fieldnames=results_norm[0].keys())
            writer.writeheader()
            writer.writerows(results_norm)
    logger.info(f"📈 Resultados de escalabilidade salvos: {csv_file}")


def save_period_results(results: dict, logger):
    """Salva resultados por período"""
    # Normalizar campos
    results_norm = {k: normalize_fieldnames(v) for k, v in results.items()}
    # Salvar em JSON
    json_file = f"{DataPaths.RESULTS_DIR}/period_analysis.json"
    with open(json_file, 'w') as f:
        json.dump(results_norm, f, indent=2)
    # Salvar em CSV
    csv_file = f"{DataPaths.RESULTS_DIR}/period_analysis.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Periodos", "Tempo (s)", "Throughput (rec/s)", "Sucesso"])
        for period_key, data in results_norm.items():
            writer.writerow([
                ", ".join(data["periods"]),
                round(data["execution_time"], 1),
                round(data["throughput"], 0),
                "Sim" if data["success"] else "Nao"
            ])
    logger.info(f"📅 Resultados por período salvos: {csv_file}")


# Comandos CLI para experimentos detalhados
def run_detailed_experiments(spark: SparkSession, df: DataFrame, experiment_type: str, dataset_size: str, periods: Optional[list] = None):
    """
    Executa experimentos detalhados
    
    Args:
        spark: Sessão Spark
        df: DataFrame com os dados
        experiment_type: Tipo de experimento ("performance", "scalability", "periods", "all")
        dataset_size: Tamanho do dataset
        periods: Lista de períodos letivos
    """
    logger = get_module_logger("experiments")
    
    logger.info(f"🧪 Executando experimentos detalhados: {experiment_type}")
    
    try:
        if experiment_type == "performance":
            run_performance_experiments_fixed(spark, df, dataset_size, logger)
        elif experiment_type == "scalability":
            run_scalability_experiments_fixed(spark, df, dataset_size, logger)
        elif experiment_type == "periods":
            run_period_experiments(df, dataset_size, logger)
        elif experiment_type == "all":
            run_performance_experiments_fixed(spark, df, dataset_size, logger)
            run_scalability_experiments_fixed(spark, df, dataset_size, logger)
            run_period_experiments(df, dataset_size, logger)
        
        logger.success(f"✅ Experimentos detalhados ({experiment_type}) concluídos!")
        
    except Exception as e:
        logger.error(f"❌ Erro nos experimentos detalhados: {e}")
        raise 