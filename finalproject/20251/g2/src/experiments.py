"""
Módulo de experimentos para implementar workloads isolados com métricas unificadas
"""

import json
import csv
import time
import os
from datetime import datetime
from typing import Optional, Dict, List, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, desc, unix_timestamp, collect_list, struct, explode, asc, avg as spark_avg, row_number, floor, count
from pyspark.sql.window import Window
from pyspark import StorageLevel
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


class WorkloadExecutor:
    """Executor de workloads isolados com métricas unificadas"""
    
    def __init__(self, spark: SparkSession, df: DataFrame, dataset_size: str, logger):
        self.spark = spark
        self.df = df
        self.dataset_size = dataset_size
        self.logger = logger
        self.measurer = SparkMeasureWrapper(spark, mode="stage")
    
    def setup_spark_config(self, config: Dict[str, Any]):
        """Configura o Spark com parâmetros específicos"""
        try:
            for key, value in config.items():
                if key == "parallelism":
                    self.spark.conf.set("spark.default.parallelism", value)
                elif key == "partitions":
                    self.spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", value)
                elif key == "memory_per_executor":
                    self.spark.conf.set("spark.executor.memory", f"{value}g")
                elif key == "executor_cores":
                    self.spark.conf.set("spark.executor.cores", value)
                # Adicionar mais configurações conforme necessário
                
            self.logger.info(f"Configuração Spark aplicada: {config}")
        except Exception as e:
            self.logger.warning(f"Erro ao aplicar configuração Spark: {e}")
    
    def workload_basic_statistics(self) -> Dict[str, Any]:
        """WORKLOAD-1: Estatísticas básicas (baseado em get_basic_statistics)"""
        self.logger.info("Executando WORKLOAD-1: Estatísticas Básicas")
        
        self.measurer.start_measurement()
        start_time = time.time()
        
        try:
            # Executar operações do get_basic_statistics
            total_registros = self.df.count()
            total_colunas = len(self.df.columns)
            
            # Contagem por tipo de consumo
            consumo_stats = self.df.groupBy("tipo_consumo").count().orderBy(desc("count"))
            consumo_count = consumo_stats.count()
            
            # Contagem por tipo de usuário
            usuario_stats = self.df.groupBy("tipo_usuario").count().orderBy(desc("count"))
            usuario_count = usuario_stats.count()
            
            # Contagem por tipo de refeição
            refeicao_stats = self.df.groupBy("tipo_refeicao").count().orderBy(desc("count"))
            refeicao_count = refeicao_stats.count()
            
            # Coletar dados para salvar
            consumo_data = consumo_stats.collect()
            usuario_data = usuario_stats.collect()
            refeicao_data = refeicao_stats.collect()
            
            execution_time = time.time() - start_time
            metrics = self.measurer.stop_measurement()
            
            return {
                "workload": "WORKLOAD-1",
                "nome": "Estatísticas Básicas",
                "resultado": {
                    "total_registros": total_registros,
                    "total_colunas": total_colunas,
                    "tipos_consumo": consumo_count,
                    "tipos_usuario": usuario_count,
                    "tipos_refeicao": refeicao_count,
                    "consumo_data": [{"tipo": row["tipo_consumo"], "count": row["count"]} for row in consumo_data],
                    "usuario_data": [{"tipo": row["tipo_usuario"], "count": row["count"]} for row in usuario_data],
                    "refeicao_data": [{"tipo": row["tipo_refeicao"], "count": row["count"]} for row in refeicao_data]
                },
                "metricas": self._extract_metrics(metrics, execution_time, total_registros)
            }
            
        except Exception as e:
            self.logger.error(f"Erro no WORKLOAD-1: {e}")
            raise
    
    def workload_users_graph(self) -> Dict[str, Any]:
        """WORKLOAD-2: Grafo de usuários (baseado em get_users_graph_window_sliding)"""
        self.logger.info("Executando WORKLOAD-2: Grafo de Usuários")
        
        self.measurer.start_measurement()
        start_time = time.time()
        
        try:
            # Executar operações do get_users_graph_window_sliding
            df_clean = self.df.select(
                col("documento"),
                col("tipo_usuario"),
                col("nome_curso"),
                unix_timestamp(col("data_consumo"), "yyyy-MM-dd'T'HH:mm:ss").alias("timestamp")
            ).filter(
                col("documento").isNotNull() &
                col("timestamp").isNotNull() &
                (col("tipo_consumo") != "Marmita")
            )
            
            df_clean.persist(StorageLevel.MEMORY_AND_DISK)
            
            # Particionar por hora
            df_struct = df_clean.withColumn(
                "user_info",
                struct(col("documento"), col("tipo_usuario"), col("nome_curso"), col("timestamp"))
            ).withColumn("timestamp_hour", floor(col("timestamp") / 3600))
            
            # Window function
            window_spec = Window.partitionBy("timestamp_hour").orderBy("timestamp").rangeBetween(-10, 10)
            df_with_neighbors = df_struct.withColumn("neighbors", collect_list("user_info").over(window_spec))
            
            # Processar pares
            df_exploded = df_with_neighbors.select("user_info", explode("neighbors").alias("other_user"))
            
            df_pairs = df_exploded.filter(
                (col("user_info.documento") < col("other_user.documento")) &
                (col("user_info.documento") != col("other_user.documento"))
            ).select(
                col("user_info.documento").alias("user1_id"),
                col("other_user.documento").alias("user2_id"),
                col("user_info.tipo_usuario").alias("user1_tipo"),
                col("other_user.tipo_usuario").alias("user2_tipo"),
                col("user_info.nome_curso").alias("user1_curso"),
                col("other_user.nome_curso").alias("user2_curso")
            )
            
            total_pairs = df_pairs.count()
            
            connections = df_pairs.groupBy(
                "user1_id", "user2_id", "user1_tipo", "user2_tipo", "user1_curso", "user2_curso"
            ).count().withColumnRenamed("count", "weight")
            
            # Métricas do grafo
            metrics_data = connections.agg(
                spark_avg("weight").alias("peso_medio"),
                count("*").alias("total_conexoes")
            ).collect()[0]
            
            total_connections = int(metrics_data["total_conexoes"])
            avg_weight = float(metrics_data["peso_medio"]) if metrics_data["peso_medio"] else 0
            
            # Top conexões limitadas
            top_connections = connections.orderBy(desc("weight")).limit(10).collect()
            
            # Usuários únicos
            total_users = self.df.select("documento").distinct().count()
            
            execution_time = time.time() - start_time
            metrics = self.measurer.stop_measurement()
            
            # Limpeza
            df_clean.unpersist()
            
            return {
                "workload": "WORKLOAD-2",
                "nome": "Grafo de Usuários",
                "resultado": {
                    "total_usuarios": total_users,
                    "total_pares": total_pairs,
                    "total_conexoes": total_connections,
                    "peso_medio": avg_weight,
                    "top_conexoes": [
                        {
                            "usuario1": conn["user1_id"],
                            "usuario2": conn["user2_id"],
                            "peso": conn["weight"],
                            "tipo1": conn["user1_tipo"],
                            "tipo2": conn["user2_tipo"]
                        } for conn in top_connections
                    ]
                },
                "metricas": self._extract_metrics(metrics, execution_time, total_pairs),
                "connections_df": connections  # Para usar na detecção de comunidades
            }
            
        except Exception as e:
            self.logger.error(f"Erro no WORKLOAD-2: {e}")
            raise
    
    def workload_community_detection(self, connections_df: DataFrame) -> Dict[str, Any]:
        """WORKLOAD-3: Detecção de comunidades (baseado em run_community_detection)"""
        self.logger.info("Executando WORKLOAD-3: Detecção de Comunidades")
        
        self.measurer.start_measurement()
        start_time = time.time()
        
        try:
            import networkx as nx
            from networkx.algorithms import community
            
            # Filtrar conexões com pelo menos 5 ocorrências
            min_connections = 5
            edges_filtered = connections_df.filter(col("weight") >= min_connections)
            
            total_edges_before = connections_df.count()
            total_edges_after = edges_filtered.count()
            
            # Criar grafo
            G = nx.Graph()
            edges_data = edges_filtered.collect()
            
            for row in edges_data:
                G.add_edge(row["user1_id"], row["user2_id"], weight=row["weight"])
            
            # Executar algoritmo Louvain
            communities = community.louvain_communities(G, weight='weight', resolution=1.0)
            
            # Calcular modularidade
            modularity = community.modularity(G, communities, weight='weight')
            
            execution_time = time.time() - start_time
            metrics = self.measurer.stop_measurement()
            
            return {
                "workload": "WORKLOAD-3",
                "nome": "Detecção de Comunidades",
                "resultado": {
                    "num_comunidades": len(communities),
                    "modularidade": modularity,
                    "num_nodes": G.number_of_nodes(),
                    "num_edges": G.number_of_edges(),
                    "edges_before_filter": total_edges_before,
                    "edges_after_filter": total_edges_after,
                    "comunidades_info": [
                        {
                            "id": i + 1,
                            "tamanho": len(community_set),
                            "usuarios_sample": list(community_set)[:5]  # Apenas amostra
                        }
                        for i, community_set in enumerate(communities[:10])  # Top 10 comunidades
                    ]
                },
                "metricas": self._extract_metrics(metrics, execution_time, len(communities))
            }
            
        except Exception as e:
            self.logger.error(f"Erro no WORKLOAD-3: {e}")
            raise
    
    def _extract_metrics(self, spark_metrics: Dict, execution_time: float, throughput_base: int) -> Dict[str, Any]:
        """Extrai métricas unificadas"""
        metrics_data = spark_metrics.get("spark_metrics", {})
        
        return {
            "execution_time_seconds": round(execution_time, 2),
            "throughput_per_second": round(throughput_base / execution_time, 2) if execution_time > 0 else 0,
            "memory_peak_gb": round(metrics_data.get("peakExecutionMemory", 0) / (1024**3), 2),
            "stages_count": metrics_data.get("numStages", 0),
            "tasks_count": metrics_data.get("numTasks", 0),
            "shuffle_read_bytes": metrics_data.get("shuffleReadBytes", 0),
            "shuffle_write_bytes": metrics_data.get("shuffleWriteBytes", 0),
            "input_bytes": metrics_data.get("inputBytes", 0),
            "output_bytes": metrics_data.get("outputBytes", 0),
            "cpu_time_ms": metrics_data.get("executorCpuTime", 0) / 1000000,  # Convert to ms
            "gc_time_ms": metrics_data.get("jvmGCTime", 0)
        }


def run_isolated_experiments(spark: SparkSession, df: DataFrame, dataset_size: str, 
                           config: Dict[str, Any], iteration: int, logger) -> Dict[str, Any]:
    """
    Executa experimentos isolados com configuração específica
    
    Args:
        spark: Sessão Spark
        df: DataFrame com dados
        dataset_size: Tamanho do dataset
        config: Configuração do Spark
        iteration: Número da iteração
        logger: Logger
        
    Returns:
        Resultados dos experimentos
    """
    logger.info(f"🧪 Executando experimentos isolados - Iteração {iteration}")
    logger.info(f"📊 Configuração: {config}")
    
    executor = WorkloadExecutor(spark, df, dataset_size, logger)
    
    # Aplicar configuração do Spark
    executor.setup_spark_config(config)
    
    results = {
        "iteration": iteration,
        "timestamp": datetime.now().isoformat(),
        "dataset_size": dataset_size,
        "spark_config": config,
        "workloads": []
    }
    
    try:
        # WORKLOAD-1: Estatísticas básicas
        workload1_result = executor.workload_basic_statistics()
        results["workloads"].append(workload1_result)
        logger.info(f"✅ WORKLOAD-1 concluído - {workload1_result['metricas']['execution_time_seconds']}s")
        
        # WORKLOAD-2: Grafo de usuários
        workload2_result = executor.workload_users_graph()
        connections_df = workload2_result.pop("connections_df", None)  # Remover do resultado para serialização
        results["workloads"].append(workload2_result)
        logger.info(f"✅ WORKLOAD-2 concluído - {workload2_result['metricas']['execution_time_seconds']}s")
        
        # WORKLOAD-3: Detecção de comunidades (se tiver conexões)
        if connections_df is not None:
            workload3_result = executor.workload_community_detection(connections_df)
            results["workloads"].append(workload3_result)
            logger.info(f"✅ WORKLOAD-3 concluído - {workload3_result['metricas']['execution_time_seconds']}s")
        
        # Calcular métricas consolidadas
        total_execution_time = sum(w["metricas"]["execution_time_seconds"] for w in results["workloads"])
        total_memory_peak = max(w["metricas"]["memory_peak_gb"] for w in results["workloads"])
        
        results["summary"] = {
            "total_execution_time_seconds": round(total_execution_time, 2),
            "peak_memory_gb": total_memory_peak,
            "workloads_completed": len(results["workloads"]),
            "config_parallelism": config.get("parallelism", 1),
            "config_partitions": config.get("partitions", 1)
        }
        
        logger.success(f"🎯 Iteração {iteration} concluída em {total_execution_time:.2f}s")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Erro na iteração {iteration}: {e}")
        results["error"] = str(e)
        return results


def save_experiment_results(results: Dict[str, Any], output_file: str, logger):
    """Salva resultados de experimento em arquivo"""
    try:
        # Normalizar campos
        results_norm = normalize_fieldnames(results)
        
        # Salvar JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results_norm, f, indent=2, ensure_ascii=False)
        
        logger.success(f"📊 Resultados salvos: {output_file}")
        
        # Salvar também em CSV para análise rápida
        csv_file = output_file.replace('.json', '_summary.csv')
        save_summary_csv(results_norm, csv_file, logger)
        
    except Exception as e:
        logger.error(f"Erro ao salvar resultados: {e}")
        raise


def save_summary_csv(results: Dict[str, Any], csv_file: str, logger):
    """Salva resumo dos resultados em CSV"""
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Cabeçalho
            writer.writerow([
                'Iteracao', 'Dataset', 'Parallelism', 'Partitions', 'Memory_Executor_GB', 'Executor_Cores',
                'Workload', 'Nome', 'Tempo_s', 'Throughput', 'Memoria_GB',
                'Stages', 'Tasks', 'CPU_ms', 'GC_ms'
            ])
            
            # Dados
            for workload in results.get("workloads", []):
                writer.writerow([
                    results["iteration"],
                    results["dataset_size"],
                    results["spark_config"].get("parallelism", 1),
                    results["spark_config"].get("partitions", 1),
                    results["spark_config"].get("memory_per_executor", 1),
                    results["spark_config"].get("executor_cores", 1),
                    workload["workload"],
                    workload["nome"],
                    workload["metricas"]["execution_time_seconds"],
                    workload["metricas"]["throughput_per_second"],
                    workload["metricas"]["memory_peak_gb"],
                    workload["metricas"]["stages_count"],
                    workload["metricas"]["tasks_count"],
                    workload["metricas"]["cpu_time_ms"],
                    workload["metricas"]["gc_time_ms"]
                ])
        
        logger.info(f"📈 Resumo CSV salvo: {csv_file}")
        
    except Exception as e:
        logger.error(f"Erro ao salvar CSV: {e}")


def get_experiment_configurations() -> List[Dict[str, Any]]:
    """Retorna configurações de teste para experimentos"""
    return [
        {"parallelism": 2, "partitions": 8, "memory_per_executor": 2, "executor_cores": 2},
        {"parallelism": 4, "partitions": 16, "memory_per_executor": 2, "executor_cores": 2},
        {"parallelism": 2, "partitions": 8, "memory_per_executor": 4, "executor_cores": 4},
        {"parallelism": 4, "partitions": 16, "memory_per_executor": 4, "executor_cores": 4},
        {"parallelism": 2, "partitions": 8, "memory_per_executor": 6, "executor_cores": 6},
        {"parallelism": 4, "partitions": 16, "memory_per_executor": 6, "executor_cores": 6}
    ]


def run_experiment_suite(spark: SparkSession, df: DataFrame, dataset_size: str, 
                        num_iterations: int = 3, logger=None):
    """
    Executa suite completa de experimentos
    
    Args:
        spark: Sessão Spark
        df: DataFrame com dados
        dataset_size: Tamanho do dataset
        num_iterations: Número de iterações por configuração
        logger: Logger
    """
    if logger is None:
        logger = get_module_logger("experiments")
    
    logger.info(f"🚀 Iniciando suite de experimentos: {num_iterations} iterações por configuração")
    
    configs = get_experiment_configurations()
    logger.info(f"📋 Configurações a testar: {len(configs)}")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for config_idx, config in enumerate(configs, 1):
        logger.info(f"🔧 Configuração {config_idx}/{len(configs)}: {config}")
        
        for iteration in range(1, num_iterations + 1):
            try:
                # Executar experimento isolado
                results = run_isolated_experiments(spark, df, dataset_size, config, iteration, logger)
                
                # Gerar nome do arquivo
                config_name = f"p{config['parallelism']}_par{config['partitions']}_mem{config['memory_per_executor']}g_cores{config['executor_cores']}"
                output_file = f"{DataPaths.RESULTS_DIR}/experiment_{dataset_size}_{config_name}_iter{iteration}_{timestamp}.json"
                
                # Salvar resultados
                save_experiment_results(results, output_file, logger)
                
                # Pausa entre iterações
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Erro na configuração {config}, iteração {iteration}: {e}")
                continue
    
    logger.success(f"🎯 Suite de experimentos concluída! Resultados em: {DataPaths.RESULTS_DIR}") 