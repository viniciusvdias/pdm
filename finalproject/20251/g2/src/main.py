"""
Aplicação principal para análise de dados do RU-UFLA
"""

import click
import sys
import os
from pathlib import Path
from typing import Optional

# Adicionar src ao path para imports
sys.path.append(str(Path(__file__).parent))

from config import SparkConfig, DataPaths
from logging_config import setup_logging, get_module_logger
from data_analysis import RUAnalyzer

@click.group()
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    default="INFO",
    help="Nível de logging",
)
@click.option(
    "--log-to-file/--no-log-to-file",
    default=True,
    help="Se deve salvar logs em arquivo",
)
@click.option(
    "--log-colors/--no-log-colors", default=True, help="Se deve usar cores nos logs"
)
@click.pass_context
def cli(ctx, log_level, log_to_file, log_colors):
    """RU-UFLA Analytics: Análise de dados do Restaurante Universitário da UFLA usando PySpark"""

    # Garantir que temos os diretórios necessários
    DataPaths.ensure_directories()

    # Configurar logging
    setup_logging(
        log_level=log_level, log_to_file=log_to_file, enable_colors=log_colors
    )

    # Contexto compartilhado
    ctx.ensure_object(dict)
    ctx.obj["log_level"] = log_level


@cli.command()
@click.option(
    "--master-url",
    help="URL do Spark master (ex: spark://spark-master:7077). Se não especificado, usa configuração padrão",
)
@click.option("--app-name", default="RU-UFLA-Analytics", help="Nome da aplicação Spark")
@click.option(
    "--mode",
    type=click.Choice(["sample", "complete"]),
    default="complete",
    help="Modo de análise: 'sample' para dados de amostra ou 'complete' para dataset completo",
)
@click.option(
    "--periods",
    help="Lista de períodos letivos separados por vírgula (formato: YYYY/S). Ex: 2024/1,2024/2",
)
@click.pass_context
def analyze(ctx, master_url: Optional[str], app_name: str, mode: str, periods: Optional[str]):
    """Executa análise dos dados do RU-UFLA"""

    logger = get_module_logger("main")
    
    # Processar lista de períodos
    periods_list = None
    if periods:
        import re
        
        # Separar períodos por vírgula e limpar espaços
        periods_list = [p.strip() for p in periods.split(',') if p.strip()]
        
        # Padrão para validar formato YYYY/S
        period_pattern = r'^\d{4}/[12]$'
        
        # Validar cada período
        for period in periods_list:
            if not re.match(period_pattern, period):
                raise click.BadParameter(f"Período inválido: {period}. Use formato YYYY/S (ex: 2024/1)")
        
        logger.info(f"Períodos definidos: {', '.join(periods_list)}")
    else:
        logger.info("Nenhum período específico. Processando todos os dados.")
    
    if mode == "sample":
        logger.info("Iniciando análise com dados de AMOSTRA do RU-UFLA")
        data_file = DataPaths.RU_DATA_SAMPLE
    else:
        logger.info("Iniciando análise COMPLETA dos dados do RU-UFLA")
        data_file = DataPaths.RU_DATA_COMPLETE

    try:
        # Configurar Spark baseado no ambiente
        if master_url:
            os.environ["SPARK_MASTER_URL"] = master_url
            logger.info(f"Usando Spark master: {master_url}")

        # Criar sessão Spark
        spark = SparkConfig.get_spark_session(app_name)

        # Verificar se precisa fazer download para análise completa
        if mode == "complete" and not os.path.exists(data_file):
            logger.info("Dataset completo não encontrado. Fazendo download automático...")
            analyzer = RUAnalyzer(spark)
            analyzer.download_complete_dataset()
            
            # Verificar se o download foi bem-sucedido
            if not os.path.exists(data_file):
                raise FileNotFoundError(f"Falha no download. Dataset não encontrado em: {data_file}")

        # Executar análise
        analyzer = RUAnalyzer(spark)
        analyzer.run_complete_analysis(data_file, periods=periods_list)

        logger.success(f"Análise ({mode}) concluída com sucesso!")

    except Exception as e:
        logger.error(f"Erro durante a análise: {e}")
        raise click.ClickException(f"Falha na análise: {e}")
    finally:
        if "spark" in locals():
            spark.stop()

@cli.command()
@click.option("--master-url", help="URL do Spark master para teste de conectividade")
@click.option(
    "--app-name",
    default="RU-UFLA-Test-Connection",
    help="Nome da aplicação Spark para teste",
)
@click.pass_context
def test_spark(ctx, master_url: Optional[str], app_name: str):
    """Testa a conectividade com o cluster Spark"""

    logger = get_module_logger("test")
    logger.info("Testando conectividade com Spark")

    try:
        if master_url:
            os.environ["SPARK_MASTER_URL"] = master_url
            logger.info(f"Testando conexão com: {master_url}")

        spark = SparkConfig.get_spark_session(app_name)

        # Teste simples
        test_data = spark.range(1000, numPartitions=4)
        count = test_data.count()

        logger.success(f"✅ Conexão com Spark OK - Processou {count:,} registros")
        logger.info(f"Versão do Spark: {spark.version}")
        logger.info(f"Master URL: {spark.conf.get('spark.master')}")
        logger.info(f"Paralelismo: {spark.sparkContext.defaultParallelism} cores")

    except Exception as e:
        logger.error(f"❌ Erro de conectividade: {e}")
        raise click.ClickException(f"Falha na conexão: {e}")
    finally:
        if "spark" in locals():
            spark.stop()

@cli.command()
@click.option(
    "--format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Formato de saída das informações",
)
def info(format):
    """Mostra informações sobre o ambiente e configurações"""

    import platform
    import pyspark
    from datetime import datetime

    info_data = {
        "timestamp": datetime.now().isoformat(),
        "python_version": platform.python_version(),
        "pyspark_version": pyspark.__version__,
        "platform": platform.platform(),
        "spark_home": os.environ.get("SPARK_HOME", "Não definido"),
        "java_home": os.environ.get("JAVA_HOME", "Não definido"),
        "data_paths": {
            "base_dir": DataPaths.BASE_DIR,
            "data_sample": DataPaths.RU_DATA_SAMPLE,
            "results_dir": DataPaths.RESULTS_DIR,
            "metrics_dir": DataPaths.METRICS_DIR,
            "logs_dir": DataPaths.LOGS_DIR,
        },
    }

    if format == "json":
        import json

        click.echo(json.dumps(info_data, indent=2, ensure_ascii=False))
    else:
        click.echo("🔧 RU-UFLA Analytics - Informações do Sistema")
        click.echo("=" * 50)
        click.echo(f"Timestamp: {info_data['timestamp']}")
        click.echo(f"Python: {info_data['python_version']}")
        click.echo(f"PySpark: {info_data['pyspark_version']}")
        click.echo(f"Plataforma: {info_data['platform']}")
        click.echo(f"SPARK_HOME: {info_data['spark_home']}")
        click.echo(f"JAVA_HOME: {info_data['java_home']}")
        click.echo()
        click.echo("📁 Caminhos de Dados:")
        for key, value in info_data["data_paths"].items():
            click.echo(f"  {key}: {value}")


@cli.command()
@click.option(
    "--master-url",
    help="URL do Spark master (ex: spark://spark-master:7077). Se não especificado, usa configuração padrão",
)
@click.option("--app-name", default="RU-UFLA-Experiments", help="Nome da aplicação Spark")
@click.option(
    "--mode",
    type=click.Choice(["sample", "complete"]),
    default="complete",
    help="Modo de experimento: 'sample' para dados de amostra ou 'complete' para dataset completo",
)
@click.option(
    "--periods",
    help="Lista de períodos letivos separados por vírgula (formato: YYYY/S). Ex: 2024/1,2024/2",
)
@click.option(
    "--iterations",
    type=int,
    default=3,
    help="Número de iterações por configuração de experimento",
)
@click.pass_context
def experiments(ctx, master_url: Optional[str], app_name: str, mode: str, periods: Optional[str], iterations: int):
    """Executa suite completa de experimentos isolados com múltiplas configurações"""

    logger = get_module_logger("experiments")
    
    # Processar lista de períodos
    periods_list = None
    if periods:
        import re
        
        # Separar períodos por vírgula e limpar espaços
        periods_list = [p.strip() for p in periods.split(',') if p.strip()]
        
        # Padrão para validar formato YYYY/S
        period_pattern = r'^\d{4}/[12]$'
        
        # Validar cada período
        for period in periods_list:
            if not re.match(period_pattern, period):
                raise click.BadParameter(f"Período inválido: {period}. Use formato YYYY/S (ex: 2024/1)")
        
        logger.info(f"Períodos definidos: {', '.join(periods_list)}")
    else:
        logger.info("Nenhum período específico. Processando todos os dados.")
    
    if mode == "sample":
        logger.info(f"Iniciando suite de experimentos com dados de AMOSTRA ({iterations} iterações)")
        data_file = DataPaths.RU_DATA_SAMPLE
    else:
        logger.info(f"Iniciando suite de experimentos com dados COMPLETOS ({iterations} iterações)")
        data_file = DataPaths.RU_DATA_COMPLETE

    try:
        # Configurar Spark baseado no ambiente
        if master_url:
            os.environ["SPARK_MASTER_URL"] = master_url
            logger.info(f"Usando Spark master: {master_url}")

        # Criar sessão Spark
        spark = SparkConfig.get_spark_session(app_name)

        # Verificar se precisa fazer download para análise completa
        if mode == "complete" and not os.path.exists(data_file):
            logger.info("Dataset completo não encontrado. Fazendo download automático...")
            analyzer = RUAnalyzer(spark)
            analyzer.download_complete_dataset()
            
            # Verificar se o download foi bem-sucedido
            if not os.path.exists(data_file):
                raise FileNotFoundError(f"Falha no download. Dataset não encontrado em: {data_file}")

        # Executar experimentos
        analyzer = RUAnalyzer(spark)
        
        # Carregar dados primeiro
        analyzer.load_data(data_file)
        if periods_list:
            analyzer.apply_period_filter(periods_list)
        
        # Executar suite de experimentos usando o módulo experiments
        from experiments import run_experiment_suite
        run_experiment_suite(spark, analyzer.df, mode, iterations, logger)

        logger.success(f"Suite de experimentos ({iterations} iterações) concluída com sucesso!")

    except Exception as e:
        logger.error(f"Erro durante os experimentos: {e}")
        raise click.ClickException(f"Falha nos experimentos: {e}")
    finally:
        if "spark" in locals():
            spark.stop()

@cli.command()
@click.option(
    "--pattern",
    default="experiment_*",
    help="Padrão para buscar arquivos de resultados (ex: experiment_complete_*)",
)
@click.option(
    "--output-format",
    type=click.Choice(["json", "csv", "both"]),
    default="both",
    help="Formato de saída da consolidação",
)
@click.pass_context
def consolidate(ctx, pattern: str, output_format: str):
    """Consolida múltiplos resultados de experimentos em um relatório único"""

    logger = get_module_logger("consolidate")
    
    import glob
    import json
    import csv
    from datetime import datetime
    
    logger.info(f"Consolidando resultados com padrão: {pattern}")
    
    # Buscar arquivos de resultados
    search_path = f"{DataPaths.RESULTS_DIR}/{pattern}.json"
    result_files = glob.glob(search_path)
    
    if not result_files:
        logger.warning(f"Nenhum arquivo encontrado com padrão: {search_path}")
        return
    
    logger.info(f"Encontrados {len(result_files)} arquivos de resultados")
    
    # Consolidar resultados
    consolidated_data = {
        "timestamp": datetime.now().isoformat(),
        "total_files": len(result_files),
        "pattern": pattern,
        "experiments": []
    }
    
    all_workloads = []
    
    for file_path in sorted(result_files):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            consolidated_data["experiments"].append({
                "file": file_path.split('/')[-1],
                "summary": data.get("summary", {}),
                "config": data.get("spark_config", {}),
                "workloads_count": len(data.get("workloads", []))
            })
            
            # Adicionar workloads individuais para análise
            for workload in data.get("workloads", []):
                workload_entry = {
                    "file": file_path.split('/')[-1],
                    "iteration": data.get("iteration", 0),
                    "dataset": data.get("dataset_size", "unknown"),
                    "config_parallelism": data.get("spark_config", {}).get("parallelism", 1),
                    "config_partitions": data.get("spark_config", {}).get("partitions", 1),
                    **workload
                }
                all_workloads.append(workload_entry)
                
            logger.info(f"Processado: {file_path.split('/')[-1]}")
            
        except Exception as e:
            logger.warning(f"Erro ao processar {file_path}: {e}")
    
    # Salvar consolidação
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if output_format in ["json", "both"]:
        json_file = f"{DataPaths.RESULTS_DIR}/consolidated_results_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(consolidated_data, f, indent=2, ensure_ascii=False)
        logger.success(f"Consolidação JSON salva: {json_file}")
    
    if output_format in ["csv", "both"]:
        csv_file = f"{DataPaths.RESULTS_DIR}/consolidated_workloads_{timestamp}.csv"
        
        if all_workloads:
            # Extrair todas as chaves possíveis
            all_keys = set()
            for workload in all_workloads:
                all_keys.update(workload.keys())
                if 'metricas' in workload:
                    for metric_key in workload['metricas'].keys():
                        all_keys.add(f"metrics_{metric_key}")
            
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Cabeçalho
                header = [
                    'File', 'Iteration', 'Dataset', 'Config_Parallelism', 'Config_Partitions',
                    'Workload', 'Nome', 'Execution_Time_s', 'Throughput', 'Memory_GB',
                    'Stages', 'Tasks', 'CPU_ms', 'GC_ms', 'Shuffle_Read_Bytes', 'Shuffle_Write_Bytes'
                ]
                writer.writerow(header)
                
                # Dados
                for workload in all_workloads:
                    metrics = workload.get('metricas', {})
                    writer.writerow([
                        workload.get('file', ''),
                        workload.get('iteration', 0),
                        workload.get('dataset', ''),
                        workload.get('config_parallelism', 1),
                        workload.get('config_partitions', 1),
                        workload.get('workload', ''),
                        workload.get('nome', ''),
                        metrics.get('execution_time_seconds', 0),
                        metrics.get('throughput_per_second', 0),
                        metrics.get('memory_peak_gb', 0),
                        metrics.get('stages_count', 0),
                        metrics.get('tasks_count', 0),
                        metrics.get('cpu_time_ms', 0),
                        metrics.get('gc_time_ms', 0),
                        metrics.get('shuffle_read_bytes', 0),
                        metrics.get('shuffle_write_bytes', 0)
                    ])
        
        logger.success(f"Consolidação CSV salva: {csv_file}")
    
    # Estatísticas resumidas
    logger.info("=== ESTATÍSTICAS CONSOLIDADAS ===")
    logger.info(f"Total de arquivos processados: {len(result_files)}")
    logger.info(f"Total de workloads: {len(all_workloads)}")
    
    if all_workloads:
        # Estatísticas por workload
        workload_stats = {}
        for workload in all_workloads:
            wl_name = workload.get('workload', 'unknown')
            if wl_name not in workload_stats:
                workload_stats[wl_name] = []
            workload_stats[wl_name].append(workload.get('metricas', {}).get('execution_time_seconds', 0))
        
        for wl_name, times in workload_stats.items():
            avg_time = sum(times) / len(times) if times else 0
            logger.info(f"{wl_name}: {len(times)} execuções, tempo médio: {avg_time:.2f}s")
    
    logger.success("Consolidação concluída com sucesso!")


if __name__ == "__main__":
    cli()
