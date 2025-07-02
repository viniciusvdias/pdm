#!/usr/bin/env python3
"""
Script principal para execução do pipeline completo de big-data
Análise de Constrained-Off Eólico
"""

import os
import sys
import logging
from pathlib import Path
import argparse
from datetime import datetime

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_execution.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def check_dependencies():
    """Verifica se todas as dependências estão instaladas"""
    logger.info("Verificando dependências...")
    
    required_packages = [
        'pandas', 'numpy', 'matplotlib', 'seaborn', 
        'plotly', 'streamlit', 'pyarrow', 'fastparquet'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            logger.info(f"✓ {package} instalado")
        except ImportError:
            missing_packages.append(package)
            logger.error(f"✗ {package} não encontrado")
    
    if missing_packages:
        logger.error(f"Pacotes faltando: {', '.join(missing_packages)}")
        logger.error("Execute: pip install -r requirements.txt")
        return False
    
    logger.info("Todas as dependências estão instaladas!")
    return True

def check_consolidated_files():
    """Verifica se os arquivos consolidados existem"""
    logger.info("Verificando arquivos consolidados...")
    
    main_file = Path("restricoes_coff_eolicas_consolidado.csv")
    detail_file = Path("restricoes_coff_eolicas_detalhamento_consolidado.csv")
    
    if not main_file.exists():
        logger.error(f"Arquivo {main_file} não encontrado!")
        logger.error("Execute primeiro: python transform.py")
        return False
    
    logger.info(f"✓ Arquivo principal encontrado: {main_file}")
    
    if detail_file.exists():
        logger.info(f"✓ Arquivo de detalhamento encontrado: {detail_file}")
    else:
        logger.warning(f"⚠ Arquivo de detalhamento não encontrado: {detail_file}")
        logger.info("O pipeline continuará apenas com os dados principais")
    
    return True

def run_step(step_name, step_function, *args, **kwargs):
    """Executa um passo do pipeline com tratamento de erro"""
    logger.info(f"=== EXECUTANDO: {step_name} ===")
    
    start_time = datetime.now()
    
    try:
        result = step_function(*args, **kwargs)
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"✓ {step_name} concluído com sucesso em {duration}")
        return True, result
        
    except Exception as e:
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.error(f"✗ Erro em {step_name} após {duration}: {str(e)}")
        return False, None

def step1_consolidation():
    """Passo 1: Verificação dos dados consolidados"""
    logger.info("Verificando se os dados já estão consolidados...")
    
    main_file = Path("restricoes_coff_eolicas_consolidado.csv")
    if main_file.exists():
        logger.info("✓ Dados já consolidados encontrados!")
        return True
    else:
        logger.info("Executando consolidação dos dados...")
        from transform import consolidate_files
        consolidate_files()
        return True

def step2_big_data_pipeline():
    """Passo 2: Pipeline de big-data"""
    from big_data_pipeline import WindFarmBigDataPipeline
    
    pipeline = WindFarmBigDataPipeline()
    pipeline.run_complete_pipeline()
    return True

def step3_anomaly_detection():
    """Passo 3: Detecção de anomalias"""
    from anomaly_detection_analysis import AnomalyDetectionAnalysis
    
    analysis = AnomalyDetectionAnalysis()
    analysis.run_complete_analysis()
    return True

def step4_dashboard():
    """Passo 4: Dashboard (opcional)"""
    logger.info("Dashboard disponível via: streamlit run visualization_dashboard.py")
    return True

def main():
    """Função principal"""
    
    parser = argparse.ArgumentParser(description="Pipeline de Big-Data para Análise de Constrained-Off Eólico")
    parser.add_argument(
        "--steps", 
        nargs="+", 
        choices=["1", "2", "3", "4", "all"],
        default=["all"],
        help="Passos a executar (1=verificação, 2=pipeline, 3=anomalias, 4=dashboard, all=todos)"
    )
    parser.add_argument(
        "--skip-checks", 
        action="store_true",
        help="Pular verificações de dependências e dados"
    )
    
    args = parser.parse_args()
    
    logger.info("🚀 Iniciando Pipeline de Big-Data - Análise de Constrained-Off Eólico")
    logger.info(f"Passos selecionados: {args.steps}")
    
    # Verificações iniciais
    if not args.skip_checks:
        if not check_dependencies():
            logger.error("Falha na verificação de dependências. Abortando.")
            return 1
        
        if not check_consolidated_files():
            logger.error("Falha na verificação de dados consolidados. Abortando.")
            return 1
    
    # Definir passos a executar
    if "all" in args.steps:
        steps_to_run = ["1", "2", "3", "4"]
    else:
        steps_to_run = args.steps
    
    # Executar passos
    step_functions = {
        "1": ("Verificação de Dados Consolidados", step1_consolidation),
        "2": ("Pipeline de Big-Data", step2_big_data_pipeline),
        "3": ("Detecção de Anomalias", step3_anomaly_detection),
        "4": ("Dashboard", step4_dashboard)
    }
    
    successful_steps = []
    failed_steps = []
    
    for step_id in steps_to_run:
        if step_id in step_functions:
            step_name, step_func = step_functions[step_id]
            success, result = run_step(step_name, step_func)
            
            if success:
                successful_steps.append(step_id)
            else:
                failed_steps.append(step_id)
                logger.warning(f"Continuando com próximos passos...")
        else:
            logger.warning(f"Passo {step_id} não reconhecido, pulando...")
    
    # Resumo final
    logger.info("=" * 60)
    logger.info("📊 RESUMO DA EXECUÇÃO")
    logger.info("=" * 60)
    
    if successful_steps:
        logger.info(f"✅ Passos executados com sucesso: {', '.join(successful_steps)}")
    
    if failed_steps:
        logger.error(f"❌ Passos com falha: {', '.join(failed_steps)}")
    
    if not failed_steps:
        logger.info("🎉 Pipeline executado com sucesso!")
        
        # Instruções finais
        logger.info("\n📋 PRÓXIMOS PASSOS:")
        logger.info("1. Verifique os arquivos gerados em processed_data/")
        logger.info("2. Execute o dashboard: streamlit run visualization_dashboard.py")
        logger.info("3. Consulte os relatórios gerados")
        logger.info("4. Analise os logs em pipeline_execution.log")
        
        return 0
    else:
        logger.error("⚠️ Pipeline executado com falhas. Verifique os logs.")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 