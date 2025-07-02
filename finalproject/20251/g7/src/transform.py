import os
import pandas as pd
import glob
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def consolidate_files():
    """
    Consolida arquivos CSV da pasta data_raw seguindo o padrão:
    - RESTRICAO_COFF_EOLICA_YYYY_MM.csv (principal)
    - RESTRICAO_COFF_EOLICA_DETAIL_YYYY_MM.csv (detalhamento)
    """
    
    # Caminho para a pasta data_raw
    data_raw_path = Path("data_raw")
    
    if not data_raw_path.exists():
        logger.error(f"Pasta {data_raw_path} não encontrada!")
        return
    
    # Listar todos os arquivos que seguem os padrões
    main_files = []
    detail_files = []
    
    # Buscar em todas as subpastas
    for subfolder in data_raw_path.iterdir():
        if subfolder.is_dir():
            # Buscar arquivos principais (NÃO devem conter 'DETAIL')
            for f in subfolder.glob("RESTRICAO_COFF_EOLICA_*.csv"):
                if "DETAIL" not in f.name:
                    main_files.append(str(f))
            # Buscar arquivos de detalhamento (DEVEM conter 'DETAIL')
            for f in subfolder.glob("RESTRICAO_COFF_EOLICA_DETAIL_*.csv"):
                if "DETAIL" in f.name:
                    detail_files.append(str(f))
    
    logger.info(f"Encontrados {len(main_files)} arquivos principais")
    logger.info(f"Encontrados {len(detail_files)} arquivos de detalhamento")
    
    # Ordenar arquivos por nome para garantir ordem cronológica
    main_files.sort()
    detail_files.sort()
    
    # Consolidar arquivos principais
    if main_files:
        logger.info("Consolidando arquivos principais...")
        consolidate_csv_files(main_files, "restricoes_coff_eolicas_consolidado.csv")
    
    # Consolidar arquivos de detalhamento
    if detail_files:
        logger.info("Consolidando arquivos de detalhamento...")
        consolidate_csv_files(detail_files, "restricoes_coff_eolicas_detalhamento_consolidado.csv")

def consolidate_csv_files(file_list, output_filename):
    """
    Consolida uma lista de arquivos CSV em um único arquivo
    
    Args:
        file_list (list): Lista de caminhos para arquivos CSV
        output_filename (str): Nome do arquivo de saída consolidado
    """
    
    consolidated_data = []
    
    for i, file_path in enumerate(file_list):
        try:
            logger.info(f"Processando arquivo {i+1}/{len(file_list)}: {file_path}")
            
            # Ler o arquivo CSV com separador correto
            df = pd.read_csv(file_path, sep=';', low_memory=False)
            
            consolidated_data.append(df)
            
            logger.info(f"Arquivo {file_path} processado com sucesso. Linhas: {len(df)}")
            
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {file_path}: {str(e)}")
            continue
    
    if consolidated_data:
        # Concatenar todos os DataFrames
        final_df = pd.concat(consolidated_data, ignore_index=True)
        
        # Salvar arquivo consolidado com separador correto
        final_df.to_csv(output_filename, sep=';', index=False)
        
        logger.info(f"Arquivo consolidado salvo: {output_filename}")
        logger.info(f"Total de linhas no arquivo consolidado: {len(final_df)}")
        logger.info(f"Colunas: {list(final_df.columns)}")
        
        # Mostrar algumas estatísticas
        logger.info(f"Tamanho do arquivo: {os.path.getsize(output_filename) / (1024*1024):.2f} MB")
        
    else:
        logger.warning("Nenhum arquivo foi processado com sucesso!")

def main():
    """
    Função principal que executa a consolidação
    """
    logger.info("Iniciando processo de consolidação de arquivos...")
    
    try:
        consolidate_files()
        logger.info("Processo de consolidação concluído com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro durante a consolidação: {str(e)}")

if __name__ == "__main__":
    main()
