"""
Módulo de análise de dados do RU-UFLA usando PySpark
"""

import os
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, desc
from logging_config import get_module_logger
from spark_measure_utils import measure_spark_operation
from config import DataPaths

class RUAnalyzer:
    """Analisador de dados do Restaurante Universitário da UFLA"""
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o analisador com uma sessão Spark
        
        Args:
            spark: Sessão Spark configurada
        """
        self.spark = spark
        self.logger = get_module_logger("ru_analyzer")
        self.df: Optional[DataFrame] = None
        
        # Garantir que os diretórios necessários existam
        DataPaths.ensure_directories()
        
    def load_data(self, file_path: str) -> DataFrame:
        """
        Carrega os dados do arquivo JSON
        
        Args:
            file_path: Caminho para o arquivo de dados
            
        Returns:
            DataFrame com os dados carregados
        """
        self.logger.info(f"Carregando dados de: {file_path}")
        
        try:
            # Verificar se o arquivo existe
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
            
            # Carregar dados JSON
            self.df = self.spark.read.option("multiline", "true").json(file_path)
            
            self.logger.success(f"Dados carregados com sucesso de {file_path}")
            return self.df
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar dados: {e}")
            raise
    
    def apply_period_filter(self, periods: Optional[list] = None):
        """
        Aplica filtro de períodos letivos no DataFrame
        
        Args:
            periods: Lista de períodos letivos (formato: YYYY/S, ex: ['2024/1', '2024/2'])
                    Se None, processa todos os períodos
        """
        if self.df is None:
            raise ValueError("Dados não carregados. Chame load_data() primeiro.")
        
        if periods is None or len(periods) == 0:
            self.logger.info("Nenhum filtro de período especificado. Processando todos os dados.")
            return
        
        self.logger.info(f"Aplicando filtro de períodos letivos: {periods}")
        
        try:
            from pyspark.sql.functions import col
            
            # Registrar contagem antes do filtro
            count_before = self.df.count()
            
            # Aplicar filtro com lista de períodos
            df_filtered = self.df.filter(col("periodo_letivo").isin(periods))
            
            # Atualizar DataFrame
            self.df = df_filtered
            
            # Registrar contagem após o filtro
            count_after = self.df.count()
            
            self.logger.success(f"Filtro aplicado com sucesso!")
            self.logger.info(f"Períodos selecionados: {', '.join(periods)}")
            self.logger.info(f"Registros antes do filtro: {count_before:,}")
            self.logger.info(f"Registros após o filtro: {count_after:,}")
            self.logger.info(f"Registros filtrados: {count_before - count_after:,}")
            
        except Exception as e:
            self.logger.error(f"Erro ao aplicar filtro de períodos letivos: {e}")
            raise
    
    def count_records(self) -> int:
        """
        Conta o número total de registros no dataset
        
        Returns:
            Número de registros
        """
        if self.df is None:
            raise ValueError("Dados não carregados. Chame load_data() primeiro.")
        
        self.logger.info("Contando registros do dataset...")
        
        try:
            record_count = self.df.count()
            self.logger.success(f"Total de registros encontrados: {record_count:,}")
            return record_count
            
        except Exception as e:
            self.logger.error(f"Erro ao contar registros: {e}")
            raise
        
    def _run_community_detection(self, edges_df: DataFrame, vertices_df: DataFrame):
        self.logger.info("Executando detecção de comunidades usando NetworkX...")
        
        try:
            import networkx as nx
            from networkx.algorithms import community
            import json
            
            # Converter para NetworkX
            edges_list = edges_df.select("user1_id", "user2_id", "weight").collect()
            
            G = nx.Graph()
            for row in edges_list:
                G.add_edge(row["user1_id"], row["user2_id"], weight=row["weight"])
            
            self.logger.info(f"Grafo criado: {G.number_of_nodes()} nós, {G.number_of_edges()} arestas")
            
            # Executar algoritmo Louvain
            self.logger.info("Executando algoritmo Louvain...")
            communities = community.louvain_communities(G, weight='weight', resolution=2.0)
            
            # Salvar resultados
            output_path = f"{DataPaths.RESULTS_DIR}"
            os.makedirs(output_path, exist_ok=True)
            
            # Calcular modularidade
            modularity = community.modularity(G, communities, weight='weight')
            
            # Salvar métricas
            with open(f"{output_path}/metrics_louvain.json", 'w') as f:
                json.dump({
                    "modularity": modularity,
                    "num_communities": len(communities),
                    "num_nodes": G.number_of_nodes(),
                    "num_edges": G.number_of_edges()
                }, f, indent=2)
            
            self.logger.success(f"✅ Detecção de comunidades concluída! Modularidade: {modularity:.4f}")
            return communities
            
        except Exception as e:
            self.logger.error(f"Erro na implementação alternativa: {e}")
            raise

        

    def get_users_graph_window_sliding(self) -> dict:
        """
        Calcula o gráfico de usuários com janela deslizante de 30 segundos utilizando Window Function
        """
        if self.df is None:
            raise ValueError("Dados não carregados. Chame load_data() primeiro.")

        self.logger.info("Calculando grafo de usuários com janela deslizante via Window Function...")

        try:
            from pyspark.sql.functions import (
                col, unix_timestamp, collect_list, struct,
                explode, asc, desc, avg as spark_avg, row_number, floor
            )
            from pyspark.sql.window import Window

            # Converter e filtrar dados essenciais
            df_clean = self.df.select(
                col("documento"),
                col("tipo_usuario"),
                col("tipo_refeicao"),
                col("tipo_consumo"),
                col("nome_curso"),
                unix_timestamp(col("data_consumo"), "yyyy-MM-dd'T'HH:mm:ss").alias("timestamp")
            ).filter(
                col("documento").isNotNull() &
                col("timestamp").isNotNull() &
                (col("tipo_consumo") != "Marmita")
            ).cache()

            self.logger.info(f"Total de registros após limpeza: {df_clean.count()}")

            # Criar uma struct para guardar os dados dos usuários
            df_struct = df_clean.withColumn(
                "user_info",
                struct(
                    col("documento"),
                    col("tipo_usuario"),
                    col("nome_curso"),
                    col("timestamp")
                )
            )

            # Definir janela de ±10 segundos ordenada por timestamp
            df_struct = df_struct.withColumn("timestamp_day", floor(col("timestamp") / 86400))

            window_spec = Window.partitionBy("timestamp_day").orderBy("timestamp").rangeBetween(-10, 10)

            # Coletar todos os usuários dentro da janela
            df_with_neighbors = df_struct.withColumn("neighbors", collect_list("user_info").over(window_spec))

            df_exploded = df_with_neighbors.select("user_info", explode("neighbors").alias("other_user"))

            # Filtrar pares distintos (user1 < user2) e diferentes
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

            self.logger.info(f"Total de pares brutos com Window: {df_pairs.count()}")

            # Contar frequência de conexões
            connections = df_pairs.groupBy(
                "user1_id", "user2_id", "user1_tipo", "user2_tipo", "user1_curso", "user2_curso"
            ).count().withColumnRenamed("count", "weight")

            total_connections = connections.count()
            self.logger.info(f"Total de conexões: {total_connections}")

            # Usuários únicos
            users_df = df_clean.select("documento", "tipo_usuario", "nome_curso").distinct()
            total_users = users_df.count()

            # Top conexões
            top_connections = connections.orderBy(desc("weight")).limit(20).collect()

            # Top usuários mais conectados
            user_degrees = connections.select("user1_id").union(connections.select("user2_id")).groupBy("user1_id").count().withColumnRenamed("user1_id", "user_id").withColumnRenamed("count", "degree")
            top_users = user_degrees.join(users_df, user_degrees["user_id"] == users_df["documento"]).orderBy(desc("degree")).limit(10).collect()

            communities = self._run_community_detection(connections, users_df)

            peso_medio_result = connections.agg(spark_avg("weight")).collect()[0][0]
            peso_medio = float(peso_medio_result) if peso_medio_result else 0

            results = {
                "estatisticas_grafo": {
                    "total_usuarios": total_users,
                    "total_conexoes": total_connections,
                    "total_comunidades": len(communities),
                    "peso_medio_conexoes": peso_medio
                },
                "top_conexoes": [
                    {
                        "usuario1": conn["user1_id"],
                        "usuario2": conn["user2_id"],
                        "frequencia": conn["weight"],
                        "tipo_usuario1": conn["user1_tipo"],
                        "tipo_usuario2": conn["user2_tipo"],
                        "curso1": conn["user1_curso"],
                        "curso2": conn["user2_curso"]
                    }
                    for conn in top_connections
                ],
                "usuarios_mais_conectados": [
                    {
                        "usuario": user["documento"],
                        "tipo_usuario": user["tipo_usuario"],
                        "curso": user["nome_curso"],
                        "conexoes": user["degree"]
                    }
                    for user in top_users
                ],
                "comunidades": [
                    {
                        "id": i + 1,
                        "tamanho": len(community),
                        "usuarios": list(community)[:10]
                    }
                    for i, community in enumerate(communities[:20])
                ],
            }

            df_clean.unpersist()
            self.logger.success("Análise com Window Function concluída com sucesso!")
            return results

        except Exception as e:
            self.logger.error(f"Erro ao calcular grafo de usuários com Window: {e}")
            raise

    def get_basic_statistics(self) -> dict:
        """
        Calcula estatísticas básicas do dataset
        
        Returns:
            Dicionário com estatísticas básicas
        """
        if self.df is None:
            raise ValueError("Dados não carregados. Chame load_data() primeiro.")
        
        self.logger.info("Calculando estatísticas básicas...")
        
        try:
            stats = {}
            
            # Contagem total
            stats['total_registros'] = self.df.count()
            
            # Número de colunas
            stats['total_colunas'] = len(self.df.columns)
            
            # Contagem por tipo de consumo
            consumo_stats = self.df.groupBy("tipo_consumo").count().orderBy(desc("count"))
            stats['consumo_por_tipo'] = consumo_stats.collect()
            
            # Contagem por tipo de usuário
            usuario_stats = self.df.groupBy("tipo_usuario").count().orderBy(desc("count"))
            stats['usuarios_por_tipo'] = usuario_stats.collect()
            
            # Contagem por tipo de refeição
            refeicao_stats = self.df.groupBy("tipo_refeicao").count().orderBy(desc("count"))
            stats['refeicoes_por_tipo'] = refeicao_stats.collect()
            
            self.logger.success("Estatísticas calculadas com sucesso")
            return stats
            
        except Exception as e:
            self.logger.error(f"Erro ao calcular estatísticas: {e}")
            raise
    
    def save_results(self, stats: dict, graph_stats: dict, output_dir: str = None):
        """
        Salva os resultados da análise
        
        Args:
            stats: Estatísticas para salvar
            output_dir: Diretório de saída (usa DataPaths.RESULTS_DIR se não especificado)
        """
        try:
            # Usar diretório padrão se não especificado
            if output_dir is None:
                output_dir = DataPaths.RESULTS_DIR
            
            # Garantir que o diretório existe
            os.makedirs(output_dir, exist_ok=True)
            from datetime import datetime
            
            # Salvar contagem de registros em arquivo texto
            count_file = os.path.join(output_dir, "contagem_registros.txt")
            with open(count_file, 'w', encoding='utf-8') as f:
                f.write(f"Total de registros no dataset: {stats['total_registros']:,}\n")
                f.write(f"Total de colunas: {stats['total_colunas']}\n\n")
                
                f.write("=== ESTATÍSTICAS POR TIPO DE CONSUMO ===\n")
                for row in stats['consumo_por_tipo']:
                    f.write(f"{row['tipo_consumo']}: {row['count']:,} registros\n")
                
                f.write("\n=== ESTATÍSTICAS POR TIPO DE USUÁRIO ===\n")
                for row in stats['usuarios_por_tipo']:
                    f.write(f"{row['tipo_usuario']}: {row['count']:,} registros\n")
                
                f.write("\n=== ESTATÍSTICAS POR TIPO DE REFEIÇÃO ===\n")
                for row in stats['refeicoes_por_tipo']:
                    f.write(f"{row['tipo_refeicao']}: {row['count']:,} registros\n")
            
            self.logger.success(f"Resultados salvos em: {count_file}")

            # Salvar resultados do grafo
            
            graph_file = os.path.join(output_dir, f"grafo_usuarios_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(graph_file, 'w', encoding='utf-8') as f:
                import json
                json.dump(graph_stats, f, indent=4, ensure_ascii=False)
            
            self.logger.success(f"Resultados do grafo salvos em: {graph_file}")
            
        except Exception as e:
            self.logger.error(f"Erro ao salvar resultados: {e}")
            raise
    
    def download_dataset(self, dataset_url: str):
        """
        Baixa o dataset do Google Drive e extrai arquivo tar.gz
        
        Args:
            dataset_url: URL do arquivo no Google Drive
        """
        import gdown
        import tarfile
        import re
        
        self.logger.info(f"Baixando dataset do Google Drive: {dataset_url}")
        
        try:
            # Extrair file ID da URL do Google Drive
            file_id_match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', dataset_url)
            if not file_id_match:
                raise ValueError("URL do Google Drive inválida. Não foi possível extrair o file ID.")
            
            file_id = file_id_match.group(1)
            self.logger.debug(f"File ID extraído: {file_id}")
            
            # Definir diretório e arquivo de destino
            download_dir = DataPaths.DATA_SAMPLE_DIR
            tar_filename = "ru_dataset.tar.gz"
            tar_filepath = os.path.join(download_dir, tar_filename)
            
            # Garantir que o diretório existe
            os.makedirs(download_dir, exist_ok=True)
            
            # URL direta para download do Google Drive
            download_url = f"https://drive.google.com/uc?id={file_id}"
            
            # Baixar arquivo
            self.logger.info(f"Iniciando download para: {tar_filepath}")
            gdown.download(download_url, tar_filepath, quiet=False)
            
            # Verificar se o arquivo foi baixado
            if not os.path.exists(tar_filepath):
                raise FileNotFoundError(f"Falha no download. Arquivo não encontrado: {tar_filepath}")
            
            file_size = os.path.getsize(tar_filepath)
            self.logger.success(f"Download concluído! Tamanho do arquivo: {file_size / (1024*1024):.2f} MB")
            
            # Extrair arquivo tar.gz
            self.logger.info("Extraindo arquivo tar.gz...")
            with tarfile.open(tar_filepath, 'r:gz') as tar:
                # Listar conteúdo do arquivo
                members = tar.getnames()
                self.logger.info(f"Arquivos no tar.gz: {len(members)} itens")
                for member in members[:5]:  # Mostrar apenas os primeiros 5
                    self.logger.debug(f"  - {member}")
                if len(members) > 5:
                    self.logger.debug(f"  ... e mais {len(members) - 5} arquivos")
                
                # Extrair tudo
                tar.extractall(path=download_dir)
            
            self.logger.success(f"Extração concluída em: {download_dir}")
            
            # Remover arquivo tar.gz após extração
            try:
                os.remove(tar_filepath)
                self.logger.info("Arquivo tar.gz removido após extração")
            except OSError as e:
                self.logger.warning(f"Não foi possível remover o arquivo tar.gz: {e}")
            
            # Listar arquivos extraídos
            extracted_files = [f for f in os.listdir(download_dir) if f != tar_filename]
            self.logger.info(f"Arquivos extraídos ({len(extracted_files)}):")
            for file in extracted_files[:10]:  # Mostrar apenas os primeiros 10
                file_path = os.path.join(download_dir, file)
                if os.path.isfile(file_path):
                    size = os.path.getsize(file_path) / (1024*1024)
                    self.logger.info(f"  📄 {file} ({size:.2f} MB)")
                else:
                    self.logger.info(f"  📁 {file}/")
            
            if len(extracted_files) > 10:
                self.logger.info(f"  ... e mais {len(extracted_files) - 10} arquivos")
            
            self.logger.success("✅ Dataset baixado e extraído com sucesso!")
            
        except Exception as e:
            self.logger.error(f"Erro ao baixar/extrair dataset: {e}")
            raise
    
    @measure_spark_operation(operation_name="analyze_data")
    def analyze_data(self, file_path: str, periods: Optional[list] = None) -> dict:
        """
        Executa análise completa dos dados
        
        Args:
            file_path: Caminho para o arquivo de dados
            periods: Lista de períodos letivos (formato: YYYY/S, ex: ['2024/1', '2024/2'])
            
        Returns:
            Dicionário com resultados da análise
        """
        self.logger.info("Iniciando análise dos dados do RU-UFLA")
        
        # URL do dataset no Google Drive
        dataset_url = "https://drive.google.com/file/d/1jJ0rnXduCLOck8BKl4PK1s0fzEMQVL2h/view?usp=drive_link"
        
        try:
            # Verificar se o arquivo existe, caso contrário baixar do Google Drive
            if not os.path.exists(file_path):
                self.logger.info(f"Arquivo {file_path} não encontrado. Baixando dataset...")
                self.download_dataset(dataset_url)
            else:
                self.logger.info(f"Arquivo {file_path} encontrado. Prosseguindo com análise...")
            
            # Carregar dados
            self.load_data(file_path)
            
            # Aplicar filtro de período se especificado
            if periods:
                self.apply_period_filter(periods)
                
            # romver esse documento aparenta ser ruído
            # pois vários registros possuem o mesmo documento
            # e o curso é Consumo Geral
            self.df = self.df.filter(col("documento") != "705.104.2c8-4a")
            
            # Contar registros
            self.count_records()
            
            # Calcular estatísticas
            stats = self.get_basic_statistics()
            
            # Calcular grafo de usuários com janela deslizante
            graph_stats = self.get_users_graph_window_sliding()
            
            # Exibir resultados
            self.logger.info("=== RESUMO DA ANÁLISE ===")
            self.logger.info(f"📊 Total de registros: {stats['total_registros']:,}")
            self.logger.info(f"📋 Total de colunas: {stats['total_colunas']}")
            
            self.logger.info("\n🍽️ Top 3 tipos de consumo:")
            for i, row in enumerate(stats['consumo_por_tipo'][:3], 1):
                self.logger.info(f"  {i}. {row['tipo_consumo']}: {row['count']:,} registros")
            
            self.logger.info("\n👥 Top 3 tipos de usuário:")
            for i, row in enumerate(stats['usuarios_por_tipo'][:3], 1):
                self.logger.info(f"  {i}. {row['tipo_usuario']}: {row['count']:,} registros")
            
            return stats, graph_stats
            
        except Exception as e:
            self.logger.error(f"Erro durante a análise: {e}")
            raise
    
    def download_complete_dataset(self):
        """
        Baixa o dataset completo do RU-UFLA e salva em misc/data/dataset.json
        
        Esta função baixa o dataset completo e extrai o arquivo JSON principal
        salvando-o no local correto para análises completas.
        """
        import gdown
        import tarfile
        import re
        import json
        import shutil
        
        dataset_url = "https://drive.google.com/file/d/1suMbHiNwAe1ZbeCH3VxVxuLK2eoGHHyg/view?usp=drive_link"
        self.logger.info("🔄 Iniciando download do dataset completo do RU-UFLA")
        
        try:
            # Extrair file ID da URL do Google Drive
            file_id_match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', dataset_url)
            if not file_id_match:
                raise ValueError("URL do Google Drive inválida. Não foi possível extrair o file ID.")
            
            file_id = file_id_match.group(1)
            self.logger.debug(f"File ID extraído: {file_id}")
            
            # Garantir que o diretório de destino existe
            os.makedirs(DataPaths.DATA_DIR, exist_ok=True)
            
            # Definir arquivo temporário para download
            temp_dir = f"{DataPaths.DATA_DIR}/temp_download"
            os.makedirs(temp_dir, exist_ok=True)
            
            tar_filename = "ru_dataset_complete.tar.gz"
            tar_filepath = os.path.join(temp_dir, tar_filename)
            
            # URL direta para download do Google Drive
            download_url = f"https://drive.google.com/uc?id={file_id}"
            
            # Baixar arquivo
            self.logger.info(f"Iniciando download para: {tar_filepath}")
            gdown.download(download_url, tar_filepath, quiet=False)
            
            # Verificar se o arquivo foi baixado
            if not os.path.exists(tar_filepath):
                raise FileNotFoundError(f"Falha no download. Arquivo não encontrado: {tar_filepath}")
            
            file_size = os.path.getsize(tar_filepath)
            self.logger.success(f"Download concluído! Tamanho do arquivo: {file_size / (1024*1024):.2f} MB")
            
            # Extrair arquivo tar.gz
            self.logger.info("Extraindo arquivo tar.gz...")
            with tarfile.open(tar_filepath, 'r:gz') as tar:
                # Listar conteúdo do arquivo
                members = tar.getnames()
                self.logger.info(f"Arquivos no tar.gz: {len(members)} itens")
                
                # Extrair tudo no diretório temporário
                tar.extractall(path=temp_dir)
            
            # Procurar pelo arquivo JSON principal no diretório extraído
            json_files = []
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file.endswith('.json') and 'dataset' in file.lower():
                        json_files.append(os.path.join(root, file))
            
            # Se não encontrar arquivo com 'dataset' no nome, procurar o maior arquivo JSON
            if not json_files:
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        if file.endswith('.json'):
                            json_files.append(os.path.join(root, file))
            
            if not json_files:
                raise FileNotFoundError("Nenhum arquivo JSON encontrado no dataset baixado")
            
            # Escolher o maior arquivo JSON (provavelmente o dataset principal)
            main_json_file = max(json_files, key=lambda x: os.path.getsize(x))
            self.logger.info(f"Arquivo JSON principal identificado: {os.path.basename(main_json_file)}")
            self.logger.info(f"Tamanho: {os.path.getsize(main_json_file) / (1024*1024):.2f} MB")
            
            # Copiar arquivo para o destino final
            shutil.copy2(main_json_file, DataPaths.RU_DATA_COMPLETE)
            
            # Limpeza do diretório temporário
            shutil.rmtree(temp_dir)
            
            self.logger.success("✅ Dataset completo baixado e salvo com sucesso!")
            self.logger.info(f"📂 Localização: {DataPaths.RU_DATA_COMPLETE}")
            
            # Verificar se o arquivo foi salvo corretamente
            if os.path.exists(DataPaths.RU_DATA_COMPLETE):
                final_size = os.path.getsize(DataPaths.RU_DATA_COMPLETE)
                self.logger.info(f"📊 Tamanho final: {final_size / (1024*1024):.2f} MB")
            else:
                raise FileNotFoundError(f"Falha ao salvar dataset em: {DataPaths.RU_DATA_COMPLETE}")
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao baixar dataset completo: {e}")
            # Limpeza em caso de erro
            temp_dir = f"{DataPaths.DATA_DIR}/temp_download"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            raise

    def run_complete_analysis(self, file_path: str, periods: Optional[list] = None):
        """
        Executa análise completa e salva resultados
        
        Args:
            file_path: Caminho para o arquivo de dados
            periods: Lista de períodos letivos (formato: YYYY/S, ex: ['2024/1', '2024/2'])
        """
        self.logger.info("Executando análise completa do dataset")
        self.logger.info(f"📁 Diretório de resultados: {DataPaths.RESULTS_DIR}")
        self.logger.info(f"📊 Diretório de métricas: {DataPaths.METRICS_DIR}")
        
        # Exibir informações sobre filtro de período
        if periods:
            self.logger.info(f"🗓️  Filtro de períodos aplicado:")
            self.logger.info(f"   Períodos selecionados: {', '.join(periods)}")
        else:
            self.logger.info("🗓️  Processando todos os períodos letivos")
        
        try:
            # Executar análise (com métricas do decorador @measure_spark_operation)
            stats, graph_stats = self.analyze_data(file_path, periods=periods)
            
            # Salvar resultados da análise
            self.save_results(stats, graph_stats)
            
            self.logger.success("Análise completa finalizada com sucesso!")
            self.logger.info("✅ Resultados salvos em: {}", DataPaths.RESULTS_DIR)
            self.logger.info("✅ Métricas salvas em: {}", DataPaths.METRICS_DIR)
            
        except Exception as e:
            self.logger.error(f"Erro na análise completa: {e}")
            raise