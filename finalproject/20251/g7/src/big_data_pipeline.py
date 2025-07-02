import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WindFarmBigDataPipeline:
    """
    Pipeline completo para análise de big-data de constrained-off em usinas eólicas
    """
    
    def __init__(self, output_path="processed_data"):
        self.output_path = Path(output_path)
        self.output_path.mkdir(exist_ok=True)
        
    def step1_data_consolidation(self):
        """
        Passo 1: Carregamento e limpeza dos dados consolidados
        """
        logger.info("=== PASSO 1: Carregamento e Limpeza dos Dados Consolidados ===")
        
        # Verificar se os arquivos consolidados existem
        main_file = Path("restricoes_coff_eolicas_consolidado.csv")
        detail_file = Path("restricoes_coff_eolicas_detalhamento_consolidado.csv")
        
        if not main_file.exists():
            logger.error(f"Arquivo {main_file} não encontrado!")
            return
        
        if not detail_file.exists():
            logger.warning(f"Arquivo {detail_file} não encontrado! Continuando apenas com dados principais.")
        
        # Carregar dados principais
        logger.info("Carregando dados principais consolidados...")
        try:
            main_df = pd.read_csv(main_file, sep=';', low_memory=False)
            logger.info(f"Dados principais carregados: {len(main_df)} registros")
            
            # Limpar e processar dados principais
            main_df = self._clean_main_data(main_df)
            
            # Salvar em lotes para otimizar memória
            self._save_in_batches(main_df, "main_batch")
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados principais: {e}")
            return
        
        # Carregar dados de detalhamento (se existir)
        if detail_file.exists():
            logger.info("Carregando dados de detalhamento consolidados...")
            try:
                detail_df = pd.read_csv(detail_file, sep=';', low_memory=False)
                logger.info(f"Dados de detalhamento carregados: {len(detail_df)} registros")
                
                # Limpar e processar dados de detalhamento
                detail_df = self._clean_detail_data(detail_df)
                
                # Salvar dados de detalhamento
                detail_df.to_parquet(self.output_path / "wind_detail_consolidated.parquet", index=False)
                logger.info("Dados de detalhamento salvos em formato Parquet")
                
            except Exception as e:
                logger.error(f"Erro ao carregar dados de detalhamento: {e}")
        
    def _save_in_batches(self, df, prefix):
        """Salva DataFrame em lotes para otimizar memória"""
        batch_size = 100000  # 100k registros por lote
        
        total_batches = (len(df) // batch_size) + 1
        
        for i in range(total_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(df))
            
            batch_df = df.iloc[start_idx:end_idx]
            
            output_file = self.output_path / f"{prefix}_{i+1}.parquet"
            batch_df.to_parquet(output_file, index=False)
            
            logger.info(f"Lote {i+1}/{total_batches} salvo: {output_file} - {len(batch_df)} registros")
    
    def _clean_main_data(self, df):
        """Limpeza dos dados principais"""
        logger.info("Iniciando limpeza dos dados principais...")
        
        # Converter colunas de data
        df['din_instante'] = pd.to_datetime(df['din_instante'])
        
        # Extrair componentes temporais
        df['ano'] = df['din_instante'].dt.year
        df['mes'] = df['din_instante'].dt.month
        df['dia'] = df['din_instante'].dt.day
        df['hora'] = df['din_instante'].dt.hour
        df['minuto'] = df['din_instante'].dt.minute
        
        # Converter colunas numéricas
        numeric_cols = ['val_geracao', 'val_geracaolimitada', 'val_disponibilidade', 
                       'val_geracaoreferencia', 'val_geracaoreferenciafinal']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Calcular métricas derivadas
        df['constrained_off'] = df['val_disponibilidade'] - df['val_geracao']
        df['constrained_off'] = df['constrained_off'].clip(lower=0)  # Não pode ser negativo
        
        df['percentual_constrained'] = np.where(
            df['val_disponibilidade'] > 0,
            (df['constrained_off'] / df['val_disponibilidade']) * 100,
            0
        )
        
        logger.info("Limpeza dos dados principais concluída")
        return df
    
    def _clean_detail_data(self, df):
        """Limpeza dos dados de detalhamento"""
        logger.info("Iniciando limpeza dos dados de detalhamento...")
        
        # Converter colunas de data
        df['din_instante'] = pd.to_datetime(df['din_instante'])
        
        # Extrair componentes temporais
        df['ano'] = df['din_instante'].dt.year
        df['mes'] = df['din_instante'].dt.month
        df['dia'] = df['din_instante'].dt.day
        df['hora'] = df['din_instante'].dt.hour
        
        # Converter colunas numéricas
        numeric_cols = ['val_ventoverificado', 'val_geracaoestimada', 'val_geracaoverificada']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        logger.info("Limpeza dos dados de detalhamento concluída")
        return df
    
    def step2_temporal_spatial_aggregations(self):
        """
        Passo 2: Agregações temporais e espaciais
        """
        logger.info("=== PASSO 2: Agregações Temporais e Espaciais ===")
        
        # Carregar dados consolidados
        main_files = list(self.output_path.glob("main_batch_*.parquet"))
        
        if not main_files:
            logger.error("Nenhum arquivo de dados principais encontrado!")
            return
        
        # Consolidar todos os dados para agregações
        logger.info("Consolidando dados para agregações...")
        all_data = []
        
        for file_path in main_files:
            df = pd.read_parquet(file_path)
            all_data.append(df)
        
        if not all_data:
            logger.error("Nenhum dado encontrado para agregação!")
            return
        
        complete_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Dados consolidados para agregação: {len(complete_df)} registros")
        
        # Criar agregações
        logger.info("Criando agregações temporais...")
        monthly_agg = self._create_monthly_aggregations(complete_df)
        monthly_agg.to_parquet(self.output_path / "monthly_aggregations.parquet", index=False)
        
        logger.info("Criando agregações espaciais...")
        spatial_agg = self._create_spatial_aggregations(complete_df)
        spatial_agg.to_parquet(self.output_path / "spatial_aggregations.parquet", index=False)
        
        logger.info("Criando agregações por estado...")
        state_agg = self._create_state_aggregations(complete_df)
        state_agg.to_parquet(self.output_path / "state_aggregations.parquet", index=False)
        
        # Criar dados para visualização
        logger.info("Criando dados para visualização...")
        self._create_visualization_data(complete_df)
    
    def _create_monthly_aggregations(self, df):
        """Cria agregações mensais"""
        monthly_agg = df.groupby(['ano', 'mes', 'nom_estado', 'nom_usina']).agg({
            'val_geracao': ['sum', 'mean', 'max', 'min'],
            'val_disponibilidade': ['sum', 'mean', 'max', 'min'],
            'constrained_off': ['sum', 'mean', 'max', 'min'],
            'percentual_constrained': ['mean', 'max', 'min'],
            'din_instante': 'count'  # Número de registros
        }).reset_index()
        
        # Renomear colunas
        monthly_agg.columns = ['ano', 'mes', 'estado', 'usina', 
                              'geracao_total', 'geracao_media', 'geracao_max', 'geracao_min',
                              'disponibilidade_total', 'disponibilidade_media', 'disponibilidade_max', 'disponibilidade_min',
                              'constrained_off_total', 'constrained_off_media', 'constrained_off_max', 'constrained_off_min',
                              'percentual_constrained_media', 'percentual_constrained_max', 'percentual_constrained_min',
                              'registros']
        
        return monthly_agg
    
    def _create_spatial_aggregations(self, df):
        """Cria agregações espaciais por usina"""
        spatial_agg = df.groupby(['nom_usina', 'nom_estado', 'id_ons']).agg({
            'val_geracao': ['sum', 'mean', 'std'],
            'val_disponibilidade': ['sum', 'mean', 'std'],
            'constrained_off': ['sum', 'mean', 'std'],
            'percentual_constrained': ['mean', 'std'],
            'din_instante': ['min', 'max', 'count']
        }).reset_index()
        
        # Renomear colunas
        spatial_agg.columns = ['usina', 'estado', 'id_ons',
                              'geracao_total', 'geracao_media', 'geracao_std',
                              'disponibilidade_total', 'disponibilidade_media', 'disponibilidade_std',
                              'constrained_off_total', 'constrained_off_media', 'constrained_off_std',
                              'percentual_constrained_media', 'percentual_constrained_std',
                              'primeiro_registro', 'ultimo_registro', 'total_registros']
        
        return spatial_agg
    
    def _create_state_aggregations(self, df):
        """Cria agregações por estado"""
        state_agg = df.groupby(['nom_estado', 'ano', 'mes']).agg({
            'val_geracao': 'sum',
            'val_disponibilidade': 'sum',
            'constrained_off': 'sum',
            'percentual_constrained': 'mean',
            'nom_usina': 'nunique'  # Número de usinas únicas
        }).reset_index()
        
        state_agg.columns = ['estado', 'ano', 'mes', 'geracao_total', 'disponibilidade_total', 
                           'constrained_off_total', 'percentual_constrained_medio', 'num_usinas']
        
        return state_agg
    
    def _create_visualization_data(self, df):
        """Cria dados otimizados para visualização"""
        
        # Resumo temporal
        temporal_summary = df.groupby(['ano', 'mes']).agg({
            'constrained_off': 'sum',
            'val_geracao': 'sum',
            'val_disponibilidade': 'sum',
            'nom_usina': 'nunique'
        }).reset_index()
        
        temporal_summary['percentual_constrained'] = (
            temporal_summary['constrained_off'] / temporal_summary['val_disponibilidade']
        ) * 100
        
        temporal_summary.to_parquet(self.output_path / "temporal_summary.parquet", index=False)
        
        # Resumo por estado
        state_summary = df.groupby(['nom_estado', 'ano', 'mes']).agg({
            'constrained_off': 'sum',
            'val_geracao': 'sum',
            'val_disponibilidade': 'sum',
            'nom_usina': 'nunique'
        }).reset_index()
        
        state_summary['percentual_constrained'] = (
            state_summary['constrained_off'] / state_summary['val_disponibilidade']
        ) * 100
        
        state_summary.to_parquet(self.output_path / "state_summary.parquet", index=False)
        
        # Top usinas com mais constrained-off
        top_usinas = df.groupby(['nom_usina', 'nom_estado']).agg({
            'constrained_off': 'sum',
            'val_disponibilidade': 'sum',
            'din_instante': 'count'
        }).reset_index()
        
        top_usinas['percentual_constrained'] = (
            top_usinas['constrained_off'] / top_usinas['val_disponibilidade']
        ) * 100
        
        top_usinas = top_usinas.sort_values('constrained_off', ascending=False).head(50)
        top_usinas.to_parquet(self.output_path / "top_usinas_constrained.parquet", index=False)
        
        logger.info("Dados para visualização criados com sucesso!")
    
    def step3_anomaly_detection_sql(self):
        """
        Passo 3: Detecção de padrões e anomalias via SQL
        """
        logger.info("=== PASSO 3: Detecção de Padrões e Anomalias ===")
        
        # Criar queries SQL para detecção de anomalias
        self._create_anomaly_detection_queries()
        
    def _create_anomaly_detection_queries(self):
        """Cria queries SQL para detecção de anomalias"""
        
        queries = {
            "anomalia_constrained_off_extremo": """
            -- Detectar usinas com constrained-off extremamente alto
            SELECT 
                nom_usina,
                nom_estado,
                ano,
                mes,
                AVG(percentual_constrained) as percentual_medio,
                MAX(percentual_constrained) as percentual_max,
                COUNT(*) as registros
            FROM wind_data
            WHERE percentual_constrained > 50  -- Mais de 50% de constrained-off
            GROUP BY nom_usina, nom_estado, ano, mes
            HAVING AVG(percentual_constrained) > 70  -- Média acima de 70%
            ORDER BY percentual_medio DESC
            """,
            
            "anomalia_variacao_geracao": """
            -- Detectar variações bruscas na geração
            WITH geracao_stats AS (
                SELECT 
                    nom_usina,
                    nom_estado,
                    ano,
                    mes,
                    AVG(val_geracao) as geracao_media,
                    STDDEV(val_geracao) as geracao_std
                FROM wind_data
                GROUP BY nom_usina, nom_estado, ano, mes
            )
            SELECT 
                w.nom_usina,
                w.nom_estado,
                w.din_instante,
                w.val_geracao,
                gs.geracao_media,
                gs.geracao_std,
                ABS(w.val_geracao - gs.geracao_media) / gs.geracao_std as z_score
            FROM wind_data w
            JOIN geracao_stats gs ON w.nom_usina = gs.nom_usina 
                AND w.ano = gs.ano 
                AND w.mes = gs.mes
            WHERE ABS(w.val_geracao - gs.geracao_media) / gs.geracao_std > 3  -- Z-score > 3
            ORDER BY z_score DESC
            """,
            
            "anomalia_padrao_temporal": """
            -- Detectar padrões temporais anômalos
            SELECT 
                nom_usina,
                nom_estado,
                hora,
                AVG(percentual_constrained) as percentual_medio_hora,
                COUNT(*) as registros_hora
            FROM wind_data
            GROUP BY nom_usina, nom_estado, hora
            HAVING AVG(percentual_constrained) > (
                SELECT AVG(percentual_constrained) * 1.5 
                FROM wind_data 
                WHERE nom_usina = wind_data.nom_usina
            )
            ORDER BY percentual_medio_hora DESC
            """,
            
            "anomalia_correlacao_vento_geracao": """
            -- Correlação entre vento e geração (usando dados de detalhamento)
            SELECT 
                w.nom_usina,
                w.nom_estado,
                w.ano,
                w.mes,
                CORR(w.val_geracao, d.val_ventoverificado) as correlacao_vento_geracao,
                AVG(w.percentual_constrained) as percentual_constrained_medio
            FROM wind_data w
            JOIN wind_detail d ON w.nom_usina = d.nom_usina 
                AND w.din_instante = d.din_instante
            GROUP BY w.nom_usina, w.nom_estado, w.ano, w.mes
            HAVING CORR(w.val_geracao, d.val_ventoverificado) < 0.3  -- Baixa correlação
                AND AVG(w.percentual_constrained) > 20  -- Alto constrained-off
            ORDER BY percentual_constrained_medio DESC
            """,
            
            "anomalia_tendencia_temporal": """
            -- Detectar tendências temporais anômalas
            WITH monthly_trends AS (
                SELECT 
                    nom_usina,
                    nom_estado,
                    ano,
                    mes,
                    AVG(percentual_constrained) as percentual_medio,
                    LAG(AVG(percentual_constrained), 1) OVER (
                        PARTITION BY nom_usina 
                        ORDER BY ano, mes
                    ) as percentual_anterior
                FROM wind_data
                GROUP BY nom_usina, nom_estado, ano, mes
            )
            SELECT 
                nom_usina,
                nom_estado,
                ano,
                mes,
                percentual_medio,
                percentual_anterior,
                (percentual_medio - percentual_anterior) as variacao,
                CASE 
                    WHEN (percentual_medio - percentual_anterior) > 20 THEN 'AUMENTO_BRUSCO'
                    WHEN (percentual_medio - percentual_anterior) < -20 THEN 'DIMINUICAO_BRUSCA'
                    ELSE 'NORMAL'
                END as tipo_anomalia
            FROM monthly_trends
            WHERE percentual_anterior IS NOT NULL
                AND ABS(percentual_medio - percentual_anterior) > 20
            ORDER BY ABS(percentual_medio - percentual_anterior) DESC
            """,
            
            "anomalia_cluster_espacial": """
            -- Detectar clusters espaciais de anomalias
            WITH state_anomalies AS (
                SELECT 
                    nom_estado,
                    ano,
                    mes,
                    AVG(percentual_constrained) as percentual_estado,
                    COUNT(DISTINCT nom_usina) as num_usinas_afetadas
                FROM wind_data
                WHERE percentual_constrained > 30
                GROUP BY nom_estado, ano, mes
            )
            SELECT 
                nom_estado,
                ano,
                mes,
                percentual_estado,
                num_usinas_afetadas,
                CASE 
                    WHEN percentual_estado > 50 AND num_usinas_afetadas > 5 THEN 'CLUSTER_CRITICO'
                    WHEN percentual_estado > 30 AND num_usinas_afetadas > 3 THEN 'CLUSTER_MODERADO'
                    ELSE 'ISOLADO'
                END as tipo_cluster
            FROM state_anomalies
            WHERE percentual_estado > 30
            ORDER BY percentual_estado DESC, num_usinas_afetadas DESC
            """
        }
        
        # Salvar queries em arquivo
        with open(self.output_path / "anomaly_detection_queries.sql", "w") as f:
            f.write("-- Queries para Detecção de Anomalias em Usinas Eólicas\n")
            f.write("-- Baseado em dados de constrained-off do ONS\n\n")
            
            for query_name, query in queries.items():
                f.write(f"-- {query_name}\n")
                f.write(query)
                f.write("\n\n")
        
        logger.info(f"Queries de detecção de anomalias salvas em: {self.output_path / 'anomaly_detection_queries.sql'}")
    
    def step4_visualization_preparation(self):
        """
        Passo 4: Preparação para visualização
        """
        logger.info("=== PASSO 4: Preparação para Visualização ===")
        
        # Dados já foram preparados no step2
        logger.info("Dados para visualização já preparados no passo 2")
    
    def step5_architecture_evaluation(self):
        """
        Passo 5: Avaliação da arquitetura proposta
        """
        logger.info("=== PASSO 5: Avaliação da Arquitetura ===")
        
        self._create_architecture_report()
    
    def _create_architecture_report(self):
        """Cria relatório de avaliação da arquitetura"""
        
        report = """
# Avaliação da Arquitetura de Big-Data para Análise de Constrained-Off

## 1. Estrutura dos Dados
- **Volume**: Dados consolidados de constrained-off eólico
- **Variedade**: Geração, disponibilidade, restrições, vento
- **Velocidade**: Dados históricos com granularidade de 30 minutos
- **Veracidade**: Dados oficiais do ONS

## 2. Arquitetura Proposta

### 2.1 Pipeline de Processamento
```
Consolidated CSV Files → Parquet Processing → Temporal/Spatial Aggregations → Anomaly Detection → Visualization
```

### 2.2 Benefícios da Arquitetura
- **Compressão**: Parquet reduz volume em ~70%
- **Performance**: Consultas SQL otimizadas
- **Escalabilidade**: Processamento em lotes
- **Flexibilidade**: Queries SQL para diferentes análises

### 2.3 Pontos de Melhoria
- Implementar Apache Spark para processamento distribuído
- Adicionar Apache Kafka para streaming em tempo real
- Usar Apache Airflow para orquestração
- Implementar data lake com Delta Lake

## 3. Detecção de Anomalias Implementada

### 3.1 Tipos de Anomalias Detectadas
1. **Constrained-off Extremo**: Usinas com >70% de restrição média
2. **Variação Brusca**: Z-score >3 na geração
3. **Padrão Temporal**: Horários com restrição anômala
4. **Correlação Vento-Geração**: Baixa correlação com alto constrained-off
5. **Tendência Temporal**: Variações >20% entre meses
6. **Cluster Espacial**: Estados com múltiplas usinas afetadas

### 3.2 Oportunidades de Análise
- **Predição**: ML para prever constrained-off
- **Otimização**: Redução de restrições via operação
- **Planejamento**: Expansão da rede de transmissão
- **Monitoramento**: Dashboard em tempo real

## 4. Recomendações

### 4.1 Tecnologias Sugeridas
- **Processamento**: Apache Spark + PySpark
- **Storage**: Delta Lake + Azure Data Lake
- **Orquestração**: Apache Airflow
- **Visualização**: Power BI + Streamlit
- **ML**: Azure ML + AutoML

### 4.2 Próximos Passos
1. Implementar pipeline com Spark
2. Criar modelos de ML para predição
3. Desenvolver dashboard interativo
4. Implementar alertas automáticos
5. Integrar com dados meteorológicos

## 5. Métricas de Sucesso
- Redução de 20% no constrained-off
- Detecção de anomalias em <5 minutos
- Precisão de predição >80%
- ROI positivo em 12 meses

## 6. Próximos Passos
1. Validação das anomalias detectadas
2. Implementação do sistema de monitoramento
3. Desenvolvimento dos modelos preditivos
4. Criação do dashboard de visualização
        """
        
        with open(self.output_path / "architecture_evaluation.md", "w") as f:
            f.write(report)
        
        logger.info(f"Relatório de arquitetura salvo em: {self.output_path / 'architecture_evaluation.md'}")
    
    def run_complete_pipeline(self):
        """
        Executa o pipeline completo
        """
        logger.info("Iniciando pipeline completo de big-data...")
        
        try:
            self.step1_data_consolidation()
            self.step2_temporal_spatial_aggregations()
            self.step3_anomaly_detection_sql()
            self.step4_visualization_preparation()
            self.step5_architecture_evaluation()
            
            logger.info("Pipeline completo executado com sucesso!")
            
        except Exception as e:
            logger.error(f"Erro durante execução do pipeline: {e}")
            raise

if __name__ == "__main__":
    # Executar pipeline
    pipeline = WindFarmBigDataPipeline()
    pipeline.run_complete_pipeline() 