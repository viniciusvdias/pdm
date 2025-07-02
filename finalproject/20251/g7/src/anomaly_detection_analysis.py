# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import sys

# Configurar encoding para UTF-8
if sys.platform.startswith('win'):
    import locale
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    except:
        pass

# Configuração de logging com encoding UTF-8
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

class AnomalyDetectionAnalysis:
    """
    Análise específica de detecção de anomalias em constrained-off de usinas eólicas
    """
    
    def __init__(self, data_path="processed_data"):
        self.data_path = Path(data_path)
        
    def analyze_constrained_off_patterns(self):
        """
        Análise 1: Padrões de Constrained-Off Extremo
        """
        logger.info("=== Análise 1: Padrões de Constrained-Off Extremo ===")
        
        # Carregar dados consolidados
        main_files = list(self.data_path.glob("main_batch_*.parquet"))
        
        if not main_files:
            logger.warning("Nenhum arquivo consolidado encontrado! Tentando carregar dados consolidados...")
            # Tentar carregar dados consolidados diretamente
            main_file = Path("restricoes_coff_eolicas_consolidado.csv")
            if main_file.exists():
                logger.info("Carregando dados consolidados diretamente...")
                complete_df = pd.read_csv(main_file, sep=';', low_memory=False)
                complete_df = self._clean_main_data(complete_df)
            else:
                logger.error("Nenhum arquivo de dados encontrado!")
                return
        else:
            # Consolidar dados processados
            all_data = []
            for file_path in main_files:
                df = pd.read_parquet(file_path)
                all_data.append(df)
            
            if not all_data:
                logger.error("Nenhum dado encontrado!")
                return
            
            complete_df = pd.concat(all_data, ignore_index=True)
        
        # Análise de constrained-off extremo
        extreme_constrained = complete_df[complete_df['percentual_constrained'] > 70]
        
        if len(extreme_constrained) > 0:
            logger.info(f"Encontradas {len(extreme_constrained)} ocorrências de constrained-off extremo (>70%)")
            
            # Análise por usina
            usina_analysis = extreme_constrained.groupby(['nom_usina', 'nom_estado']).agg({
                'percentual_constrained': ['mean', 'max', 'count'],
                'constrained_off': 'sum',
                'din_instante': ['min', 'max']
            }).reset_index()
            
            usina_analysis.columns = ['usina', 'estado', 'percentual_medio', 'percentual_max', 
                                    'ocorrencias', 'constrained_off_total', 'primeiro_evento', 'ultimo_evento']
            
            # Top 10 usinas com mais constrained-off extremo
            top_usinas = usina_analysis.sort_values('constrained_off_total', ascending=False).head(10)
            
            logger.info("Top 10 usinas com mais constrained-off extremo:")
            for _, row in top_usinas.iterrows():
                logger.info(f"{row['usina']} ({row['estado']}): {row['constrained_off_total']:.2f} MW, {row['ocorrencias']} eventos")
            
            # Salvar análise
            usina_analysis.to_parquet(self.data_path / "extreme_constrained_analysis.parquet", index=False)
            
        else:
            logger.info("Nenhuma ocorrência de constrained-off extremo encontrada")
    
    def analyze_generation_variations(self):
        """
        Análise 2: Variações Bruscas na Geração
        """
        logger.info("=== Análise 2: Variações Bruscas na Geração ===")
        
        # Carregar dados
        main_files = list(self.data_path.glob("main_batch_*.parquet"))
        
        if not main_files:
            # Tentar carregar dados consolidados diretamente
            main_file = Path("restricoes_coff_eolicas_consolidado.csv")
            if main_file.exists():
                complete_df = pd.read_csv(main_file, sep=';', low_memory=False)
                complete_df = self._clean_main_data(complete_df)
            else:
                logger.error("Nenhum arquivo de dados encontrado!")
                return
        else:
            all_data = []
            for file_path in main_files:
                df = pd.read_parquet(file_path)
                all_data.append(df)
            
            if not all_data:
                logger.error("Nenhum dado encontrado!")
                return
            
            complete_df = pd.concat(all_data, ignore_index=True)
        
        # Verificar se as colunas necessárias existem
        required_cols = ['nom_usina', 'nom_estado', 'ano', 'mes', 'val_geracao']
        missing_cols = [col for col in required_cols if col not in complete_df.columns]
        
        if missing_cols:
            logger.error(f"Colunas faltando: {missing_cols}")
            logger.info(f"Colunas disponíveis: {list(complete_df.columns)}")
            return
        
        # Verificar se há dados válidos
        if complete_df['val_geracao'].isna().all():
            logger.error("Todos os valores de geração são nulos!")
            return
        
        # Calcular estatísticas por usina e mês
        try:
            generation_stats = complete_df.groupby(['nom_usina', 'nom_estado', 'ano', 'mes']).agg({
                'val_geracao': ['mean', 'std']
            }).reset_index()
            
            generation_stats.columns = ['usina', 'estado', 'ano', 'mes', 'geracao_media', 'geracao_std']
            
            # Calcular z-score para cada registro
            complete_df = complete_df.merge(generation_stats, on=['nom_usina', 'nom_estado', 'ano', 'mes'])
            complete_df['z_score'] = np.where(
                complete_df['geracao_std'] > 0,
                abs(complete_df['val_geracao'] - complete_df['geracao_media']) / complete_df['geracao_std'],
                0
            )
            
            # Detectar anomalias (z-score > 3)
            generation_anomalies = complete_df[complete_df['z_score'] > 3]
            
            if len(generation_anomalies) > 0:
                logger.info(f"Encontradas {len(generation_anomalies)} anomalias de geração (z-score > 3)")
                
                # Análise das anomalias
                anomaly_summary = generation_anomalies.groupby(['nom_usina', 'nom_estado']).agg({
                    'z_score': ['mean', 'max', 'count'],
                    'val_geracao': ['mean', 'min', 'max'],
                    'din_instante': ['min', 'max']
                }).reset_index()
                
                anomaly_summary.columns = ['usina', 'estado', 'z_score_medio', 'z_score_max', 'anomalias_count',
                                         'geracao_media', 'geracao_min', 'geracao_max', 'primeiro_evento', 'ultimo_evento']
                
                # Top 10 usinas com mais anomalias
                top_anomalies = anomaly_summary.sort_values('anomalias_count', ascending=False).head(10)
                
                logger.info("Top 10 usinas com mais anomalias de geração:")
                for _, row in top_anomalies.iterrows():
                    logger.info(f"{row['usina']} ({row['estado']}): {row['anomalias_count']} anomalias, z-score médio: {row['z_score_medio']:.2f}")
                
                # Salvar análise
                anomaly_summary.to_parquet(self.data_path / "generation_anomalies_analysis.parquet", index=False)
                
            else:
                logger.info("Nenhuma anomalia de geração encontrada")
                
        except Exception as e:
            logger.error(f"Erro ao calcular estatísticas de geração: {e}")
            logger.info("Verificando estrutura dos dados...")
            logger.info(f"Colunas disponíveis: {list(complete_df.columns)}")
            logger.info(f"Primeiras linhas: {complete_df.head()}")
    
    def analyze_temporal_patterns(self):
        """
        Análise 3: Padrões Temporais Anômalos
        """
        logger.info("=== Análise 3: Padrões Temporais Anômalos ===")
        
        # Carregar dados
        main_files = list(self.data_path.glob("main_batch_*.parquet"))
        
        if not main_files:
            # Tentar carregar dados consolidados diretamente
            main_file = Path("restricoes_coff_eolicas_consolidado.csv")
            if main_file.exists():
                complete_df = pd.read_csv(main_file, sep=';', low_memory=False)
                complete_df = self._clean_main_data(complete_df)
            else:
                logger.error("Nenhum arquivo de dados encontrado!")
                return
        else:
            all_data = []
            for file_path in main_files:
                df = pd.read_parquet(file_path)
                all_data.append(df)
            
            if not all_data:
                logger.error("Nenhum dado encontrado!")
                return
            
            complete_df = pd.concat(all_data, ignore_index=True)
        
        # Verificar se as colunas necessárias existem
        required_cols = ['nom_usina', 'nom_estado', 'hora', 'percentual_constrained']
        missing_cols = [col for col in required_cols if col not in complete_df.columns]
        
        if missing_cols:
            logger.error(f"Colunas faltando: {missing_cols}")
            return
        
        # Análise por hora do dia
        hourly_analysis = complete_df.groupby(['nom_usina', 'nom_estado', 'hora']).agg({
            'percentual_constrained': 'mean',
            'din_instante': 'count'
        }).reset_index()
        
        # Calcular média geral por usina
        usina_avg = complete_df.groupby(['nom_usina', 'nom_estado']).agg({
            'percentual_constrained': 'mean'
        }).reset_index()
        usina_avg.columns = ['usina', 'estado', 'percentual_medio_geral']
        
        # Identificar horários anômalos (>1.5x a média da usina)
        hourly_analysis = hourly_analysis.merge(usina_avg, left_on=['nom_usina', 'nom_estado'], 
                                              right_on=['usina', 'estado'])
        
        temporal_anomalies = hourly_analysis[
            hourly_analysis['percentual_constrained'] > (hourly_analysis['percentual_medio_geral'] * 1.5)
        ]
        
        if len(temporal_anomalies) > 0:
            logger.info(f"Encontrados {len(temporal_anomalies)} padrões temporais anômalos")
            
            # Análise por horário
            hora_analysis = temporal_anomalies.groupby('hora').agg({
                'percentual_constrained': 'mean',
                'usina': 'nunique'
            }).reset_index()
            
            logger.info("Horários com mais anomalias:")
            for _, row in hora_analysis.sort_values('usina', ascending=False).head(5).iterrows():
                logger.info(f"Hora {int(row['hora']):02d}:00 - {int(row['usina'])} usinas afetadas, {row['percentual_constrained']:.2f}% constrained-off médio")
            
            # Salvar análise
            temporal_anomalies.to_parquet(self.data_path / "temporal_anomalies_analysis.parquet", index=False)
            
        else:
            logger.info("Nenhum padrão temporal anômalo encontrado")
    
    def analyze_spatial_clusters(self):
        """
        Análise 4: Clusters Espaciais de Anomalias
        """
        logger.info("=== Análise 4: Clusters Espaciais de Anomalias ===")
        
        # Carregar dados
        main_files = list(self.data_path.glob("main_batch_*.parquet"))
        
        if not main_files:
            # Tentar carregar dados consolidados diretamente
            main_file = Path("restricoes_coff_eolicas_consolidado.csv")
            if main_file.exists():
                complete_df = pd.read_csv(main_file, sep=';', low_memory=False)
                complete_df = self._clean_main_data(complete_df)
            else:
                logger.error("Nenhum arquivo de dados encontrado!")
                return
        else:
            all_data = []
            for file_path in main_files:
                df = pd.read_parquet(file_path)
                all_data.append(df)
            
            if not all_data:
                logger.error("Nenhum dado encontrado!")
                return
            
            complete_df = pd.concat(all_data, ignore_index=True)
        
        # Verificar se as colunas necessárias existem
        required_cols = ['nom_estado', 'ano', 'mes', 'percentual_constrained', 'nom_usina', 'constrained_off', 'val_disponibilidade']
        missing_cols = [col for col in required_cols if col not in complete_df.columns]
        
        if missing_cols:
            logger.error(f"Colunas faltando: {missing_cols}")
            return
        
        # Análise por estado e mês
        state_monthly = complete_df.groupby(['nom_estado', 'ano', 'mes']).agg({
            'percentual_constrained': 'mean',
            'nom_usina': 'nunique',
            'constrained_off': 'sum',
            'val_disponibilidade': 'sum'
        }).reset_index()
        
        state_monthly['percentual_constrained_estado'] = (
            state_monthly['constrained_off'] / state_monthly['val_disponibilidade']
        ) * 100
        
        # Identificar clusters espaciais
        spatial_clusters = state_monthly[
            (state_monthly['percentual_constrained_estado'] > 30) & 
            (state_monthly['nom_usina'] > 3)
        ]
        
        if len(spatial_clusters) > 0:
            logger.info(f"Encontrados {len(spatial_clusters)} clusters espaciais de anomalias")
            
            # Classificar clusters
            spatial_clusters['tipo_cluster'] = np.where(
                (spatial_clusters['percentual_constrained_estado'] > 50) & 
                (spatial_clusters['nom_usina'] > 5),
                'CRITICO',
                np.where(
                    (spatial_clusters['percentual_constrained_estado'] > 30) & 
                    (spatial_clusters['nom_usina'] > 3),
                    'MODERADO',
                    'ISOLADO'
                )
            )
            
            # Análise por tipo de cluster
            cluster_summary = spatial_clusters.groupby('tipo_cluster').agg({
                'nom_estado': 'nunique',
                'percentual_constrained_estado': 'mean',
                'nom_usina': 'mean'
            }).reset_index()
            
            logger.info("Resumo dos clusters espaciais:")
            for _, row in cluster_summary.iterrows():
                logger.info(f"Cluster {row['tipo_cluster']}: {row['nom_estado']} estados, "
                           f"{row['percentual_constrained_estado']:.2f}% constrained-off médio, "
                           f"{row['nom_usina']:.1f} usinas em média")
            
            # Salvar análise
            spatial_clusters.to_parquet(self.data_path / "spatial_clusters_analysis.parquet", index=False)
            
        else:
            logger.info("Nenhum cluster espacial encontrado")
    
    def _clean_main_data(self, df):
        """Limpeza dos dados principais"""
        logger.info("Iniciando limpeza dos dados principais...")
        
        # Verificar se as colunas necessárias existem
        required_cols = ['din_instante', 'val_geracao', 'val_disponibilidade']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.error(f"Colunas faltando para limpeza: {missing_cols}")
            logger.info(f"Colunas disponíveis: {list(df.columns)}")
            return df
        
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
    
    def create_anomaly_summary_report(self):
        """
        Cria relatório resumo das anomalias detectadas
        """
        logger.info("=== Criando Relatório Resumo de Anomalias ===")
        
        report = """
# Relatório de Detecção de Anomalias - Constrained-Off Eólico

## Resumo Executivo
Este relatório apresenta as principais anomalias detectadas nos dados de constrained-off de usinas eólicas do ONS.

## 1. Anomalias de Constrained-Off Extremo
- **Critério**: >70% de restrição média
- **Impacto**: Perda significativa de geração renovável
- **Ação Recomendada**: Análise detalhada das causas e otimização operacional

## 2. Anomalias de Variação de Geração
- **Critério**: Z-score >3 na geração
- **Impacto**: Instabilidade na rede elétrica
- **Ação Recomendada**: Investigar causas técnicas e meteorológicas

## 3. Padrões Temporais Anômalos
- **Critério**: Horários com >1.5x a média de constrained-off
- **Impacto**: Padrões recorrentes de restrição
- **Ação Recomendada**: Otimização do despacho por horário

## 4. Clusters Espaciais
- **Critério**: Estados com >3 usinas e >30% constrained-off
- **Impacto**: Problemas regionais de transmissão
- **Ação Recomendada**: Investimento em infraestrutura de transmissão

## 5. Recomendações Prioritárias

### 5.1 Curto Prazo (1-3 meses)
1. Implementar monitoramento em tempo real das usinas críticas
2. Criar alertas automáticos para anomalias
3. Otimizar despacho das usinas mais afetadas

### 5.2 Médio Prazo (3-12 meses)
1. Desenvolver modelos preditivos de constrained-off
2. Implementar otimização automática do despacho
3. Investir em infraestrutura de transmissão crítica

### 5.3 Longo Prazo (1-3 anos)
1. Integrar dados meteorológicos para predição
2. Implementar machine learning para otimização
3. Desenvolver estratégias de armazenamento de energia

## 6. Métricas de Sucesso
- Redução de 20% no constrained-off total
- Detecção de anomalias em <5 minutos
- Precisão de predição >80%
- ROI positivo em 12 meses

## 7. Próximos Passos
1. Validação das anomalias detectadas
2. Implementação do sistema de monitoramento
3. Desenvolvimento dos modelos preditivos
4. Criação do dashboard de visualização
        """
        
        with open(self.data_path / "anomaly_detection_report.md", "w") as f:
            f.write(report)
        
        logger.info(f"Relatório de anomalias salvo em: {self.data_path / 'anomaly_detection_report.md'}")
    
    def run_complete_analysis(self):
        """
        Executa análise completa de detecção de anomalias
        """
        logger.info("Iniciando análise completa de detecção de anomalias...")
        
        try:
            self.analyze_constrained_off_patterns()
            self.analyze_generation_variations()
            self.analyze_temporal_patterns()
            self.analyze_spatial_clusters()
            self.create_anomaly_summary_report()
            
            logger.info("Análise completa de anomalias concluída!")
            
        except Exception as e:
            logger.error(f"Erro durante análise de anomalias: {e}")
            raise

if __name__ == "__main__":
    # Executar análise de anomalias
    analysis = AnomalyDetectionAnalysis()
    analysis.run_complete_analysis() 