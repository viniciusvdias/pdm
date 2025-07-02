import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from pathlib import Path
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ConstrainedOffDashboard:
    """
    Dashboard interativo para análise de constrained-off de usinas eólicas
    """
    
    def __init__(self, data_path="processed_data"):
        self.data_path = Path(data_path)
        
    def load_data(self):
        """Carrega dados processados"""
        try:
            # Carregar dados temporais
            temporal_data = pd.read_parquet(self.data_path / "temporal_summary.parquet")
            
            # Carregar dados por estado
            state_data = pd.read_parquet(self.data_path / "state_summary.parquet")
            
            # Carregar top usinas
            top_usinas = pd.read_parquet(self.data_path / "top_usinas_constrained.parquet")
            
            # Carregar análises de anomalias
            extreme_analysis = None
            generation_anomalies = None
            temporal_anomalies = None
            spatial_clusters = None
            
            if (self.data_path / "extreme_constrained_analysis.parquet").exists():
                extreme_analysis = pd.read_parquet(self.data_path / "extreme_constrained_analysis.parquet")
            
            if (self.data_path / "generation_anomalies_analysis.parquet").exists():
                generation_anomalies = pd.read_parquet(self.data_path / "generation_anomalies_analysis.parquet")
            
            if (self.data_path / "temporal_anomalies_analysis.parquet").exists():
                temporal_anomalies = pd.read_parquet(self.data_path / "temporal_anomalies_analysis.parquet")
            
            if (self.data_path / "spatial_clusters_analysis.parquet").exists():
                spatial_clusters = pd.read_parquet(self.data_path / "spatial_clusters_analysis.parquet")
            
            return {
                'temporal': temporal_data,
                'state': state_data,
                'top_usinas': top_usinas,
                'extreme_analysis': extreme_analysis,
                'generation_anomalies': generation_anomalies,
                'temporal_anomalies': temporal_anomalies,
                'spatial_clusters': spatial_clusters
            }
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {e}")
            return None
    
    def create_dashboard(self):
        """Cria o dashboard Streamlit"""
        
        st.set_page_config(
            page_title="Análise de Constrained-Off Eólico",
            page_icon="🌪️",
            layout="wide"
        )
        
        st.title("🌪️ Dashboard de Análise de Constrained-Off Eólico")
        st.markdown("Análise de big-data para detecção de padrões e anomalias em usinas eólicas")
        
        # Carregar dados
        data = self.load_data()
        
        if data is None:
            st.error("Erro ao carregar dados. Verifique se os arquivos processados existem.")
            return
        
        # Sidebar para filtros
        st.sidebar.header("Filtros")
        
        # Filtro de período
        if 'temporal' in data and data['temporal'] is not None:
            min_date = f"{data['temporal']['ano'].min()}-{data['temporal']['mes'].min():02d}"
            max_date = f"{data['temporal']['ano'].max()}-{data['temporal']['mes'].max():02d}"
            
            st.sidebar.subheader("Período")
            start_date = st.sidebar.text_input("Data Inicial", min_date)
            end_date = st.sidebar.text_input("Data Final", max_date)
        
        # Filtro de estado
        if 'state' in data and data['state'] is not None:
            estados = ['Todos'] + list(data['state']['nom_estado'].unique())
            estado_selecionado = st.sidebar.selectbox("Estado", estados)
        
        # Métricas principais
        self.show_main_metrics(data)
        
        # Tabs para diferentes análises
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Visão Geral", 
            "🔍 Detecção de Anomalias", 
            "📈 Análise Temporal", 
            "🗺️ Análise Espacial",
            "📋 Relatórios"
        ])
        
        with tab1:
            self.show_overview_tab(data)
        
        with tab2:
            self.show_anomalies_tab(data)
        
        with tab3:
            self.show_temporal_tab(data)
        
        with tab4:
            self.show_spatial_tab(data)
        
        with tab5:
            self.show_reports_tab(data)
    
    def show_main_metrics(self, data):
        """Mostra métricas principais"""
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if 'temporal' in data and data['temporal'] is not None:
                total_constrained = data['temporal']['constrained_off'].sum()
                st.metric(
                    label="Total Constrained-Off",
                    value=f"{total_constrained:,.0f} MW",
                    delta=None
                )
        
        with col2:
            if 'temporal' in data and data['temporal'] is not None:
                avg_percentual = data['temporal']['percentual_constrained'].mean()
                st.metric(
                    label="% Constrained-Off Médio",
                    value=f"{avg_percentual:.1f}%",
                    delta=None
                )
        
        with col3:
            if 'top_usinas' in data and data['top_usinas'] is not None:
                num_usinas = len(data['top_usinas'])
                st.metric(
                    label="Usinas Analisadas",
                    value=f"{num_usinas}",
                    delta=None
                )
        
        with col4:
            if 'extreme_analysis' in data and data['extreme_analysis'] is not None:
                num_extreme = len(data['extreme_analysis'])
                st.metric(
                    label="Anomalias Extremas",
                    value=f"{num_extreme}",
                    delta=None
                )
    
    def show_overview_tab(self, data):
        """Tab de visão geral"""
        
        st.header("📊 Visão Geral do Constrained-Off")
        
        # Gráfico temporal
        if 'temporal' in data and data['temporal'] is not None:
            st.subheader("Evolução Temporal do Constrained-Off")
            
            # Criar coluna de data
            temporal_df = data['temporal'].copy()
            temporal_df['data'] = pd.to_datetime(temporal_df['ano'].astype(str) + '-' + temporal_df['mes'].astype(str).str.zfill(2) + '-01')
            
            fig = px.line(
                temporal_df, 
                x='data', 
                y='percentual_constrained',
                title="Percentual de Constrained-Off ao Longo do Tempo",
                labels={'percentual_constrained': '% Constrained-Off', 'data': 'Data'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Top usinas
        if 'top_usinas' in data and data['top_usinas'] is not None:
            st.subheader("Top 10 Usinas com Mais Constrained-Off")
            
            top_10 = data['top_usinas'].head(10)
            
            fig = px.bar(
                top_10,
                x='constrained_off',
                y='nom_usina',
                orientation='h',
                title="Usinas com Maior Volume de Constrained-Off",
                labels={'constrained_off': 'Constrained-Off (MW)', 'nom_usina': 'Usina'}
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    def show_anomalies_tab(self, data):
        """Tab de detecção de anomalias"""
        
        st.header("🔍 Detecção de Anomalias")
        
        # Anomalias extremas
        if 'extreme_analysis' in data and data['extreme_analysis'] is not None:
            st.subheader("Anomalias de Constrained-Off Extremo (>70%)")
            
            extreme_df = data['extreme_analysis']
            
            # Gráfico de dispersão
            fig = px.scatter(
                extreme_df,
                x='constrained_off_total',
                y='percentual_medio',
                size='ocorrencias',
                color='estado',             
                hover_data=['usina'],        
                title="Anomalias Extremas por Usina",
                labels={
                    'constrained_off_total': 'Total Constrained-Off (MW)',
                    'percentual_medio': '% Constrained-Off Médio',
                    'ocorrencias': 'Número de Ocorrências'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Tabela detalhada
            st.dataframe(extreme_df.sort_values('constrained_off_total', ascending=False))
        
        # Anomalias de geração
        if 'generation_anomalies' in data and data['generation_anomalies'] is not None:
            st.subheader("Anomalias de Variação de Geração (Z-score > 3)")
            
            gen_df = data['generation_anomalies']
            
            fig = px.histogram(
                gen_df,
                x='z_score_medio',
                nbins=20,
                title="Distribuição dos Z-scores das Anomalias de Geração",
                labels={'z_score_medio': 'Z-score Médio', 'count': 'Número de Usinas'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    def show_temporal_tab(self, data):
        """Tab de análise temporal"""
        
        st.header("📈 Análise Temporal")
        
        if 'temporal_anomalies' in data and data['temporal_anomalies'] is not None:
            st.subheader("Padrões Temporais Anômalos")
            
            temp_df = data['temporal_anomalies']
            
            # Análise por hora
            hora_analysis = temp_df.groupby('hora').agg({
                'percentual_constrained': 'mean',
                'nom_usina': 'nunique'
            }).reset_index()
            
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('Constrained-Off Médio por Hora', 'Número de Usinas Afetadas por Hora'),
                vertical_spacing=0.1
            )
            
            fig.add_trace(
                go.Bar(x=hora_analysis['hora'], y=hora_analysis['percentual_constrained'], name='% Constrained-Off'),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Bar(x=hora_analysis['hora'], y=hora_analysis['nom_usina'], name='Usinas Afetadas'),
                row=2, col=1
            )
            
            fig.update_layout(height=600, title_text="Análise Temporal de Anomalias")
            st.plotly_chart(fig, use_container_width=True)
    
    def show_spatial_tab(self, data):
        """Tab de análise espacial"""
        
        st.header("🗺️ Análise Espacial")
        
        if 'state' in data and data['state'] is not None:
            st.subheader("Constrained-Off por Estado")
            
            state_df = data['state']
            
            # Mapa de calor por estado
            state_heatmap = state_df.groupby('nom_estado').agg({
                'percentual_constrained': 'mean',
                'constrained_off': 'sum'
            }).reset_index()
            
            fig = px.bar(
                state_heatmap,
                x='nom_estado',
                y='percentual_constrained',
                title="Percentual Médio de Constrained-Off por Estado",
                labels={'percentual_constrained': '% Constrained-Off Médio', 'nom_estado': 'Estado'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        if 'spatial_clusters' in data and data['spatial_clusters'] is not None:
            st.subheader("Clusters Espaciais de Anomalias")
            
            cluster_df = data['spatial_clusters']
            
            # Análise por tipo de cluster
            cluster_summary = cluster_df.groupby('tipo_cluster').agg({
                'nom_estado': 'nunique',
                'percentual_constrained_estado': 'mean',
                'nom_usina': 'mean'
            }).reset_index()
            
            fig = px.pie(
                cluster_summary,
                values='nom_estado',
                names='tipo_cluster',
                title="Distribuição de Estados por Tipo de Cluster"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    def show_reports_tab(self, data):
        """Tab de relatórios"""
        
        st.header("📋 Relatórios e Insights")
        
        # Resumo executivo
        st.subheader("Resumo Executivo")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            ### Principais Descobertas:
            
            🔴 **Anomalias Críticas Detectadas**
            - Usinas com >70% de constrained-off
            - Variações bruscas na geração (Z-score >3)
            - Padrões temporais recorrentes
            
            🟡 **Impactos Identificados**
            - Perda significativa de geração renovável
            - Instabilidade na rede elétrica
            - Problemas regionais de transmissão
            
            🟢 **Oportunidades de Melhoria**
            - Otimização do despacho
            - Investimento em infraestrutura
            - Implementação de alertas automáticos
            """)
        
        with col2:
            st.markdown("""
            ### Recomendações Prioritárias:
            
            **Curto Prazo (1-3 meses):**
            1. Monitoramento em tempo real
            2. Alertas automáticos
            3. Otimização operacional
            
            **Médio Prazo (3-12 meses):**
            1. Modelos preditivos
            2. Otimização automática
            3. Infraestrutura crítica
            
            **Longo Prazo (1-3 anos):**
            1. Integração meteorológica
            2. Machine Learning
            3. Armazenamento de energia
            """)
        
        # Download de relatórios
        st.subheader("Download de Relatórios")
        
        if st.button("📥 Gerar Relatório Completo"):
            st.info("Relatório sendo gerado... (funcionalidade em desenvolvimento)")
        
        if st.button("📊 Exportar Dados para Excel"):
            st.info("Exportação sendo preparada... (funcionalidade em desenvolvimento)")

def main():
    """Função principal do dashboard"""
    
    # Criar e executar dashboard
    dashboard = ConstrainedOffDashboard()
    dashboard.create_dashboard()

if __name__ == "__main__":
    main() 