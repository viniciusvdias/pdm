import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv("/home/matheus-furtado/Documentos/big-sales-predictor/results/resultados.csv")

sns.set(style="whitegrid")
plt.rcParams["axes.titlesize"] = 14
plt.rcParams["axes.labelsize"] = 12

plt.figure(figsize=(10, 6))
tempo_df = df.set_index("Variação")[["Preproc. (s)", "Treino (s)", "Predição (s)"]]
tempo_df.plot(kind="bar", figsize=(10, 6))
plt.title("Tempo de Execução por Etapa")
plt.ylabel("Tempo (s)")
plt.xlabel("Variação")
plt.xticks(rotation=45)
plt.tight_layout()
plt.legend(title="Etapas")
plt.savefig("tempo_execucao.png")
plt.close()

plt.figure(figsize=(10, 6))
erro_df = df.set_index("Variação")[["MAE (R$)", "RMSE (R$)"]]
erro_df.plot(kind="bar", figsize=(10, 6), color=["#ff7f0e", "#1f77b4"])
plt.title("Métricas de Erro")
plt.ylabel("Erro (R$)")
plt.xlabel("Variação")
plt.xticks(rotation=45)
plt.tight_layout()
plt.legend(title="Métricas")
plt.savefig("metricas_erro.png")
plt.close()

plt.figure(figsize=(8, 5))
sns.barplot(x="Variação", y="R²", data=df, palette="viridis")
plt.title("Coeficiente de Determinação (R²)")
plt.ylabel("R²")
plt.xlabel("Variação")
plt.ylim(0.7, 0.9)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("r2.png")
plt.close()

print("✅ Gráficos salvos na pasta 'graficos/' com sucesso.")
