# Plano de Desenvolvimento — Preprocessador Paralelizado

## Objetivo

Adaptar o `DataSetSummary` do VisFlow-MM para processar datasets grandes em chunks paralelos,
variando o número de workers e medindo speedup, throughput e eficiência.

---

## Estrutura de Arquivos do Projeto

O projeto novo (separado do VisFlow-MM principal) terá a seguinte organização:

```
bigdata-preprocessing/
├── docker-compose.yml          ← orquestra os serviços
├── Dockerfile                  ← imagem do preprocessador
├── src/
│   ├── profiler.py             ← lógica de profiling por chunk
│   ├── aggregator.py           ← merge dos resultados parciais
│   └── benchmark.py            ← harness que roda os experimentos
├── datasets/                   ← montado como volume (arquivos grandes ficam aqui)
└── results/                    ← montado como volume (CSVs de saída ficam aqui)
```

O código-fonte (`src/`) é montado como volume no container, então qualquer edição
local é refletida imediatamente sem precisar rebuildar a imagem.

---

## Configuração Docker

### Imagem

Baseada em Python 3.11 slim. Dependências instaladas via `requirements.txt` (pandas,
numpy, etc.). O código entra via volume, não copiado para a imagem — isso agiliza
o ciclo de desenvolvimento.

### docker-compose.yml — decisões principais

- **Volumes**: `./src` mapeado para dentro do container; `./datasets` mapeado para
  onde o código espera os arquivos; `./results` mapeado para onde o código salva os CSVs.

- **CPUs**: o serviço **não** tem limite de CPU configurado (`cpus:` ausente),
  portanto o container enxerga e pode usar todos os núcleos do host. Isso é intencional
  para que o experimento com 8 workers realmente execute em paralelo.

- **N_WORKERS**: passado como variável de ambiente. Para rodar experimentos diferentes,
  basta trocar o valor e subir novamente — não precisa alterar o código.

- **DATASET_PATH**: também variável de ambiente, apontando para o arquivo dentro
  do volume de datasets.

### Fluxo de execução

Ao rodar `docker compose up`, o container inicia, lê `N_WORKERS` e `DATASET_PATH`
do ambiente, executa o benchmark completo (N=3 repetições por configuração) e
salva o resultado em `/results/metrics.csv`. Ao terminar, o container para.
Os resultados ficam acessíveis na pasta `results/` no host.

---

## Passo 1 — Versão Serial Baseline

**O que fazer**: reescrever o `DataSetSummary.summarize()` para ler o CSV em chunks
sequenciais usando o `chunksize` do pandas. Para cada chunk, computar as estatísticas
parciais. No final, agregar todos os parciais num resultado único.

**Por que isso antes do paralelo**: essa versão serial com chunks é o baseline correto.
Ela já garante que datasets maiores que a RAM funcionem, e serve de referência para
calcular o speedup das versões paralelas.

**Estatísticas por chunk** (o que extrair de cada pedaço):
- Contagem de nulos por coluna
- Mínimo e máximo por coluna numérica
- Soma e contagem para calcular média ponderada depois
- Conjunto de valores únicos para colunas categóricas com baixa cardinalidade
- Contagem de aparições de cada valor (para `value_counts`)

**Agregação final**:
- Nulos: soma simples entre chunks
- Min/Max: min dos mínimos, max dos máximos
- Média: média ponderada pelo tamanho de cada chunk
- Desvio padrão: recalculado a partir de soma e soma dos quadrados (não dá somar diretamente)
- Valores únicos: união dos conjuntos — **ponto de atenção**: pra colunas de alta
  cardinalidade isso pode ser caro em memória; nesse caso, guardar apenas a contagem
  aproximada e documentar a limitação

---

## Passo 2 — Versão Paralela com ProcessPoolExecutor

**O que fazer**: substituir o loop serial sobre os chunks por um pool de processos.
Cada worker recebe um chunk e devolve as estatísticas parciais. O processo principal
coleta todos os resultados e agrega.

**Por que ProcessPoolExecutor e não ThreadPoolExecutor**:
O profiling (nunique, value_counts, min, max) é CPU-bound. O GIL do Python impede
que threads rodem em paralelo real para trabalho CPU-bound. Processos separados não
têm esse problema. O custo é que cada chunk precisa ser serializado (pickle) para
ser enviado ao worker — isso é overhead real que vai aparecer nas medições.

**Parâmetro N_WORKERS**:
Lido da variável de ambiente. Valores a testar: 1, 2, 4, 8 (e possivelmente 16
dependendo do host). O valor 1 com ProcessPoolExecutor ainda tem overhead de
processo, então vai ser ligeiramente mais lento que o serial puro — isso é esperado
e interessante de observar.

**Comportamento esperado**:
- De 1 para 2 workers: ganho significativo
- De 2 para 4: ainda bom ganho
- De 4 para 8: ganho menor, começa a aparecer overhead de coordenação e contenção de I/O
- Além do número de CPUs físicas: pode piorar (mais workers que núcleos = thrashing)

---

## Passo 3 — Harness de Benchmark

**O que fazer**: script separado que automatiza os experimentos. Ele não deve
conter lógica de profiling — só orquestra chamadas ao profiler e registra os tempos.

**Configurações a executar** (cada uma 3 vezes):
- 1 GB com 1 worker (baseline serial)
- 1 GB com 2 workers
- 1 GB com 4 workers
- 1 GB com 8 workers
- 5 GB com 1 worker
- 5 GB com 4 workers
- 5 GB com 8 workers

**O que registrar por execução**:
- Volume do dataset
- Número de workers
- Número da repetição (1, 2 ou 3)
- Tempo total em segundos
- Throughput calculado: tamanho do arquivo em MB dividido pelo tempo

**Cálculos derivados** (feitos depois, na análise):
- Média e desvio padrão das 3 repetições
- Speedup: tempo serial dividido pelo tempo paralelo
- Eficiência: speedup dividido pelo número de workers × 100

**Saída**: arquivo `results/metrics.csv` com uma linha por execução.

---

## Passo 4 — Análise dos Resultados

**O que fazer**: script de análise que lê o CSV de métricas e gera os gráficos.

**Gráficos a gerar**:
1. Speedup vs. número de workers (uma linha por volume de dataset)
2. Eficiência paralela vs. número de workers (mostra o ponto de saturação)
3. Throughput (MB/s) vs. número de workers
4. Tempo médio com barras de erro (desvio padrão das 3 repetições)

**Pontos de discussão esperados nos resultados**:
- Speedup nunca é linear (lei de Amdahl — parte do código é serial)
- Com muitos workers, overhead de serialização e I/O começa a dominar
- Volumes maiores tendem a escalar melhor (overhead fixo se dilui)

---

## Passo 5 — Extensão Spark (opcional)

Reimplementar o mesmo profiling usando PySpark. Adicionar um segundo serviço no
docker-compose (Spark master + worker). Rodar os experimentos E8 e E9 da proposta
(1 GB e 5 GB com 4 workers) e comparar diretamente com o multiprocessing.

O ponto central da comparação é o overhead do Spark (JVM, scheduler, serialização)
vs. o ganho em datasets muito grandes onde a paralelização distribuída compensa.

---

## Ordem Recomendada de Desenvolvimento

1. Criar estrutura de pastas e `Dockerfile` mínimo funcional
2. Montar `docker-compose.yml` com volumes e variáveis de ambiente
3. Validar que o container sobe e enxerga os arquivos de dataset
4. Implementar o profiler serial com chunks e testar com dataset pequeno (~100MB)
5. Implementar a agregação e validar que os resultados batem com o pandas direto
6. Adicionar o ProcessPoolExecutor e testar com 2 workers
7. Implementar o harness de benchmark
8. Gerar datasets sintéticos nos volumes desejados (1GB, 5GB) se não tiver reais
9. Rodar todos os experimentos e validar o CSV de saída
10. Implementar o script de análise e gerar os gráficos

---

## Observações Importantes

**Datasets sintéticos**: se não tiver os datasets reais disponíveis, é válido gerar
arquivos CSV sintéticos com o número de linhas necessário para atingir o volume.
O importante é que o schema seja variado (colunas numéricas, categóricas, datas)
para estressar o profiling de forma realista.

**Reprodutibilidade**: fixar seed aleatória nos datasets sintéticos. Rodar experimentos
com o sistema em idle (sem outros processos pesados). Registrar número de CPUs e RAM
do host na documentação dos resultados.

**Limitação da cardinalidade**: documentar explicitamente que o `nunique` final é
exato apenas quando os conjuntos de valores únicos cabem em memória. Para colunas
de alta cardinalidade em datasets de 5GB+, isso pode ser inviável — mencionar
HyperLogLog como solução alternativa.
