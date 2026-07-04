# Final project report: *\<include your title here\>*

## 1. Context and motivation

- What is the main goal? What problem are you trying to solve with big data?

## 2. Data

### 2.1 Detailed description

- Describe the dataset(s) used in your project.
  - Where does the data come from? Include the source/link.
  - What does the data contain? (number of records, type of features, etc.)
  - If data is generated, explain how and include instructions to generate a sample with Docker.

### 2.2 How to obtain the data

- A small sample dataset (at most 1MB, preferably less) must be included in the `datasample/` folder of this repository. This sample is required so the project can be tested quickly.

- For the full dataset only (not the sample), provide clear instructions on how to download or generate it. Do not include the full dataset in the repository.

- For example, you can provide public links to the data and commands on how to download them using tools like `wget` (this can also be wrapped in a Docker container):
  
  ```bash
  wget https://path-to-your-public-dataset/data.zip
  unzip data.zip -d data/
  ```

- If the dataset is hosted on cloud storage (Google Drive, HTTPS, AWS S3, etc.), provide the public link and the necessary commands.

- If the data must be generated, provide the Docker command to generate it.

- **Important:** do not include full datasets into your pull request.

## 3. How to install and run

> Observation: The project must be compatible with a default Docker installation and use only Docker containers for running. No external tools or installations should be necessary — this is a strict requirement.

### 3.1 Quick start (using sample data in `datasample/`)

- Give the exact command(s) to run your project out of the box using Docker or your scripts.

- Example:
  
  ```bash
  docker compose up --build
  # or if using a script:
  ./bin/run.sh
  ```

### 3.2 How to run with the full dataset

- Explain clearly how to configure or mount the full dataset (if different from default sample).

## 4. Project architecture

- Include a diagram of your system’s architecture and a description of how the components interact. Images are encouraged for clarity.

- What are the main components? How do they interact?

- Example diagram (replace with your own):
  
  ```
  [Data Source] → [Data Ingestion] → [Processing] → [Results Storage]
  ```

- Mention which parts run in which containers, and how data flows between them.

## 5. Workloads evaluated

- Specify each big data task evaluated in your project (queries, data pre-processing, sub-routines, etc.).
- Be specific: describe the details of each workload, and give each a clear name. These named workloads will be referenced and evaluated via performance experiments in the next section.
  - Example: [WORKLOAD-1] Query that computes the average occupation within each
    time window (include query below). [WORKLOAD-2] Preprocessing, including
    removing duplicates, standardization, etc.

## 6. Experiments and results

> **MANDATORY**: This section is a core requirement of the project. You MUST perform experimental benchmarking with multiple repetitions and statistical analysis.

### 6.1 Experimental environment

- Describe the environment used for experiments (machine/VM specs, OS, Docker version, etc.).
- Example:
  
  > Experiments were run on a virtual machine with 4 vCPUs, 8GB RAM, Ubuntu 22.04, Docker 24.x.

### 6.2 How to perform benchmarking (simple guide)

**Step 1: Define what to measure**

- Choose metrics relevant to your workload:
  - **Execution time** (wall-clock time): Total time to complete the task
  - **Throughput**: Records/events processed per second
  - **Latency**: Time to process a single record/query
  - **Resource usage**: CPU, memory, disk I/O

**Step 2: Run multiple repetitions**

- **MANDATORY**: Run each experiment at least 3 times (preferably 5-10 times)
- This helps account for system variability and ensures reliable results
- Example command:
  
  ```bash
  for i in {1..5}; do
    echo "Run $i"
    docker compose up
    # For example, you stack may generate runtime statistics (metrics) in the logs
  done
  ```

**Step 3: Collect and record data**

- Create a spreadsheet or CSV file with your measurements
- Example data collection:
  
  ```
  Run 1: 45.2 seconds
  Run 2: 46.1 seconds
  Run 3: 44.8 seconds
  Run 4: 45.5 seconds
  Run 5: 45.9 seconds
  ```

**Step 4: Calculate statistics**

- **Average (mean)**: Sum all values and divide by number of runs
- **Standard deviation**: Measure of variability in your results
- Most tools (Excel, Python, R) can calculate these automatically
- Python example:
  
  ```python
  import numpy as np
  times = [45.2, 46.1, 44.8, 45.5, 45.9]
  avg = np.mean(times)
  std = np.std(times, ddof=1)  # Sample standard deviation
  print(f"Average: {avg:.2f}s ± {std:.2f}s")
  ```

**Step 5: Present results in tables**

- Use clear tables showing average ± standard deviation
- See examples in section 6.3 below

### 6.3 What did you test?

- What parameters did you vary? (e.g., data size, number of workers, batch size)
- What metrics did you measure? (execution time, throughput, memory usage, etc.)
- **MANDATORY**: For each configuration, run multiple repetitions (minimum 3) and report average and standard deviation

### 6.4 Results

**MANDATORY**: Results must include tables with average and standard deviation. Below are simple examples:

#### Example 1: Execution Time Comparison (not real measurements)

| Workload   | Configuration | Avg Time (s) | Std Dev (s) | Runs |
| ---------- | ------------- | ------------ | ----------- | ---- |
| WORKLOAD-1 | Single worker | 145.3        | 3.2         | 5    |
| WORKLOAD-1 | 2 workers     | 78.6         | 2.1         | 5    |
| WORKLOAD-1 | 4 workers     | 42.4         | 1.8         | 5    |
| WORKLOAD-2 | Single worker | 89.2         | 4.5         | 5    |
| WORKLOAD-2 | 2 workers     | 46.1         | 2.3         | 5    |

**Discussion**: Increasing the number of workers from 1 to 4 reduced execution time by ~70% for WORKLOAD-1. The low standard deviation (<5% of mean) indicates consistent performance.

#### Example 2: Throughput Analysis (not real measurements)

| Data Size (GB) | Throughput (MB/s) | Std Dev (MB/s) | Runs |
| -------------- | ----------------- | -------------- | ---- |
| 1              | 125.4             | 5.2            | 5    |
| 5              | 118.7             | 6.8            | 5    |
| 10             | 112.3             | 4.1            | 5    |
| 20             | 108.9             | 7.3            | 5    |

**Discussion**: Throughput decreased slightly (~13%) as data size increased from 1GB to 20GB, suggesting good scalability with minimal degradation.

#### Example 3: Resource Usage (not real measurements)

| Configuration    | Avg Memory (GB) | Std Dev (GB) | Peak CPU (%) | Runs |
| ---------------- | --------------- | ------------ | ------------ | ---- |
| Batch size 1000  | 2.4             | 0.15         | 78           | 3    |
| Batch size 5000  | 3.8             | 0.22         | 85           | 3    |
| Batch size 10000 | 5.2             | 0.31         | 92           | 3    |

**Discussion**: Memory usage scales linearly with batch size. Larger batches improve efficiency but require more memory.

#### Plots (recommended)

- Include bar charts or line plots showing your results
- Always include error bars representing standard deviation

**Key points for your results**:

- Always report both average AND standard deviation (or variance)
- Explain what the numbers mean in plain language
- Discuss trends, patterns, and unexpected results
- Compare different configurations or approaches
- Relate results back to your workload characteristics

## 7. Limitations and conclusions

- Summarize what worked and what did not.
- Discuss any challenges or limitations of this work.

## 8. References and external resources

- List all external resources, datasets, libraries, and tools you used (with links).
