# Final project

*Objective:* to propose, to develop, and to present a processing workflow that
necessarily uses some of the concepts and systems discussed in this course.

## Steps (overall)

1. Define a data source. The term "big-data" here is a bit misleading, because
   we have limited resources, let us consider a valid data any data source
   requires **at least 1GB storage and/or is expensive to process**. The argument
   on why this is big data is part of your work.

2. Define the workload (see examples below). In other words, what kind of
   processing you are going to implement over this data source, why this is
   relevant to the chosen data source?

3. Define the architecture of the workflow: systems to be used, file input
   formats to be used, file output format to be used, how modules interact, etc.
   In the proposal of this project you must present your progress until here:
   data source, workload and processing architecture.

4. Implement and document your project. Important: **the application MUST be
   configured to run over Docker Swarm**.

5. Present your work, include motivation, objective, method, results and
   conclusion. You must present also some kind of performance analysis
  (throughput or latency studies).

## Questions for Project proposal (first assignment)

Assignment: (1) presentation slides containing

1. Which data source you are going to use? What is the context and motivation?
   Link to where it is available ...
2. What processing you intent to perform over the data source?
3. Why is this processing important or relevant?
4. What kind of processing is this? Batch, structured, stream, graph, etc.
5. Why this processing is challenging from the perspective of Big Data? In other
   words, why is this Big Data?
6. Architecture proposal: how you intend to process this data?

## Final project presentation (second assignment)

Assignment: (1) slides containing the presentation, including results; (2)
**Public GitHub Repo** containing documentation and code Dockerized -- in this
documentation you must include a **full report, with content, results and
discussion of the results**.

## Grading

1. In-class proposal presentation (submission: PDF slides)
2. Github repository including Code+Documentation (submission: Github Repository)
3. Final project presentation to the professor (submission: PDF slides)

Grading considers the following, but not limited to, criteria:

- **100\%**: An outstanding assignment that is significantly above average and
clearly stands out from the majority.  
- **90\%**: An above-average assignment, well-organized, with a clear presentation,
demonstrating a significant amount of work.  
- **80\%**: A good assignment that shows the students’s effort and meets all
expected criteria.  
- **70\%**: An assignment that falls short, either due to a very confusing
presentation or an effort level below average.  
- **60\%**: An assignment with significant issues, barely meeting the acceptable
standard.  
- **50\%**: Only if the assignment did not meet the expectations for the course evaluation.

## Open data sources

- [Open Data Brazil](https://dados.gov.br/)
- [Stanford Large Network Dataset Collection (SNAP)](https://snap.stanford.edu/data/index.html)
- [Kaggle Datasets](https://www.kaggle.com/datasets)
- [Hugging Face Datasets](https://huggingface.co/docs/datasets/en/index)
- [Zenodo](https://zenodo.org/)
- [Awesome Public Datasets](https://github.com/awesomedata/awesome-public-datasets)
- [Awesome JSON Datasets](https://github.com/jdorfman/awesome-json-datasets)
- You can also use APIs to generate the data (ask the professor)
- Any other [open data](https://en.wikipedia.org/wiki/Open_data) resource (ask
the professor)

## Workload examples

What to do with my data? A few examples ...

- **Batch Processing**: Load large CSV files into memory-aware data structures (e.g., Pandas, Dask, or Spark DataFrames).  
- **Streaming Processing**: Process CSV data in chunks to handle large-scale data (e.g., using `csv.reader()` in Python, `read_csv` with `chunksize`, or Dask/Spark streaming).  
- **Schema Validation**: Ensure correct column types, missing values handling, and format consistency.  
- **Handling Missing Values**: Drop, impute (mean, median, mode), or flag missing entries.  
- **Type Conversion & Normalization**: Convert string to numeric, categorical encoding, date-time parsing.  
- **Deduplication & Anomaly Detection**: Identify and remove duplicate or inconsistent records.  
- **Standardization**: Uniform formats for text (lowercasing, trimming), numbers (scaling), and dates (UTC conversion).  
- **Data Splitting & Sampling**: Select subsets for further analysis, balancing different categories.  
- **Merging Multiple CSVs**: Inner, left, right, and outer joins to unify datasets.  
- **Grouping & Aggregation**: Compute statistics (sum, mean, count, percentiles) on different levels (e.g., per user, per timestamp).  
- **Pivoting & Reshaping**: Transform data layouts for easier analysis (wide vs. long format).  
- **Indexing for Faster Lookups**: Using hash maps, multi-level indexes, or precomputed tables.  
- **Window Functions**: Compute rolling statistics (moving average, cumulative sum).  
- **Multi-threading & Parallelism**: Speed up computations using multiprocessing, Dask, or Spark.  
- **Chunked Processing for Large Files**: Process CSVs in partitions to avoid memory overflows.  
- **Distributed Computation**: Run jobs on a cluster (e.g., Apache Spark, Ray).  
- **Vectorized Operations**: Use NumPy/Pandas instead of loops for performance optimization.  
- **Compression & Efficient Storage**: Convert CSV to more efficient formats (Parquet, Arrow, HDF5).  
- **Detecting Invalid Entries**: Identify impossible values (e.g., negative ages, incorrect timestamps).  
- **Cross-checking Constraints**: Ensure relationships hold (e.g., sum of parts equals the whole).  
- **Duplicate Record Detection**: Identify records that may be unintended duplicates with fuzzy matching.  
- **Time-Series Data Handling**: Resampling, interpolation, and anomaly detection over time-based data.  
- **Sliding Window Analysis**: Compute statistics over a rolling window (e.g., last 10 events).  
- **Lagged Feature Computation**: Compute differences between past and current values.  
- **Event Deduplication & Sessionization**: Identify user sessions from logs based on time gaps.  
- **CSV to Graph Conversion**: Convert tabular data into a node-edge structure.  
- **Connected Component Analysis**: Identify clusters or communities in related data.  
- **Path & Distance Computation**: Calculate shortest paths between related entities.  
- **Link Prediction**: Infer missing relationships based on existing data patterns.  
- **CSV to Database**: Load structured data into SQL or NoSQL databases.  
- **Data Format Conversion**: Convert CSV to JSON, Parquet, Arrow, or HDF5 for better efficiency.  
- **Report Generation**: Generate summary tables, charts, or interactive dashboards from CSV data.  
- **APIs & Data Feeds**: Stream or serve CSV data for integration with other systems.  

## Frequently Asked Questions

1. How to describe a data architecture? There is no final answer, but basically you
   need to provide a diagram showing how the data flows in your system,
including details such as file formats (if any), processing modules, output,
etc. Below an example of a clear an comprehensible architecture diagram:

<center>
<img src="https://miro.medium.com/v2/resize:fit:1100/format:webp/0*Mu6CzNQKbugA7iZu" height=400>

source: [Himeji: A Scalable Centralized System for Authorization at Airbnb](https://medium.com/airbnb-engineering/himeji-a-scalable-centralized-system-for-authorization-at-airbnb-341664924574)
</center>

Other good reference (with not that many details) can be found [here](https://www.geeksforgeeks.org/data-architecture-diagrams/).
