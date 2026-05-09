# Databricks Batch Analytics Spark Optimization

Production-grade batch analytics lakehouse built using PySpark, Delta Lake, and Databricks-style Medallion Architecture.

This project focuses on advanced Spark batch processing and Spark performance engineering concepts commonly used in large-scale production analytics systems and senior data engineering interviews.

---

# Project Goal

The goal of this project is to demonstrate:

- Large-scale batch analytics processing
- Distributed joins and aggregations
- Delta Lake medallion architecture
- Spark performance optimization techniques
- Spark physical plan analysis
- Shuffle tuning
- Broadcast joins
- AQE (Adaptive Query Execution)
- Data skew analysis and salting
- Caching and persistence
- Repartition vs coalesce behavior

---

# Architecture

```text
Raw CSV Files
      ↓
Bronze Delta Layer
      ↓
Silver Delta Layer
      ↓
Gold Analytics Layer
```

---

# Tech Stack

- PySpark
- Delta Lake
- Python
- Pandas
- Faker
- Databricks-style Lakehouse Design
- Spark SQL
- Adaptive Query Execution (AQE)

---

# Datasets

The project uses synthetic retail analytics datasets.

| Dataset | Description |
|---|---|
| products | Product catalog and categories |
| customers | Customer dimension data |
| orders | Order transactions |
| order_items | Line-item transactional data |

---

# Medallion Architecture

## Bronze Layer

Raw ingestion layer storing source data in Delta format.

### Features

- Raw CSV ingestion
- Audit columns
- Delta storage
- Schema inference
- Source lineage tracking

---

## Silver Layer

Cleaned and validated business-ready datasets.

### Features

- Data quality validation
- Null filtering
- Deduplication
- Standardization
- Recomputed metrics
- Trusted business datasets

---

## Gold Layer

Business analytics and Spark optimization experiments.

### Features

- Multi-table joins
- Aggregations
- Performance tuning
- Spark optimization analysis
- Physical plan inspection

---

# Spark Concepts Demonstrated

| Concept | Status |
|---|---|
| Delta Lake | ✅ |
| Medallion Architecture | ✅ |
| Broadcast Joins | ✅ |
| AQE | ✅ |
| Shuffle Partition Tuning | ✅ |
| Physical Plan Analysis | ✅ |
| Data Skew Analysis | ✅ |
| Salting | ✅ |
| Caching / Persistence | ✅ |
| Repartition vs Coalesce | ✅ |
| Aggregations & Joins | ✅ |

---

# Gold Analytics Use Case

## Daily Sales by Category

Built a Gold analytics table answering:

> “How much revenue is generated per category per day?”

### Pipeline

```text
orders
   JOIN
order_items
   JOIN
products
   ↓
daily_sales_by_category
```

### Metrics

- Total sales
- Total items sold
- Total orders
- Daily category-level aggregations

---

# Performance Optimization Experiments

## 1. Shuffle Partition Tuning

Compared Spark shuffle partition counts for aggregation workloads.

| Shuffle Partitions | Runtime |
|---|---:|
| 200 | 11.37 sec |
| 8 | 0.62 sec |

### Key Learning

Reducing shuffle partitions significantly improved runtime for this dataset by lowering task scheduling and shuffle overhead. AQE still coalesced partitions dynamically, but the initial shuffle partition count had a major impact on performance.

---

## 2. Broadcast Join Optimization

Compared SortMergeJoin vs BroadcastHashJoin strategies.

| Experiment | Runtime |
|---|---:|
| Broadcast disabled (`autoBroadcastJoinThreshold = -1`) | 20.68 sec |
| Automatic broadcast enabled | 3.79 sec |
| Explicit broadcast hint | 4.32 sec |

### Key Learning

When broadcast joins were disabled, Spark used `SortMergeJoin`, which required expensive shuffle and sort operations on both sides of the join.

With broadcast enabled, Spark switched to `BroadcastHashJoin`, avoiding large shuffle operations and significantly improving runtime.

---

## 3. Adaptive Query Execution (AQE)

Compared static execution vs adaptive runtime optimization.

| AQE Setting | Runtime |
|---|---:|
| AQE disabled | 34.21 sec |
| AQE enabled | 2.90 sec |

### Key Learning

With AQE disabled, Spark executed a static physical plan without runtime optimizations.

With AQE enabled, Spark dynamically optimized shuffle partitions and execution strategy during runtime using `AdaptiveSparkPlan`, dramatically improving performance.

---

## 4. Data Skew Analysis

Generated intentional category skew to simulate real-world hot-key imbalance.

### Category Distribution

| Category | Row Count |
|---|---:|
| electronics | 120,606 |
| grocery | 26,209 |
| clothing | 25,930 |
| home | 25,775 |
| sports | 25,686 |
| toys | 25,483 |
| automotive | 25,388 |
| beauty | 24,924 |

### Partition Distribution Before Salting

```text
Partition row counts: [50312, 26209, 120606, 51616, 51258]
Max partition size: 120606
Min partition size: 26209
```

### Key Learning

The skewed `electronics` category caused one partition to become nearly 4.6x larger than the smallest partition, demonstrating how hot keys can create straggler tasks and uneven workload distribution in Spark.

---

## 5. Salting for Skew Mitigation

Applied salting to distribute the skewed `electronics` category across multiple buckets.

### Salted Category Distribution

| Salted Category | Row Count |
|---|---:|
| electronics_4 | 24,219 |
| electronics_3 | 24,212 |
| electronics_0 | 24,110 |
| electronics_2 | 24,106 |
| electronics_1 | 23,959 |

### Partition Distribution After Salting

```text
Partition row counts: [49607, 48883, 74425, 75828, 51258]
Max partition size: 75828
Min partition size: 48883
```

### Key Learning

Salting distributed the skewed category across multiple partition keys, significantly reducing partition imbalance and improving workload distribution.

---

## 6. Caching / Persistence

Compared repeated operations with and without caching.

| Experiment | Runtime |
|---|---:|
| Caching disabled | 20.21 sec |
| Caching enabled | 1.96 sec |

### Key Learning

Caching avoided recomputation of expensive joins and transformations across multiple actions, significantly improving repeated query performance.

---

## 7. Repartition vs Coalesce

Compared repartitioning and coalescing behavior.

### Repartition(5)

```text
Partition sizes: [2923, 2923, 2923, 2923, 2923]
Runtime: 1.17 sec
```

### Coalesce(5)

```text
Partition sizes: [2922, 2923, 2923, 2924, 2923]
Runtime: 0.70 sec
```

### Key Learning

`repartition()` performs a full shuffle and redistributes data evenly, while `coalesce()` reduces partitions with minimal shuffle overhead and is more efficient for reducing output file counts.

---

# Physical Plan Analysis

The project extensively analyzes Spark execution plans using:

`python
df.explain(True)
`

Key Spark execution concepts observed:

- BroadcastHashJoin
- SortMergeJoin
- Exchange / shuffle
- HashAggregate
- AQE
- AdaptiveSparkPlan
- Column pruning
- Shuffle coalescing

---

# Project Structure

```text
databricks-batch-analytics-spark-optimization/
│
├── configs/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── data_generator/
│
├── src/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── utils/
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

# How to Run

## Install dependencies

```bash
pip install -r requirements.txt
```

---

## Generate datasets

```bash
python3 -m data_generator.generate_products
python3 -m data_generator.generate_customers
python3 -m data_generator.generate_orders
python3 -m data_generator.generate_order_items
```

---

## Bronze ingestion

```bash
python3 -m src.bronze.ingest_bronze
python3 -m src.bronze.validate_bronze
```

---

## Silver processing

```bash
python3 -m src.silver.process_products
python3 -m src.silver.process_customers
python3 -m src.silver.process_orders
python3 -m src.silver.process_order_items
```

---

## Gold analytics

```bash
python3 -m src.gold.daily_sales_by_category
```

---

## Optimization experiments

```bash
python3 -m src.gold.shuffle_partition_experiment

python3 -m src.gold.broadcast_join_experiment

python3 -m src.gold.aqe_experiment

python3 -m src.gold.skew_analysis

python3 -m src.gold.salting_experiment

python3 -m src.gold.cache_experiment

python3 -m src.gold.repartition_vs_coalesce
```

---

# Project Summary

I built a production-style batch analytics lakehouse using PySpark and Delta Lake focused on advanced Spark performance engineering.

The project demonstrates distributed joins, aggregations, shuffle optimization, AQE, broadcast joins, skew handling, salting, caching, repartitioning behavior, and Spark physical plan analysis using realistic retail analytics workloads and synthetic skewed datasets.