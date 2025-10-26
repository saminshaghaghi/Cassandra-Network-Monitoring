# Cassandra Time-Series Network Monitoring System

This project is a high-performance network monitoring system designed to handle large-scale time-series data (from 200 resources, every 5 minutes) using Apache Cassandra.

The data model is built from the ground up using Cassandra's **Query-Driven Design** philosophy to answer two key queries with high efficiency.

## Key Challenges & Data Model

The system was designed to solve two main analytical queries:
1.  **Q1:** Get all metrics for a single resource within a short time frame (< 1 week).
2.  **Q2:** Get a list of all unique metrics available for a specific resource.

To solve this, two separate tables were created:

### 1. `metrics_by_resource_week` (for Q1)
To prevent partitions from becoming too large (a common problem with time-series data), this model uses **Time Bucketing**. Data is partitioned by `(resource, year, week_of_year)`. This ensures that any query for a single week's data for one resource reads **exactly one partition**, making it extremely fast.

```cql
CREATE TABLE metrics_by_resource_week (
    resource TEXT,
    year INT,
    week_of_year INT,
    metric_name TEXT,
    collected_at TIMESTAMP,
    value DOUBLE,
    PRIMARY KEY ((resource, year, week_of_year), metric_name, collected_at)
) WITH CLUSTERING ORDER BY (metric_name ASC, collected_at DESC);

2.metrics_by_resource_list (for Q2)
This is a simple lookup table, partitioned by resource, to instantly return all available metrics for that resource.

Code snippet

CREATE TABLE metrics_by_resource_list (
    resource TEXT,
    metric_name TEXT,
    PRIMARY KEY (resource, metric_name)
);
ETL Optimization (Python)
Instead of the inefficient method of generating a massive multi-GB CSV file (100M+ rows) and re-reading it, this project uses an optimized Python script (generate_and_ingest.py).

This script performs all ETL (Generate, Transform, Load) operations in-memory:

Generate: Data is created on the fly.

Transform: Time buckets (year, week_of_year) are calculated in real-time.

Load: Data is efficiently ingested into Cassandra using BatchStatement.

Technologies Used
Apache Cassandra (NoSQL Database)

Python (for ETL)

CQL (Cassandra Query Language)

Docker (for database deployment)
