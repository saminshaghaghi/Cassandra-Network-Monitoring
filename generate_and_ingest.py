import random
import time
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel

# Config 
KEYSPACE = "network_monitoring"
CONTACT_POINTS = ['127.0.0.1']

# Data Generation Settings 
N_RESOURCES = 200 
METRICS_BASE = {
    'router': ['cpu_usage', 'memory_usage', 'packets_in', 'packets_out', 'latency_ms'],
    'firewall': ['active_connections', 'dropped_packets', 'cpu_usage', 'bytes_in', 'bytes_out'],
    'switch': ['port_1_errors', 'port_2_errors', 'packets_total', 'cpu_usage', 'uptime_sec']
} 

# Time Range Settings 
DAYS_TO_GENERATE = 7 
DATA_INTERVAL_MINUTES = 5 
START_DATE = datetime(2023, 1, 1)

def get_resource_list():
    """Generates 200 sample resources."""
    resources = []
    for i in range(N_RESOURCES):
        res_type = random.choice(list(METRICS_BASE.keys()))
        resources.append(f"{res_type}-{i+1}")
    print(f"Generated {len(resources)} sample resources...")
    return resources

def connect_to_cassandra():
    """Connects to Cassandra and sets keyspace."""
    try:
        cluster = Cluster(CONTACT_POINTS)
        session = cluster.connect()
        session.set_keyspace(KEYSPACE)
        print("Successfully connected to Cassandra.")
        return cluster, session
    except Exception as e:
        print(f"Error connecting to Cassandra: {e}")
        return None, None

def generate_and_ingest():
    cluster, session = connect_to_cassandra()
    if not session:
        return

    # Prepare CQL statements
    try:
        stmt_q1 = session.prepare(
            f"INSERT INTO {KEYSPACE}.metrics_by_resource_week (resource, year, week_of_year, metric_name, collected_at, value) VALUES (?, ?, ?, ?, ?, ?)"
        )
        stmt_q2 = session.prepare(
            f"INSERT INTO {KEYSPACE}.metrics_by_resource_list (resource, metric_name) VALUES (?, ?)"
        )
    except Exception as e:
        print(f"Error preparing statements. Did you create the tables? Error: {e}")
        cluster.shutdown()
        return

    resources = get_resource_list()
    current_time = START_DATE
    end_time = START_DATE + timedelta(days=DAYS_TO_GENERATE)
    
    total_inserts = 0
    print(f"Starting data generation and ingestion from {START_DATE} to {end_time}...")
    
    start_of_run = time.time()

    while current_time < end_time:
    
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        
        for resource in resources:
            res_type = resource.split('-')[0]
            metrics = METRICS_BASE.get(res_type, [])
            
            for metric in metrics:
                cal = current_time.isocalendar()
                year = cal[0]
                week = cal[1] 
                
                value = random.uniform(10.0, 5000.0)
               
                batch.add(stmt_q1, (resource, year, week, metric, current_time, value))
                batch.add(stmt_q2, (resource, metric))

        # Execute the batch
        try:
            session.execute(batch)
        except Exception as e:
            print(f"Error executing batch: {e}")
            
        total_inserts += len(batch)
        if total_inserts % 10000 == 0:
            print(f" ... {total_inserts} records inserted. (Last timestamp: {current_time})")

        # Move to next time interval
        current_time += timedelta(minutes=DATA_INTERVAL_MINUTES)

    end_of_run = time.time()
    print("--- Data Ingestion Complete ---")
    print(f"Total records inserted: {total_inserts}")
    print(f"Total execution time: {end_of_run - start_of_run:.2f} seconds")

    # Clean up
    cluster.shutdown()

if __name__ == "__main__":
    generate_and_ingest()