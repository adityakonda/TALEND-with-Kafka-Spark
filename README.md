# TALEND-with-Kafka-Spark

## Big Data Advance - Spark 6.0 ##

1. **Publishing Messages to Kafka Topic**
2. **Consuming Messsage from Kafka**


## Kafka Commands ##

1.  ###### List all Topic in a Kafka ######
```
	 $ kafka-topics --zookeeper quickstart.cloudera:2181 --list 
```
	
2. ###### Create a Topic in Kafka ######
```
	$ kafka-topics --create --zookeeper quickstart.cloudera:2181 --replication-factor 1 --partitions 3 --topic mytopic
```
3.  ###### Creating Producer in Kafka ######
```
	$  kafka-console-producer --broker-list quickstart.cloudera:9092 --topic mytopic
```
4. ###### Creating Consumer in Kafka ######
```
	$ kafka-console-consumer --zookeeper quickstart.cloudera:2181 --topic mytopic --from-beginning
```
```
import re
import collections
import os
import logging
import urllib.parse
import statistics
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_query_patterns(log_file):
    """Extracts query patterns, sort patterns, and QTime statistics per collection from INFO log entries."""
    query_data = collections.defaultdict(lambda: collections.defaultdict(list))

    # Compiled regex patterns
    info_log_regex = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\s+INFO\b")  # Match timestamp + INFO
    query_regex = re.compile(r"[?&]q=([^&\s]+)")  # Extract query string
    qtime_regex = re.compile(r"QTime=(\d+)")  # Extract QTime
    collection_regex = re.compile(r"[?&]collection=([^&\s]+)")  # Extract collection name
    sort_regex = re.compile(r"[?&]sort=([^&\s]+)")  # Extract sort pattern

    # Check if file exists
    if not os.path.exists(log_file):
        logging.error(f"File '{log_file}' not found.")
        return None

    with open(log_file, 'r', encoding='utf-8-sig') as file:
        for line in file:
            if info_log_regex.search(line):  # Process only INFO logs
                query_match = query_regex.search(line)
                qtime_match = qtime_regex.search(line)
                collection_match = collection_regex.search(line)
                sort_match = sort_regex.search(line)

                if query_match and qtime_match and collection_match:
                    raw_query = urllib.parse.unquote(query_match.group(1).strip())  # Decode URL encoding
                    collection = urllib.parse.unquote(collection_match.group(1).strip())  # Extract collection
                    qtime = int(qtime_match.group(1))  # Extract QTime as integer
                    sort_pattern = sort_match.group(1) if sort_match else "default"  # Extract sort, default if missing

                    # Normalize query pattern (remove values)
                    query_pattern = normalize_query(raw_query)

                    # Store QTime for (collection, query pattern, sort pattern)
                    query_data[collection][(query_pattern, sort_pattern)].append(qtime)

    return query_data

def normalize_query(query):
    """Replaces dynamic values in queries with placeholders to detect patterns."""
    query = re.sub(r"\b\d+\b", "<NUM>", query)  # Replace numbers
    query = re.sub(r"\b(YES|NO|TRUE|FALSE)\b", "<VAL>", query, flags=re.IGNORECASE)  # Boolean values
    query = re.sub(r"\b[A-Fa-f0-9]{8,}\b", "<ID>", query)  # Replace IDs, GUIDs
    query = re.sub(r"([a-zA-Z_]+):([^\s&]+)", r"\1:<VAL>", query)  # Handle key-value pairs more robustly
    return query

def compute_qtime_statistics(query_data):
    """Computes count, min, max, and avg QTime per (collection, query pattern, sort pattern)."""
    result = []
    for collection, queries in query_data.items():
        for (query_pattern, sort_pattern), qtimes in queries.items():
            result.append({
                "Collection": collection,
                "Query Pattern": query_pattern,
                "Sort Pattern": sort_pattern,
                "Count": len(qtimes),
                "Min QTime (ms)": min(qtimes),
                "Max QTime (ms)": max(qtimes),
                "Avg QTime (ms)": round(statistics.mean(qtimes), 2),
            })
    return result

def main(log_file):
    query_data = extract_query_patterns(log_file)

    if not query_data:
        logging.info("No query patterns found in INFO logs.")
        return

    # Compute QTime statistics
    stats = compute_qtime_statistics(query_data)

    # Convert results to DataFrame and display
    df = pd.DataFrame(stats)
    df = df.sort_values(by=["Collection", "Avg QTime (ms)"], ascending=[True, False])  # Sort by Collection & Avg QTime
    import ace_tools as tools
    tools.display_dataframe_to_user(name="Query Patterns QTime Statistics", dataframe=df)

if __name__ == "__main__":
    log_file_path = r"C:\Users\adity\Downloads\test.log"  # Update with your actual file path
    main(log_file_path)

```


@echo off
setlocal
set "input_file=hostnames.txt"
set "output_file=output.csv"

if exist "%output_file%" del "%output_file%"

echo Hostname,IP Address > "%output_file%"

for /f "usebackq delims=" %%A in ("%input_file%") do (
    echo Resolving %%A...
    for /f "tokens=2 delims=[]" %%B in ('ping -n 1 %%A ^| findstr "["') do (
        echo %%A,%%B >> "%output_file%"
        echo %%A,%%B
    )
)

echo Results saved to %output_file%
endlocal


