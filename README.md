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

import os
import re
import collections

def extract_info_queries_from_folder(folder_path):
    """Extracts query patterns from all log files in a folder."""
    collection_queries = collections.defaultdict(collections.Counter)

    # Get all .log files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".log"):  # Process only .log files
            file_path = os.path.join(folder_path, filename)
            print(f"Processing: {file_path}")  # Debug: Show which file is being processed
            process_log_file(file_path, collection_queries)  # Process each file

    return collection_queries

def process_log_file(log_file, collection_queries):
    """Extracts queries from a single log file and updates collection_queries dictionary."""
    info_log_regex = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\s+INFO\b")  # Match timestamp + INFO
    collection_regex = re.compile(r"\[c:\s*([\w\s-]+)]")  # Extract collection name
    query_regex = re.compile(r"q=([^&\s]+)")  # Extract query string

    with open(log_file, 'r', encoding='utf-8-sig') as file:
        for line in file:
            if info_log_regex.search(line):  # Process only INFO logs
                collection_match = collection_regex.search(line)
                query_match = query_regex.search(line)

                if collection_match and query_match:
                    collection = collection_match.group(1).strip()  # Extract collection name
                    raw_query = query_match.group(1).strip()  # Extract raw query

                    # Normalize query pattern
                    query_pattern = normalize_query(raw_query)

                    # Count occurrences of query patterns per collection
                    collection_queries[collection][query_pattern] += 1

def normalize_query(query):
    """Replaces dynamic values in queries with placeholders to detect patterns."""
    query = re.sub(r"\b\d+\b", "<NUM>", query)  # Replace numbers
    query = re.sub(r"\b(YES|NO|TRUE|FALSE)\b", "<VAL>", query, flags=re.IGNORECASE)  # Boolean values
    query = re.sub(r"\b[A-Fa-f0-9]{8,}\b", "<ID>", query)  # Replace IDs, GUIDs
    query = re.sub(r"([a-zA-Z_]+):([^:\s]+)", r"\1:<VAL>", query)  # Replace key-value pairs
    return query

def main(folder_path):
    collection_queries = extract_info_queries_from_folder(folder_path)

    if not collection_queries:
        print("\nNo INFO log entries with queries found in the folder.")
        return

    # Display results
    print("\nQuery Patterns Count Per Collection (INFO Logs Only):\n")
    for collection, query_patterns in collection_queries.items():
        print(f"\nCollection: {collection}")
        for pattern, count in sorted(query_patterns.items(), key=lambda x: x[1], reverse=True):
            print(f"  Pattern: {pattern} -> Count: {count}")

if __name__ == "__main__":
    folder_path = r"C:\Users\adity\Downloads\logs_folder"  # Update with your actual folder path
    main(folder_path)


def normalize_query(query):
    """Replaces multiple values in queries with placeholders and ensures single placeholders inside parentheses."""
    
    # Replace multiple numbers inside parentheses with a single <NUM>
    query = re.sub(r"\(\d+(?:\s+\d+)*\)", "(<NUM>)", query)
    
    # Replace multiple numbers (outside parentheses) with a single <NUM>
    query = re.sub(r"\b\d+(\s+\d+)*\b", "<NUM>", query)

    # Replace boolean values (YES/NO/TRUE/FALSE) with <VAL>
    query = re.sub(r"\b(YES|NO|TRUE|FALSE)\b", "<VAL>", query, flags=re.IGNORECASE)

    # Replace multiple <VAL> occurrences inside parentheses with a single <VAL>
    query = re.sub(r"\([^)]+\)", "(<VAL>)", query)

    # Replace GUIDs, hashes, and long alphanumeric IDs with <ID>
    query = re.sub(r"\b[A-Fa-f0-9]{8,}\b", "<ID>", query)

    # Replace key=value pairs with key:<VAL> for consistency
    query = re.sub(r"([a-zA-Z_]+):([^:\s]+)", r"\1:(<VAL>)", query)

    return query



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


