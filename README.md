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

def extract_log_details(log_file):
    """Extracts log level, collection name, and query pattern from Solr logs and groups them."""
    log_data = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(int)))

    # Regex patterns
    log_level_regex = re.compile(r"\b(INFO|WARN|ERROR)\b")  # Extract log level
    collection_regex = re.compile(r"\[c:\s*([^]\s]+)]")  # Extract collection name
    query_regex = re.compile(r"q=([^&\s]+)")  # Extract query

    with open(log_file, 'r', encoding='utf-8') as file:
        for line in file:
            log_level_match = log_level_regex.search(line)
            collection_match = collection_regex.search(line)
            query_match = query_regex.search(line)

            if log_level_match and collection_match and query_match:
                log_level = log_level_match.group(1)  # Extract log level (INFO, WARN, ERROR)
                collection = collection_match.group(1)  # Extract collection name
                raw_query = query_match.group(1)  # Extract query

                # Normalize query pattern
                query_pattern = normalize_query(raw_query)

                # Group under Log Level -> Collection -> Query Pattern
                log_data[log_level][collection][query_pattern] += 1

    return log_data

def normalize_query(query):
    """Replaces dynamic values in queries with placeholders for pattern recognition."""
    query = re.sub(r"\b\d+\b", "<NUM>", query)  # Replace numbers with <NUM>
    query = re.sub(r"\b(YES|NO|TRUE|FALSE)\b", "<VAL>", query, flags=re.IGNORECASE)  # Boolean values
    query = re.sub(r"\b[A-Fa-f0-9]{8,}\b", "<ID>", query)  # Replace IDs, GUIDs
    query = re.sub(r"([a-zA-Z_]+):([^:\s]+)", r"\1:<VAL>", query)  # General field:value placeholders
    return query

def main(log_file):
    log_data = extract_log_details(log_file)

    # Display results grouped by Log Level -> Collection -> Query Pattern
    print("\nGrouped Log Details:\n")
    for log_level, collections in log_data.items():
        print(f"\n### Log Level: {log_level}")
        for collection, query_patterns in collections.items():
            print(f"  - Collection: {collection}")
            for pattern, count in sorted(query_patterns.items(), key=lambda x: x[1], reverse=True):
                print(f"    - Pattern: {pattern} -> Count: {count}")

if __name__ == "__main__":
    log_file_path = "solr_logs.txt"  # Replace with your actual log file path
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


