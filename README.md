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

def extract_query_patterns(log_file):
    """Extracts and counts unique query patterns from INFO log entries."""
    query_patterns = collections.Counter()

    # Updated regex patterns
    info_log_regex = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\s+INFO\b")  # Match timestamp + INFO
    query_regex = re.compile(r"[?&]q=([^&\s]+)")  # Extract query string

    # Check if file exists
    if not os.path.exists(log_file):
        print(f"Error: File '{log_file}' not found.")
        return None

    with open(log_file, 'r', encoding='utf-8-sig') as file:
        for line in file:
            if info_log_regex.search(line):  # Process only INFO logs
                query_match = query_regex.search(line)
                if query_match:
                    raw_query = query_match.group(1).strip()  # Extract raw query
                    
                    # Normalize query pattern (remove values)
                    query_pattern = normalize_query(raw_query)

                    # Count the query patterns
                    query_patterns[query_pattern] += 1

    return query_patterns

def normalize_query(query):
    """Replaces dynamic values in queries with placeholders to detect patterns."""
    query = re.sub(r"\b\d+\b", "<NUM>", query)  # Replace numbers
    query = re.sub(r"\b(YES|NO|TRUE|FALSE)\b", "<VAL>", query, flags=re.IGNORECASE)  # Boolean values
    query = re.sub(r"\b[A-Fa-f0-9]{8,}\b", "<ID>", query)  # Replace IDs, GUIDs
    query = re.sub(r"([a-zA-Z_]+):([^:\s]+)", r"\1:<VAL>", query)  # Replace key-value pairs
    return query

def main(log_file):
    query_patterns = extract_query_patterns(log_file)

    if not query_patterns:
        print("\nNo query patterns found in INFO logs.")
        return

    # Display results
    print("\nQuery Patterns Count (INFO Logs Only):\n")
    for pattern, count in sorted(query_patterns.items(), key=lambda x: x[1], reverse=True):
        print(f"Pattern: {pattern} -> Count: {count}")

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


