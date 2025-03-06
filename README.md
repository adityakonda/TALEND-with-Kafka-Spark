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
from datetime import datetime

def extract_timestamps_from_folder(folder_path):
    """Extracts all timestamps from log files in a folder."""
    timestamp_regex = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}")  # Match timestamp format
    timestamps = []

    # Loop through all log files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".log"):  # Process only .log files
            file_path = os.path.join(folder_path, filename)
            print(f"Processing file: {file_path}")  # Debug: Show which file is being processed
            with open(file_path, 'r', encoding='utf-8-sig') as file:
                for line in file:
                    match = timestamp_regex.search(line)
                    if match:
                        timestamps.append(match.group())  # Store timestamp strings

    return timestamps

def calculate_time_range(timestamps):
    """Finds min(TS), max(TS) and calculates time difference."""
    if not timestamps:
        return None, None, None  # Return None if no timestamps found

    # Convert timestamps to datetime objects
    datetime_timestamps = [datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f") for ts in timestamps]

    # Get min and max timestamps
    min_ts = min(datetime_timestamps)
    max_ts = max(datetime_timestamps)

    # Calculate total time difference
    total_time_diff = (max_ts - min_ts).total_seconds()  # Convert to seconds

    return min_ts, max_ts, total_time_diff

def main(folder_path):
    timestamps = extract_timestamps_from_folder(folder_path)

    if not timestamps:
        print("No timestamps found in the log files.")
        return

    min_ts, max_ts, total_diff = calculate_time_range(timestamps)

    # Print the results
    print("\nTimestamp Summary:")
    print(f"Min(TS) : {min_ts}")
    print(f"Max(TS) : {max_ts}")
    print(f"Total Time Difference: {total_diff} seconds")

if __name__ == "__main__":
    folder_path = r"C:\Users\adity\Downloads\logs_folder"  # Update with your actual folder path
    main(folder_path)



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


