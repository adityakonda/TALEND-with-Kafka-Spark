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
from datetime import datetime
import requests

# Get January 1st of last year
last_year_start = f"{datetime.now().year - 1}-01-01"

payload = {
    "from": "your_table_dbid",
    "select": [6, 7, 20],  # Update with relevant field IDs
    "where": f"{{20.EX.'{last_year_start}'}}"
}

headers = {
    "QB-Realm-Hostname": "yourrealm.quickbase.com",
    "User-Agent": "YourAppName",
    "Authorization": "QB-USER-TOKEN your_user_token",
    "Content-Type": "application/json"
}

response = requests.post(
    "https://api.quickbase.com/v1/records/query",
    json=payload,
    headers=headers
)

print(response.json())


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


