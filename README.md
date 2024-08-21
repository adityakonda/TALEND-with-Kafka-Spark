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

import random
import datetime
import json
import csv

# Function to generate a random timestamp
def random_timestamp(start_date, end_date):
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    random_timestamp = random.randint(start_timestamp, end_timestamp)
    return datetime.datetime.utcfromtimestamp(random_timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')

# Function to generate a random name
def random_name(name_list):
    return random.choice(name_list)

# List of example first names and last names
first_names = ["John", "Jane", "Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Tim"]
last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Potter"]

# Function to generate a random JSON object
def generate_random_json_object(id):
    TS = random_timestamp(datetime.datetime(2020, 1, 1), datetime.datetime(2024, 12, 31))
    FST_NM = random_name(first_names)
    LST_NM = random_name(last_names)
    return {
        "TS": TS,
        "LST_NM": LST_NM.upper(),
        "FST_NM": FST_NM.upper(),
        "id": str(id)
    }

# Main function to generate 'n' JSON objects and save to CSV
def generate_n_json_objects(n, id):
    json_objects = []
    highest_ts = None
    
    for _ in range(n):
        obj = generate_random_json_object(id)
        json_objects.append(obj)
        
        # Update the highest timestamp
        current_ts = datetime.datetime.strptime(obj['TS'], '%Y-%m-%dT%H:%M:%SZ')
        if highest_ts is None or current_ts > highest_ts:
            highest_ts = current_ts
    
    # Save to CSV
    csv_file = 'output.csv'
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["TS", "LST_NM", "FST_NM", "id"])
        writer.writeheader()
        writer.writerows(json_objects)
    
    # Print the highest timestamp
    print("Highest TS:", highest_ts.strftime('%Y-%m-%dT%H:%M:%SZ'))
    
    return json_objects

# Get the number of JSON objects to generate from the user
n = int(input("Enter the number of JSON objects to generate: "))
id_value = 101

# Generate the JSON objects
json_objects = generate_n_json_objects(n, id_value)

# Output the JSON objects
print(json.dumps(json_objects, indent=2))





```



