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
```json
import json
import random

def generate_json(n):
    data = []
    used_ids = set()

    for _ in range(n):
        while True:
            rand_id = random.randint(1, 50)
            if rand_id not in used_ids:
                used_ids.add(rand_id)
                break
        entry = {"id": str(rand_id), "dept": "emp"}
        data.append(entry)

    with open('output.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)

if __name__ == "__main__":
    n = int(input("Enter the number of IDs to generate: "))
    if n > 50:
        print("Number of IDs cannot exceed 50.")
    else:
        generate_json(n)
        print(f"JSON file with {n} entries has been generated as 'output.json'")


```



