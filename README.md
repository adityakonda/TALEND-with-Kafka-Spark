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
[
  {
    "operation": "shift",
    "spec": {
      "authrzd": {
        "*": {
          "*": "&",
          "enty_func": {
            "*": {
              "@": "&2.@(3,authrzd_mbr_id).@(1,enty_func_id)"
            }
          }
        }
      }
    }
  },
  {
    "operation": "modify-overwrite-beta",
    "spec": {
      "enty_func": {
        "*": {
          "*": "=lastElement(@(1,&))"
        }
      }
    }
},
  {
    "operation": "shift",
    "spec": {
      "enty_func": {
        "*": {
          "$": "&.authrzd_mbr_id",
          "*": "&1.&2[]"
        }
      }
    }
  },
  {
    "operation": "shift",
    "spec": {
      "*": "authrzd"
    }
  }
]

```

```
# Define the file path (replace with your file path)
file_path = r'C:\path\to\your\file.txt'

# Define the list of words to remove lines containing them
words_to_remove = ["replace", "selected", "1111"]

# Read the file and process its lines
with open(file_path, 'r', encoding='utf-8') as file:
    lines = file.readlines()

# Remove empty lines and lines containing words_to_remove
filtered_lines = [line for line in lines if line.strip() and not any(word in line for word in words_to_remove)]

# Write the filtered lines back to the file
with open(file_path, 'w', encoding='utf-8') as file:
    file.writelines(filtered_lines)


----


# Define the file path (replace with your file path)
file_path = r'C:\path\to\your\file.txt'

# Define the list of words to remove lines containing them
words_to_remove = ["replace", "selected", "1111"]

# Read the file and process its lines
with open(file_path, 'r', encoding='utf-8') as file:
    lines = file.readlines()

# Remove empty lines, lines containing words_to_remove, and substrings like "_dev" and "_prod"
filtered_lines = [line for line in lines if line.strip() and not any(word in line for word in words_to_remove)]
filtered_lines = [line.replace("_dev", "").replace("_prod", "") for line in filtered_lines]

# Write the filtered lines back to the file
with open(file_path, 'w', encoding='utf-8') as file:
    file.writelines(filtered_lines)


=IF(ISNUMBER(MATCH(A2&B2, 'Sheet B'!A:A&'Sheet B'!B:B, 0)), "Found", "Not Found")



```



