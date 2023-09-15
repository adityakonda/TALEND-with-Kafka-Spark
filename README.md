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
import xml.etree.ElementTree as ET

# Replace 'input.xml' with the path to your input XML file
input_file = 'input.xml'

# Replace 'output.xml' with the desired output file name
output_file = 'formatted_output.xml'

try:
    # Parse the XML File
    tree = ET.parse(input_file)
    root = tree.getroot()

    # Create a new XML string with pretty formatting
    xml_string = ET.tostring(root, encoding='utf-8', method='xml').decode()

    # Write the Reformatted XML to the Output File
    with open(output_file, 'w') as file:
        file.write(xml_string)

    print(f'XML formatting completed. Formatted XML saved to {output_file}')
except FileNotFoundError:
    print(f'Error: The input XML file "{input_file}" was not found.')
except Exception as e:
    print(f'An error occurred: {str(e)}')



```


