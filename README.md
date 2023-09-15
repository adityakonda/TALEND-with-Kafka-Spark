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
import sys
import xml.dom.minidom as minidom

def format_xml(input_file):
    try:
        with open(input_file, "r") as file:
            xml_content = file.read()
    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
        sys.exit(1)

    try:
        dom = minidom.parseString(xml_content)
        pretty_xml = dom.toprettyxml(indent="  ")
    except Exception as e:
        print(f"Error parsing or formatting XML: {str(e)}")
        sys.exit(1)

    try:
        with open(input_file, "w") as file:
            file.write(pretty_xml)
        print(f"XML in '{input_file}' has been formatted and overwritten.")
    except Exception as e:
        print(f"Error writing formatted XML: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python format_xml.py <xml_file>")
        sys.exit(1)

    xml_file_path = sys.argv[1]
    format_xml(xml_file_path)




```


