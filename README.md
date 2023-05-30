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

import java.nio.file.{Files, Path, Paths}

object FolderDeletion {
  def main(args: Array[String]): Unit = {
    val folderPath = "path/to/folder" // Replace with the actual folder path
    
    val folderName = "folderName" // Replace with the name of the folder to delete
    
    val directory = Paths.get(folderPath)
    
    if (Files.exists(directory) && Files.isDirectory(directory)) {
      val subDirectories = Files.newDirectoryStream(directory)
      val matchingDirectories = subDirectories.filter(p => p.getFileName.toString == folderName)
      
      matchingDirectories.forEach(deleteDirectory)
    } else {
      println("Invalid folder path.")
    }
  }
  
  def deleteDirectory(directory: Path): Unit = {
    if (Files.isDirectory(directory)) {
      val subDirectories = Files.newDirectoryStream(directory)
      subDirectories.forEach(deleteDirectory)
    }
    
    Files.deleteIfExists(directory)
    println(s"Deleted: ${directory.toString}")
  }
}

```
