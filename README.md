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
import java.io.File

object FolderDeletion {
  def main(args: Array[String]): Unit = {
    val folderPath = "path/to/folder" // Replace with the actual folder path
    
    val directory = new File(folderPath)
    
    if (directory.exists && directory.isDirectory) {
      deleteFilesInSubdirectories(directory)
    } else {
      println("Invalid folder path.")
    }
  }
  
  def deleteFilesInSubdirectories(directory: File): Unit = {
    val files = directory.listFiles
    if (files != null) {
      files.foreach { file =>
        if (file.isDirectory) {
          if (file.getName + ".csv" == directory.getName + ".csv") {
            deleteFilesInSubdirectories(file)
          } else {
            deleteDirectory(file)
          }
        } else {
          file.delete()
        }
      }
    }
  }
  
  def deleteDirectory(directory: File): Unit = {
    val files = directory.listFiles
    if (files != null) {
      files.foreach { file =>
        if (file.isDirectory) {
          deleteDirectory(file)
        } else {
          file.delete()
        }
      }
    }
    directory.delete()
    println(s"Deleted directory: ${directory.getAbsolutePath}")
  }
}

```

SELECT REGEXP_EXTRACT("$695.6 M", r'([0-9]+(\.[0-9]+)?)') AS result

