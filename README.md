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
#!/bin/bash

# Load properties
source kafka.properties

if [[ -z "$BROKERS" || -z "$TOPICS" ]]; then
  echo "BROKERS and TOPICS must be set in kafka.properties"
  exit 1
fi

# Output file
OUTPUT_FILE="kafka_topic_summary.txt"
echo "Topic|TotalMessageCount|LastMessage" > "$OUTPUT_FILE"

# Split topics into an array
IFS=',' read -ra TOPIC_ARRAY <<< "$TOPICS"

echo "Using brokers: $BROKERS"
echo "Topics to process: ${#TOPIC_ARRAY[@]}"
echo "Writing output to: $OUTPUT_FILE"
echo "----------------------------------------"

# Process each topic
for TOPIC in "${TOPIC_ARRAY[@]}"; do
  echo "Processing topic: $TOPIC"

  # Get latest offsets (per partition)
  offsets_output=$(kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "$BROKERS" --topic "$TOPIC" --time -1 2>/dev/null)

  if [[ -z "$offsets_output" ]]; then
    echo "  ❌ Topic not found or no data available"
    echo "$TOPIC|ERROR|No Data" >> "$OUTPUT_FILE"
    continue
  fi

  # Calculate total messages
  total_count=$(echo "$offsets_output" | awk -F ':' '{sum += $3} END {print sum}')
  echo "  ✅ Total messages: $total_count"

  # Get the highest partition (last one)
  last_partition=$(echo "$offsets_output" | awk -F ':' '{print $2}' | sort -nr | head -1)

  # Get last offset in that partition
  last_offset=$(echo "$offsets_output" | grep ":$last_partition:" | awk -F ':' '{print $3}')
  read_offset=$((last_offset - 1))

  # Read last message
  last_msg=$(kafka-console-consumer.sh \
    --bootstrap-server "$BROKERS" \
    --topic "$TOPIC" \
    --partition "$last_partition" \
    --offset "$read_offset" \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null | tr -d '\n' | tr -d '\r')

  # If empty, set a default
  if [[ -z "$last_msg" ]]; then
    last_msg="NO_MESSAGE"
  fi

  # Write to file
  echo "$TOPIC|$total_count|$last_msg" >> "$OUTPUT_FILE"

  echo "  ✅ Last message (partition $last_partition): $last_msg"
  echo
done

echo "✅ Done. Output saved to $OUTPUT_FILE"

```


db.claims.createIndexes([
  { key: { fNbr: 1 }, name: "fNbr_1" },
  { key: { cntrId: 1 }, name: "cntrId_1" },
  { key: { cntrId: 1, mbrId: 1, fstSvcBegnDt: 1 }, name: "cntrId_1_mbrId_1_fstSvcBegnDt_1" },
  { key: { cntrId: 1, mbrId: 1, recvdDt: 1 }, name: "cntrId_1_mbrId_1_recvdDt_1" }
])
