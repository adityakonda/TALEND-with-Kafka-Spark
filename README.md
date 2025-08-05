# TALEND-with-Kafka-Spark

## Big Data Advance - Spark 6.0 ##

1. **Publishing Messages to Kafka Topic**
2. **Consuming Messsage from Kafka**


## Kafka Commands ##

1.  ###### List all Topic in a Kafka ######
```
| tail -n 1 | tr -d '\n' | tr -d '\r' | sed 's/^[ \t]*//;s/[ \t]*$//' | tr -s ' '
```
	
2. ###### Create a Topic in Kafka ######
```
	$ kafka-topics --create --zookeeper quickstart.cloudera:2181 --replication-factor 1 --partitions 3 --topic mytopic
```
3.  ###### Creating Producer in Kafka ######
```
# Step 5: Fetch the last message (if any)
if [[ "$last_offset" -eq 0 ]]; then
  last_msg="NO_MESSAGE"
else
  read_offset=$((last_offset - 1))
  last_msg=$(kafka-console-consumer.sh \
    --bootstrap-server "$BROKERS" \
    --topic "$TOPIC" \
    --partition "$last_partition" \
    --offset "$read_offset" \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null | tr -d '\n' | tr -d '\r')

  # Fallback: if message still empty, read from beginning and get last one
  if [[ -z "$last_msg" ]]; then
    last_msg=$(kafka-console-consumer.sh \
      --bootstrap-server "$BROKERS" \
      --topic "$TOPIC" \
      --partition "$last_partition" \
      --from-beginning \
      --timeout-ms 5000 2>/dev/null | tail -n 1 | tr -d '\n' | tr -d '\r')
  fi

  # Final fallback check
  if [[ -z "$last_msg" ]]; then
    last_msg="NO_MESSAGE"
  fi
fi
```
4. ###### Creating Consumer in Kafka ######
```
IFS=',' read -ra TOPIC_ARRAY <<< "$TOPICS"

echo "📦 Topics to process: ${#TOPIC_ARRAY[@]}"

for TOPIC in "${TOPIC_ARRAY[@]}"; do
  echo "🔄 Processing topic: $TOPIC"
```
```
#!/bin/bash

# Load Kafka config
source kafka.properties

if [[ -z "$BROKERS" ]]; then
  echo "❌ BROKERS must be set in kafka.properties"
  exit 1
fi

# Output file
OUTPUT_FILE="kafka_topic_summary.txt"
echo "Topic|TotalMessageCount|LastMessage" > "$OUTPUT_FILE"

echo "📡 Connecting to cluster: $BROKERS"
echo "🔍 Fetching all topics..."

# Get all topic names
TOPICS=$(kafka-topics.sh --bootstrap-server "$BROKERS" --list 2>/dev/null)

if [[ -z "$TOPICS" ]]; then
  echo "❌ No topics found or broker unreachable."
  exit 1
fi

echo "✅ Found $(echo "$TOPICS" | wc -l) topics"
echo "----------------------------------------"

# Loop through each topic
for TOPIC in $TOPICS; do
  echo "🔄 Processing topic: $TOPIC"

  # Step 1: Get latest offsets per partition
  offsets_output=$(kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "$BROKERS" --topic "$TOPIC" --time -1 2>/dev/null)

  if [[ -z "$offsets_output" ]]; then
    echo "  ❌ Could not fetch offsets"
    echo "$TOPIC|ERROR|NO DATA" >> "$OUTPUT_FILE"
    continue
  fi

  # Step 2: Sum total message count across all partitions
  total_count=$(echo "$offsets_output" | awk -F ':' '{sum += $3} END {print sum}')
  echo "  📊 Total messages: $total_count"

  # Step 3: Identify the highest partition number
  last_partition=$(echo "$offsets_output" | awk -F ':' '{print $2}' | sort -nr | head -1)

  # Step 4: Get the last offset of that partition
  last_offset=$(echo "$offsets_output" | grep ":$last_partition:" | awk -F ':' '{print $3}')
  read_offset=$((last_offset - 1))

  # Step 5: Fetch the last message (if any)
  last_msg=$(kafka-console-consumer.sh \
    --bootstrap-server "$BROKERS" \
    --topic "$TOPIC" \
    --partition "$last_partition" \
    --offset "$read_offset" \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null | tr -d '\n' | tr -d '\r')

  if [[ -z "$last_msg" ]]; then
    last_msg="NO_MESSAGE"
  fi

  # Step 6: Append result to output file
  echo "$TOPIC|$total_count|$last_msg" >> "$OUTPUT_FILE"
  echo "  📥 Last message (partition $last_partition): $last_msg"
  echo
done

echo "✅ All topics processed."
echo "📄 Results written to: $OUTPUT_FILE"


```


db.claims.createIndexes([
  { key: { fNbr: 1 }, name: "fNbr_1" },
  { key: { cntrId: 1 }, name: "cntrId_1" },
  { key: { cntrId: 1, mbrId: 1, fstSvcBegnDt: 1 }, name: "cntrId_1_mbrId_1_fstSvcBegnDt_1" },
  { key: { cntrId: 1, mbrId: 1, recvdDt: 1 }, name: "cntrId_1_mbrId_1_recvdDt_1" }
])
