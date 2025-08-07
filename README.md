# TALEND-with-Kafka-Spark

## Big Data Advance - Spark 6.0 ##

if [[ -z "$retention_ms" ]]; then
  retention_ms="default"
  retention_days="default"
else
  retention_days=$((retention_ms / 1000 / 60 / 60 / 24))
fi

echo "  🕒 Retention.ms: $retention_ms ($retention_days days)"


1. **Publishing Messages to Kafka Topic**
2. **Consuming Messsage from Kafka**


## Kafka Commands ##

1.  ###### List all Topic in a Kafka ######
```
#!/bin/bash

# Load properties
source kafka.properties

if [[ -z "$BROKERS" || -z "$TOPICS" ]]; then
  echo "BROKERS and TOPICS must be set in kafka.properties"
  exit 1
fi

OUTPUT_FILE="kafka_topic_summary_$(date +%Y%m%d_%H%M%S).txt"
echo "Topic|TotalMessageCount|EarliestOffsets|LatestOffsets|LastMessage" > "$OUTPUT_FILE"

IFS=',' read -ra TOPIC_ARRAY <<< "$TOPICS"

echo "Using brokers: $BROKERS"
echo "Topics to process: ${#TOPIC_ARRAY[@]}"
echo "Writing output to: $OUTPUT_FILE"
echo "----------------------------------------"

for TOPIC in "${TOPIC_ARRAY[@]}"; do
  echo "🔄 Processing topic: $TOPIC"

  # Get earliest and latest offsets
  earliest_output=$(kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$BROKERS" --topic "$TOPIC" --time -2 2>/dev/null)

  latest_output=$(kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$BROKERS" --topic "$TOPIC" --time -1 2>/dev/null)

  if [[ -z "$latest_output" || -z "$earliest_output" ]]; then
    echo "  ❌ Topic not found or no data available"
    echo "$TOPIC|ERROR|||No Data" >> "$OUTPUT_FILE"
    continue
  fi

  total_count=0
  earliest_offsets=""
  latest_offsets=""

  while IFS= read -r line; do
    partition=$(echo "$line" | awk -F ':' '{print $2}')
    earliest=$(echo "$line" | awk -F ':' '{print $3}')
    latest=$(echo "$latest_output" | grep ":$partition:" | awk -F ':' '{print $3}')

    if [[ -n "$earliest" && -n "$latest" ]]; then
      count=$((latest - earliest))
      total_count=$((total_count + count))

      earliest_offsets+="${partition}=${earliest},"
      latest_offsets+="${partition}=${latest},"
    fi
  done <<< "$earliest_output"

  # Trim trailing commas
  earliest_offsets=${earliest_offsets%,}
  latest_offsets=${latest_offsets%,}

  echo "  ✅ Total messages: $total_count"

  # Get last message
  last_partition=$(echo "$latest_output" | awk -F ':' '{print $2}' | sort -nr | head -1)
  last_offset=$(echo "$latest_output" | grep ":$last_partition:" | awk -F ':' '{print $3}')
  read_offset=$((last_offset - 1))

  if [[ $total_count -gt 0 && $read_offset -ge 0 ]]; then
    last_msg=$(kafka-console-consumer.sh \
      --bootstrap-server "$BROKERS" \
      --topic "$TOPIC" \
      --partition "$last_partition" \
      --offset "$read_offset" \
      --max-messages 1 \
      --timeout-ms 5000 2>/dev/null | tr -d '\n' | tr -d '\r')
  else
    last_msg="NO_MESSAGE"
  fi

  [[ -z "$last_msg" ]] && last_msg="NO_MESSAGE"

  # Write final output line
  echo "$TOPIC|$total_count|$earliest_offsets|$latest_offsets|$last_msg" >> "$OUTPUT_FILE"

  echo "  🧾 Earliest Offsets: $earliest_offsets"
  echo "  🧾 Latest Offsets:   $latest_offsets"
  echo "  ✅ Last message (partition $last_partition): $last_msg"
  echo
done

echo "✅ Done. Output saved to $OUTPUT_FILE"


```
	
2. ###### Create a Topic in Kafka ######
```
if [[ "$hash_1" == "$hash_2" ]]; then
  result=true
else
  result=false
fi

echo "$result"```
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
#!/bin/bash

# Load properties
source kafka.properties

if [[ -z "$BROKERS" || -z "$TOPICS" ]]; then
  echo "BROKERS and TOPICS must be set in kafka.properties"
  exit 1
fi

OUTPUT_FILE="kafka_topic_summary_$(date +%Y%m%d_%H%M%S).txt"
echo "Topic|TotalMessageCount|EarliestOffsets|LatestOffsets|LastMessage" > "$OUTPUT_FILE"

IFS=',' read -ra TOPIC_ARRAY <<< "$TOPICS"

echo "Using brokers: $BROKERS"
echo "Topics to process: ${#TOPIC_ARRAY[@]}"
echo "Writing output to: $OUTPUT_FILE"
echo "----------------------------------------"

for TOPIC in "${TOPIC_ARRAY[@]}"; do
  echo "🔄 Processing topic: $TOPIC"

  # Get earliest and latest offsets
  earliest_output=$(kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$BROKERS" --topic "$TOPIC" --time -2 2>/dev/null)

  latest_output=$(kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$BROKERS" --topic "$TOPIC" --time -1 2>/dev/null)

  if [[ -z "$latest_output" || -z "$earliest_output" ]]; then
    echo "  ❌ Topic not found or no data available"
    echo "$TOPIC|ERROR|||No Data" >> "$OUTPUT_FILE"
    continue
  fi

  total_count=0
  earliest_offsets=""
  latest_offsets=""

  while IFS= read -r line; do
    partition=$(echo "$line" | awk -F ':' '{print $2}')
    earliest=$(echo "$line" | awk -F ':' '{print $3}')
    latest=$(echo "$latest_output" | grep ":$partition:" | awk -F ':' '{print $3}')

    if [[ -n "$earliest" && -n "$latest" ]]; then
      count=$((latest - earliest))
      total_count=$((total_count + count))

      earliest_offsets+="${partition}=${earliest},"
      latest_offsets+="${partition}=${latest},"
    fi
  done <<< "$earliest_output"

  # Trim trailing commas
  earliest_offsets=${earliest_offsets%,}
  latest_offsets=${latest_offsets%,}

  echo "  ✅ Total messages: $total_count"

  # Get last message
  last_partition=$(echo "$latest_output" | awk -F ':' '{print $2}' | sort -nr | head -1)
  last_offset=$(echo "$latest_output" | grep ":$last_partition:" | awk -F ':' '{print $3}')
  read_offset=$((last_offset - 1))

  if [[ $total_count -gt 0 && $read_offset -ge 0 ]]; then
    last_msg=$(kafka-console-consumer.sh \
      --bootstrap-server "$BROKERS" \
      --topic "$TOPIC" \
      --partition "$last_partition" \
      --offset "$read_offset" \
      --max-messages 1 \
      --timeout-ms 5000 2>/dev/null | tr -d '\n' | tr -d '\r')
  else
    last_msg="NO_MESSAGE"
  fi

  [[ -z "$last_msg" ]] && last_msg="NO_MESSAGE"

  # Write final output line
  echo "$TOPIC|$total_count|$earliest_offsets|$latest_offsets|$last_msg" >> "$OUTPUT_FILE"

  echo "  🧾 Earliest Offsets: $earliest_offsets"
  echo "  🧾 Latest Offsets:   $latest_offsets"
  echo "  ✅ Last message (partition $last_partition): $last_msg"
  echo
done

echo "✅ Done. Output saved to $OUTPUT_FILE"

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

if [[ -z "$retention_ms" ]]; then
  retention_ms="default"
  retention_days="default"
else
  retention_days=$((retention_ms / 1000 / 60 / 60 / 24))
fi

echo "  🕒 Retention.ms: $retention_ms ($retention_days days)"



```


db.claims.createIndexes([
  { key: { fNbr: 1 }, name: "fNbr_1" },
  { key: { cntrId: 1 }, name: "cntrId_1" },
  { key: { cntrId: 1, mbrId: 1, fstSvcBegnDt: 1 }, name: "cntrId_1_mbrId_1_fstSvcBegnDt_1" },
  { key: { cntrId: 1, mbrId: 1, recvdDt: 1 }, name: "cntrId_1_mbrId_1_recvdDt_1" }
])
