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
import pandas as pd
import re

# Sample data
df1 = pd.DataFrame({
    'product_family': ['cat+dog', 'bird_sr, fish', 'horse.cat', 'tiger+lion'],
    'description': ['pets', 'aquatic', 'farm animal', 'wildlife']
})

df2 = pd.DataFrame({
    'product_name': ['dog', 'fish', 'lion', 'whale', 'bird', 'sr'],
    'other_info_df2': [10, 20, 30, 40, 50, 60]
})

# Function to clean and split text into tokens
def clean_and_split(text):
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', text)
    return text.lower().split()

# Tokenize both DataFrames
df1['pf_tokens'] = df1['product_family'].apply(clean_and_split)
df2['pn_tokens'] = df2['product_name'].apply(clean_and_split)

# Create cross join
df1['key'] = 1
df2['key'] = 1
cross = df2.merge(df1, on='key').drop('key', axis=1)

# Check if any tokens match
cross['match'] = cross.apply(lambda row: any(token in row['pf_tokens'] for token in row['pn_tokens']), axis=1)

# Filter only matching rows
matched = cross[cross['match']].copy()

# In case of multiple matches, keep one or aggregate — here, we’ll keep the first match
matched = matched.drop_duplicates(subset=['product_name'])

# Now merge back with df2 to ensure left join
result = df2.merge(matched[['product_name', 'description']], on='product_name', how='left')

# Final result
print(result)


def clean_and_split(text):
    if not isinstance(text, str):
        return []
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', text)  # remove punctuation
    tokens = text.lower().split()
    return [token for token in tokens if token != 'sr']  # remove "sr" token

```


db.claims.createIndexes([
  { key: { fNbr: 1 }, name: "fNbr_1" },
  { key: { cntrId: 1 }, name: "cntrId_1" },
  { key: { cntrId: 1, mbrId: 1, fstSvcBegnDt: 1 }, name: "cntrId_1_mbrId_1_fstSvcBegnDt_1" },
  { key: { cntrId: 1, mbrId: 1, recvdDt: 1 }, name: "cntrId_1_mbrId_1_recvdDt_1" }
])
