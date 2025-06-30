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
    'other_info_df1': [1, 2, 3, 4]
})

df2 = pd.DataFrame({
    'product_name': ['dog', 'fish', 'lion', 'whale', 'bird', 'sr'],
    'other_info_df2': [10, 20, 30, 40, 50, 60]
})

# Function to clean and split text into tokens
def clean_and_split(text):
    # Remove special characters (keep only alphanumerics and space)
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', text)
    # Convert to lowercase and split into words
    return text.lower().split()

# Apply cleaning
df1['pf_tokens'] = df1['product_family'].apply(clean_and_split)
df2['pn_tokens'] = df2['product_name'].apply(clean_and_split)

# Cross join
df1['key'] = 1
df2['key'] = 1
cross = df1.merge(df2, on='key').drop('key', axis=1)

# Match if any token in df2 exists in df1 token list
cross['match'] = cross.apply(lambda row: any(token in row['pf_tokens'] for token in row['pn_tokens']), axis=1)

# Get matching df2 rows only
result = cross[cross['match']].drop(columns=['pf_tokens', 'pn_tokens', 'match', 'product_family', 'other_info_df1'])

# Optional: drop duplicates
result = result.drop_duplicates().reset_index(drop=True)

import ace_tools as tools; tools.display_dataframe_to_user(name="Cleaned Matching Rows from df2", dataframe=result)


```


@echo off
setlocal
set "input_file=hostnames.txt"
set "output_file=output.csv"

if exist "%output_file%" del "%output_file%"

echo Hostname,IP Address > "%output_file%"

for /f "usebackq delims=" %%A in ("%input_file%") do (
    echo Resolving %%A...
    for /f "tokens=2 delims=[]" %%B in ('ping -n 1 %%A ^| findstr "["') do (
        echo %%A,%%B >> "%output_file%"
        echo %%A,%%B
    )
)

echo Results saved to %output_file%
endlocal


