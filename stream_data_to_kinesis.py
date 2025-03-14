import glob
import json
import time
import boto3
import random
import pandas as pd
from datetime import datetime, timezone

kdsname = 'stream-breakabletoy'
clientkinesis = boto3.client(
    'kinesis',
    region_name='your region',
    aws_access_key_id="your aws_access_key_id",
    aws_secret_access_key="your aws_secret_access_key"
)

folder_path = "data/*.csv"
files = glob.glob(folder_path)
df = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)

types = ['like', 'comment', 'share']

i = 0

while True:
    randomIndex = random.randrange(df.shape[0])
    record = {
        'post_id': df.iloc[randomIndex,0],
        'platform': df.iloc[randomIndex,1],
        'engagement_type': random.choice(types),
        'user_id': f'{random.randrange(10000):04}',
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    response = clientkinesis.put_record(
        StreamName=kdsname,
        Data=json.dumps(record),
        PartitionKey=str(record['post_id'])
    )

    i += 1
    print(f"Total ingested: {i}, ReqID: {response['ResponseMetadata']['RequestId']}, HTTPStatusCode: {response['ResponseMetadata']['HTTPStatusCode']}")

    time.sleep(1)