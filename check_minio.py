import boto3
from botocore.config import Config

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1',
    config=Config(s3={'addressing_style': 'path'})
)

for bucket in ['bronze', 'silver', 'gold']:
    try:
        resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=5)
        keys = [c['Key'] for c in resp.get('Contents', [])]
        print(bucket, 'count=', len(keys), 'sample=', keys[:5])
    except Exception as e:
        print(bucket, 'ERROR', e)
