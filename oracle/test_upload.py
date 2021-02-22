import boto3
s3 = boto3.resource(
    's3',
)
content="String content to write to a new S3 file"
s3.Object('test', 'newfile.txt').put(Body=content)
