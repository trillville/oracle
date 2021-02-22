import boto3
s3 = boto3.resource(
    's3',
    aws_access_key_id="AKIA37SVVXBH5OJNYPHL",
    aws_secret_access_key="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"
)
content="String content to write to a new S3 file"
s3.Object('test', 'newfile.txt').put(Body=content)
