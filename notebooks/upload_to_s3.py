import boto3
import os

s3 = boto3.client('s3')
bucket_name = 'retail-insights-lakehouse'
files = ['sales_transformed.csv', 'sales_enriched.csv']

for file in files:
    local_path = os.path.join('curated', file)
    s3_path = f'curated/{file}'

    # Upload file
    try:
        s3.upload_file(local_path, bucket_name, s3_path)
        print(f"✅ Uploaded {file} to s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"❌ Failed to upload {file}: {e}")
        continue

    # Verify file exists in S3
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_path)
        print(f"✅ Verified {file} exists in s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"❌ {file} not found in S3: {e}")

print("🎯 All upload attempts completed.")
