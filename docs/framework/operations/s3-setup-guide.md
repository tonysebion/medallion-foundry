# S3 Setup Guide

This guide walks through setting up AWS S3 for medallion-foundry, including credentials, bucket creation, and permissions.

## Prerequisites

- AWS account with appropriate permissions
- AWS CLI installed (`pip install awscli` or system package)
- medallion-foundry installed

## Step 1: Configure AWS Credentials

### Option A: AWS CLI Configuration (Recommended)

```bash
# Configure AWS credentials
aws configure

# Enter your values:
# AWS Access Key ID: [your-access-key]
# AWS Secret Access Key: [your-secret-key]
# Default region name: us-east-1  # or your preferred region
# Default output format: json
```

### Option B: Environment Variables

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1
```

### Option C: IAM Roles (EC2/ECS)

If running on EC2 or ECS with IAM roles, no additional configuration needed.

## Step 2: Create S3 Bucket

### Using AWS CLI

```bash
# Create bucket (choose a unique name)
aws s3 mb s3://my-medallion-bucket

# Enable versioning (recommended for data integrity)
aws s3api put-bucket-versioning \
  --bucket my-medallion-bucket \
  --versioning-configuration Status=Enabled

# Set up lifecycle policy for cost optimization (optional)
aws s3api put-bucket-lifecycle-configuration \
  --bucket my-medallion-bucket \
  --lifecycle-configuration file://lifecycle-policy.json
```

Create `lifecycle-policy.json`:
```json
{
  "Rules": [
    {
      "ID": "Transition old versions to IA",
      "Status": "Enabled",
      "Prefix": "",
      "NoncurrentVersionTransitions": [
        {
          "NoncurrentDays": 30,
          "StorageClass": "STANDARD_IA"
        }
      ]
    }
  ]
}
```

### Using AWS Console

1. Go to S3 service in AWS Console
2. Click "Create bucket"
3. Enter bucket name (must be globally unique)
4. Choose region
5. Enable versioning
6. Configure public access (block all public access)
7. Create bucket

## Step 3: Set Up IAM Permissions

### Minimal Required Permissions

Create an IAM policy with these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketVersioning",
        "s3:PutBucketVersioning"
      ],
      "Resource": [
        "arn:aws:s3:::my-medallion-bucket",
        "arn:aws:s3:::my-medallion-bucket/*"
      ]
    }
  ]
}
```

### Attach to User/Role

- For IAM user: Attach the policy to the user
- For EC2: Attach to instance profile
- For ECS: Attach to task execution role

## Step 4: Test Connection

### Verify Credentials

```bash
# Test basic connectivity
aws sts get-caller-identity

# List buckets
aws s3 ls
```

### Test Bucket Access

```bash
# Create test file
echo "test" > test.txt

# Upload to bucket
aws s3 cp test.txt s3://my-medallion-bucket/test.txt

# List bucket contents
aws s3 ls s3://my-medallion-bucket/

# Download and verify
aws s3 cp s3://my-medallion-bucket/test.txt test_download.txt
diff test.txt test_download.txt

# Clean up
aws s3 rm s3://my-medallion-bucket/test.txt
rm test.txt test_download.txt
```

## Step 5: Configure medallion-foundry

### Update Configuration

Use the S3 example config as a starting point:

```bash
cp docs/examples/configs/examples/s3_example.yaml config/my_s3_config.yaml
```

Edit the config to use your bucket:

```yaml
storage:
  source:
    backend: s3
    bucket: my-medallion-bucket
    prefix: source_samples/
  bronze:
    backend: s3
    bucket: my-medallion-bucket
    prefix: bronze_samples/
  silver:
    backend: s3
    bucket: my-medallion-bucket
    prefix: silver_samples/
```

### Upload Sample Data (Optional)

If using sample data:

```bash
# Generate samples locally
python scripts/generate_sample_data.py

# Upload to S3
python scripts/generate_sample_data.py --upload-to-s3 my-medallion-bucket
```

## Step 6: Run Test Extraction

```bash
# Test Bronze extraction
python bronze_extract.py --config config/my_s3_config.yaml --date 2025-11-27

# Test Silver promotion
python silver_extract.py --config config/my_s3_config.yaml --date 2025-11-27

# Verify outputs
aws s3 ls s3://my-medallion-bucket/bronze_samples/
aws s3 ls s3://my-medallion-bucket/silver_samples/
```

## Troubleshooting

### Common Issues

**Access Denied**
- Verify credentials: `aws sts get-caller-identity`
- Check IAM permissions
- Ensure bucket exists in correct region

**NoSuchBucket**
- Verify bucket name spelling
- Check region: `aws configure get region`

**InvalidAccessKeyId**
- Reconfigure credentials: `aws configure`
- Check environment variables

**SignatureDoesNotMatch**
- Credentials may be expired
- Check system clock is correct

### Advanced Configuration

**Custom Endpoint (for S3-compatible services)**

```yaml
storage:
  source:
    backend: s3
    bucket: my-bucket
    endpoint_url: https://custom-s3-endpoint.com
```

**Server-Side Encryption**

Enable SSE-KMS:

```yaml
storage:
  source:
    backend: s3
    bucket: my-bucket
    sse: AES256  # or aws:kms
```

## Security Best Practices

- Use IAM roles instead of access keys when possible
- Rotate access keys regularly
- Enable S3 server-side encryption
- Use VPC endpoints for private access
- Monitor access logs
- Implement least-privilege policies

## Cost Optimization

- Use appropriate storage classes (Standard, IA, Glacier)
- Set up lifecycle policies for automatic transitions
- Enable versioning only if needed
- Use S3 batch operations for bulk operations
- Monitor usage with Cost Allocation Tags