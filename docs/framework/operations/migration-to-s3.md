# Migration Guide: Local to S3 Storage

This guide helps migrate medallion-foundry configurations from local filesystem storage to S3/cloud storage for production deployments.

## Overview

The framework supports both local and cloud storage backends. While local storage is convenient for development, S3 provides scalability, durability, and integration with cloud data platforms.

## Key Changes

### Configuration Structure

**Old (Local-focused):**
```yaml
platform:
  bronze:
    s3_bucket: "my-bucket"
    s3_prefix: "bronze"
source:
  run:
    s3_enabled: true
    local_output_dir: "./output"
```

**New (Storage-agnostic):**
```yaml
storage:
  source:
    backend: s3
    prefix: source_samples/
  bronze:
    backend: s3
    prefix: bronze_samples/
  silver:
    backend: s3
    prefix: silver_samples/
source:
  # ... extraction config
silver:
  # ... promotion config
```

### Benefits of New Structure

- **Explicit backends**: Clearly declare storage type per layer
- **Flexible prefixes**: Customize paths without bucket constraints
- **Easy switching**: Change `backend: local` for development
- **Unified config**: Single file for Bronze + Silver

## Migration Steps

### 1. Update Configuration

1. **Backup your config**: `cp my_config.yaml my_config.backup.yaml`

2. **Replace platform section** with storage section:
   ```yaml
   # Remove this:
   platform:
     bronze:
       s3_bucket: "my-bucket"
       s3_prefix: "bronze"

   # Add this:
   storage:
     source:
       backend: s3
       bucket: "my-bucket"  # Note: bucket at storage level
       prefix: source_samples/
     bronze:
       backend: s3
       bucket: "my-bucket"
       prefix: bronze_samples/
     silver:
       backend: s3
       bucket: "my-bucket"
       prefix: silver_samples/
   ```

3. **Update source.run**:
   ```yaml
   # Remove these (handled by storage section):
   # s3_enabled: true
   # local_output_dir: "./output"
   ```

### 2. Prepare S3 Environment

1. **Create S3 bucket** (if not exists):
   ```bash
   aws s3 mb s3://my-medallion-bucket
   ```

2. **Set permissions** for medallion-foundry:
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
           "s3:ListBucket"
         ],
         "Resource": [
           "arn:aws:s3:::my-medallion-bucket",
           "arn:aws:s3:::my-medallion-bucket/*"
         ]
       }
     ]
   }
   ```

3. **Configure AWS credentials**:
   ```bash
   aws configure
   # Or set environment variables:
   # export AWS_ACCESS_KEY_ID=...
   # export AWS_SECRET_ACCESS_KEY=...
   ```

### 3. Upload Sample Data (if using samples)

If your config references sample data, upload it to S3:

```bash
# Generate samples locally
python scripts/generate_sample_data.py

# Upload to S3
python scripts/generate_sample_data.py --upload-to-s3 my-medallion-bucket
```

### 4. Test Migration

1. **Validate config**:
   ```bash
   python bronze_extract.py --config my_config.yaml --validate-only
   python silver_extract.py --config my_config.yaml --validate-only
   ```

2. **Run dry-run**:
   ```bash
   python bronze_extract.py --config my_config.yaml --dry-run --date 2025-11-27
   ```

3. **Run full pipeline**:
   ```bash
   python bronze_extract.py --config my_config.yaml --date 2025-11-27
   python silver_extract.py --config my_config.yaml --date 2025-11-27
   ```

4. **Verify outputs in S3**:
   ```bash
   aws s3 ls s3://my-medallion-bucket/bronze_samples/
   aws s3 ls s3://my-medallion-bucket/silver_samples/
   ```

## Troubleshooting

### Common Issues

**"Access Denied" errors:**
- Verify AWS credentials: `aws sts get-caller-identity`
- Check bucket permissions
- Ensure bucket exists in correct region

**"No data found" in Bronze:**
- Confirm source data exists at specified S3 prefix
- Check date range matches sample data
- Verify bucket name spelling

**Config validation fails:**
- Ensure `storage` section has all required fields
- Check YAML indentation
- Compare with `docs/examples/configs/examples/s3_example.yaml`

### Switching Back to Local

For debugging, temporarily switch to local storage:

```yaml
storage:
  source:
    backend: local
    prefix: ./sampledata/source_samples/
  bronze:
    backend: local
    prefix: ./output/
  silver:
    backend: local
    prefix: ./silver_output/
```

## Best Practices

### Production Setup

- Use dedicated buckets per environment (dev/staging/prod)
- Enable S3 versioning for data recovery
- Set up lifecycle policies for cost optimization
- Use IAM roles instead of access keys when possible

### Security

- Rotate access keys regularly
- Use least-privilege IAM policies
- Enable S3 server-side encryption
- Monitor access logs

### Performance

- Choose appropriate S3 storage class (Standard, IA, Glacier)
- Use prefixes for partitioning to avoid list operation limits
- Consider S3 batch operations for large data migrations

## Support

If you encounter issues:
1. Check the [troubleshooting guide](../onboarding/troubleshooting.md)
2. Review [S3 configuration examples](../../examples/configs/examples/s3_example.yaml)
3. Open an issue with your config (redact secrets) and error messages