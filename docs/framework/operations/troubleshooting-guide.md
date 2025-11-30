# Troubleshooting Guide

This guide helps diagnose and resolve common issues with medallion-foundry. Start here before diving into logs or code.

## Quick Diagnosis

### 1. Check Exit Codes
- `0`: Success
- `1-99`: Configuration/validation errors
- `100+`: Runtime errors

### 2. Enable Debug Logging
```bash
export BRONZE_LOG_LEVEL=DEBUG
python bronze_extract.py --config config.yaml --date 2025-11-27
```

### 3. Run Validation First
```bash
python bronze_extract.py --config config.yaml --validate-only
python silver_extract.py --config config.yaml --validate-only
```

## Common Issues and Solutions

### Configuration Issues

#### "Config file not found"
**Symptoms**: `FileNotFoundError` or "No such file"
**Solutions**:
- Check file path: `ls -la config/my_config.yaml`
- Use absolute paths
- Verify YAML syntax: `python -c "import yaml; yaml.safe_load(open('config.yaml'))"`

#### "Invalid configuration"
**Symptoms**: Validation errors about missing fields
**Solutions**:
- Compare with examples: `diff config.yaml docs/examples/configs/examples/file_example.yaml`
- Check required fields in CONFIG_REFERENCE.md
- Use `--validate-only` flag

#### "Storage backend not supported"
**Symptoms**: "Unsupported backend: xyz"
**Solutions**:
- Supported: `s3`, `azure`, `local`
- Check spelling in `storage.*.backend`
- For S3: ensure `boto3` installed

### Authentication Issues

#### "Access denied" (S3)
**Symptoms**: `ClientError: AccessDenied`
**Solutions**:
- Verify AWS credentials: `aws sts get-caller-identity`
- Check bucket permissions
- Ensure bucket exists in correct region
- Test with: `aws s3 ls s3://my-bucket/`

#### "Invalid credentials" (API)
**Symptoms**: HTTP 401/403 errors
**Solutions**:
- Check environment variables: `echo $MY_API_TOKEN`
- Verify token format and expiration
- Test API manually with curl

#### "ODBC connection failed" (Database)
**Symptoms**: Connection timeout or authentication errors
**Solutions**:
- Verify connection string
- Check ODBC driver installation
- Test connection: `sqlcmd -S server -U user -P pass`

### Data Issues

#### "No data found"
**Symptoms**: Empty output directories
**Solutions**:
- Check source data exists
- Verify date range matches data
- Test with smaller date range
- Use `--dry-run` to see planned paths

#### "Schema mismatch"
**Symptoms**: Column not found errors
**Solutions**:
- Check source schema vs config expectations
- Update `natural_keys` or column names
- Use `schema.rename_map` for column mapping

#### "Duplicate key errors"
**Symptoms**: Primary key violations in Silver
**Solutions**:
- Verify `natural_keys` configuration
- Check data quality in Bronze
- Use appropriate Silver model for deduplication

### Performance Issues

#### "Extraction too slow"
**Symptoms**: Long run times, timeouts
**Solutions**:
- Enable async: `async: true` (API configs)
- Increase parallel workers: `parallel_workers: 4`
- Use pagination for large APIs
- Check rate limits

#### "Memory issues"
**Symptoms**: Out of memory errors
**Solutions**:
- Reduce `max_rows_per_file`
- Enable file splitting
- Use streaming for large datasets
- Increase system memory

#### "Storage timeouts"
**Symptoms**: S3/Azure upload failures
**Solutions**:
- Check network connectivity
- Reduce file sizes
- Use retry configurations
- Verify storage quotas

### Runtime Issues

#### "Process killed"
**Symptoms**: SIGKILL or abrupt termination
**Solutions**:
- Check system resources (memory/CPU)
- Look for OOM in system logs
- Reduce batch sizes
- Enable circuit breakers

#### "File locking issues"
**Symptoms**: Permission denied on files
**Solutions**:
- Check output directory permissions
- Avoid concurrent runs on same config
- Use unique output paths per run

#### "Zombie processes"
**Symptoms**: Hung processes
**Solutions**:
- Set timeouts in config
- Use `--timeout` CLI flags
- Check for network hangs

## Debugging Workflow

### Step 1: Isolate the Problem
```bash
# Test config validation
python bronze_extract.py --config config.yaml --validate-only

# Test connectivity (API)
curl -H "Authorization: Bearer $TOKEN" https://api.example.com/data

# Test storage access
aws s3 ls s3://my-bucket/
```

### Step 2: Run with Minimal Data
```bash
# Use dry-run
python bronze_extract.py --config config.yaml --dry-run --date 2025-11-27

# Limit data volume
# Add to config: run.limit_rows: 100
```

### Step 3: Enable Detailed Logging
```bash
export BRONZE_LOG_LEVEL=DEBUG
export BRONZE_TRACING=1  # Enable OpenTelemetry if available

python bronze_extract.py --config config.yaml --date 2025-11-27 2>&1 | tee debug.log
```

### Step 4: Check Generated Files
```bash
# Bronze outputs
ls -la output/system=*/table=*/dt=*/

# Metadata files
cat output/system=*/table=*/dt=*/_metadata.json
cat output/system=*/table=*/dt=*/_checksums.json

# Silver outputs
ls -la silver_output/domain=*/entity=*/v*/load_date=*/
```

## Advanced Troubleshooting

### Network Issues
- Use `tcpdump` or Wireshark to inspect traffic
- Check DNS resolution
- Verify proxy settings
- Test with different network interfaces

### Memory Profiling
```bash
# Install memory profiler
pip install memory-profiler

# Run with profiling
mprof run python bronze_extract.py --config config.yaml
mprof plot
```

### Performance Analysis
```bash
# Use built-in benchmarking
python scripts/benchmark.py --config config.yaml

# Profile with cProfile
python -m cProfile -o profile.out bronze_extract.py --config config.yaml
snakeviz profile.out  # Install snakeviz for visualization
```

### Log Analysis
```bash
# Search for patterns
grep "ERROR" logs/*.log
grep "WARNING" logs/*.log

# Count error types
grep "ERROR" logs/*.log | cut -d' ' -f4 | sort | uniq -c | sort -nr
```

## Getting Help

### Self-Service Resources
- [Error Codes Reference](ERROR_CODES.md)
- [Config Doctor](CONFIG_DOCTOR.md)
- [Operations Playbook](OPS_PLAYBOOK.md)
- [GitHub Issues](https://github.com/tonysebion/medallion-foundry/issues)

### When to Ask for Help
Include in your issue/bug report:
- Full command and error output
- Config file (redact secrets)
- Environment details (OS, Python version)
- Sample data if possible
- Debug logs with `BRONZE_LOG_LEVEL=DEBUG`

### Prevention Tips
- Always run `--validate-only` before production
- Use `--dry-run` for testing
- Keep configs in version control
- Monitor resource usage
- Set up alerts for failures