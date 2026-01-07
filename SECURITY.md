# Security Policy

## Supported Versions

We release patches for security vulnerabilities for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.0.x   | :white_check_mark: |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to the project maintainers. You should receive a response within 48 hours.

Please include the following information:

- Type of issue (e.g., buffer overflow, SQL injection, cross-site scripting, etc.)
- Full paths of source file(s) related to the manifestation of the issue
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

This information will help us triage your report more quickly.

## Security Best Practices

When using Medallion Foundry, please follow these security best practices:

### Credentials Management

- **Never commit credentials** to version control
- Use environment variables for sensitive configuration
- Use secret management services (AWS Secrets Manager, Azure Key Vault, etc.)
- Rotate credentials regularly
- Use the `${VAR_NAME}` syntax in pipeline configurations

### Data Security

- Enable encryption at rest for storage backends (S3, Azure Blob)
- Enable encryption in transit (HTTPS/TLS for APIs, encrypted database connections)
- Use private networks for database connections when possible
- Implement least-privilege access controls
- Validate and sanitize all input data

### Pipeline Security

- Review pipeline configurations before deployment
- Use `--dry-run` to validate before executing
- Monitor pipeline execution logs for anomalies
- Implement data quality checks to detect tampering
- Verify checksums for critical data

### Dependency Security

- Keep dependencies up to date
- Review security advisories for dependencies
- Use `pip install --upgrade` regularly
- Consider using `pip-audit` or `safety` tools

### Network Security

- Use VPCs or private networks for cloud resources
- Implement network segmentation
- Use IAM roles instead of access keys when possible
- Enable MFA for cloud accounts
- Restrict API access with rate limiting

## Known Security Considerations

### Environment Variable Expansion

The framework supports environment variable expansion in configuration files using `${VAR_NAME}` syntax. Ensure that:

- Environment variables are properly scoped
- Untrusted input cannot control environment variable names
- Sensitive variables are not logged

### File System Access

Bronze and Silver layers write to file systems. Ensure that:

- Output paths are validated and sanitized
- File permissions are properly configured
- Untrusted input cannot control file paths

### API Integrations

When integrating with external APIs:

- Validate SSL/TLS certificates
- Use API keys with minimal required permissions
- Implement rate limiting to prevent abuse
- Handle sensitive data in responses appropriately

### Database Connections

When connecting to databases:

- Use connection pooling with limits
- Implement query timeouts
- Use parameterized queries (built-in)
- Restrict database user permissions

## Disclosure Policy

When we receive a security bug report, we will:

1. Confirm the problem and determine affected versions
2. Audit code to find any similar problems
3. Prepare fixes for all supported versions
4. Release new security versions as quickly as possible

## Comments on this Policy

If you have suggestions on how this process could be improved, please submit a pull request or open an issue.
