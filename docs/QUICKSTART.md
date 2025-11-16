# Quick Start Guide

## 🎯 2-Minute API Test (For Product/API Teams)

**You are here because:** You need to know if your API is solid enough for the data team to use as a data product.

**What we'll do:** Extract data from your API and save it locally. If it works, you're done!

**Time:** About 5 minutes (2 minutes after first-time setup)

---

### Step 1: Download the Project

**If you have Git installed:**

```powershell
# Open PowerShell and navigate where you want the project
cd C:\Projects

# Download the project
git clone https://github.com/bronze-factory/medallion-foundry.git
cd medallion-foundry
```

**If you don't have Git:**

1. Go to https://github.com/bronze-factory/medallion-foundry
2. Click the green "Code" button
3. Click "Download ZIP"
4. Extract the ZIP file to `C:\Projects\medallion-foundry`
5. Open PowerShell and navigate there:
   ```powershell
   cd C:\Projects\medallion-foundry
   ```

---

### Step 2: Setup Python Environment (One-Time)

**Check if Python is installed:**

```powershell
python --version
```

If you see `Python 3.8` or higher, you're good. If not, download Python from https://www.python.org/downloads/

**Create a virtual environment:**

```powershell
# Create the environment
python -m venv .venv

# Activate it (you'll need to do this each time you open a new PowerShell)
.venv\Scripts\activate

# Your prompt should now show (.venv) at the beginning
```

**Install required packages:**

```powershell
pip install -r requirements.txt
```

This will take 1-2 minutes. You'll see a lot of text scroll by - that's normal.

---

### Step 3: Create Your Test Config File

**Copy the example:**

```powershell
# Create config folder if it doesn't exist
mkdir config -ErrorAction SilentlyContinue

# Copy the quick test example
copy docs\examples\quick_test.yaml config\test.yaml
```

**Edit the config file:**

Open `config\test.yaml` in Notepad or any text editor. You need to change **5 things**:

1. **Line 6** - `s3_bucket`: Change to anything (doesn't matter for testing)
   ```yaml
   s3_bucket: "my-test-bucket"
   ```

2. **Line 24** - `system`: Your system/product name (e.g., "salesforce", "shopify", "myapi")
   ```yaml
   system: "myproduct"
   ```

3. **Line 25** - `table`: What data you're getting (e.g., "customers", "orders", "users")
   ```yaml
   table: "customers"
   ```

4. **Line 28** - `base_url`: Your API's base URL
   ```yaml
   base_url: "https://api.yourcompany.com"
   ```

5. **Line 29** - `endpoint`: Your API endpoint (the part after the base URL)
   ```yaml
   endpoint: "/v1/customers"
   ```

**Choose your authentication type:**

Find the authentication section (around line 33). **Uncomment** (remove the `#`) from the auth type your API uses:

- **Bearer token** (most common - like "Authorization: Bearer abc123"):
  ```yaml
  auth_type: "bearer"
  auth_token_env: "MY_API_TOKEN"
  ```

- **API key in header** (like "X-API-Key: abc123"):
  ```yaml
  # Uncomment these lines:
  # auth_type: "api_key"
  # api_key_header: "X-API-Key"
  # api_key_env: "MY_API_KEY"
  ```

- **Username/password**:
  ```yaml
  # Uncomment these lines:
  # auth_type: "basic"
  # basic_user_env: "API_USER"
  # basic_pass_env: "API_PASS"
  ```

**Set pagination** (if your API has lots of data):

Most APIs return data in chunks. Find the pagination section (around line 49):

- If your API returns everything at once, leave it as:
  ```yaml
  pagination:
    type: "none"
  ```

- If your API uses offset/limit (like `?offset=0&limit=100`), uncomment:
  ```yaml
  # pagination:
  #   type: "offset"
  #   page_size: 100
  #   offset_param: "offset"
  #   limit_param: "limit"
  ```

- If your API uses page numbers (like `?page=1`), uncomment:
  ```yaml
  # pagination:
  #   type: "page"
  #   page_size: 100
  #   page_param: "page"
  ```

**Save the file** and close your editor.

---

### Step 4: Set Your API Token

In PowerShell, set your API token as an environment variable:

```powershell
# Replace "your-actual-token-here" with your real API token
$env:MY_API_TOKEN="your-actual-token-here"
```

**Note:** This token is only set for this PowerShell session. If you close PowerShell, you'll need to set it again.

---

### Step 5: Run the Test!

```powershell
python bronze_extract.py --config config\test.yaml
```

You should see output like:
```
INFO - Starting Bronze extraction...
INFO - Fetching data from API...
INFO - Writing 150 records to output...
INFO - Successfully wrote part-0001.csv
INFO - Successfully wrote part-0001.parquet
INFO - Extraction complete!
```

---

### Step 6: Check Your Results

**Open File Explorer** and navigate to the output folder:

```
C:\Projects\medallion-foundry\output\system=myproduct\table=customers\dt=2025-11-12\
```

You should see files like:
- `part-0001.csv` - Your data in CSV format (open with Excel)
- `part-0001.parquet` - Your data in Parquet format (for analytics)

**Open the CSV file** in Excel or Notepad. Do you see your data? **Success!** 🎉

---

### ✅ What This Means

**Your API is ready for data extraction!** You've proven that:
- Your API is accessible
- Authentication works
- Data can be extracted
- The data structure is consistent

### 🎁 Hand Off to Data Team

Your work is done! Give the data team:
1. Your `config\test.yaml` file
2. Let them know your API token (securely)
3. They'll:
   - Enable S3 storage
   - Schedule daily/hourly runs
   - Create analytics tables
   - Your API is now a data product!

---

## 🔧 Troubleshooting

**"python: command not found"**
- Install Python from https://www.python.org/downloads/
- Make sure to check "Add Python to PATH" during installation

**"Access denied" or "401 Unauthorized"**
- Check your API token is correct
- Make sure you set it with `$env:MY_API_TOKEN="..."`
- Verify the token works by testing in Postman or curl

**"No data returned"**
- Check your endpoint is correct
- Try the API in a browser or Postman first
- Look at the logs for error messages

**"Module not found"**
- Make sure you ran `pip install -r requirements.txt`
- Make sure your virtual environment is activated (you should see `(.venv)` in your prompt)

**"No output folder created"**
- Check the logs for errors
- Your API might not be returning data
- Try adding `--date 2025-11-12` to specify a date

**Need more help?**
- Check the full documentation: [DOCUMENTATION.md](DOCUMENTATION.md)
- Detailed examples below

---

## 🚀 Full Setup (For Data Teams)

### 1. Install
```powershell
# Clone (if you haven't already)
git clone https://github.com/bronze-factory/medallion-foundry.git
cd medallion-foundry

# Setup virtual environment
python -m venv .venv
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Create Your First Config

Create `config/my_api.yaml`:

```yaml
platform:
  bronze:
    s3_bucket: "my-bronze-bucket"
    s3_prefix: "bronze"
    partitioning:
      use_dt_partition: true
    output_defaults:
      allow_csv: true
      allow_parquet: true
      parquet_compression: "snappy"

  s3_connection:
    endpoint_url_env: "BRONZE_S3_ENDPOINT"
    access_key_env: "AWS_ACCESS_KEY_ID"
    secret_key_env: "AWS_SECRET_ACCESS_KEY"

source:
  type: "api"
  system: "my_system"
  table: "my_data"

  api:
    base_url: "https://api.example.com"
    endpoint: "/v1/data"
    auth_type: "bearer"
    auth_token_env: "MY_API_TOKEN"
    
    pagination:
      type: "offset"
      page_size: 100
      offset_param: "offset"
      limit_param: "limit"

  run:
    max_rows_per_file: 50000
    write_csv: true
    write_parquet: true
    s3_enabled: false  # Set to true when ready
    local_output_dir: "./output"
    timeout_seconds: 30
```

### 3. Set Environment Variables

```powershell
# API Authentication
$env:MY_API_TOKEN="your-api-token-here"

# S3 (if s3_enabled: true)
$env:AWS_ACCESS_KEY_ID="your-access-key"
$env:AWS_SECRET_ACCESS_KEY="your-secret-key"
$env:BRONZE_S3_ENDPOINT="https://s3.amazonaws.com"  # Optional
```

### 4. Run Your First Extract

```powershell
python bronze_extract.py --config config/my_api.yaml
```

Output will be in `./output/system=my_system/table=my_data/dt=2025-11-12/`

### Optional: Offline Local Test (No API Required)

Want to see the pipeline without calling a real API? Use the bundled file example:

```powershell
# Prepare sample data (one-time)
python scripts/generate_sample_data.py

# Run Bronze + Silver with the same config
python bronze_extract.py --config docs/examples/configs/file_example.yaml --date 2025-11-13
python silver_extract.py --config docs/examples/configs/file_example.yaml --date 2025-11-13

# Inspect outputs
tree output/system=retail_demo
tree silver_output/domain=retail_demo
```

Great for demos, workshops, or air-gapped laptops.

## 📋 Common Scenarios

### API with Bearer Token
```yaml
source:
  type: "api"
  api:
    base_url: "https://api.example.com"
    endpoint: "/data"
    auth_type: "bearer"
    auth_token_env: "API_TOKEN"
```

### API with API Key Header
```yaml
source:
  type: "api"
  api:
    base_url: "https://api.example.com"
    endpoint: "/data"
    auth_type: "api_key"
    auth_key_env: "API_KEY"
    auth_key_header: "X-API-Key"
```

### Database with Incremental Load
```yaml
source:
  type: "db"
  db:
    conn_str_env: "DB_CONN_STR"
    base_query: |
      SELECT id, name, updated_at
      FROM my_table
    incremental:
      enabled: true
      cursor_column: "updated_at"
    fetch_batch_size: 10000
```

Set environment variable:
```powershell
$env:DB_CONN_STR="DRIVER={ODBC Driver 17 for SQL Server};SERVER=myserver;DATABASE=mydb;UID=user;PWD=pass"
```

### Cursor-Based Pagination
```yaml
source:
  type: "api"
  api:
    base_url: "https://api.example.com"
    endpoint: "/items"
    pagination:
      type: "cursor"
      cursor_param: "next"
      cursor_path: "pagination.next_cursor"
```

## 🔧 Troubleshooting

### "Import could not be resolved"
This is a Pylance warning - the code will still run. To fix, install the package:
```powershell
pip install -e .
```

### "Environment variable not set"
Make sure you set all required env vars:
```powershell
$env:MY_VAR="value"
```

### "No records returned"
- Check your API endpoint is correct
- Verify authentication is working
- Check API response format matches config

### Database connection fails
- Verify ODBC driver is installed
- Check connection string format
- Test connection string independently

## 📚 Learn More

- See `docs/examples/` for more configuration examples
- Read `docs/CONFIG_REFERENCE.md` for all config options
- Check `docs/ARCHITECTURE.md` for design details
- Review `INSTALLATION.md` for detailed setup

## 🎯 Next Steps

1. **Test locally** - Run with `s3_enabled: false` first
2. **Verify output** - Check CSV/Parquet files in `./output/`
3. **Enable S3** - Set `s3_enabled: true` when ready
4. **Add to scheduler** - Integrate with your workflow orchestrator
5. **Monitor logs** - Check for errors and warnings
6. **Set up incremental** - Enable incremental loading for efficiency

## ⚡ Tips

- Start with small page sizes to test pagination
- Use CSV output for debugging, Parquet for production
- Check `.state/` directory for incremental cursors
- Set `cleanup_on_failure: true` to auto-delete partial files
- Use `--date` parameter for backfills: `--date 2025-11-01`
