# Installation Guide

## Prerequisites

- Python 3.8 or higher
- pip
- Virtual environment (recommended)

## Installation Steps

### Clone Repository

```bash
git clone https://github.com/tonysebion/medallion-foundry.git
cd medallion-foundry
```

### 2. Create a Virtual Environment

**On Windows:**
```powershell
python -m venv .venv
.venv\Scripts\activate
```

**On Linux/Mac:**
```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Install the Package (Optional)

For development mode:
```bash
pip install -e .
```

For production:
```bash
pip install .
```

## Next steps & references

- Need to run the automated suite? See `docs/framework/operations/TESTING.md` for the current test commands and expectations.
- Building configs and running Bronze/Silver happens in the usage track (`docs/usage/index.md`); start there once your environment is ready.
