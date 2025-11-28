# Configuration Examples

This directory contains example configurations for medallion-foundry, organized by learning progression and use case.

## Directory Structure

- **`examples/`** - **Primary examples** - Main configs referenced in documentation and tutorials
- **`patterns/`** - **Pattern demonstrations** - Examples of different data loading patterns (full, CDC, SCD)
- **`advanced/`** - **Specialized configs** - Advanced features and complex scenarios
- **`templates/`** - **Starting templates** - Base configs for creating new configurations

## Choosing a Config

### For Learning & Getting Started:
- **Complete beginner?** Start with `examples/file_example.yaml` (works with sample data)
- **Have an API?** Use `examples/api_example.yaml` as your starting point
- **Need a database?** Copy `examples/db_example.yaml`

### For Production:
- **Simple production setup?** Start with `examples/` configs and customize
- **Complex requirements?** Reference `advanced/` configs for patterns
- **Specific data patterns?** Check `patterns/` for your loading strategy

### For Development:
- **Creating new configs?** Use templates in `templates/`
- **Testing patterns?** Use configs in `patterns/`

## Migration Notes

**Removed directories** (consolidated to reduce confusion):
- `simple/` → Use `examples/` instead (they serve the same purpose)
- `complex/` → Use `advanced/` instead (renamed for clarity)
- `quickstart/` → Use `examples/file_example.yaml` instead

All functionality is preserved in the remaining directories.
