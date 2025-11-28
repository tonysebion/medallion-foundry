# Config Templates

These are template configurations that serve as starting points for creating new configs. They contain placeholder values and comments to guide customization.

## Files

- `owner_intent_template.yaml` - Template for data owners to specify their extraction requirements
- `reference_intent.yaml` - Reference implementation showing all possible config options

## Purpose

These templates are designed for:
- Creating new configs from scratch
- Understanding all available configuration options
- Following best practices for config structure
- Documentation of the full configuration schema

## Usage

Copy these templates and replace placeholder values with your actual configuration. The `owner_intent_template.yaml` is designed for non-technical users to specify requirements, while `reference_intent.yaml` shows advanced options.

**Note**: To see how the owner intent template gets processed, run:
```bash
python scripts/expand_owner_intent.py --config docs/examples/configs/templates/owner_intent_template.yaml
```
