# Quick Reference: Common Scenarios

This cheat sheet helps you quickly identify the right pattern for typical data scenarios. For detailed guidance, see `pattern_matrix.md`.

## By Data Source Type

### **API Endpoints**
| Scenario | Pattern | Config Example |
|----------|---------|----------------|
| REST API with pagination | Pattern 1 (Full Events) | `docs/examples/configs/examples/api_example.yaml` |
| REST API with change timestamps | Pattern 2 (CDC Events) | `docs/examples/configs/patterns/pattern_cdc.yaml` |
| REST API with full snapshots | Pattern 1 (Full Events) | `docs/examples/configs/simple/api_simple.yaml` |

### **Database Tables**
| Scenario | Pattern | Config Example |
|----------|---------|----------------|
| Full table export (daily) | Pattern 1 (Full Events) | `docs/examples/configs/examples/db_example.yaml` |
| CDC with change tracking | Pattern 2 (CDC Events) | `docs/examples/configs/patterns/pattern_cdc.yaml` |
| Slowly Changing Dimensions | Pattern 3 (SCD State) | `docs/examples/configs/patterns/pattern_current_history.yaml` |

### **File Sources**
| Scenario | Pattern | Config Example |
|----------|---------|----------------|
| CSV/JSON files (batch) | Pattern 1 (Full Events) | `docs/examples/configs/examples/file_example.yaml` |
| Daily file dumps | Pattern 1 (Full Events) | `docs/examples/configs/simple/file_simple.yaml` |
| Change data capture files | Pattern 2 (CDC Events) | `docs/examples/configs/patterns/pattern_cdc.yaml` |

## By Business Need

### **Event Tracking**
- **Transactional logs**: Pattern 2 (CDC Events)
- **API call logs**: Pattern 1 (Full Events)
- **User activity streams**: Pattern 2 (CDC Events)

### **Current State**
- **Customer profiles**: Pattern 3 (SCD State) or Pattern 6 (Latest-only)
- **Product catalog**: Pattern 1 (Full Events) with Silver `scd_type_1`
- **Account balances**: Pattern 6 (Latest-only)

### **Historical Analysis**
- **Complete audit trail**: Pattern 3 (SCD State)
- **Financial transactions**: Pattern 2 (CDC Events)
- **Status change history**: Pattern 3 (SCD State)

## Quick Pattern Selection

**Answer these 3 questions:**

1. **Does your source send complete snapshots or just changes?**
   - Complete snapshots → Pattern 1
   - Just changes → Go to question 2

2. **Do you need historical versions of records?**
   - Yes → Pattern 3 (SCD State)
   - No → Pattern 2 (CDC Events)

3. **Is this event data or current state?**
   - Events (logs, transactions) → Patterns 1-2
   - State (current values) → Patterns 3, 6

## Getting Started

1. **Copy a template**: Start with `docs/examples/configs/templates/owner_intent_template.yaml`
2. **Choose your pattern**: Use the table above or `pattern_matrix.md`
3. **Copy the config**: Use the example configs listed above
4. **Customize**: Update connection details, field names, and paths
5. **Test**: Run with `--dry-run` first, then check `bronze_readiness_checklist.md`

## Need Help?

- **Detailed guidance**: `EXTRACTION_GUIDANCE.md`
- **All patterns**: `pattern_matrix.md`
- **Advanced features**: `ENHANCED_FEATURES.md`
- **Onboarding**: `../onboarding/intent-owner-guide.md`