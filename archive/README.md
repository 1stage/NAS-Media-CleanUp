# Archive Folder - Legacy Duplicate Detection Scripts

This folder contains the original scripts that were consolidated into the unified `detect_duplicates.py` v1.0.

## Archived Files:

### `detect_duplicates_legacy_backup.py`
- Original performance-focused script (v0.3)
- High-speed batch processing for analysis
- Used database: `photo_hashes.db`
- **Replaced by**: Performance Mode in unified script (`--performance-mode`)

### `detect_duplicates_safe.py` 
- Original safety-focused script (v0.3)
- Conservative phased processing with binary verification
- Used database: `multi_folder_duplicates.db` or `safe_duplicates.db`
- **Replaced by**: Safety Mode in unified script (default mode)

### `detect_duplicates_safe_backup.py`
- Backup copy of the safety-focused script

## Migration to Unified Script

All functionality from these scripts has been consolidated into `detect_duplicates.py` v1.0:

### Performance Mode (replaces legacy script):
```bash
python detect_duplicates.py "\\NAS-MEDIA\photo\Sorted" --performance-mode --build-database
python detect_duplicates.py "\\NAS-MEDIA\photo\Sorted" --performance-mode --find-duplicates
```

### Safety Mode (replaces safe script):
```bash
python detect_duplicates.py "\\NAS-MEDIA\photo\Sorted\2010 - Photos" --scan
python detect_duplicates.py "\\NAS-MEDIA\photo\Sorted\2010 - Photos" --flag-deletions
python detect_duplicates.py "\\NAS-MEDIA\photo\Sorted\2010 - Photos" --execute-deletions
```

## Unified Database
- **Old**: Multiple databases (`photo_hashes.db`, `multi_folder_duplicates.db`, `safe_duplicates.db`)
- **New**: Single `photo_duplicates.db` with comprehensive schema

These files are kept for reference and can be removed after confirming the unified script meets all requirements.

---
*Archived on: September 27, 2025*
*Reason: Script consolidation and unification*