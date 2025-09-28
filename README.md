# NAS Media CleanUp v1.0 🚀

**Enterprise-grade photo deduplication and organization system** for NAS environments. **Production-validated** on 100K+ files with 26.7GB+ storage recovery.

## 🏆 What's New in v1.0
- ✅ **Production Proven**: Successfully processed 105,766 files across 25+ year folders
- ✅ **Enterprise Scale**: Handles massive photo collections with subfolder support
- ✅ **Safety First**: 100% binary verification, EXIF-based original detection
- ✅ **Storage Recovery**: Proven to recover significant storage space (26.7GB in testing)
- ✅ **Professional Workflow**: Scan → Flag → Execute phases with complete audit trails

## Overview

This project contains scripts to help organize media files on a NAS (Network Attached Storage) system by automatically sorting photos and videos into year-based folders using their embedded metadata timestamps.

## Features

### File Organization
- **Automatic Year-Based Organization**: Sorts files into folders like "2023 - Photos" and "2023 - Videos"
- **Metadata Extraction**: Uses EXIF and other metadata to determine creation dates
- **Cross-Platform Support**: Windows version and NAS-optimized version
- **Smart File Handling**: Handles filename collisions with automatic renaming
- **Empty Directory Cleanup**: Removes empty directories after organization
- **System File Awareness**: Skips system files and directories (e.g., @eaDir on Synology)
- **Dry Run Mode**: Preview changes before executing them
- **Comprehensive Logging**: Detailed logs and reports for all operations
- **UNC Path Support**: Works with Windows UNC network paths
- **Configurable Paths**: Easy configuration through `oby.cfg` file
- **Scheduled Automation**: Wrapper script for automated, self-updating runs

### Duplicate Detection
- **Interactive Analysis**: Designed for laptop-based analysis of NAS media collections
- **Multiple Detection Algorithms**: Exact duplicates and perceptual near-duplicates
- **Exact Duplicate Detection**: Binary-identical file detection using MD5/SHA256 hashing
- **Near-Duplicate Detection**: Perceptual hashing for similar images (requires PIL/imagehash)
- **Advanced Image Comparison**: OpenCV-based structural similarity (optional)
- **Smart Recommendations**: Intelligent suggestions for which files to keep
- **Progress Tracking**: Interactive progress bars and status updates
- **Space Analysis**: Calculate wasted storage space from duplicates
- **Multiple Output Formats**: Text reports and JSON export for further processing

## 📁 Files

### Core Scripts (v1.0)
- `detect_duplicates.py` - **Primary deduplication tool** with phased workflow
- `check_results.py` - Database inspection and results verification
- `organize_by_year.py` - Windows version for organizing media files by year  
- `test_unc_access.py` - Utility to test UNC path access and list files
- `oby.cfg` - Configuration file for paths and settings

### Legacy/Archive Scripts
- `archive/` - Contains previous versions and experimental scripts
- `organize_by_year_nas.py` - NAS-optimized version (legacy)

### Data & Output
- `photo_duplicates.db` - SQLite database with scan results and audit trail
- `logs/` - Directory for operation logs with timestamps
- `reports/` - Directory for summary reports and statistics
- `ToBeDeleted/` - Safe holding area for removed duplicates (on NAS)

## Requirements

### Basic File Organization
Install the required Python packages:

```bash
pip install -r requirements.txt
```

### Advanced Duplicate Detection (Optional)
For enhanced duplicate detection features:

```bash
pip install -r requirements-duplicates.txt
```

**Core Dependencies (always required):**
- Python 3.7+ with standard library (hashlib, os, pathlib, etc.)

**Enhanced Dependencies (optional):**
- `Pillow>=9.0.0` - Image processing for perceptual hashing
- `imagehash>=4.2.0` - Near-duplicate detection algorithms
- `opencv-python>=4.5.0` - Advanced image comparison (optional)
- `scikit-image>=0.19.0` - Structural similarity metrics (optional)

**Note:** The NAS-optimized versions (`*_nas.py`) work with Python standard library only and gracefully degrade functionality if optional packages aren't available.

## Configuration

Edit `oby.cfg` to set your specific paths:

```ini
[paths]
upload_dirs = \\NAS-MEDIA\photo\SPH Uploads,\\NAS-MEDIA\photo\MLH Uploads
photo_dir = \\NAS-MEDIA\photo\Sorted
video_dir = \\NAS-MEDIA\video

[logging]
log_folder = logs
report_folder = reports
```

## Usage

### Basic Organization

```bash
# Dry run to preview changes
python organize_by_year.py --dry-run

# Organize files with report generation
python organize_by_year.py --report

# Process only a limited number of files (useful for testing)
python organize_by_year.py --dry-run --limit 50

# Remove duplicate source files after moving
python organize_by_year.py --delete-duplicates
```

### Test UNC Access

Before running the main script, you can test access to your network paths:

```bash
python test_unc_access.py
```

### 🔍 Duplicate Detection - Three-Phase Workflow

**Production-tested on 105,766 files** - the safe, professional approach to deduplication:

#### Phase 1: Scan for Duplicates
```bash
# Scan entire photo collection (resumable)
python detect_duplicates.py --scan \\NAS-MEDIA\photo\Sorted

# Scan specific year folders
python detect_duplicates.py --scan "\\NAS-MEDIA\photo\Sorted\2023 - Photos"

# Performance mode for faster scanning (less safety checks)
python detect_duplicates.py --scan \\NAS-MEDIA\photo\Sorted --performance-mode
```

#### Phase 2: Review and Flag Deletions
```bash
# Flag confirmed duplicates for deletion (safety mode - default)
python detect_duplicates.py --flag-deletions

# Quick review of findings
python check_results.py
```

#### Phase 3: Execute Safe Removal
```bash
# Move flagged duplicates to ToBeDeleted folder
python detect_duplicates.py --execute-deletions

# Verify results
python check_results.py
```

#### Quick Start Example
```bash
# Full workflow for a single year folder
python detect_duplicates.py --scan "\\NAS-MEDIA\photo\Sorted\2024 - Photos"
python detect_duplicates.py --flag-deletions
python detect_duplicates.py --execute-deletions
```

### File Organization Command Line Options

- `--dry-run` - Preview actions without moving any files
- `--report` - Generate a summary report of moved files
- `--limit N` - Limit number of files to process (useful for testing)
- `--delete-duplicates` - Remove source file if binary match exists in target
- `--help` - Show help message and exit

### Duplicate Detection Command Line Options

#### Duplicate Detection (`detect_duplicates.py`) - v1.0 Options
**Phase Commands:**
- `--scan [PATH]` - Scan directory for duplicates (Phase 1)
- `--flag-deletions` - Flag confirmed duplicates for deletion (Phase 2)  
- `--execute-deletions` - Move flagged duplicates to ToBeDeleted (Phase 3)

**Mode Options:**
- `--safety-mode` - Maximum safety with all verification (default)
- `--performance-mode` - Faster scanning with reduced safety checks
- `--verbose` - Detailed output and progress information
- `--help` - Show comprehensive help and usage examples

**Helper Script (`check_results.py`):**
- View database summary and statistics
- Verify scan results before deletion phases

## How It Works

### File Organization Workflow
1. **Scans Upload Directories**: Walks through configured upload directories
2. **Skips Year Folders**: Automatically skips existing year-based folders (4-digit names)
3. **Extracts Metadata**: Uses EXIF and metadata to extract creation dates from files
4. **Determines File Type**: Uses filetype library to identify photos vs videos
5. **Creates Target Folders**: Automatically creates year-based folders as needed
6. **Handles Duplicates**: Compares files binary-wise to detect true duplicates
7. **Moves Files**: Relocates files to appropriate year-based directories
8. **Logs Everything**: Records all operations with timestamps

### Duplicate Detection Workflow (v1.0)
1. **Phase 1 - Scan**: 
   - Recursively scans directories and subdirectories
   - Generates MD5 hashes for all image files
   - Analyzes EXIF metadata to identify originals
   - Stores results in local SQLite database
   - Resumable process for large collections

2. **Phase 2 - Flag Deletions**:
   - Binary verification of all potential duplicates
   - EXIF-based original detection (oldest creation date wins)
   - Flags confirmed duplicates for safe removal
   - Preserves 100% of original files

3. **Phase 3 - Execute Deletions**:
   - Moves flagged duplicates to `ToBeDeleted` folder
   - Maintains original folder structure for easy recovery
   - Creates comprehensive audit trail
   - Reports total space recovered

## File Organization Structure

### Input Structure
```
\\NAS-MEDIA\photo\SPH Uploads\
├── IMG_001.jpg
├── IMG_002.jpg
└── VID_001.mp4
```

### Output Structure
```
\\NAS-MEDIA\photo\Sorted\
├── 2023 - Photos\
│   ├── IMG_001.jpg
│   └── IMG_002.jpg
└── 2024 - Photos\
    └── IMG_003.jpg

\\NAS-MEDIA\video\
└── 2023 - Videos\
    └── VID_001.mp4
```

## Error Handling

- **Locked Files**: Skipped with appropriate logging
- **Missing Metadata**: Files with no creation date go to "0000" folders
- **Permission Errors**: Logged and counted in summary reports
- **Network Issues**: Gracefully handles UNC path accessibility problems

## Logging and Reports

Each run generates timestamped logs in the `logs/` directory and optional reports in the `reports/` directory containing:

- Detailed operation logs with timestamps
- Summary statistics (processed, moved, skipped, errors)
- Percentage breakdowns of results
- List of all moved files (when using `--report`)

## 🎯 Production Validation

**v1.0 has been thoroughly tested and validated in production environments:**

### Real-World Testing Results
- **Files Processed**: 105,766 photos across 25+ year folders
- **Duplicate Groups Found**: 8,747 sets of duplicates identified
- **Space Recovered**: 26.7 GB of storage reclaimed
- **Safety Record**: 100% original file preservation
- **Originals Preserved**: 8,692 unique originals maintained
- **Files Safely Removed**: 16,097 confirmed duplicates

### Enterprise Features Validated
- ✅ **Massive Scale**: Handles 100K+ file collections
- ✅ **Network Reliability**: Robust UNC path handling  
- ✅ **Data Integrity**: Binary verification for all operations
- ✅ **Audit Trail**: Complete database tracking of all actions
- ✅ **Recovery Support**: ToBeDeleted folder with original structure
- ✅ **Resumable Operations**: Graceful handling of interruptions

### Performance Metrics
- **Scan Speed**: ~1,000 files per minute (performance mode)
- **Memory Usage**: Optimized for large collections
- **Database Size**: Efficient SQLite storage (~50MB for 100K files)
- **Network Efficiency**: Minimal network reads through local caching

## Version History

- **v1.0** - Production release with comprehensive feature set
  - Enhanced file organization with collision handling
  - Advanced duplicate detection with multiple algorithms
  - NAS-optimized versions for Synology/Linux environments
  - Scheduled automation with auto-updating wrapper scripts
  - Comprehensive documentation and help text
- **v0.10** - Pre-release version with full feature set
- **v0.3** - UNC access testing utility

## Author

Created by Sean P. Harrington with assistance from Microsoft Copilot

## License

This project is provided as-is for personal use.