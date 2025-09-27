# NAS Media Organization v1.0

Professional-grade media or### Duplicate Detection (Optional)
For duplicate detection features:

```bash
pip install -r requirements-duplicate-detection.txt
```

**Core Dependencies (always required):**
- Python 3.7+ with standard library (hashlib, os, pathlib, etc.)

**Enhanced Dependencies (recommended):**
- `Pillow>=9.0.0` - Image processing for perceptual hashing
- `imagehash>=4.2.0` - Near-duplicate detection algorithms
- `tqdm>=4.60.0` - Progress bars for better user experience
- `opencv-python>=4.5.0` - Advanced image comparison (optional)
- `scikit-image>=0.19.0` - Structural similarity metrics (optional)

**Note:** The duplicate detection script gracefully degrades functionality if packages aren't available (exact duplicates only without near-duplicate detection).for automatic photo and video management. Designed for both Windows and NAS environments with enterprise-level reliability and features.

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

## Files

### File Organization Scripts
- `organize_by_year.py` - Windows version for organizing media files
- `organize_by_year_nas.py` - NAS-optimized version for Synology/Linux environments
- `run_organize_nas.sh` - Wrapper script for scheduled, auto-updating organization runs
- `test_unc_access.py` - Utility to test UNC path access and list files
- `oby.cfg` - Configuration file for paths and settings

### Duplicate Detection Scripts  
- `detect_duplicates.py` - Interactive duplicate detection for laptop/desktop use
- `requirements-duplicate-detection.txt` - Python dependencies for duplicate detection

### Output Directories
- `logs/` - Directory for operation logs
- `reports/` - Directory for summary reports

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

### Duplicate Detection

#### Duplicate Detection (Interactive - Run from Laptop)
```bash
# Basic duplicate detection with report
python detect_duplicates.py \\NAS-MEDIA\photo\Sorted --report duplicates.txt

# Find 90% similar images with JSON output  
python detect_duplicates.py \\NAS-MEDIA\photo\Sorted --similarity 0.9 --json-output results.json

# Multiple directories, exact duplicates only (faster)
python detect_duplicates.py \\NAS-MEDIA\photo \\NAS-MEDIA\video --no-near-duplicates --report

# Preview mode (safe - no actions taken)
python detect_duplicates.py \\NAS-MEDIA\photo --dry-run

# Batch processing - scan specific year folders
python detect_duplicates.py "\\NAS-MEDIA\photo\Sorted\2023 - Photos" "\\NAS-MEDIA\photo\Sorted\2024 - Photos"
```

### File Organization Command Line Options

- `--dry-run` - Preview actions without moving any files
- `--report` - Generate a summary report of moved files
- `--limit N` - Limit number of files to process (useful for testing)
- `--delete-duplicates` - Remove source file if binary match exists in target
- `--help` - Show help message and exit

### Duplicate Detection Command Line Options

#### Duplicate Detection (`detect_duplicates.py`)
- `--similarity N` - Similarity threshold for near-duplicates (0.0-1.0, default: 0.95)
- `--no-near-duplicates` - Disable near-duplicate detection (faster, exact only)
- `--no-recursive` - Don't scan subdirectories
- `--report FILE` - Save detailed report to file
- `--dry-run` - Preview mode - no actions taken
- `--json-output FILE` - Save results as JSON file

## How It Works

1. **Scans Upload Directories**: Walks through configured upload directories
2. **Skips Year Folders**: Automatically skips existing year-based folders (4-digit names)
3. **Extracts Metadata**: Uses hachoir library to extract creation dates from files
4. **Determines File Type**: Uses filetype library to identify photos vs videos
5. **Creates Target Folders**: Automatically creates year-based folders as needed
6. **Handles Duplicates**: Compares files binary-wise to detect true duplicates
7. **Moves Files**: Relocates files to appropriate year-based directories
8. **Logs Everything**: Records all operations with timestamps

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