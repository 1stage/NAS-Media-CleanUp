# NAS Media CleanUp

A Python utility for organizing photos and videos from NAS upload directories into year-based folders based on their metadata creation date.

## Overview

This project contains scripts to help organize media files on a NAS (Network Attached Storage) system by automatically sorting photos and videos into year-based folders using their embedded metadata timestamps.

## Features

- **Automatic Year-Based Organization**: Sorts files into folders like "2023 - Photos" and "2023 - Videos"
- **Metadata Extraction**: Uses EXIF and other metadata to determine creation dates
- **Duplicate Detection**: Identifies and handles duplicate files with binary comparison
- **Dry Run Mode**: Preview changes before executing them
- **Comprehensive Logging**: Detailed logs and reports for all operations
- **UNC Path Support**: Works with Windows UNC network paths
- **Configurable Paths**: Easy configuration through `oby.cfg` file

## Files

- `organize_by_year.py` - Main script for organizing media files
- `test_unc_access.py` - Utility to test UNC path access and list files
- `oby.cfg` - Configuration file for paths and settings
- `logs/` - Directory for operation logs
- `reports/` - Directory for summary reports

## Requirements

Install the required Python packages:

```bash
pip install -r requirements.txt
```

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

### Command Line Options

- `--dry-run` - Preview actions without moving any files
- `--report` - Generate a summary report of moved files
- `--limit N` - Limit number of files to process (useful for testing)
- `--delete-duplicates` - Remove source file if binary match exists in target
- `--help` - Show help message and exit

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

- **v0.10** - Current pre-release version with full feature set
- **v0.3** - UNC access testing utility

## Author

Created by Sean P. Harrington with assistance from Microsoft Copilot

## License

This project is provided as-is for personal use.