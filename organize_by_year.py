"""
organize_by_year.py
Version 0.10 (Pre-release) — Created by Sean P. Harrington with assistance from Microsoft Copilot  
Date: Friday, 26 September 2025, 10:00 AM PDT

Organizes photos and videos from one or more upload directories into year-based folders
based on their metadata creation date. Designed for use with a NAS setup.

Usage:
    python organize_by_year.py [--dry-run] [--report] [--limit N] [--delete-duplicates]

Options:
    --dry-run            Preview actions without moving any files.
    --report             Generate a summary report of moved files.
    --limit N            Limit number of files to process (useful for testing).
    --delete-duplicates  Remove source file if binary match exists in target.
    --help               Show this help message and exit.
"""

import os
import shutil
import filetype
import argparse
import configparser
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata
from datetime import datetime

# === LOAD CONFIG ===
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "oby.cfg"))

UPLOAD_DIRS = [d.strip() for d in config.get("paths", "upload_dirs").split(",")]
PHOTO_DIR = config.get("paths", "photo_dir")
VIDEO_DIR = config.get("paths", "video_dir")

# === TIMESTAMPED LOG/REPORT PATHS ===
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
base_dir = os.path.dirname(os.path.abspath(__file__))
log_folder = os.path.join(base_dir, config.get("logging", "log_folder"))
report_folder = os.path.join(base_dir, config.get("logging", "report_folder"))
os.makedirs(log_folder, exist_ok=True)
os.makedirs(report_folder, exist_ok=True)
LOG_PATH = os.path.join(log_folder, f"organize_log_{timestamp}.txt")
REPORT_PATH = os.path.join(report_folder, f"organize_report_{timestamp}.txt")

log_entries = []
report_entries = []
summary = {
    "processed": 0,
    "moved": 0,
    "locked": 0,
    "duplicates": 0,
    "errors": 0
}

def get_creation_year(filepath):
    try:
        parser = createParser(filepath)
        if not parser:
            return None
        metadata = extractMetadata(parser)
        if metadata and metadata.has("creation_date"):
            return metadata.get("creation_date").year
    except Exception as e:
        log(f"[ERROR] Failed to extract metadata from {filepath}: {e}")
        summary["errors"] += 1
    return None

def log(message):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    entry = f"{timestamp} {message}"
    log_entries.append(entry)
    print(entry)

def get_target_folder(base_dir, year, is_photo):
    label = "Photos" if is_photo else "Videos"
    folder_name = f"{year:04d} - {label}"
    return os.path.join(base_dir, folder_name)

def find_existing_duplicate(filepath, is_photo):
    """Check if file already exists in any year folder without EXIF extraction."""
    filename = os.path.basename(filepath)
    
    # Check common years (2020-2025) and 0000 folder first
    common_years = [2025, 2024, 2023, 2022, 2021, 2020, 0]
    target_base = PHOTO_DIR if is_photo else VIDEO_DIR
    
    for year in common_years:
        target_dir = get_target_folder(target_base, year, is_photo)
        target_path = os.path.join(target_dir, filename)
        if os.path.exists(target_path):
            if is_fast_duplicate(filepath, target_path):
                return target_path
    
    return None

def is_fast_duplicate(src_path, dst_path):
    """Fast duplicate detection using size and modification time only - no binary comparison."""
    try:
        # Check file sizes first (instant)
        src_size = os.path.getsize(src_path)
        dst_size = os.path.getsize(dst_path)
        if src_size != dst_size:
            return False
        
        # Check modification times (instant)
        src_mtime = os.path.getmtime(src_path)
        dst_mtime = os.path.getmtime(dst_path)
        
        # Allow for small time differences due to file system precision
        time_diff = abs(src_mtime - dst_mtime)
        if time_diff > 2.0:  # More than 2 seconds difference
            return False
        
        # If size and time match, consider it a duplicate without binary comparison
        return True
        
    except Exception as e:
        log(f"[ERROR] Failed to compare file metadata: {e}")
        summary["errors"] += 1
        return False

def is_binary_duplicate(src_path, dst_path):
    """Full binary duplicate detection - only use when fast method is inconclusive."""
    try:
        if os.path.getsize(src_path) != os.path.getsize(dst_path):
            return False
        
        # Explicit file handling with forced close
        f1 = open(src_path, "rb")
        f2 = open(dst_path, "rb")
        try:
            result = f1.read() == f2.read()
        finally:
            f1.close()
            f2.close()
        
        # Longer delay to allow network file handles to release
        import time
        time.sleep(2.0)
        
        return result
    except Exception as e:
        log(f"[ERROR] Failed to compare files: {e}")
        summary["errors"] += 1
        return False

def organize_file(filepath, dry_run=False, report=False, delete_duplicates=False):
    summary["processed"] += 1
    kind = filetype.guess(filepath)
    if not kind:
        log(f"[SKIP] Unknown file type: {filepath}")
        summary["errors"] += 1
        return

    is_photo = kind.mime.startswith("image")
    
    # CHECK FOR DUPLICATES FIRST - before any EXIF extraction!
    existing_duplicate = find_existing_duplicate(filepath, is_photo)
    if existing_duplicate:
        log(f"[DUPLICATE] Fast match found (size + time): {filepath} == {existing_duplicate}")
        if delete_duplicates and not dry_run:
            try:
                os.remove(filepath)
                log(f"[REMOVED] Source file deleted: {filepath}")
                summary["duplicates"] += 1
            except Exception as e:
                log(f"[ERROR] Failed to delete duplicate source: {e}")
                summary["errors"] += 1
        else:
            summary["duplicates"] += 1
        return
    
    # Only extract EXIF data if file is NOT a duplicate
    year = get_creation_year(filepath)
    if not year:
        year = 0  # Use 0000 for undated files

    target_base = PHOTO_DIR if is_photo else VIDEO_DIR
    target_dir = get_target_folder(target_base, year, is_photo)
    os.makedirs(target_dir, exist_ok=True)

    filename = os.path.basename(filepath)
    target_path = os.path.join(target_dir, filename)
    if os.path.exists(target_path):
        # This should rarely happen now since we checked common years above
        if is_fast_duplicate(filepath, target_path):
            log(f"[DUPLICATE] Fast match found (size + time): {filepath} == {target_path}")
            if delete_duplicates and not dry_run:
                try:
                    os.remove(filepath)
                    log(f"[REMOVED] Source file deleted: {filepath}")
                    summary["duplicates"] += 1
                except Exception as e:
                    log(f"[ERROR] Failed to delete duplicate source: {e}")
                    summary["errors"] += 1
            else:
                summary["duplicates"] += 1
            return
        else:
            log(f"[SKIP] Filename collision, but content differs: {target_path}")
            summary["duplicates"] += 1
            return

    if dry_run:
        log(f"[DRY-RUN] Would move {filename} → {target_dir}")
    else:
        try:
            log(f"[MOVE] {filename} → {target_dir}")
            shutil.move(filepath, target_path)
            summary["moved"] += 1
        except PermissionError:
            log(f"[SKIP] Locked file (in use): {filepath}")
            summary["locked"] += 1
            return

    if report:
        report_entries.append(f"{filename} → {target_dir}")

def write_logs():
    with open(LOG_PATH, "a", encoding="utf-8") as log_file:
        log_file.write("\n".join(log_entries) + "\n")

def write_report():
    def pct(part):
        return f"{(part / summary['processed'] * 100):.1f}%" if summary["processed"] else "0.0%"

    with open(REPORT_PATH, "w", encoding="utf-8") as report_file:
        report_file.write("Moved Files Report\n")
        report_file.write("==================\n")
        report_file.write("\n".join(report_entries))
        report_file.write("\n\n------------------------------\n")
        report_file.write("Summary\n")
        report_file.write("------------------------------\n")
        report_file.write(f"Total files processed: {summary['processed']}\n")
        report_file.write(f"Files moved: {summary['moved']} / {summary['processed']}, {pct(summary['moved'])}\n")
        report_file.write(f"Locked files skipped: {summary['locked']} / {summary['processed']}, {pct(summary['locked'])}\n")
        report_file.write(f"Duplicate files skipped or removed: {summary['duplicates']} / {summary['processed']}, {pct(summary['duplicates'])}\n")
        report_file.write(f"Unknown type or metadata errors: {summary['errors']} / {summary['processed']}, {pct(summary['errors'])}\n")

def main():
    parser = argparse.ArgumentParser(
        description="Organize NAS media by year based on metadata.",
        epilog="Example: python organize_by_year.py --dry-run --report --limit 50 --delete-duplicates"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without moving files.")
    parser.add_argument("--report", action="store_true", help="Generate a summary report of moved files.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of files to process (useful for testing).")
    parser.add_argument("--delete-duplicates", action="store_true", help="Delete source file if binary duplicate exists in target.")
    args = parser.parse_args()

    try:
        for upload_dir in UPLOAD_DIRS:
            for root, _, files in os.walk(upload_dir):
                folder_name = os.path.basename(root)
                if folder_name.isdigit() and len(folder_name) == 4:
                    continue  # Skip year-based subfolders
                for file in files:
                    if args.limit is not None and summary["processed"] >= args.limit:
                        log(f"[LIMIT] Reached file limit of {args.limit}. Stopping.")
                        write_logs()
                        if args.report:
                            write_report()
                        return
                    filepath = os.path.join(root, file)
                    organize_file(
                        filepath,
                        dry_run=args.dry_run,
                        report=args.report,
                        delete_duplicates=args.delete_duplicates
                    )
    except KeyboardInterrupt:
        log("[INTERRUPTED] Script stopped by user.")

    write_logs()
    if args.report:
        write_report()

if __name__ == "__main__":
    main()