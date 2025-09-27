#!/usr/bin/env python3
"""
organize_by_year_nas.py
Simplified NAS version - uses only built-in Python libraries
Works with file modification dates instead of EXIF data
"""

import os
import shutil
import argparse
import configparser
import time
from datetime import datetime

# === LOAD CONFIG ===
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "oby-nas.cfg"))

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
    "duplicates": 0,
    "locked": 0,
    "errors": 0,
    "empty_dirs_removed": 0
}

def log(message):
    print(message)
    log_entries.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def is_system_file(filepath):
    """Check if file is a Synology system file that should be skipped."""
    filename = os.path.basename(filepath)
    return (filename.startswith('.') or 
            filename.startswith('@') or 
            '@SynoEAStream' in filepath or 
            '@eaDir' in filepath or
            filename == 'Thumbs.db')

# === FILE TYPE DETECTION (Built-in) ===
def is_photo_file(filepath):
    """Simple file type detection based on extension."""
    photo_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.heic', '.heif', '.raw', '.cr2', '.nef', '.arw'}
    ext = os.path.splitext(filepath)[1].lower()
    return ext in photo_extensions

def is_video_file(filepath):
    """Simple video file detection based on extension."""
    video_extensions = {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm', '.m4v', '.3gp', '.mpg', '.mpeg', '.mts', '.m2ts'}
    ext = os.path.splitext(filepath)[1].lower()
    return ext in video_extensions

# === DATE EXTRACTION (Using file modification time) ===
def get_creation_year(filepath):
    """Extract year from file modification time."""
    try:
        # Try file modification time
        mtime = os.path.getmtime(filepath)
        return datetime.fromtimestamp(mtime).year
    except Exception as e:
        log(f"[WARN] Could not get date for {filepath}: {e}")
        return None

# === DUPLICATE DETECTION ===
def is_fast_duplicate(src_path, dst_path):
    """Fast duplicate detection using size and modification time."""
    try:
        # Check file sizes first
        src_size = os.path.getsize(src_path)
        dst_size = os.path.getsize(dst_path)
        if src_size != dst_size:
            return False
        
        # Check modification times
        src_mtime = os.path.getmtime(src_path)
        dst_mtime = os.path.getmtime(dst_path)
        
        # Allow for small time differences
        time_diff = abs(src_mtime - dst_mtime)
        if time_diff > 2.0:  # More than 2 seconds difference
            return False
        
        return True
        
    except Exception as e:
        log(f"[ERROR] Fast duplicate check failed for {src_path}: {e}")
        return False

def find_existing_duplicate(source_file, is_photo):
    """Check common target year folders for duplicates."""
    filename = os.path.basename(source_file)
    target_base = PHOTO_DIR if is_photo else VIDEO_DIR
    
    # Check most common years first
    common_years = [2025, 2024, 2023, 2022, 2021, 2020, 0]
    
    for year in common_years:
        target_dir = get_target_folder(target_base, year, is_photo)
        if not os.path.exists(target_dir):
            continue
            
        potential_path = os.path.join(target_dir, filename)
        if os.path.exists(potential_path):
            if is_fast_duplicate(source_file, potential_path):
                return potential_path
    
    return None

# === FILE OPERATIONS ===
def move_file_with_retry(source_path, target_path, filename, target_dir, max_retries=3, delay=1.0):
    """Move file with retry logic."""
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                wait_time = delay * (2 ** (attempt - 1))
                log(f"[RETRY {attempt}] Waiting {wait_time:.1f}s before retry: {filename}")
                time.sleep(wait_time)
            
            log(f"[MOVE] {filename} → {target_dir}")
            shutil.move(source_path, target_path)
            
            if attempt > 0:
                log(f"[SUCCESS] File moved after {attempt} retries: {filename}")
            
            return True
            
        except PermissionError as e:
            if attempt < max_retries:
                log(f"[LOCKED] File in use, will retry ({attempt + 1}/{max_retries}): {filename}")
            else:
                log(f"[SKIP] File locked after {max_retries} retries: {filename}")
                return False
        except Exception as e:
            log(f"[ERROR] Unexpected error moving {filename}: {str(e)}")
            return False
    
    return False

def delete_file_with_retry(filepath, max_retries=3, delay=0.5):
    """Delete file with retry logic."""
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                wait_time = delay * attempt
                log(f"[RETRY DELETE {attempt}] Waiting {wait_time:.1f}s: {os.path.basename(filepath)}")
                time.sleep(wait_time)
            
            os.remove(filepath)
            log(f"[REMOVED] Source file deleted: {filepath}")
            
            if attempt > 0:
                log(f"[SUCCESS] File deleted after {attempt} retries: {os.path.basename(filepath)}")
            
            return True
            
        except PermissionError:
            if attempt < max_retries:
                log(f"[LOCKED DELETE] File in use, will retry ({attempt + 1}/{max_retries}): {os.path.basename(filepath)}")
            else:
                log(f"[SKIP DELETE] File locked after {max_retries} retries: {filepath}")
                return False
        except Exception as e:
            log(f"[ERROR] Unexpected error deleting {filepath}: {str(e)}")
            return False
    
    return False

# === DIRECTORY MANAGEMENT ===
def get_target_folder(base_dir, year, is_photo):
    """Generate target folder path."""
    if year == 0:
        folder_name = "0000 - Photos" if is_photo else "0000 - Videos"
    else:
        folder_name = f"{year} - Photos" if is_photo else f"{year} - Videos"
    return os.path.join(base_dir, folder_name)

def get_unique_filename(target_dir, filename):
    """Generate a unique filename if collision exists."""
    base_path = os.path.join(target_dir, filename)
    if not os.path.exists(base_path):
        return filename, base_path
    
    name, ext = os.path.splitext(filename)
    counter = 1
    
    while True:
        new_filename = f"{name}_copy{counter}{ext}"
        new_path = os.path.join(target_dir, new_filename)
        if not os.path.exists(new_path):
            return new_filename, new_path
        counter += 1

def remove_empty_directories(root_dir, dry_run=False):
    """Remove empty directories within root_dir, including system directories."""
    removed_dirs = []
    
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        if dirpath == root_dir:
            continue
            
        try:
            # Check if directory is empty or contains only system files
            non_system_files = [f for f in filenames if not is_system_file(os.path.join(dirpath, f))]
            non_system_dirs = [d for d in dirnames if not d.startswith('@')]
            
            if not non_system_files and not non_system_dirs:
                if dry_run:
                    log(f"[DRY-RUN] Would remove empty directory: {dirpath}")
                    removed_dirs.append(dirpath)
                else:
                    # Remove any remaining system files first
                    for f in filenames:
                        try:
                            os.remove(os.path.join(dirpath, f))
                        except:
                            pass
                    # Remove any system subdirectories
                    for d in dirnames:
                        try:
                            import shutil
                            shutil.rmtree(os.path.join(dirpath, d))
                        except:
                            pass
                    # Now remove the directory itself
                    os.rmdir(dirpath)
                    log(f"[CLEANUP] Removed empty directory: {dirpath}")
                    removed_dirs.append(dirpath)
        except Exception as e:
            log(f"[ERROR] Failed to remove empty directory {dirpath}: {e}")
    
    return removed_dirs

# === MAIN PROCESSING ===
def organize_file(filepath, dry_run=False, delete_duplicates=False, report=False, handle_collisions=False):
    """Process a single file."""
    summary["processed"] += 1
    

    
    # Determine file type
    if is_photo_file(filepath):
        is_photo = True
    elif is_video_file(filepath):
        is_photo = False
    else:
        log(f"[SKIP] Unknown file type: {filepath}")
        summary["errors"] += 1
        return
    
    # Check for duplicates first
    existing_duplicate = find_existing_duplicate(filepath, is_photo)
    if existing_duplicate:
        log(f"[DUPLICATE] Fast match found (size + time): {filepath} == {existing_duplicate}")
        if delete_duplicates and not dry_run:
            if delete_file_with_retry(filepath):
                summary["duplicates"] += 1
            else:
                summary["errors"] += 1
        else:
            summary["duplicates"] += 1
        return
    
    # Get creation year from file modification time
    year = get_creation_year(filepath)
    if not year:
        year = 0  # Use 0000 for undated files

    target_base = PHOTO_DIR if is_photo else VIDEO_DIR
    target_dir = get_target_folder(target_base, year, is_photo)
    os.makedirs(target_dir, exist_ok=True)

    filename = os.path.basename(filepath)
    target_path = os.path.join(target_dir, filename)
    
    if os.path.exists(target_path):
        if is_fast_duplicate(filepath, target_path):
            log(f"[DUPLICATE] Fast match found (size + time): {filepath} == {target_path}")
            if delete_duplicates and not dry_run:
                if delete_file_with_retry(filepath):
                    summary["duplicates"] += 1
                else:
                    summary["errors"] += 1
            else:
                summary["duplicates"] += 1
            return
        else:
            if handle_collisions:
                new_filename, target_path = get_unique_filename(target_dir, filename)
                log(f"[COLLISION] Renaming due to collision: {filename} → {new_filename}")
                filename = new_filename
            else:
                log(f"[SKIP] Filename collision, but content differs: {target_path}")
                summary["duplicates"] += 1
                return

    if dry_run:
        log(f"[DRY-RUN] Would move {filename} → {target_dir}")
    else:
        success = move_file_with_retry(filepath, target_path, filename, target_dir)
        if success:
            summary["moved"] += 1
        else:
            summary["locked"] += 1
            return

    if report:
        report_entries.append(f"{filename} → {target_dir}")

# === REPORTING ===
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

# === MAIN FUNCTION ===
def main():
    parser = argparse.ArgumentParser(
        description="Organize NAS media by year (simplified version using file dates).",
        epilog="Example: python3 organize_by_year_nas.py --dry-run --report --limit 50"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without moving files.")
    parser.add_argument("--report", action="store_true", help="Generate a summary report of moved files.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of files to process.")
    parser.add_argument("--delete-duplicates", action="store_true", help="Delete source file if duplicate exists.")
    parser.add_argument("--handle-collisions", action="store_true", help="Rename files when filename collisions occur.")
    parser.add_argument("--cleanup-empty-dirs", action="store_true", help="Remove empty directories after processing.")
    
    args = parser.parse_args()
    
    log("="*50)
    log("NAS Media Organization Script v0.12-NAS")
    log("="*50)
    
    if args.dry_run:
        log("[DRY RUN MODE] No files will be moved or deleted.")
    if args.delete_duplicates:
        log("[DELETE DUPLICATES] Source files will be removed if duplicates exist.")
    if args.limit:
        log(f"[LIMIT] Processing only {args.limit} files.")
    
    processed_count = 0
    
    for upload_dir in UPLOAD_DIRS:
        if not os.path.exists(upload_dir):
            log(f"[SKIP] Upload directory not found: {upload_dir}")
            continue
        
        log(f"[SCAN] Processing directory: {upload_dir}")
        
        for root, dirs, files in os.walk(upload_dir):
            for file in files:
                filepath = os.path.join(root, file)
                
                # Skip system files before counting them
                if is_system_file(filepath):
                    log(f"[SKIP] System file: {filepath}")
                    continue
                
                if args.limit and processed_count >= args.limit:
                    log(f"[LIMIT] Reached file limit of {args.limit}. Stopping.")
                    break
                
                organize_file(filepath, args.dry_run, args.delete_duplicates, args.report, args.handle_collisions)
                processed_count += 1
            
            if args.limit and processed_count >= args.limit:
                break
        
        if args.limit and processed_count >= args.limit:
            break
    
    # Clean up empty directories if requested
    if args.cleanup_empty_dirs:
        log("="*50)
        log("CLEANING UP EMPTY DIRECTORIES")
        log("="*50)
        total_removed = 0
        for upload_dir in UPLOAD_DIRS:
            if os.path.exists(upload_dir):
                log(f"[CLEANUP] Scanning for empty directories in: {upload_dir}")
                removed_dirs = remove_empty_directories(upload_dir, args.dry_run)
                total_removed += len(removed_dirs)
        
        summary["empty_dirs_removed"] = total_removed
        if total_removed > 0:
            log(f"[CLEANUP] Removed {total_removed} empty directories")
        else:
            log("[CLEANUP] No empty directories found")
    
    # Write logs and report
    write_logs()
    if args.report:
        write_report()
        log(f"[REPORT] Summary written to: {REPORT_PATH}")
    
    log(f"[LOG] Full log written to: {LOG_PATH}")
    log("="*50)
    log("PROCESSING COMPLETE")
    log("="*50)
    log(f"Files processed: {summary['processed']}")
    log(f"Files moved: {summary['moved']}")
    log(f"Duplicates handled: {summary['duplicates']}")
    log(f"Files locked/skipped: {summary['locked']}")
    log(f"Errors: {summary['errors']}")
    if args.cleanup_empty_dirs:
        log(f"Empty dirs removed: {summary['empty_dirs_removed']}")

if __name__ == "__main__":
    main()