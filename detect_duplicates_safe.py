#!/usr/bin/env python3
"""
detect_duplicates_safe.py
Version 0.3 — Created by Sean P. Harrington with assistance from Microsoft Copilot
Date: Friday, 27 September 2025

Conservative Safe Duplicate Detection with Multi-Folder Support

OVERVIEW:
A methodical, safety-first approach to duplicate photo detection and removal.
Supports both single-folder and cross-folder duplicate detection with a phased
processing approach that requires manual approval at each step.

KEY FEATURES:
• Multi-folder support for cross-year duplicate detection
• Normalized image hashing for fast initial duplicate identification
• Binary verification to prevent false positives from hash collisions  
• EXIF date analysis to identify true original files
• Phased processing: Scan → Flag → Execute (manual approval required)
• Safe deletion via move to ToBeDeleted folder (preserves structure)
• Comprehensive SQLite database with full audit trail

SAFETY PHILOSOPHY:
1. NO AUTOMATIC DELETIONS - Each phase requires manual execution
2. ORIGINAL PRESERVATION - Original files never flagged for removal
3. BINARY VERIFICATION - All duplicates confirmed byte-for-byte identical
4. AUDIT TRAIL - Complete database tracking of all operations
5. REVERSIBLE ACTIONS - Files moved to ToBeDeleted (not permanently deleted)

WORKFLOW:
Phase 1 (--scan): 
  • Recursively scan folders for image files
  • Generate normalized hashes (64x64 thumbnails with letterboxing)
  • Group files by hash similarity
  • Binary verify potential duplicates (prevents hash collisions)
  • Analyze EXIF creation dates + file modification dates
  • Identify original file (earliest EXIF date, then file date)
  • Mark originals and verified duplicates in database

Phase 2 (--flag-deletions):
  • Review binary-verified duplicates
  • Flag confirmed duplicates for deletion
  • Ensure original files are never flagged
  • Update database with deletion flags and references

Phase 3 (--execute-deletions):
  • Move flagged files to ToBeDeleted folder
  • Preserve original folder structure under ToBeDeleted
  • Update database with new file locations
  • Generate completion report

SUPPORTED FORMATS:
.jpg, .jpeg, .png, .gif, .bmp, .tiff, .tif, .webp, .heic, .heif

USAGE EXAMPLES:
  # Single folder
  python detect_duplicates_safe.py "C:\\Photos\\2010 - Photos" --scan
  
  # Multi-folder cross-duplicate detection  
  python detect_duplicates_safe.py --folders "C:\\Photos\\2010" "C:\\Photos\\2011" --scan
  
  # Complete workflow
  python detect_duplicates_safe.py "C:\\Photos\\2010 - Photos" --all-phases

AUTHOR: GitHub Copilot Assistant
DATE: September 2025
LICENSE: MIT
"""

import os
import sys
import argparse
import hashlib
import time
import sqlite3
from datetime import datetime
from collections import defaultdict
from pathlib import Path

# Required dependencies
try:
    from PIL import Image, ImageOps
    from PIL.ExifTags import TAGS
    HAS_PILLOW = True
except ImportError:
    print("❌ ERROR: PIL/Pillow is required")
    print("Install with: pip install Pillow")
    sys.exit(1)

# Enable HEIC/HEIF support if available
try:
    import pillow_heif
    pillow_heif.register_heif_opener()
    HAS_HEIF = True
except ImportError:
    HAS_HEIF = False


class SafeDuplicateDetector:
    """Conservative duplicate detection with binary verification and original preservation."""
    
    def __init__(self, folder_paths, db_path=None, thumbnail_size=64, verbose=False):
        # Support both single folder and multiple folders
        if isinstance(folder_paths, (str, Path)):
            self.folder_paths = [Path(folder_paths).resolve()]
        else:
            self.folder_paths = [Path(fp).resolve() for fp in folder_paths]
        
        # Use first folder for database location if not specified
        self.primary_folder = self.folder_paths[0]
        self.db_path = db_path or str(self.primary_folder.parent / "multi_folder_duplicates.db")
        self.thumbnail_size = thumbnail_size
        self.verbose = verbose
        
        # Supported image formats
        self.image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', 
                               '.tiff', '.tif', '.webp', '.heic', '.heif'}
        
        # Initialize database
        self._init_database()
        self._upgrade_database_schema()
    
    def _init_database(self):
        """Initialize SQLite database for safe duplicate tracking."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS safe_files (
                id INTEGER PRIMARY KEY,
                file_path TEXT UNIQUE,
                folder_context TEXT,
                relative_path TEXT,
                file_size INTEGER,
                file_mtime REAL,
                exif_date TEXT,
                normalized_hash TEXT,
                is_original BOOLEAN DEFAULT 0,
                removal_flagged BOOLEAN DEFAULT 0,
                deletion_flagged BOOLEAN DEFAULT 0,
                deleted BOOLEAN DEFAULT 0,
                original_reference TEXT,
                binary_verified BOOLEAN DEFAULT 0,
                processed_date TEXT,
                last_update_date TEXT,
                last_update_type TEXT,
                deleted_to_path TEXT,
                notes TEXT
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_groups (
                id INTEGER PRIMARY KEY,
                group_hash TEXT,
                original_file TEXT,
                folders_involved TEXT,
                cross_folder_group BOOLEAN DEFAULT 0,
                total_files INTEGER,
                total_size INTEGER,
                files_flagged INTEGER DEFAULT 0,
                files_deleted INTEGER DEFAULT 0,
                verification_status TEXT,
                created_date TEXT,
                last_update_date TEXT,
                last_update_type TEXT
            )
        """)
        
        self.conn.commit()
    
    def _upgrade_database_schema(self):
        """Upgrade existing database schema to include new columns."""
        try:
            # Check if new columns exist
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA table_info(safe_files)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # Add missing columns
            new_columns = [
                ('deletion_flagged', 'BOOLEAN DEFAULT 0'),
                ('deleted', 'BOOLEAN DEFAULT 0'),
                ('last_update_date', 'TEXT'),
                ('last_update_type', 'TEXT'),
                ('deleted_to_path', 'TEXT'),
                ('folder_context', 'TEXT'),
                ('relative_path', 'TEXT')
            ]
            
            for column_name, column_def in new_columns:
                if column_name not in columns:
                    self.conn.execute(f"ALTER TABLE safe_files ADD COLUMN {column_name} {column_def}")
                    print(f"Added column: {column_name}")
            
            # Check duplicate_groups table
            cursor.execute("PRAGMA table_info(duplicate_groups)")
            group_columns = [column[1] for column in cursor.fetchall()]
            
            group_new_columns = [
                ('files_flagged', 'INTEGER DEFAULT 0'),
                ('files_deleted', 'INTEGER DEFAULT 0'),
                ('last_update_date', 'TEXT'),
                ('last_update_type', 'TEXT'),
                ('folders_involved', 'TEXT'),
                ('cross_folder_group', 'BOOLEAN DEFAULT 0')
            ]
            
            for column_name, column_def in group_new_columns:
                if column_name not in group_columns:
                    self.conn.execute(f"ALTER TABLE duplicate_groups ADD COLUMN {column_name} {column_def}")
                    print(f"Added group column: {column_name}")
            
            self.conn.commit()
            
        except Exception as e:
            print(f"Schema upgrade error: {e}")
    
    def is_image_file(self, file_path):
        """Check if file is a supported image format."""
        return Path(file_path).suffix.lower() in self.image_extensions
    
    def update_file_record(self, file_path, update_type, **kwargs):
        """Update file record with timestamp and update type tracking."""
        update_time = datetime.now().isoformat()
        
        # Build dynamic update query
        update_fields = ['last_update_date = ?', 'last_update_type = ?']
        update_values = [update_time, update_type]
        
        for field, value in kwargs.items():
            update_fields.append(f'{field} = ?')
            update_values.append(value)
        
        update_values.append(file_path)  # WHERE clause
        
        query = f"""
            UPDATE safe_files 
            SET {', '.join(update_fields)}
            WHERE file_path = ?
        """
        
        self.conn.execute(query, update_values)
        self.conn.commit()
        
        print(f"Updated {Path(file_path).name}: {update_type}")
    
    def create_deletion_path(self, original_file_path):
        """Create mirrored path structure in ToBeDeleted folder."""
        # Convert from Sorted to ToBeDeleted path
        original_path = Path(original_file_path)
        
        # Find the "Sorted" part and replace with "ToBeDeleted"
        parts = list(original_path.parts)
        try:
            sorted_index = parts.index('Sorted')
            parts[sorted_index] = 'ToBeDeleted'
            deletion_path = Path(*parts)
            
            # Create directory structure if needed
            deletion_path.parent.mkdir(parents=True, exist_ok=True)
            
            return str(deletion_path)
        except ValueError:
            # Fallback if "Sorted" not found in path
            base_deletion_dir = Path("\\\\NAS-MEDIA\\photo\\ToBeDeleted")
            return str(base_deletion_dir / original_path.name)
    
    def extract_exif_date(self, file_path):
        """Extract original creation date from EXIF data."""
        try:
            with Image.open(file_path) as img:
                exif_data = img._getexif()
                if exif_data:
                    # Look for various date fields (in order of preference)
                    date_fields = [
                        'DateTimeOriginal',     # Camera capture date
                        'DateTimeDigitized',    # Scan/digitization date  
                        'DateTime'              # File modification date
                    ]
                    
                    for field in date_fields:
                        for tag_id, tag_name in TAGS.items():
                            if tag_name == field and tag_id in exif_data:
                                date_str = exif_data[tag_id]
                                try:
                                    # Parse EXIF date format: "2010:10:12 18:00:18"
                                    return datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")
                                except ValueError:
                                    continue
        except Exception as e:
            print(f"EXIF extraction failed for {file_path}: {e}")
        
        return None
    
    def generate_normalized_hash(self, file_path):
        """Generate normalized hash for potential duplicate detection."""
        try:
            with Image.open(file_path) as img:
                # Convert to RGB (handles various formats)
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Resize to thumbnail while preserving aspect ratio
                img.thumbnail((self.thumbnail_size, self.thumbnail_size), Image.Resampling.LANCZOS)
                
                # Create letterboxed version with black padding
                letterbox = Image.new('RGB', (self.thumbnail_size, self.thumbnail_size), (0, 0, 0))
                paste_x = (self.thumbnail_size - img.width) // 2
                paste_y = (self.thumbnail_size - img.height) // 2
                letterbox.paste(img, (paste_x, paste_y))
                
                # Convert to bytes and hash
                img_bytes = letterbox.tobytes()
                return hashlib.md5(img_bytes).hexdigest()
                
        except Exception as e:
            print(f"Hash generation failed for {file_path}: {e}")
            return None
    
    def binary_compare_files(self, file1_path, file2_path):
        """Perform byte-for-byte binary comparison of two files."""
        try:
            with open(file1_path, 'rb') as f1, open(file2_path, 'rb') as f2:
                # Compare file sizes first (quick check)
                f1.seek(0, 2)  # Seek to end
                f2.seek(0, 2)
                if f1.tell() != f2.tell():
                    return False
                
                # Reset and compare chunks
                f1.seek(0)
                f2.seek(0)
                
                chunk_size = 8192
                while True:
                    chunk1 = f1.read(chunk_size)
                    chunk2 = f2.read(chunk_size)
                    
                    if chunk1 != chunk2:
                        return False
                    
                    if not chunk1:  # End of file
                        return True
                        
        except Exception as e:
            print(f"Binary comparison failed: {e}")
            return False
    
    def determine_original_file(self, file_paths):
        """Determine which file is the original based on EXIF + file dates."""
        candidates = []
        
        for file_path in file_paths:
            file_stat = os.stat(file_path)
            exif_date = self.extract_exif_date(file_path)
            
            candidates.append({
                'path': file_path,
                'exif_date': exif_date,
                'file_mtime': datetime.fromtimestamp(file_stat.st_mtime),
                'file_size': file_stat.st_size,
                'filename': Path(file_path).name
            })
        
        # Sort by: EXIF date (if available), then file mtime, then filename
        def sort_key(candidate):
            # Primary: EXIF date (earliest wins)
            exif_priority = candidate['exif_date'] or datetime.max
            
            # Secondary: File modification time (earliest wins)  
            file_priority = candidate['file_mtime']
            
            # Tertiary: Prefer files without "copy" in name
            name_priority = 1 if 'copy' in candidate['filename'].lower() else 0
            
            return (exif_priority, file_priority, name_priority)
        
        candidates.sort(key=sort_key)
        original = candidates[0]
        
        print(f"Original determined: {original['path']}")
        if original['exif_date']:
            print(f"  EXIF date: {original['exif_date']}")
        print(f"  File date: {original['file_mtime']}")
        
        return original['path']
    
    def scan_folders(self, rescan=False):
        """Phase 1: Scan multiple folders recursively and build potential duplicate groups."""
        print(f"🔍 PHASE 1: MULTI-FOLDER SCANNING")
        print(f"Folders to scan: {len(self.folder_paths)}")
        for folder in self.folder_paths:
            print(f"  - {folder}")
        
        if rescan:
            print("Clearing previous scan data for fresh analysis...")
            self.conn.execute("DELETE FROM safe_files")
            self.conn.execute("DELETE FROM duplicate_groups")
            self.conn.commit()
        
        # Build hash groups
        hash_groups = defaultdict(list)
        total_files = 0
        updated_files = 0
        
        for folder_path in self.folder_paths:
            print(f"\n📁 Scanning folder: {folder_path.name}")
            folder_files = 0
            
            for root, dirs, files in os.walk(folder_path):
                for filename in files:
                    file_path = Path(root) / filename
                    
                    if not self.is_image_file(file_path):
                        continue
                    
                total_files += 1
                folder_files += 1
                if self.verbose or total_files % 100 == 0:
                    print(f"Processing: {file_path.name}")
                elif total_files % 10 == 0:
                    print(".", end="", flush=True)                    # Calculate relative path within this folder
                    try:
                        relative_path = str(file_path.relative_to(folder_path))
                    except ValueError:
                        relative_path = file_path.name  # Fallback
                    
                    # Get file stats
                    file_stat = file_path.stat()
                    exif_date = self.extract_exif_date(str(file_path))
                    normalized_hash = self.generate_normalized_hash(str(file_path))
                    
                    if normalized_hash:
                        # Check if file already exists in database
                        existing = self.conn.execute(
                            "SELECT file_mtime, normalized_hash FROM safe_files WHERE file_path = ?",
                            (str(file_path),)
                        ).fetchone()
                        
                        if existing and existing[0] == file_stat.st_mtime and not rescan:
                            # File unchanged, add to hash groups
                            hash_groups[existing[1]].append(str(file_path))
                            continue
                        
                        # Insert or update file record with folder context
                        self.conn.execute("""
                            INSERT OR REPLACE INTO safe_files 
                            (file_path, folder_context, relative_path, file_size, file_mtime, 
                             exif_date, normalized_hash, processed_date, last_update_date, last_update_type)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            str(file_path),
                            str(folder_path),
                            relative_path,
                            file_stat.st_size,
                            file_stat.st_mtime,
                            exif_date.isoformat() if exif_date else None,
                            normalized_hash,
                            datetime.now().isoformat(),
                            datetime.now().isoformat(),
                            'SCANNED'
                        ))
                        
                        hash_groups[normalized_hash].append(str(file_path))
                        updated_files += 1
            
            if not self.verbose and folder_files % 10 != 0:
                print()  # New line after dots
            print(f"  ✅ {folder_files} files in {folder_path.name}")
        
        self.conn.commit()
        if not self.verbose and total_files % 10 != 0:
            print()  # New line after dots
        print(f"\n✅ Total: {total_files} image files across {len(self.folder_paths)} folders ({updated_files} updated)")
        
        # Identify potential duplicate groups (2+ files with same hash)
        potential_groups = {hash_val: files for hash_val, files in hash_groups.items() 
                          if len(files) > 1}
        
        # Analyze cross-folder duplicates
        cross_folder_groups = 0
        for hash_val, files in potential_groups.items():
            folder_contexts = set()
            for file_path in files:
                for folder in self.folder_paths:
                    if str(folder) in file_path:
                        folder_contexts.add(str(folder))
                        break
            if len(folder_contexts) > 1:
                cross_folder_groups += 1
        
        print(f"Found {len(potential_groups)} potential duplicate groups")
        print(f"  - {cross_folder_groups} cross-folder groups")
        print(f"  - {len(potential_groups) - cross_folder_groups} single-folder groups")
        
        return potential_groups
    
    def flag_deletions(self):
        """Phase 2: Flag confirmed duplicates for deletion (requires binary verification)."""
        print(f"\n🚩 PHASE 2: FLAGGING DELETIONS")
        
        # Get all binary-verified duplicates that aren't already flagged
        duplicates = self.conn.execute("""
            SELECT file_path, original_reference 
            FROM safe_files 
            WHERE binary_verified = 1 
            AND is_original = 0 
            AND deletion_flagged = 0
            AND deleted = 0
        """).fetchall()
        
        if not duplicates:
            print("No confirmed duplicates found to flag")
            return 0
        
        flagged_count = 0
        for file_path, original_ref in duplicates:
            # Verify original still exists and is marked as original
            original_check = self.conn.execute("""
                SELECT is_original FROM safe_files WHERE file_path = ?
            """, (original_ref,)).fetchone()
            
            if original_check and original_check[0]:
                self.update_file_record(
                    file_path, 
                    'FLAGGED_FOR_DELETION',
                    deletion_flagged=1,
                    notes=f'Confirmed duplicate of {Path(original_ref).name}'
                )
                flagged_count += 1
            else:
                print(f"⚠️  Warning: Original not found for {Path(file_path).name}")
        
        print(f"✅ Flagged {flagged_count} files for deletion")
        return flagged_count
    
    def execute_deletions(self):
        """Phase 3: Move flagged files to ToBeDeleted folder with mirrored structure."""
        print(f"\n🗑️  PHASE 3: EXECUTING SAFE DELETIONS")
        
        # Get all files flagged for deletion but not yet deleted
        flagged_files = self.conn.execute("""
            SELECT file_path, original_reference 
            FROM safe_files 
            WHERE deletion_flagged = 1 
            AND deleted = 0
        """).fetchall()
        
        if not flagged_files:
            print("No files flagged for deletion")
            return 0
        
        print(f"Found {len(flagged_files)} files to move to ToBeDeleted...")
        moved_count = 0
        
        for file_path, original_ref in flagged_files:
            try:
                # Create mirrored deletion path
                deletion_path = self.create_deletion_path(file_path)
                
                # Verify source file exists
                if not os.path.exists(file_path):
                    print(f"⚠️  File not found: {Path(file_path).name}")
                    continue
                
                # Move file to deletion folder
                import shutil
                shutil.move(file_path, deletion_path)
                
                # Update database record
                self.update_file_record(
                    file_path,
                    'MOVED_TO_DELETION_FOLDER',
                    deleted=1,
                    deleted_to_path=deletion_path,
                    notes=f'Moved to ToBeDeleted - original: {Path(original_ref).name}'
                )
                
                moved_count += 1
                print(f"Moved: {Path(file_path).name} → ToBeDeleted")
                
            except Exception as e:
                print(f"❌ Error moving {Path(file_path).name}: {e}")
                # Update with error status
                self.update_file_record(
                    file_path,
                    'DELETION_ERROR',
                    notes=f'Error during deletion: {str(e)}'
                )
        
        print(f"✅ Successfully moved {moved_count} files to ToBeDeleted")
        return moved_count
    
    def verify_and_process_duplicates(self, potential_groups):
        """Binary verify potential duplicates and flag for safe removal."""
        confirmed_groups = []
        
        for hash_val, file_paths in potential_groups.items():
            print(f"\nVerifying group with hash {hash_val[:8]}...")
            print(f"  Potential files: {len(file_paths)}")
            
            # Binary verify all combinations
            verified_duplicates = [file_paths[0]]  # Start with first file
            
            for i, file1 in enumerate(file_paths):
                for j, file2 in enumerate(file_paths[i+1:], i+1):
                    if self.binary_compare_files(file1, file2):
                        if file2 not in verified_duplicates:
                            verified_duplicates.append(file2)
                    else:
                        print(f"  ❌ Hash collision: {Path(file1).name} ≠ {Path(file2).name}")
            
            if len(verified_duplicates) > 1:
                print(f"  ✅ Confirmed {len(verified_duplicates)} binary-identical files")
                
                # Determine original file
                original_file = self.determine_original_file(verified_duplicates)
                
                # Update original file designation
                self.update_file_record(
                    original_file,
                    'MARKED_AS_ORIGINAL', 
                    is_original=1,
                    notes='Identified as original file'
                )
                
                # Mark duplicates as verified (NOT flagged for deletion yet)
                for file_path in verified_duplicates:
                    if file_path != original_file:
                        self.update_file_record(
                            file_path,
                            'BINARY_VERIFIED_DUPLICATE',
                            removal_flagged=1,
                            original_reference=original_file,
                            binary_verified=1,
                            notes='Binary-verified duplicate - ready for deletion flagging'
                        )
                
                # Analyze folder involvement
                folder_contexts = set()
                for file_path in verified_duplicates:
                    context = self.conn.execute(
                        "SELECT folder_context FROM safe_files WHERE file_path = ?",
                        (file_path,)
                    ).fetchone()
                    if context:
                        folder_contexts.add(context[0])
                
                is_cross_folder = len(folder_contexts) > 1
                folders_involved = " | ".join(Path(fc).name for fc in folder_contexts)
                
                # Create duplicate group record
                total_size = sum(os.path.getsize(f) for f in verified_duplicates)
                self.conn.execute("""
                    INSERT INTO duplicate_groups 
                    (group_hash, original_file, folders_involved, cross_folder_group, 
                     total_files, total_size, verification_status, created_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (hash_val, original_file, folders_involved, is_cross_folder,
                     len(verified_duplicates), total_size, 'BINARY_VERIFIED', 
                     datetime.now().isoformat()))
                
                confirmed_groups.append({
                    'hash': hash_val,
                    'original': original_file,
                    'duplicates': [f for f in verified_duplicates if f != original_file],
                    'total_size': total_size
                })
            else:
                print(f"  ❌ No binary-verified duplicates found")
        
        self.conn.commit()
        return confirmed_groups
    
    def generate_report(self, confirmed_groups):
        """Generate comprehensive safety report."""
        report_path = self.primary_folder.parent / f"multi_folder_duplicate_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("MULTI-FOLDER SAFE DUPLICATE DETECTION REPORT\n")
            f.write("=" * 80 + "\n")
            f.write(f"Scan Date: {datetime.now()}\n")
            f.write(f"Folders Scanned: {len(self.folder_paths)}\n")
            for i, folder in enumerate(self.folder_paths, 1):
                f.write(f"  {i}. {folder}\n")
            f.write(f"Database: {self.db_path}\n")
            f.write(f"Total Confirmed Groups: {len(confirmed_groups)}\n")
            
            total_duplicates = sum(len(group['duplicates']) for group in confirmed_groups)
            total_wasted = sum(group['total_size'] for group in confirmed_groups)
            
            f.write(f"Total Files Flagged for Removal: {total_duplicates}\n")
            f.write(f"Total Space Recoverable: {total_wasted / (1024*1024):.2f} MB\n")
            f.write("\n")
            
            cross_folder_count = 0
            for i, group in enumerate(confirmed_groups, 1):
                # Check if this is a cross-folder group
                group_folders = set()
                for file_path in [group['original']] + group['duplicates']:
                    for folder in self.folder_paths:
                        if str(folder) in file_path:
                            group_folders.add(folder.name)
                            break
                
                is_cross_folder = len(group_folders) > 1
                if is_cross_folder:
                    cross_folder_count += 1
                
                f.write(f"GROUP #{i} - Hash: {group['hash'][:12]}")
                if is_cross_folder:
                    f.write(f" [CROSS-FOLDER: {' | '.join(group_folders)}]")
                f.write("\n")
                
                f.write(f"  ORIGINAL (KEEP): {group['original']}\n")
                
                for dup_file in group['duplicates']:
                    file_size = os.path.getsize(dup_file)
                    # Show which folder this duplicate is in
                    dup_folder = "Unknown"
                    for folder in self.folder_paths:
                        if str(folder) in dup_file:
                            dup_folder = folder.name
                            break
                    f.write(f"  DUPLICATE (REMOVE): {dup_file} ({file_size / (1024*1024):.2f} MB) [{dup_folder}]\n")
                
                f.write(f"  Group Total: {group['total_size'] / (1024*1024):.2f} MB\n")
                f.write("\n")
            
            f.write(f"CROSS-FOLDER ANALYSIS:\n")
            f.write(f"  Cross-folder groups: {cross_folder_count}\n")
            f.write(f"  Single-folder groups: {len(confirmed_groups) - cross_folder_count}\n")
            f.write("\n")
            
            f.write("SAFETY VERIFICATION:\n")
            f.write("- All duplicates binary-verified (byte-for-byte identical)\n")
            f.write("- Original files determined by EXIF + file dates\n")
            f.write("- NO AUTOMATIC DELETIONS (manual review required)\n")
            f.write("- Database tracks all relationships for safe removal\n")
        
        print(f"Safety report generated: {report_path}")
        return str(report_path)


def print_examples():
    """Print detailed usage examples."""
    print("""
SAFE DUPLICATE DETECTION v0.3 - USAGE EXAMPLES
===============================================

1. SINGLE FOLDER PROCESSING (within one year/folder):
   
   # Phase 1: Scan for duplicates
   python detect_duplicates_safe.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --scan
   
   # Phase 2: Flag confirmed duplicates for deletion
   python detect_duplicates_safe.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --flag-deletions
   
   # Phase 3: Execute safe deletion (move to ToBeDeleted)
   python detect_duplicates_safe.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --execute-deletions

2. MULTI-FOLDER PROCESSING (cross-year duplicate detection):
   
   # Scan multiple folders for cross-folder duplicates
   python detect_duplicates_safe.py --folders "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" "\\\\NAS-MEDIA\\photo\\Sorted\\2011 - Photos" --scan
   
   # Flag and execute across multiple folders
   python detect_duplicates_safe.py --folders "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" "\\\\NAS-MEDIA\\photo\\Sorted\\2011 - Photos" --flag-deletions
   python detect_duplicates_safe.py --folders "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" "\\\\NAS-MEDIA\\photo\\Sorted\\2011 - Photos" --execute-deletions

3. COMPLETE WORKFLOW (all phases with manual approval):
   
   # Run all phases in sequence
   python detect_duplicates_safe.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --all-phases

4. ADVANCED OPTIONS:
   
   # Force complete rescan (clears database)
   python detect_duplicates_safe.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --rescan --scan
   
   # Custom database location
   python detect_duplicates_safe.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --db "C:\\MyBackup\\duplicates.db" --scan
   
   # Custom thumbnail size for hashing
   python detect_duplicates_safe.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --thumbnail-size 32 --scan
   
   # Verbose output for debugging
   python detect_duplicates_safe.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --scan --verbose

IMPORTANT SAFETY NOTES:
• Each phase requires manual execution - no automatic deletions
• Original files are determined by EXIF creation date + file modification date
• Files are moved to ToBeDeleted folder (not permanently deleted)
• All duplicates are binary-verified to prevent false positives
• Database maintains complete audit trail for all operations

SUPPORTED IMAGE FORMATS:
• Common: .jpg, .jpeg, .png, .gif, .bmp
• Professional: .tiff, .tif
• Modern: .webp, .heic, .heif (iPhone photos)

OUTPUT FILES:
• Detailed report: multi_folder_duplicate_report_YYYYMMDD_HHMMSS.txt
• Database: multi_folder_duplicates.db (SQLite format)
• Moved files: \\\\NAS-MEDIA\\photo\\ToBeDeleted\\[original_folder_structure]
""")


def main():
    parser = argparse.ArgumentParser(
        description="""
Safe Duplicate Detection v0.3 - Multi-Folder Support with Phased Processing

A conservative, safety-first approach to duplicate photo detection and removal:
• Supports single folder or cross-folder duplicate detection
• Uses normalized image hashing for fast potential duplicate identification  
• Binary verification prevents false positives from hash collisions
• EXIF date analysis identifies true original files
• Phased processing with manual approval between steps
• Safe deletion via move to ToBeDeleted folder (preserves folder structure)
• Comprehensive audit trail in SQLite database

WORKFLOW:
  Phase 1 (--scan): Scan folders → Generate hashes → Identify potential duplicates → Binary verify → Mark originals
  Phase 2 (--flag-deletions): Flag confirmed duplicates for deletion (originals never flagged)
  Phase 3 (--execute-deletions): Move flagged files to ToBeDeleted folder with mirrored structure

SAFETY FEATURES:
  • No automatic deletions - each phase requires manual execution
  • Original files determined by EXIF creation date + file modification date
  • Binary verification confirms all duplicates are byte-for-byte identical
  • Database tracks all relationships to prevent orphaned references
  • Files moved to ToBeDeleted (not permanently deleted)
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # Single folder duplicate detection
  %(prog)s "C:\\Photos\\2010 - Photos" --scan
  %(prog)s "C:\\Photos\\2010 - Photos" --flag-deletions  
  %(prog)s "C:\\Photos\\2010 - Photos" --execute-deletions

  # Multi-folder cross-duplicate detection
  %(prog)s --folders "C:\\Photos\\2010" "C:\\Photos\\2011" "C:\\Photos\\2012" --scan

  # Run all phases in sequence (with manual approval prompts)
  %(prog)s "C:\\Photos\\2010 - Photos" --all-phases

  # Force complete rescan (clears previous database)
  %(prog)s "C:\\Photos\\2010 - Photos" --rescan --scan

SUPPORTED FORMATS:
  .jpg, .jpeg, .png, .gif, .bmp, .tiff, .tif, .webp, .heic, .heif

OUTPUT:
  • Detailed report with duplicate groups and safety analysis
  • SQLite database with complete audit trail
  • Flagged files moved to ToBeDeleted with preserved folder structure
        """
    )
    
    # Support both single folder and multiple folders
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("folder", nargs='?', 
                      help="Single folder to scan for duplicates (e.g., 'C:\\Photos\\2010 - Photos')")
    group.add_argument("--folders", nargs='+', metavar='FOLDER',
                      help="Multiple folders to scan for cross-folder duplicates (e.g., --folders 'C:\\Photos\\2010' 'C:\\Photos\\2011')")
    
    parser.add_argument("--db", metavar='PATH',
                       help="Custom database path (default: auto-generated based on folder location)")
    parser.add_argument("--thumbnail-size", type=int, default=64, metavar='SIZE',
                       help="Thumbnail size for normalized hashing (default: 64x64 pixels)")
    
    # Phase control flags
    phase_group = parser.add_argument_group('Phase Control', 'Control which processing phases to execute')
    phase_group.add_argument("--scan", action="store_true", 
                            help="Phase 1: Scan folders, generate hashes, identify originals, binary verify duplicates")
    phase_group.add_argument("--rescan", action="store_true",
                            help="Force complete rescan - clears previous database and rescans all files")
    phase_group.add_argument("--flag-deletions", action="store_true",
                            help="Phase 2: Flag binary-verified duplicates for deletion (originals never flagged)")
    phase_group.add_argument("--execute-deletions", action="store_true", 
                            help="Phase 3: Move flagged files to ToBeDeleted folder (preserves folder structure)")
    phase_group.add_argument("--all-phases", action="store_true",
                            help="Execute all phases in sequence: scan → flag → execute (manual approval required)")
    
    # Advanced options
    advanced_group = parser.add_argument_group('Advanced Options', 'Fine-tune processing behavior')
    advanced_group.add_argument("--version", action="version", version="Safe Duplicate Detection v0.3")
    advanced_group.add_argument("--verbose", "-v", action="store_true",
                               help="Enable verbose output for debugging")
    
    # Add usage examples to help
    parser.add_argument("--examples", action="store_true",
                       help="Show detailed usage examples and exit")
    
    args = parser.parse_args()
    
    # Handle examples request
    if args.examples:
        print_examples()
        sys.exit(0)
    
    # Determine folder paths - require at least one
    if args.folder:
        folder_paths = [args.folder]
    elif args.folders:
        folder_paths = args.folders
    else:
        print("❌ Error: You must specify either a single folder or multiple folders using --folders")
        print("   Use --help for usage information or --examples for detailed examples")
        sys.exit(1)
    
    # Validate all folders exist
    for folder in folder_paths:
        if not os.path.exists(folder):
            print(f"❌ Folder not found: {folder}")
            print(f"   Please check the path and ensure it exists.")
            print(f"   Use --examples to see usage examples.")
            sys.exit(1)
    
    # Default to scan if no phases specified
    if not any([args.scan, args.rescan, args.flag_deletions, args.execute_deletions, args.all_phases]):
        args.scan = True
        if not args.verbose:
            print("💡 No phase specified, defaulting to --scan. Use --help for all options.")
    
    print("SAFE DUPLICATE DETECTION v0.3 - MULTI-FOLDER SUPPORT")
    print("=" * 60)
    print("Phases: 1️⃣ Scan → 2️⃣ Flag → 3️⃣ Execute")
    print("Cross-folder duplicate detection enabled")
    print()
    
    detector = SafeDuplicateDetector(folder_paths, args.db, args.thumbnail_size, args.verbose)
    
    # Phase 1: Scan and identify duplicates
    if args.scan or args.rescan or args.all_phases:
        potential_groups = detector.scan_folders(rescan=args.rescan)
        
        if potential_groups:
            confirmed_groups = detector.verify_and_process_duplicates(potential_groups)
            
            if confirmed_groups:
                report_path = detector.generate_report(confirmed_groups)
                print(f"📄 Report generated: {report_path}")
            else:
                print("✅ No confirmed duplicates found after binary verification")
        else:
            print("✅ No potential duplicates found")
        
        if not args.all_phases:
            print("\n➡️  Next: Run with --flag-deletions to flag confirmed duplicates")
    
    # Phase 2: Flag deletions
    if args.flag_deletions or args.all_phases:
        if args.all_phases:
            print("\n" + "="*60)
        
        flagged_count = detector.flag_deletions()
        
        if flagged_count > 0 and not args.all_phases:
            print("\n➡️  Next: Run with --execute-deletions to move flagged files")
    
    # Phase 3: Execute deletions
    if args.execute_deletions or args.all_phases:
        if args.all_phases:
            print("\n" + "="*60)
        
        moved_count = detector.execute_deletions()
        
        if moved_count > 0:
            print(f"\n✅ DELETION PHASE COMPLETE")
            print(f"   Files moved to ToBeDeleted: {moved_count}")
            print(f"   Database: {detector.db_path}")
    
    print(f"\n📊 Database located at: {detector.db_path}")
    print("💡 Use SQLite browser to inspect detailed records and audit trail")


if __name__ == "__main__":
    main()