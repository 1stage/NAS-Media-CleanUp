#!/usr/bin/env python3
"""
detect_duplicates.py (Unified Version)
Version 1.0 — Created by Sean P. Harrington with assistance from Microsoft Copilot
Date: Friday, 27 September 2025

Unified Duplicate Detection - High Performance & Safety Combined

OVERVIEW:
A comprehensive duplicate detection tool that combines high-performance batch processing
with safety-first phased operations. Choose your approach based on your needs:
• Performance Mode: Fast batch processing for analysis and reporting
• Safety Mode: Conservative phased processing for actual duplicate removal

KEY FEATURES:
• Single unified database (no more split schemas)
• Normalized image hashing for fast initial duplicate identification
• Binary verification to prevent false positives from hash collisions
• EXIF date analysis to identify true original files
• Multi-folder support for cross-year duplicate detection
• Flexible processing modes: batch analysis OR phased safety workflow
• Comprehensive audit trail in safety mode
• Safe deletion via move to ToBeDeleted folder (preserves structure)

PROCESSING MODES:

Performance Mode (--performance-mode):
  • High-speed batch processing for analysis
  • Builds comprehensive hash database quickly
  • Generates detailed reports for review
  • No automatic deletions - analysis only
  • Perfect for initial assessment of large collections

Safety Mode (--safety-mode or default):
  • Conservative phased approach with manual approval
  • Phase 1 (--scan): Scan → Hash → Binary verify → Mark originals
  • Phase 2 (--flag-deletions): Flag confirmed duplicates for removal
  • Phase 3 (--execute-deletions): Move flagged files to ToBeDeleted
  • Complete audit trail with reversible operations

WORKFLOW OPTIONS:

Performance Workflow:
  python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted" --performance-mode --build-database
  python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted" --performance-mode --find-duplicates

Safety Workflow:
  python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --scan
  python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --flag-deletions
  python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --execute-deletions

SUPPORTED FORMATS:
.jpg, .jpeg, .png, .gif, .bmp, .tiff, .tif, .webp, .heic, .heif

DATABASE:
Single unified SQLite database (photo_duplicates.db) with comprehensive schema
supporting both performance and safety operations.
"""

import os
import sys
import argparse
import hashlib
import json
import time
import sqlite3
import re
import shutil
from datetime import datetime
from collections import defaultdict, namedtuple
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

# Optional dependencies
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

# Data structures
FileRecord = namedtuple('FileRecord', ['path', 'size', 'mtime', 'hash_md5', 'normalized_hash'])
DuplicateGroup = namedtuple('DuplicateGroup', ['method', 'similarity', 'files', 'recommended_action'])


class UnifiedDuplicateDetector:
    """Unified duplicate detection supporting both performance and safety modes."""
    
    def __init__(self, folder_paths, db_path=None, thumbnail_size=64, 
                 performance_mode=False, verbose=False):
        # Support both single folder and multiple folders
        if isinstance(folder_paths, (str, Path)):
            self.folder_paths = [Path(folder_paths).resolve()]
        else:
            self.folder_paths = [Path(fp).resolve() for fp in folder_paths]
        
        self.primary_folder = self.folder_paths[0]
        # Default database to local project directory instead of NAS server
        self.db_path = db_path or str(Path.cwd() / "photo_duplicates.db")
        self.thumbnail_size = thumbnail_size
        self.performance_mode = performance_mode
        self.verbose = verbose
        
        # Supported image formats
        self.image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', 
                               '.tiff', '.tif', '.webp', '.heic', '.heif'}
        
        # Statistics
        self.stats = {
            'files_scanned': 0,
            'files_processed': 0,
            'files_skipped': 0,
            'exact_duplicates': 0,
            'near_duplicates': 0,
            'processing_time': 0,
            'database_size': 0
        }
        
        # Initialize unified database
        self._init_unified_database()
    
    def _init_unified_database(self):
        """Initialize unified SQLite database supporting both modes."""
        self.conn = sqlite3.connect(self.db_path)
        
        # Main files table - supports both performance and safety operations
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS photo_files (
                id INTEGER PRIMARY KEY,
                file_path TEXT UNIQUE,
                folder_context TEXT,
                relative_path TEXT,
                file_size INTEGER,
                file_mtime REAL,
                exif_date TEXT,
                md5_hash TEXT,
                normalized_hash TEXT,
                thumbnail_size INTEGER,
                processing_date TEXT,
                folder_year INTEGER,
                
                -- Safety mode fields
                is_original BOOLEAN DEFAULT 0,
                removal_flagged BOOLEAN DEFAULT 0,
                deletion_flagged BOOLEAN DEFAULT 0,
                deleted BOOLEAN DEFAULT 0,
                original_reference TEXT,
                binary_verified BOOLEAN DEFAULT 0,
                deleted_to_path TEXT,
                last_update_date TEXT,
                last_update_type TEXT,
                notes TEXT,
                
                -- Performance mode fields  
                status TEXT DEFAULT 'processed'
            )
        """)
        
        # Duplicate groups table - for both modes
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_groups (
                id INTEGER PRIMARY KEY,
                group_hash TEXT,
                group_type TEXT,  -- 'exact', 'near', 'normalized'
                similarity REAL,
                original_file TEXT,
                recommended_action TEXT,
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
        
        # Create indexes for performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_md5_hash ON photo_files(md5_hash)",
            "CREATE INDEX IF NOT EXISTS idx_normalized_hash ON photo_files(normalized_hash)",
            "CREATE INDEX IF NOT EXISTS idx_file_path ON photo_files(file_path)",
            "CREATE INDEX IF NOT EXISTS idx_folder_year ON photo_files(folder_year)",
            "CREATE INDEX IF NOT EXISTS idx_folder_context ON photo_files(folder_context)",
            "CREATE INDEX IF NOT EXISTS idx_group_hash ON duplicate_groups(group_hash)"
        ]
        
        for index_sql in indexes:
            self.conn.execute(index_sql)
        
        self.conn.commit()
        
        # Get database statistics
        cursor = self.conn.execute("SELECT COUNT(*) FROM photo_files")
        self.stats['database_size'] = cursor.fetchone()[0]
        
        print(f"📊 Unified database: {self.stats['database_size']} files tracked")
    
    def log(self, message, level="INFO"):
        """Enhanced logging with timestamps and emojis."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        if level == "ERROR":
            print(f"🚨 [{timestamp}] {message}")
        elif level == "WARNING":
            print(f"⚠️  [{timestamp}] {message}")
        elif level == "SUCCESS":
            print(f"✅ [{timestamp}] {message}")
        elif level == "INFO":
            print(f"ℹ️  [{timestamp}] {message}")
        else:
            print(f"[{timestamp}] [{level}] {message}")
    
    def is_image_file(self, file_path):
        """Check if file is a supported image format."""
        return Path(file_path).suffix.lower() in self.image_extensions
    
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
        except Exception:
            pass
        
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
                if self.performance_mode:
                    # Performance mode: use SHA256 for better distribution
                    img_bytes = letterbox.tobytes()
                    return hashlib.sha256(img_bytes).hexdigest()
                else:
                    # Safety mode: use MD5 for compatibility
                    img_bytes = letterbox.tobytes()
                    return hashlib.md5(img_bytes).hexdigest()
                
        except Exception as e:
            if self.verbose:
                print(f"Hash generation failed for {file_path}: {e}")
            return None
    
    def get_file_md5(self, file_path):
        """Calculate MD5 hash of file for exact duplicate detection."""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            if self.verbose:
                self.log(f"Error calculating MD5 for {file_path}: {e}", "ERROR")
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
            if self.verbose:
                print(f"Binary comparison failed: {e}")
            return False
    
    def extract_year_from_path(self, file_path):
        """Extract year from folder path like '2024 - Photos'."""
        match = re.search(r'(\d{4})\s*-\s*Photos', file_path, re.IGNORECASE)
        return int(match.group(1)) if match else None
    
    def discover_photo_folders(self, base_path):
        """Automatically discover year-based photo folders."""
        folders = []
        try:
            for item in os.listdir(base_path):
                item_path = os.path.join(base_path, item)
                if os.path.isdir(item_path):
                    # Match patterns like "2024 - Photos", "2010 - Photos", etc.
                    if re.match(r'^\d{4}\s*-\s*Photos$', item, re.IGNORECASE):
                        folders.append(item_path)
            return sorted(folders)
        except Exception as e:
            self.log(f"Error discovering folders in {base_path}: {e}", "ERROR")
            return []
    
    def scan_folder_recursive(self, folder_path):
        """Recursively find all image files in folder and subfolders."""
        image_files = []
        try:
            for root, dirs, files in os.walk(folder_path):
                # Skip system directories
                dirs[:] = [d for d in dirs if not d.startswith('@')]
                
                for file in files:
                    if self.is_image_file(file):
                        full_path = os.path.join(root, file)
                        image_files.append(full_path)
                        
        except Exception as e:
            self.log(f"Error scanning {folder_path}: {e}", "ERROR")
            
        return image_files
    
    # PERFORMANCE MODE METHODS
    def build_database_performance(self, base_path, force_reprocess=False, years=None, incremental=False):
        """Performance mode: Build comprehensive database quickly."""
        print(f"🚀 PERFORMANCE MODE: Building database")
        print(f"📁 Scanning: {base_path}")
        
        folders = self.discover_photo_folders(base_path)
        
        if years:
            # Filter by year range
            year_range = years.split('-')
            if len(year_range) == 2:
                start_year = int(year_range[0])
                end_year = int(year_range[1])
                folders = [f for f in folders if start_year <= self.extract_year_from_path(f) <= end_year]
        
        print(f"📂 Processing {len(folders)} photo folders")
        
        for folder in folders:
            folder_name = os.path.basename(folder)
            print(f"\n📁 Processing {folder_name}...")
            
            files = self.scan_folder_recursive(folder)
            if files:
                self._process_files_performance(files, force_reprocess, incremental)
        
        self.log("✅ Performance database build complete!", "SUCCESS")
    
    def _process_files_performance(self, file_paths, force_reprocess=False, incremental=False):
        """Process files in performance mode - fast batch processing."""
        processed = 0
        skipped = 0
        
        # Progress bar for large batches
        if HAS_TQDM and len(file_paths) > 20:
            file_iter = tqdm(file_paths, desc="Processing images", unit="files")
        else:
            file_iter = file_paths
            
        for file_path in file_iter:
            try:
                # Get file stats
                stat = os.stat(file_path)
                file_size = stat.st_size
                file_mtime = stat.st_mtime
                folder_year = self.extract_year_from_path(file_path)
                folder_context = str(Path(file_path).parent)
                
                # Calculate relative path
                try:
                    relative_path = str(Path(file_path).relative_to(Path(folder_context).parent))
                except ValueError:
                    relative_path = Path(file_path).name
                
                # Check if already processed (unless force reprocess)
                if not force_reprocess:
                    cursor = self.conn.execute(
                        "SELECT file_mtime FROM photo_files WHERE file_path = ?",
                        (file_path,)
                    )
                    row = cursor.fetchone()
                    if row and row[0] >= file_mtime:
                        skipped += 1
                        continue
                
                # Calculate hashes
                md5_hash = self.get_file_md5(file_path)
                normalized_hash = self.generate_normalized_hash(file_path)
                exif_date = self.extract_exif_date(file_path)
                
                if md5_hash and normalized_hash:
                    # Store in unified database
                    self.conn.execute('''
                        INSERT OR REPLACE INTO photo_files 
                        (file_path, folder_context, relative_path, file_size, file_mtime, 
                         exif_date, md5_hash, normalized_hash, thumbnail_size, processing_date, 
                         folder_year, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (file_path, folder_context, relative_path, file_size, file_mtime,
                          exif_date.isoformat() if exif_date else None,
                          md5_hash, normalized_hash, self.thumbnail_size, 
                          datetime.now().isoformat(), folder_year, 'processed'))
                    
                    processed += 1
                
            except Exception as e:
                if self.verbose:
                    self.log(f"Error processing {file_path}: {e}", "WARNING")
                continue
        
        self.conn.commit()
        self.stats['files_processed'] = processed
        self.stats['files_skipped'] = skipped
        
        print(f"✅ Processed {processed} files, skipped {skipped} already processed")
    
    def find_duplicates_performance(self, similarity_threshold=0.95, include_near_duplicates=True):
        """Performance mode: Find duplicates using existing database."""
        print(f"🔍 PERFORMANCE MODE: Finding duplicates")
        
        all_duplicates = []
        
        # Find exact duplicates
        exact_dupes = self._find_exact_duplicates_performance()
        all_duplicates.extend(exact_dupes)
        
        # Find near duplicates if requested
        if include_near_duplicates:
            near_dupes = self._find_near_duplicates_performance(similarity_threshold)
            all_duplicates.extend(near_dupes)
        
        return all_duplicates
    
    def _find_exact_duplicates_performance(self):
        """Find exact duplicates based on MD5 hash."""
        self.log("🔍 Finding exact duplicates...")
        
        duplicates = []
        
        # Find MD5 hashes that appear more than once
        cursor = self.conn.execute('''
            SELECT md5_hash, COUNT(*) as count 
            FROM photo_files 
            GROUP BY md5_hash 
            HAVING count > 1
            ORDER BY count DESC
        ''')
        
        for md5_hash, count in cursor:
            # Get all files with this hash
            files_cursor = self.conn.execute('''
                SELECT file_path, file_size FROM photo_files 
                WHERE md5_hash = ?
                ORDER BY file_path
            ''', (md5_hash,))
            
            files = files_cursor.fetchall()
            if len(files) > 1:
                group = DuplicateGroup(
                    method='exact',
                    similarity=1.0,
                    files=[f[0] for f in files],
                    recommended_action=self._recommend_action([f[0] for f in files])
                )
                duplicates.append(group)
                self.stats['exact_duplicates'] += len(files) - 1
        
        self.log(f"Found {len(duplicates)} exact duplicate groups")
        return duplicates
    
    def _find_near_duplicates_performance(self, similarity_threshold=0.95):
        """Find near-duplicates using normalized hash comparison."""
        self.log(f"🖼️  Finding near-duplicates (similarity >= {similarity_threshold})...")
        
        near_duplicates = []
        
        # Get all normalized hashes
        cursor = self.conn.execute('SELECT file_path, normalized_hash FROM photo_files')
        photos = cursor.fetchall()
        
        self.log(f"Comparing {len(photos)} photos for near-duplicates...")
        
        # For performance mode, use simpler comparison
        processed_pairs = set()
        
        if HAS_TQDM and len(photos) > 100:
            photo_iter = tqdm(photos, desc="Comparing images", unit="images")
        else:
            photo_iter = photos
        
        for i, (path1, hash1) in enumerate(photo_iter):
            for j, (path2, hash2) in enumerate(photos[i+1:], i+1):
                # Skip if already processed this pair
                pair = tuple(sorted([path1, path2]))
                if pair in processed_pairs:
                    continue
                processed_pairs.add(pair)
                
                # Simple string comparison for performance mode
                if hash1 == hash2 and hash1 is not None:
                    similarity = 1.0
                elif hash1 and hash2:
                    # Calculate Hamming distance approximation
                    similarity = self._calculate_hash_similarity(hash1, hash2)
                else:
                    continue
                
                if similarity >= similarity_threshold and similarity < 1.0:
                    group = DuplicateGroup(
                        method='normalized_hash',
                        similarity=similarity,
                        files=[path1, path2],
                        recommended_action=self._recommend_action([path1, path2])
                    )
                    near_duplicates.append(group)
                    self.stats['near_duplicates'] += 1
        
        self.log(f"Found {len(near_duplicates)} near-duplicate groups")
        return near_duplicates
    
    def _calculate_hash_similarity(self, hash1, hash2):
        """Calculate similarity between two hash strings."""
        if len(hash1) != len(hash2):
            return 0.0
        
        # Simple character-level comparison for performance
        matches = sum(c1 == c2 for c1, c2 in zip(hash1, hash2))
        return matches / len(hash1)
    
    def _recommend_action(self, files):
        """Recommend which file to keep based on various criteria."""
        if len(files) < 2:
            return "keep_all"
        
        # Score files based on multiple criteria
        scored_files = []
        
        for file_path in files:
            try:
                stat = os.stat(file_path)
                score = 0
                
                # Prefer newer files (higher mtime)
                score += stat.st_mtime / 1000000  # Normalize
                
                # Prefer larger files (likely higher quality)
                score += stat.st_size / (1024 * 1024)  # MB
                
                # Prefer files in organized structure (shorter relative paths)
                path_depth = len(Path(file_path).parts)
                score -= path_depth  # Fewer directories = higher score
                
                # Prefer certain file types
                ext = Path(file_path).suffix.lower()
                if ext in ['.png', '.tiff']:  # Lossless formats
                    score += 10
                elif ext in ['.jpg', '.jpeg']:  # Common format
                    score += 5
                
                scored_files.append((score, file_path))
                
            except OSError:
                scored_files.append((0, file_path))
        
        # Sort by score (highest first)
        scored_files.sort(reverse=True)
        recommended = scored_files[0][1]
        
        return f"keep:{os.path.basename(recommended)}"
    
    # SAFETY MODE METHODS
    def scan_folders_safety(self, rescan=False):
        """Safety mode: Scan folders with full binary verification."""
        print(f"🔍 SAFETY MODE: Multi-folder scanning with binary verification")
        print(f"Folders to scan: {len(self.folder_paths)}")
        for folder in self.folder_paths:
            print(f"  - {folder}")
        
        if rescan:
            print("Clearing previous scan data for fresh analysis...")
            self.conn.execute("DELETE FROM photo_files WHERE last_update_type = 'SCANNED'")
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
                        print(".", end="", flush=True)
                    
                    # Calculate relative path within this folder
                    try:
                        relative_path = str(file_path.relative_to(folder_path))
                    except ValueError:
                        relative_path = file_path.name  # Fallback
                    
                    # Get file stats
                    file_stat = file_path.stat()
                    exif_date = self.extract_exif_date(str(file_path))
                    normalized_hash = self.generate_normalized_hash(str(file_path))
                    md5_hash = self.get_file_md5(str(file_path))
                    
                    if normalized_hash and md5_hash:
                        # Check if file already exists in database
                        existing = self.conn.execute(
                            "SELECT file_mtime, normalized_hash FROM photo_files WHERE file_path = ?",
                            (str(file_path),)
                        ).fetchone()
                        
                        if existing and existing[0] == file_stat.st_mtime and not rescan:
                            # File unchanged, add to hash groups
                            hash_groups[existing[1]].append(str(file_path))
                            continue
                        
                        # Insert or update file record with folder context
                        self.conn.execute("""
                            INSERT OR REPLACE INTO photo_files 
                            (file_path, folder_context, relative_path, file_size, file_mtime, 
                             exif_date, md5_hash, normalized_hash, thumbnail_size, processing_date, 
                             folder_year, last_update_date, last_update_type)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            str(file_path),
                            str(folder_path),
                            relative_path,
                            file_stat.st_size,
                            file_stat.st_mtime,
                            exif_date.isoformat() if exif_date else None,
                            md5_hash,
                            normalized_hash,
                            self.thumbnail_size,
                            datetime.now().isoformat(),
                            self.extract_year_from_path(str(file_path)),
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
        
        print(f"Found {len(potential_groups)} potential duplicate groups")
        
        return potential_groups
    
    def verify_and_process_duplicates_safety(self, potential_groups):
        """Safety mode: Binary verify potential duplicates and flag for safe removal."""
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
                original_file = self._determine_original_file_safety(verified_duplicates)
                
                # Update original file designation
                self._update_file_record_safety(
                    original_file,
                    'MARKED_AS_ORIGINAL', 
                    is_original=1,
                    notes='Identified as original file'
                )
                
                # Mark duplicates as verified
                for file_path in verified_duplicates:
                    if file_path != original_file:
                        self._update_file_record_safety(
                            file_path,
                            'BINARY_VERIFIED_DUPLICATE',
                            removal_flagged=1,
                            original_reference=original_file,
                            binary_verified=1,
                            notes='Binary-verified duplicate - ready for deletion flagging'
                        )
                
                # Create duplicate group record
                total_size = sum(os.path.getsize(f) for f in verified_duplicates)
                self.conn.execute("""
                    INSERT INTO duplicate_groups 
                    (group_hash, group_type, similarity, original_file, total_files, total_size, 
                     verification_status, created_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (hash_val, 'exact', 1.0, original_file, len(verified_duplicates), 
                     total_size, 'BINARY_VERIFIED', datetime.now().isoformat()))
                
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
    
    def _determine_original_file_safety(self, file_paths):
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
    
    def _update_file_record_safety(self, file_path, update_type, **kwargs):
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
            UPDATE photo_files 
            SET {', '.join(update_fields)}
            WHERE file_path = ?
        """
        
        self.conn.execute(query, update_values)
        self.conn.commit()
        
        if self.verbose:
            print(f"Updated {Path(file_path).name}: {update_type}")
    
    def flag_deletions_safety(self):
        """Safety mode: Flag confirmed duplicates for deletion."""
        print(f"\n🚩 SAFETY MODE: Flagging deletions")
        
        # Get all binary-verified duplicates that aren't already flagged
        duplicates = self.conn.execute("""
            SELECT file_path, original_reference 
            FROM photo_files 
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
                SELECT is_original FROM photo_files WHERE file_path = ?
            """, (original_ref,)).fetchone()
            
            if original_check and original_check[0]:
                self._update_file_record_safety(
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
    
    def execute_deletions_safety(self):
        """Safety mode: Move flagged files to ToBeDeleted folder."""
        print(f"\n🗑️  SAFETY MODE: Executing safe deletions")
        
        # Get all files flagged for deletion but not yet deleted
        flagged_files = self.conn.execute("""
            SELECT file_path, original_reference 
            FROM photo_files 
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
                deletion_path = self._create_deletion_path(file_path)
                
                # Verify source file exists
                if not os.path.exists(file_path):
                    print(f"⚠️  File not found: {Path(file_path).name}")
                    continue
                
                # Move file to deletion folder
                shutil.move(file_path, deletion_path)
                
                # Update database record
                self._update_file_record_safety(
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
                self._update_file_record_safety(
                    file_path,
                    'DELETION_ERROR',
                    notes=f'Error during deletion: {str(e)}'
                )
        
        print(f"✅ Successfully moved {moved_count} files to ToBeDeleted")
        return moved_count
    
    def _create_deletion_path(self, original_file_path):
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
    
    # REPORTING METHODS
    def generate_report_performance(self, duplicate_groups, output_file=None):
        """Generate performance mode report."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Calculate statistics
        total_duplicates = sum(len(group.files) - 1 for group in duplicate_groups)
        total_wasted_space = 0
        
        for group in duplicate_groups:
            if group.method == 'exact':
                sizes = []
                for file_path in group.files:
                    try:
                        sizes.append(os.path.getsize(file_path))
                    except OSError:
                        pass
                if sizes:
                    total_wasted_space += sum(sizes) - max(sizes)
        
        report_lines = [
            "="*80,
            f"UNIFIED DUPLICATE DETECTION REPORT (Performance Mode) - {timestamp}",
            "="*80,
            f"Database: {self.db_path}",
            f"Thumbnail size: {self.thumbnail_size}x{self.thumbnail_size}",
            f"Photos in database: {self.stats['database_size']}",
            f"Files processed this run: {self.stats['files_processed']}",
            f"Files skipped (already processed): {self.stats['files_skipped']}",
            "",
            "DUPLICATE SUMMARY:",
            f"  Exact duplicate groups: {len([g for g in duplicate_groups if g.method == 'exact'])}",
            f"  Near-duplicate groups: {len([g for g in duplicate_groups if g.method == 'normalized_hash'])}",
            f"  Total duplicate files: {total_duplicates}",
            f"  Estimated wasted space: {total_wasted_space / (1024*1024*1024):.2f} GB",
            "",
            "DUPLICATE GROUPS:",
            "-" * 50
        ]
        
        for i, group in enumerate(duplicate_groups, 1):
            report_lines.extend([
                f"\nGroup #{i} ({group.method.upper()}) - Similarity: {group.similarity:.3f}",
                f"Recommended action: {group.recommended_action}"
            ])
            
            for file_path in group.files:
                try:
                    size_mb = os.path.getsize(file_path) / (1024*1024)
                    mtime = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y-%m-%d %H:%M:%S")
                    report_lines.append(f"  - {file_path} ({size_mb:.2f} MB, {mtime})")
                except OSError:
                    report_lines.append(f"  - {file_path} (file not accessible)")
        
        report_content = "\n".join(report_lines)
        
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report_content)
                self.log(f"Report saved to: {output_file}")
            except Exception as e:
                self.log(f"Error saving report: {e}", "ERROR")
                print(report_content)
        else:
            print("\n" + report_content)
        
        return report_content
    
    def generate_report_safety(self, confirmed_groups):
        """Generate safety mode report."""
        # Store reports in local project directory instead of NAS server
        report_path = Path.cwd() / "reports" / f"unified_duplicate_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        # Ensure reports directory exists
        report_path.parent.mkdir(exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("UNIFIED DUPLICATE DETECTION REPORT (Safety Mode)\n")
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
            
            for i, group in enumerate(confirmed_groups, 1):
                f.write(f"GROUP #{i} - Hash: {group['hash'][:12]}\n")
                f.write(f"  ORIGINAL (KEEP): {group['original']}\n")
                
                for dup_file in group['duplicates']:
                    file_size = os.path.getsize(dup_file)
                    f.write(f"  DUPLICATE (REMOVE): {dup_file} ({file_size / (1024*1024):.2f} MB)\n")
                
                f.write(f"  Group Total: {group['total_size'] / (1024*1024):.2f} MB\n")
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
UNIFIED DUPLICATE DETECTION v1.0 - USAGE EXAMPLES
=================================================

PERFORMANCE MODE (Fast Analysis):
1. Build comprehensive database quickly:
   python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted" --performance-mode --build-database
   
2. Find duplicates using existing database:
   python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted" --performance-mode --find-duplicates
   
3. Year-specific analysis:
   python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted" --performance-mode --build-database --years 2020-2024
   
4. Test single folder quickly:
   python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted" --performance-mode --test-folder "2010 - Photos"

SAFETY MODE (Conservative Removal):
1. Single folder processing:
   python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --scan
   python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --flag-deletions
   python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --execute-deletions
   
2. Multi-folder cross-duplicate detection:
   python detect_duplicates.py --folders "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" "\\\\NAS-MEDIA\\photo\\Sorted\\2011 - Photos" --scan
   python detect_duplicates.py --folders "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" "\\\\NAS-MEDIA\\photo\\Sorted\\2011 - Photos" --flag-deletions
   python detect_duplicates.py --folders "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" "\\\\NAS-MEDIA\\photo\\Sorted\\2011 - Photos" --execute-deletions

3. Complete workflow (all phases):
   python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --all-phases

ADVANCED OPTIONS:
1. Custom database location:
   python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted" --performance-mode --build-database --database "C:\\MyBackup\\duplicates.db"
   
2. Force complete rescan:
   python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --rescan --scan
   
3. Verbose debugging:
   python detect_duplicates.py "\\\\NAS-MEDIA\\photo\\Sorted\\2010 - Photos" --scan --verbose

MODE COMPARISON:
• Performance Mode: Fast batch processing, analysis only, no deletions
• Safety Mode: Conservative phased processing with binary verification and safe deletion

UNIFIED DATABASE:
All operations use the same database (photo_duplicates.db) with comprehensive schema
supporting both performance analysis and safety operations.
""")


def main():
    parser = argparse.ArgumentParser(
        description="""
Unified Duplicate Detection v1.0 - High Performance & Safety Combined

Choose your approach:
• Performance Mode (--performance-mode): Fast batch processing for analysis
• Safety Mode (default): Conservative phased processing for actual removal

Single unified database supports both operational modes with comprehensive tracking.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
PERFORMANCE MODE EXAMPLES:
  %(prog)s "\\\\NAS-MEDIA\\photo\\Sorted" --performance-mode --build-database
  %(prog)s "\\\\NAS-MEDIA\\photo\\Sorted" --performance-mode --find-duplicates

SAFETY MODE EXAMPLES:
  %(prog)s "C:\\Photos\\2010 - Photos" --scan
  %(prog)s "C:\\Photos\\2010 - Photos" --flag-deletions
  %(prog)s "C:\\Photos\\2010 - Photos" --execute-deletions

See --examples for comprehensive usage guide.
        """
    )
    
    # Folder specification - support both single and multiple
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("folder", nargs='?', 
                      help="Single folder to process")
    group.add_argument("--folders", nargs='+', metavar='FOLDER',
                      help="Multiple folders for cross-folder duplicate detection")
    
    # Mode selection
    mode_group = parser.add_argument_group('Processing Modes', 'Choose between performance and safety modes')
    mode_group.add_argument("--performance-mode", action="store_true",
                           help="High-speed batch processing for analysis (no deletions)")
    mode_group.add_argument("--safety-mode", action="store_true",
                           help="Conservative phased processing with binary verification (default)")
    
    # Performance mode operations
    perf_group = parser.add_argument_group('Performance Mode Operations', 'Fast batch processing')
    perf_group.add_argument("--build-database", action="store_true",
                           help="Build/update hash database (performance mode)")
    perf_group.add_argument("--find-duplicates", action="store_true",
                           help="Find duplicates using existing database (performance mode)")
    perf_group.add_argument("--test-folder", metavar='FOLDER',
                           help="Test with single folder (performance mode)")
    perf_group.add_argument("--incremental", action="store_true",
                           help="Process only new/changed files (performance mode)")
    perf_group.add_argument("--years", metavar='RANGE',
                           help="Process specific year range, e.g., '2010-2015' (performance mode)")
    perf_group.add_argument("--similarity", type=float, default=0.95,
                           help="Similarity threshold for near-duplicates (default: 0.95)")
    
    # Safety mode operations
    safety_group = parser.add_argument_group('Safety Mode Operations', 'Conservative phased processing')
    safety_group.add_argument("--scan", action="store_true",
                             help="Phase 1: Scan folders and identify duplicates with binary verification")
    safety_group.add_argument("--rescan", action="store_true",
                             help="Force complete rescan - clears previous data")
    safety_group.add_argument("--flag-deletions", action="store_true",
                             help="Phase 2: Flag binary-verified duplicates for deletion")
    safety_group.add_argument("--execute-deletions", action="store_true",
                             help="Phase 3: Move flagged files to ToBeDeleted folder")
    safety_group.add_argument("--all-phases", action="store_true",
                             help="Execute all safety phases in sequence")
    
    # Common options
    common_group = parser.add_argument_group('Common Options', 'Options for both modes')
    common_group.add_argument("--database", metavar='PATH',
                             help="Custom database path")
    common_group.add_argument("--thumbnail-size", type=int, choices=[32, 64, 128], default=64,
                             help="Normalized thumbnail size (default: 64)")
    common_group.add_argument("--report", metavar='FILE',
                             help="Save report to file")
    common_group.add_argument("--force-reprocess", action="store_true",
                             help="Reprocess all files (ignore cache)")
    common_group.add_argument("--verbose", "-v", action="store_true",
                             help="Enable verbose output")
    common_group.add_argument("--version", action="version", version="Unified Duplicate Detection v1.0")
    common_group.add_argument("--examples", action="store_true",
                             help="Show detailed usage examples and exit")
    
    args = parser.parse_args()
    
    # Handle examples request
    if args.examples:
        print_examples()
        sys.exit(0)
    
    # Determine folder paths
    if args.folder:
        folder_paths = [args.folder]
    elif args.folders:
        folder_paths = args.folders
    elif args.build_database or args.find_duplicates:
        # Performance mode might work without specific folders for existing database
        if args.find_duplicates and not (args.folder or args.folders):
            print("❌ Error: --find-duplicates requires folder specification")
            sys.exit(1)
        folder_paths = [args.folder] if args.folder else []
    else:
        print("❌ Error: You must specify either a single folder or multiple folders using --folders")
        print("   Use --help for usage information or --examples for detailed examples")
        sys.exit(1)
    
    # Validate folder existence
    for folder in folder_paths:
        if not os.path.exists(folder):
            print(f"❌ Folder not found: {folder}")
            sys.exit(1)
    
    # Determine processing mode
    if args.performance_mode:
        performance_mode = True
        print("🚀 PERFORMANCE MODE: High-speed batch processing")
    else:
        performance_mode = False
        print("🛡️  SAFETY MODE: Conservative phased processing (default)")
    
    print("=" * 60)
    print("UNIFIED DUPLICATE DETECTION v1.0")
    print("Single Database - Multiple Processing Modes")
    print()
    
    # Initialize detector
    detector = UnifiedDuplicateDetector(
        folder_paths, 
        args.database, 
        args.thumbnail_size,
        performance_mode,
        args.verbose
    )
    
    start_time = time.time()
    
    try:
        if performance_mode:
            # Performance mode operations
            if args.test_folder:
                # Test mode with single folder
                test_path = os.path.join(folder_paths[0], args.test_folder)
                if not os.path.exists(test_path):
                    print(f"❌ Test folder not found: {test_path}")
                    sys.exit(1)
                
                print(f"🧪 TEST MODE: Processing {args.test_folder}")
                files = detector.scan_folder_recursive(test_path)
                print(f"Found {len(files)} image files")
                
                if files:
                    detector._process_files_performance(files, args.force_reprocess)
                    duplicates = detector.find_duplicates_performance(args.similarity)
                    
                    if duplicates:
                        detector.generate_report_performance(duplicates, args.report)
                    else:
                        detector.log("✅ No duplicates found in test folder!")
            
            elif args.build_database:
                if not folder_paths:
                    print("❌ Error: --build-database requires folder specification")
                    sys.exit(1)
                detector.build_database_performance(
                    folder_paths[0], 
                    args.force_reprocess, 
                    args.years, 
                    args.incremental
                )
            
            elif args.find_duplicates:
                print("🔍 Performance mode: Finding duplicates...")
                duplicates = detector.find_duplicates_performance(args.similarity)
                
                if duplicates:
                    detector.generate_report_performance(duplicates, args.report)
                    print(f"\n📊 Found {len(duplicates)} duplicate groups!")
                else:
                    detector.log("✅ No duplicates found!")
            
            else:
                print("❌ Performance mode requires --build-database, --find-duplicates, or --test-folder")
                parser.print_help()
                sys.exit(1)
        
        else:
            # Safety mode operations
            if not any([args.scan, args.rescan, args.flag_deletions, args.execute_deletions, args.all_phases]):
                args.scan = True
                print("💡 No phase specified, defaulting to --scan")
            
            # Phase 1: Scan and identify duplicates
            if args.scan or args.rescan or args.all_phases:
                potential_groups = detector.scan_folders_safety(rescan=args.rescan)
                
                if potential_groups:
                    confirmed_groups = detector.verify_and_process_duplicates_safety(potential_groups)
                    
                    if confirmed_groups:
                        report_path = detector.generate_report_safety(confirmed_groups)
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
                
                flagged_count = detector.flag_deletions_safety()
                
                if flagged_count > 0 and not args.all_phases:
                    print("\n➡️  Next: Run with --execute-deletions to move flagged files")
            
            # Phase 3: Execute deletions
            if args.execute_deletions or args.all_phases:
                if args.all_phases:
                    print("\n" + "="*60)
                
                moved_count = detector.execute_deletions_safety()
                
                if moved_count > 0:
                    print(f"\n✅ DELETION PHASE COMPLETE")
                    print(f"   Files moved to ToBeDeleted: {moved_count}")
    
    except KeyboardInterrupt:
        print("\n⚠️  Processing interrupted by user")
    
    except Exception as e:
        detector.log(f"Unexpected error: {e}", "ERROR")
        raise
    
    finally:
        # Final statistics
        processing_time = time.time() - start_time
        print(f"\n⏱️  Processing completed in {processing_time:.1f} seconds")
        print(f"📊 Database: {detector.db_path}")
        
        if detector.stats['files_processed'] > 0:
            print(f"📈 Files processed: {detector.stats['files_processed']}")
        if detector.stats['files_skipped'] > 0:
            print(f"📋 Files skipped: {detector.stats['files_skipped']}")


if __name__ == "__main__":
    main()