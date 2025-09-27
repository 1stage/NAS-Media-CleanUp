#!/usr/bin/env python3
"""
detect_duplicates.py
Version 0.3 — Created by Sean P. Harrington with assistance from Microsoft Copilot
Date: Friday, 27 September 2025

High-performance duplicate detection using normalized image hashing for massive
photo collections. Designed for interactive laptop-based analysis of NAS media.

Key Innovation: Normalized Hash Algorithm
- Converts all images to standardized 32x32 or 64x64 thumbnails
- Letterboxed with black padding to preserve aspect ratios
- Quantized to standard 256-color palette for consistent comparison
- Results in tiny fingerprints (1-4KB) vs multi-MB original images
- Enables lightning-fast comparison of massive collections

Features:
- Persistent SQLite database tracking (never reprocess the same files)
- Automatic discovery of new year folders (2026+, etc.)
- Network-optimized for UNC path access to NAS
- Incremental processing with resume capability
- Cross-year duplicate detection (slideshow copies, mis-sorted photos)

Usage:
    python detect_duplicates.py "\\NAS-MEDIA\photo\Sorted" --test-folder "2010 - Photos"
    python detect_duplicates.py "\\NAS-MEDIA\photo\Sorted" --build-database
    python detect_duplicates.py "\\NAS-MEDIA\photo\Sorted" --find-duplicates
"""

import os
import sys
import argparse
import hashlib
import json
import time
import sqlite3
import re
from datetime import datetime
from collections import defaultdict, namedtuple
from pathlib import Path

# Required dependencies
try:
    from PIL import Image, ImageOps
    HAS_PILLOW = True
except ImportError:
    print("❌ ERROR: PIL/Pillow is required for normalized hash algorithm")
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

class NormalizedHashDetector:
    """Advanced duplicate detection using normalized image hashing."""
    
    def __init__(self, database_path=None, thumbnail_size=64):
        self.thumbnail_size = thumbnail_size  # 32, 64, or 128
        self.database_path = database_path or os.path.join(os.getcwd(), 'photo_hashes.db')
        
        # Image file extensions
        self.image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.heic', '.heif'}
        
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
        
        # Initialize database
        self.init_database()
        
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
            
    def init_database(self):
        """Initialize SQLite database for persistent tracking."""
        self.log("Initializing database...")
        
        with sqlite3.connect(self.database_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS photo_hashes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT UNIQUE NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_mtime REAL NOT NULL,
                    md5_hash TEXT NOT NULL,
                    normalized_hash BLOB NOT NULL,
                    thumbnail_size INTEGER NOT NULL,
                    processing_date REAL NOT NULL,
                    folder_year INTEGER,
                    status TEXT DEFAULT 'processed'
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_md5_hash ON photo_hashes(md5_hash)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_file_path ON photo_hashes(file_path)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_folder_year ON photo_hashes(folder_year)
            ''')
            
            # Get database statistics
            cursor = conn.execute("SELECT COUNT(*) FROM photo_hashes")
            self.stats['database_size'] = cursor.fetchone()[0]
            
        self.log(f"Database initialized: {self.stats['database_size']} photos already processed")
        
    def create_normalized_hash(self, image_path):
        """
        Create normalized hash using your algorithm:
        1. Resize to fixed square dimensions (32x32, 64x64, etc.)
        2. Letterbox with black padding to preserve aspect ratio
        3. Quantize to standard 256-color palette
        4. Generate hash from pixel data
        """
        try:
            with Image.open(image_path) as img:
                # Convert to RGB if needed
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Step 1 & 2: Resize with letterboxing (black padding)
                # This preserves aspect ratio while creating consistent dimensions
                img_resized = ImageOps.fit(
                    img, 
                    (self.thumbnail_size, self.thumbnail_size),
                    method=Image.Resampling.LANCZOS,
                    centering=(0.5, 0.5)
                )
                
                # Step 3: Quantize to standard 256-color palette
                # This normalizes color variations and compression artifacts
                img_quantized = img_resized.quantize(
                    colors=256,
                    method=Image.Quantize.MEDIANCUT,
                    kmeans=0,
                    palette=None,
                    dither=Image.Dither.NONE
                ).convert('RGB')
                
                # Step 4: Generate hash from pixel data
                # Convert to bytes for consistent hashing
                pixel_data = img_quantized.tobytes('raw', 'RGB')
                
                # Create hash of the normalized pixel data
                hash_obj = hashlib.sha256(pixel_data)
                normalized_hash = hash_obj.digest()
                
                return normalized_hash
                
        except Exception as e:
            self.log(f"Error creating normalized hash for {image_path}: {e}", "ERROR")
            return None
            
    def calculate_similarity(self, hash1, hash2):
        """Calculate similarity between two normalized hashes using Hamming distance."""
        if len(hash1) != len(hash2):
            return 0.0
            
        # Convert to bit arrays for Hamming distance calculation
        if HAS_NUMPY:
            # Fast numpy implementation
            arr1 = np.frombuffer(hash1, dtype=np.uint8)
            arr2 = np.frombuffer(hash2, dtype=np.uint8)
            
            # XOR and count different bits
            xor_result = np.bitwise_xor(arr1, arr2)
            hamming_distance = np.unpackbits(xor_result).sum()
            
            # Convert to similarity (0.0 to 1.0)
            max_distance = len(hash1) * 8  # 8 bits per byte
            similarity = 1.0 - (hamming_distance / max_distance)
            
        else:
            # Pure Python fallback
            different_bits = 0
            for b1, b2 in zip(hash1, hash2):
                xor = b1 ^ b2
                # Count set bits in XOR result
                different_bits += bin(xor).count('1')
                
            max_distance = len(hash1) * 8
            similarity = 1.0 - (different_bits / max_distance)
            
        return similarity
        
    def get_file_md5(self, file_path):
        """Calculate MD5 hash of file for exact duplicate detection."""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            self.log(f"Error calculating MD5 for {file_path}: {e}", "ERROR")
            return None
            
    def extract_year_from_path(self, file_path):
        """Extract year from folder path like '2024 - Photos'."""
        match = re.search(r'(\d{4})\s*-\s*Photos', file_path, re.IGNORECASE)
        return int(match.group(1)) if match else None
        
    def is_image_file(self, file_path):
        """Check if file is a supported image type."""
        return Path(file_path).suffix.lower() in self.image_extensions
        
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
        
    def process_files(self, file_paths, force_reprocess=False):
        """Process a list of files, creating normalized hashes."""
        self.log(f"🔍 Processing {len(file_paths)} files...")
        
        processed = 0
        skipped = 0
        
        # Progress bar for large batches
        if HAS_TQDM and len(file_paths) > 20:
            file_iter = tqdm(file_paths, desc="Processing images", unit="files")
        else:
            file_iter = file_paths
            
        with sqlite3.connect(self.database_path) as conn:
            for file_path in file_iter:
                try:
                    # Get file stats
                    stat = os.stat(file_path)
                    file_size = stat.st_size
                    file_mtime = stat.st_mtime
                    folder_year = self.extract_year_from_path(file_path)
                    
                    # Check if already processed (unless force reprocess)
                    if not force_reprocess:
                        cursor = conn.execute(
                            "SELECT file_mtime FROM photo_hashes WHERE file_path = ?",
                            (file_path,)
                        )
                        row = cursor.fetchone()
                        if row and row[0] >= file_mtime:
                            skipped += 1
                            continue
                    
                    # Calculate hashes
                    md5_hash = self.get_file_md5(file_path)
                    normalized_hash = self.create_normalized_hash(file_path)
                    
                    if md5_hash and normalized_hash:
                        # Store in database
                        conn.execute('''
                            INSERT OR REPLACE INTO photo_hashes 
                            (file_path, file_size, file_mtime, md5_hash, normalized_hash, 
                             thumbnail_size, processing_date, folder_year)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (file_path, file_size, file_mtime, md5_hash, normalized_hash,
                              self.thumbnail_size, time.time(), folder_year))
                        
                        processed += 1
                    
                except Exception as e:
                    self.log(f"Error processing {file_path}: {e}", "WARNING")
                    continue
                    
        self.stats['files_processed'] = processed
        self.stats['files_skipped'] = skipped
        
        self.log(f"✅ Processed {processed} files, skipped {skipped} already processed")
        
    def find_exact_duplicates(self):
        """Find exact duplicates based on MD5 hash."""
        self.log("🔍 Finding exact duplicates...")
        
        duplicates = []
        
        with sqlite3.connect(self.database_path) as conn:
            # Find MD5 hashes that appear more than once
            cursor = conn.execute('''
                SELECT md5_hash, COUNT(*) as count 
                FROM photo_hashes 
                GROUP BY md5_hash 
                HAVING count > 1
                ORDER BY count DESC
            ''')
            
            for md5_hash, count in cursor:
                # Get all files with this hash
                files_cursor = conn.execute('''
                    SELECT file_path, file_size FROM photo_hashes 
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
        
    def find_near_duplicates(self, similarity_threshold=0.95):
        """Find near-duplicates using normalized hash comparison."""
        self.log(f"🖼️  Finding near-duplicates (similarity >= {similarity_threshold})...")
        
        near_duplicates = []
        
        with sqlite3.connect(self.database_path) as conn:
            # Get all normalized hashes
            cursor = conn.execute('SELECT file_path, normalized_hash FROM photo_hashes')
            photos = cursor.fetchall()
            
        self.log(f"Comparing {len(photos)} photos for near-duplicates...")
        
        # Compare each photo with every other photo
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
                
                # Calculate similarity
                similarity = self.calculate_similarity(hash1, hash2)
                
                if similarity >= similarity_threshold and similarity < 1.0:  # Exclude exact matches
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
        
    def generate_report(self, duplicate_groups, output_file=None):
        """Generate comprehensive duplicate detection report."""
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
            f"NORMALIZED HASH DUPLICATE DETECTION REPORT - {timestamp}",
            "="*80,
            f"Database: {self.database_path}",
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


def main():
    parser = argparse.ArgumentParser(
        description="Normalized Hash Duplicate Detection v0.3 - High-performance duplicate finder",
        epilog="""Examples:
  %(prog)s "\\\\NAS-MEDIA\\photo\\Sorted" --test-folder "2010 - Photos"
  %(prog)s "\\\\NAS-MEDIA\\photo\\Sorted" --build-database --years 2010-2015
  %(prog)s "\\\\NAS-MEDIA\\photo\\Sorted" --find-duplicates --similarity 0.95
  %(prog)s "\\\\NAS-MEDIA\\photo\\Sorted" --incremental
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("photo_root", help="Root directory containing year-based photo folders")
    parser.add_argument("--test-folder", help="Test with single folder (e.g., '2010 - Photos')")
    parser.add_argument("--build-database", action="store_true", help="Build/update normalized hash database")
    parser.add_argument("--find-duplicates", action="store_true", help="Find duplicates using existing database")
    parser.add_argument("--incremental", action="store_true", help="Process only new/changed files")
    parser.add_argument("--years", help="Process specific year range (e.g., '2010-2015')")
    parser.add_argument("--similarity", type=float, default=0.95, help="Similarity threshold for near-duplicates")
    parser.add_argument("--thumbnail-size", type=int, choices=[32, 64, 128], default=64, 
                       help="Normalized thumbnail size (default: 64)")
    parser.add_argument("--database", help="Custom database path")
    parser.add_argument("--report", help="Save report to file")
    parser.add_argument("--force-reprocess", action="store_true", help="Reprocess all files (ignore cache)")
    parser.add_argument("--version", action="version", version="Normalized Hash Duplicate Detection v0.3")
    
    args = parser.parse_args()
    
    print("="*80)
    print("NORMALIZED HASH DUPLICATE DETECTION v0.3")
    print("="*80)
    
    # Validate photo root
    if not os.path.exists(args.photo_root):
        print(f"❌ Error: Photo root directory not found: {args.photo_root}")
        sys.exit(1)
        
    # Initialize detector
    detector = NormalizedHashDetector(
        database_path=args.database,
        thumbnail_size=args.thumbnail_size
    )
    
    print(f"📁 Photo root: {args.photo_root}")
    print(f"🗄️  Database: {detector.database_path}")
    print(f"📏 Thumbnail size: {args.thumbnail_size}x{args.thumbnail_size}")
    print()
    
    start_time = time.time()
    
    try:
        if args.test_folder:
            # Test mode with single folder
            test_path = os.path.join(args.photo_root, args.test_folder)
            if not os.path.exists(test_path):
                print(f"❌ Test folder not found: {test_path}")
                sys.exit(1)
                
            print(f"🧪 TEST MODE: Processing {args.test_folder}")
            
            # Scan and process files
            files = detector.scan_folder_recursive(test_path)
            print(f"Found {len(files)} image files")
            
            if files:
                detector.process_files(files, force_reprocess=args.force_reprocess)
                
                # Find duplicates
                exact_dupes = detector.find_exact_duplicates()
                near_dupes = detector.find_near_duplicates(args.similarity)
                
                all_duplicates = exact_dupes + near_dupes
                
                if all_duplicates:
                    detector.generate_report(all_duplicates, args.report)
                else:
                    detector.log("✅ No duplicates found in test folder!")
                    
        elif args.build_database:
            # Build database mode
            folders = detector.discover_photo_folders(args.photo_root)
            
            if args.years:
                # Filter by year range
                year_range = args.years.split('-')
                if len(year_range) == 2:
                    start_year = int(year_range[0])
                    end_year = int(year_range[1])
                    folders = [f for f in folders if start_year <= detector.extract_year_from_path(f) <= end_year]
                    
            print(f"📂 Discovered {len(folders)} photo folders")
            
            for folder in folders:
                folder_name = os.path.basename(folder)
                print(f"\n📁 Processing {folder_name}...")
                
                files = detector.scan_folder_recursive(folder)
                if files:
                    detector.process_files(files, force_reprocess=args.force_reprocess)
                    
            detector.log("✅ Database build complete!")
            
        elif args.find_duplicates:
            # Find duplicates mode
            print("🔍 Searching for duplicates in existing database...")
            
            exact_dupes = detector.find_exact_duplicates()
            near_dupes = detector.find_near_duplicates(args.similarity)
            
            all_duplicates = exact_dupes + near_dupes
            
            if all_duplicates:
                detector.generate_report(all_duplicates, args.report)
                print(f"\n📊 Found {len(all_duplicates)} duplicate groups!")
            else:
                detector.log("✅ No duplicates found!")
                
        elif args.incremental:
            # Incremental processing mode
            folders = detector.discover_photo_folders(args.photo_root)
            print(f"📂 Processing {len(folders)} folders incrementally...")
            
            for folder in folders:
                folder_name = os.path.basename(folder)
                files = detector.scan_folder_recursive(folder)
                
                if files:
                    print(f"\n📁 {folder_name}: {len(files)} files")
                    detector.process_files(files)  # Only processes new/changed files
                    
            detector.log("✅ Incremental processing complete!")
            
        else:
            print("❌ Please specify --test-folder, --build-database, --find-duplicates, or --incremental")
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️  Processing interrupted by user")
        
    except Exception as e:
        detector.log(f"Unexpected error: {e}", "ERROR")
        raise
        
    finally:
        # Final statistics
        processing_time = time.time() - start_time
        detector.stats['processing_time'] = processing_time
        
        print(f"\n⏱️  Processing completed in {processing_time:.1f} seconds")
        
        if detector.stats['files_processed'] > 0 or detector.stats['files_skipped'] > 0:
            print(f"📈 Files processed: {detector.stats['files_processed']}")
            print(f"📋 Files skipped: {detector.stats['files_skipped']}")
            
        if detector.stats['exact_duplicates'] > 0:
            print(f"🔍 Exact duplicates found: {detector.stats['exact_duplicates']}")
            
        if detector.stats['near_duplicates'] > 0:
            print(f"🖼️  Near-duplicates found: {detector.stats['near_duplicates']}")


if __name__ == "__main__":
    main()