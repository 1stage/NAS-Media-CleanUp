#!/usr/bin/env python3
"""
detect_duplicates.py
Version 1.0 - Advanced Photo & Video Duplicate Detection

A comprehensive duplicate and near-duplicate detection system for media files.
Features multiple detection algorithms for different types of duplicates:

- Exact duplicates (binary identical)
- Near-duplicates (similar images with different compression)
- Same content, different formats (JPEG vs PNG)
- Different resolutions of same image
- Similar photos taken in sequence

Designed to work with both Windows and NAS environments, with optional
dependencies for advanced image comparison features.

Usage:
    python detect_duplicates.py [options] directory1 [directory2 ...]

Features:
- Multiple detection algorithms (hash, perceptual, structural)
- Configurable similarity thresholds
- Safe preview mode with detailed reporting
- Batch processing with progress tracking
- Integration with existing organization tools
"""

import os
import sys
import argparse
import hashlib
import json
import time
from datetime import datetime
from collections import defaultdict, namedtuple
from pathlib import Path

# Optional advanced dependencies - graceful fallback if not available
try:
    from PIL import Image
    import imagehash
    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False

try:
    import cv2
    import numpy as np
    HAS_OPENCV = True
except ImportError:
    HAS_OPENCV = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# Duplicate detection result structure
DuplicateGroup = namedtuple('DuplicateGroup', ['method', 'similarity', 'files', 'recommended_action'])
FileInfo = namedtuple('FileInfo', ['path', 'size', 'mtime', 'hash_md5', 'hash_sha256'])

class DuplicateDetector:
    """Advanced duplicate detection with multiple algorithms."""
    
    def __init__(self, similarity_threshold=0.95, enable_near_duplicates=True):
        self.similarity_threshold = similarity_threshold
        self.enable_near_duplicates = enable_near_duplicates and HAS_PILLOW
        self.enable_opencv = HAS_OPENCV
        
        # Statistics tracking
        self.stats = {
            'files_scanned': 0,
            'exact_duplicates': 0,
            'near_duplicates': 0,
            'total_groups': 0,
            'space_wasted': 0,
            'processing_time': 0
        }
        
        # Supported file types
        self.image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.heic', '.heif'}
        self.video_extensions = {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.m4v', '.3gp', '.webm'}
        
        # File information cache
        self.file_cache = {}
        self.duplicate_groups = []
        
    def log(self, message, level="INFO"):
        """Enhanced logging with timestamps."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        if level == "ERROR":
            print(f"🚨 [{timestamp}] {message}")
        elif level == "WARNING":
            print(f"⚠️  [{timestamp}] {message}")
        elif level == "INFO":
            print(f"ℹ️  [{timestamp}] {message}")
        else:
            print(f"[{timestamp}] [{level}] {message}")
        
    def get_file_hash(self, filepath, algorithm='md5', chunk_size=8192):
        """Calculate file hash with progress tracking for large files."""
        hash_func = hashlib.md5() if algorithm == 'md5' else hashlib.sha256()
        
        try:
            file_size = os.path.getsize(filepath)
            with open(filepath, 'rb') as f:
                processed = 0
                while chunk := f.read(chunk_size):
                    hash_func.update(chunk)
                    processed += len(chunk)
                    
                    # Progress for large files (>100MB)
                    if file_size > 100 * 1024 * 1024 and processed % (10 * 1024 * 1024) == 0:
                        progress = (processed / file_size) * 100
                        self.log(f"Hashing {os.path.basename(filepath)}: {progress:.1f}%", "DEBUG")
                        
            return hash_func.hexdigest()
        except (IOError, OSError) as e:
            self.log(f"Error hashing {filepath}: {e}", "ERROR")
            return None
            
    def get_file_info(self, filepath):
        """Get comprehensive file information."""
        if filepath in self.file_cache:
            return self.file_cache[filepath]
            
        try:
            stat = os.stat(filepath)
            info = FileInfo(
                path=filepath,
                size=stat.st_size,
                mtime=stat.st_mtime,
                hash_md5=None,  # Calculated on demand
                hash_sha256=None  # Calculated on demand
            )
            self.file_cache[filepath] = info
            return info
        except (IOError, OSError) as e:
            self.log(f"Error getting file info for {filepath}: {e}", "ERROR")
            return None
            
    def find_exact_duplicates(self, filepaths):
        """Find exact duplicates using file size and hash comparison."""
        self.log("🔍 Finding exact duplicates...")
        
        # Group by size first (fast pre-filter)
        self.log(f"📊 Analyzing {len(filepaths)} files by size...")
        size_groups = defaultdict(list)
        for filepath in filepaths:
            info = self.get_file_info(filepath)
            if info and info.size > 0:  # Skip empty files
                size_groups[info.size].append(filepath)
                
        # Count potential duplicates
        potential_dupes = sum(len(files) for files in size_groups.values() if len(files) > 1)
        if potential_dupes == 0:
            self.log("✅ No files with matching sizes found - no exact duplicates possible")
            return []
            
        self.log(f"🎯 Found {potential_dupes} files with matching sizes, calculating hashes...")
        exact_duplicates = []
        
        # Progress tracking for hash calculation
        files_to_hash = [files for files in size_groups.values() if len(files) > 1]
        total_hash_files = sum(len(files) for files in files_to_hash)
        
        if HAS_TQDM and total_hash_files > 10:
            pbar = tqdm(total=total_hash_files, desc="Hashing files", unit="files")
        else:
            pbar = None
            
        try:
            # Check files with same size
            for size, files in size_groups.items():
                if len(files) < 2:
                    continue
                    
                # Group by hash
                hash_groups = defaultdict(list)
                for filepath in files:
                    file_hash = self.get_file_hash(filepath, 'md5')
                    if file_hash:
                        hash_groups[file_hash].append(filepath)
                    if pbar:
                        pbar.update(1)
                        
                # Find duplicate groups
                for file_hash, duplicate_files in hash_groups.items():
                    if len(duplicate_files) > 1:
                        # Verify with SHA256 for security
                        sha256_groups = defaultdict(list)
                        for filepath in duplicate_files:
                            sha256_hash = self.get_file_hash(filepath, 'sha256')
                            if sha256_hash:
                                sha256_groups[sha256_hash].append(filepath)
                                
                        for sha256_hash, verified_files in sha256_groups.items():
                            if len(verified_files) > 1:
                                group = DuplicateGroup(
                                    method='exact',
                                    similarity=1.0,
                                    files=verified_files,
                                    recommended_action=self._recommend_action(verified_files)
                                )
                                exact_duplicates.append(group)
                                self.stats['exact_duplicates'] += len(verified_files) - 1
                                self.log(f"🔍 Found exact duplicate group: {len(verified_files)} identical files")
        finally:
            if pbar:
                pbar.close()
                                
        return exact_duplicates
        
    def find_near_duplicates(self, filepaths):
        """Find near-duplicates using perceptual hashing (requires PIL)."""
        if not self.enable_near_duplicates:
            self.log("⚠️  Near-duplicate detection disabled (PIL not available)")
            return []
            
        self.log("🖼️  Finding near-duplicates using perceptual hashing...")
        
        # Filter to image files only
        image_files = [f for f in filepaths if self._is_image_file(f)]
        if len(image_files) < 2:
            self.log("ℹ️  Not enough image files for near-duplicate detection")
            return []
            
        self.log(f"📸 Processing {len(image_files)} image files...")
        
        # Calculate perceptual hashes with progress tracking
        hash_data = {}
        if HAS_TQDM and len(image_files) > 20:
            image_iter = tqdm(image_files, desc="Calculating perceptual hashes", unit="images")
        else:
            image_iter = image_files
            
        processed = 0
        for filepath in image_iter:
            try:
                with Image.open(filepath) as img:
                    # Use multiple hash algorithms for better accuracy
                    phash = imagehash.phash(img)
                    dhash = imagehash.dhash(img)
                    whash = imagehash.whash(img)
                    
                    hash_data[filepath] = {
                        'phash': phash,
                        'dhash': dhash,
                        'whash': whash
                    }
                    processed += 1
            except Exception as e:
                self.log(f"Error processing image {os.path.basename(filepath)}: {e}", "WARNING")
                continue
                
        self.log(f"✅ Successfully processed {processed} images for comparison")
                
        # Compare hashes to find similar images
        near_duplicates = []
        processed_pairs = set()
        
        for filepath1, hashes1 in hash_data.items():
            for filepath2, hashes2 in hash_data.items():
                if filepath1 >= filepath2:  # Avoid duplicate comparisons
                    continue
                    
                pair = tuple(sorted([filepath1, filepath2]))
                if pair in processed_pairs:
                    continue
                processed_pairs.add(pair)
                
                # Calculate similarity using multiple hash types
                similarities = []
                for hash_type in ['phash', 'dhash', 'whash']:
                    hash_diff = hashes1[hash_type] - hashes2[hash_type]
                    similarity = 1.0 - (hash_diff / 64.0)  # 64-bit hash
                    similarities.append(similarity)
                    
                avg_similarity = sum(similarities) / len(similarities)
                
                if avg_similarity >= self.similarity_threshold:
                    group = DuplicateGroup(
                        method='perceptual',
                        similarity=avg_similarity,
                        files=[filepath1, filepath2],
                        recommended_action=self._recommend_action([filepath1, filepath2])
                    )
                    near_duplicates.append(group)
                    self.stats['near_duplicates'] += 1
                    
        return near_duplicates
        
    def find_opencv_duplicates(self, filepaths):
        """Find duplicates using OpenCV structural similarity (requires OpenCV)."""
        if not self.enable_opencv:
            self.log("OpenCV detection disabled (cv2 not available)")
            return []
            
        self.log("Finding duplicates using OpenCV structural similarity...")
        
        image_files = [f for f in filepaths if self._is_image_file(f)]
        if len(image_files) < 2:
            return []
            
        # Load and preprocess images
        images = {}
        for filepath in image_files:
            try:
                img = cv2.imread(filepath)
                if img is not None:
                    # Resize for consistent comparison
                    img_resized = cv2.resize(img, (256, 256))
                    img_gray = cv2.cvtColor(img_resized, cv2.COLOR_BGR2GRAY)
                    images[filepath] = img_gray
            except Exception as e:
                self.log(f"Error loading image {filepath}: {e}", "WARNING")
                continue
                
        # Compare images using structural similarity
        opencv_duplicates = []
        processed_pairs = set()
        
        for filepath1, img1 in images.items():
            for filepath2, img2 in images.items():
                if filepath1 >= filepath2:
                    continue
                    
                pair = tuple(sorted([filepath1, filepath2]))
                if pair in processed_pairs:
                    continue
                processed_pairs.add(pair)
                
                # Calculate structural similarity
                try:
                    from skimage.metrics import structural_similarity as ssim
                    similarity = ssim(img1, img2)
                    
                    if similarity >= self.similarity_threshold:
                        group = DuplicateGroup(
                            method='structural',
                            similarity=similarity,
                            files=[filepath1, filepath2],
                            recommended_action=self._recommend_action([filepath1, filepath2])
                        )
                        opencv_duplicates.append(group)
                except ImportError:
                    # Fallback to simpler comparison if scikit-image not available
                    diff = cv2.absdiff(img1, img2)
                    similarity = 1.0 - (np.mean(diff) / 255.0)
                    
                    if similarity >= self.similarity_threshold:
                        group = DuplicateGroup(
                            method='opencv',
                            similarity=similarity,
                            files=[filepath1, filepath2],
                            recommended_action=self._recommend_action([filepath1, filepath2])
                        )
                        opencv_duplicates.append(group)
                        
        return opencv_duplicates
        
    def _is_image_file(self, filepath):
        """Check if file is an image."""
        return Path(filepath).suffix.lower() in self.image_extensions
        
    def _is_video_file(self, filepath):
        """Check if file is a video."""
        return Path(filepath).suffix.lower() in self.video_extensions
        
    def _recommend_action(self, files):
        """Recommend which file to keep based on various criteria."""
        if len(files) < 2:
            return "keep_all"
            
        # Scoring criteria (higher is better)
        def score_file(filepath):
            score = 0
            stat = os.stat(filepath)
            
            # Prefer larger files (higher quality)
            score += stat.st_size / (1024 * 1024)  # MB
            
            # Prefer newer files
            score += (stat.st_mtime - 1000000000) / 100000  # Normalized timestamp
            
            # Prefer certain formats
            ext = Path(filepath).suffix.lower()
            if ext in ['.png', '.tiff']:  # Lossless formats
                score += 10
            elif ext == '.jpg':  # Common format
                score += 5
                
            # Prefer shorter paths (likely organized)
            score -= len(filepath) / 100
            
            return score
            
        # Score all files
        scored_files = [(score_file(f), f) for f in files]
        scored_files.sort(reverse=True)  # Highest score first
        
        recommended_keep = scored_files[0][1]
        return f"keep:{os.path.basename(recommended_keep)}"
        
    def scan_directories(self, directories, recursive=True):
        """Scan directories for media files."""
        self.log(f"Scanning {len(directories)} directories...")
        
        all_files = []
        for directory in directories:
            if not os.path.exists(directory):
                self.log(f"Directory not found: {directory}", "WARNING")
                continue
                
            self.log(f"Scanning: {directory}")
            
            if recursive:
                for root, dirs, files in os.walk(directory):
                    # Skip system directories
                    dirs[:] = [d for d in dirs if not d.startswith('@')]
                    
                    for file in files:
                        filepath = os.path.join(root, file)
                        if self._is_media_file(filepath):
                            all_files.append(filepath)
                            self.stats['files_scanned'] += 1
            else:
                for file in os.listdir(directory):
                    filepath = os.path.join(directory, file)
                    if os.path.isfile(filepath) and self._is_media_file(filepath):
                        all_files.append(filepath)
                        self.stats['files_scanned'] += 1
                        
        self.log(f"Found {len(all_files)} media files")
        return all_files
        
    def _is_media_file(self, filepath):
        """Check if file is a supported media type."""
        ext = Path(filepath).suffix.lower()
        return ext in self.image_extensions or ext in self.video_extensions
        
    def detect_all_duplicates(self, directories, recursive=True):
        """Run all duplicate detection algorithms."""
        start_time = time.time()
        
        # Scan for files
        all_files = self.scan_directories(directories, recursive)
        if len(all_files) < 2:
            self.log("Not enough files found for duplicate detection")
            return []
            
        # Run detection algorithms
        self.duplicate_groups = []
        
        # 1. Exact duplicates (always run)
        exact_dupes = self.find_exact_duplicates(all_files)
        self.duplicate_groups.extend(exact_dupes)
        
        # 2. Near duplicates (if enabled)
        if self.enable_near_duplicates:
            near_dupes = self.find_near_duplicates(all_files)
            self.duplicate_groups.extend(near_dupes)
            
        # 3. OpenCV duplicates (if available)
        if self.enable_opencv:
            opencv_dupes = self.find_opencv_duplicates(all_files)
            self.duplicate_groups.extend(opencv_dupes)
            
        # Calculate statistics
        self.stats['total_groups'] = len(self.duplicate_groups)
        self.stats['processing_time'] = time.time() - start_time
        
        # Calculate space wasted
        for group in self.duplicate_groups:
            if group.method == 'exact':
                # For exact duplicates, all but one file are wasted space
                sizes = [os.path.getsize(f) for f in group.files if os.path.exists(f)]
                if sizes:
                    self.stats['space_wasted'] += sum(sizes) - max(sizes)
                    
        return self.duplicate_groups
        
    def generate_report(self, output_file=None):
        """Generate comprehensive duplicate detection report."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        report_lines = [
            "="*80,
            f"DUPLICATE DETECTION REPORT - {timestamp}",
            "="*80,
            f"Files scanned: {self.stats['files_scanned']}",
            f"Exact duplicate groups: {len([g for g in self.duplicate_groups if g.method == 'exact'])}",
            f"Near-duplicate groups: {len([g for g in self.duplicate_groups if g.method == 'perceptual'])}",
            f"Total duplicate groups: {self.stats['total_groups']}",
            f"Space wasted by duplicates: {self.stats['space_wasted'] / (1024*1024):.2f} MB",
            f"Processing time: {self.stats['processing_time']:.2f} seconds",
            "",
            "DUPLICATE GROUPS:",
            "-" * 40
        ]
        
        for i, group in enumerate(self.duplicate_groups, 1):
            report_lines.extend([
                f"\nGroup #{i} ({group.method.upper()}) - Similarity: {group.similarity:.3f}",
                f"Recommended action: {group.recommended_action}"
            ])
            
            for filepath in group.files:
                size_mb = os.path.getsize(filepath) / (1024*1024) if os.path.exists(filepath) else 0
                mtime = datetime.fromtimestamp(os.path.getmtime(filepath)).strftime("%Y-%m-%d %H:%M:%S") if os.path.exists(filepath) else "N/A"
                report_lines.append(f"  - {filepath} ({size_mb:.2f} MB, {mtime})")
                
        report_content = "\n".join(report_lines)
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_content)
            self.log(f"Report saved to: {output_file}")
        else:
            print("\n" + report_content)
            
        return report_content


def main():
    parser = argparse.ArgumentParser(
        description="Advanced Duplicate Detection v1.0 - Find exact and near-duplicate media files",
        epilog="""Examples:
  %(prog)s /path/to/photos --dry-run                    # Preview duplicate detection
  %(prog)s /path/to/photos --similarity 0.9 --report   # Find 90%+ similar images  
  %(prog)s /path1 /path2 --no-near-duplicates          # Exact duplicates only
  %(prog)s /volume1/photo --recursive --report duplicate_report.txt
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("directories", nargs="+", help="Directories to scan for duplicates")
    parser.add_argument("--similarity", type=float, default=0.95, help="Similarity threshold for near-duplicates (0.0-1.0)")
    parser.add_argument("--no-near-duplicates", action="store_true", help="Disable near-duplicate detection (faster)")
    parser.add_argument("--no-recursive", action="store_true", help="Don't scan subdirectories")
    parser.add_argument("--report", type=str, help="Save report to file")
    parser.add_argument("--dry-run", action="store_true", help="Preview mode - no actions taken")
    parser.add_argument("--json-output", type=str, help="Save results as JSON file")
    
    args = parser.parse_args()
    
    # Validate directories
    for directory in args.directories:
        if not os.path.exists(directory):
            print(f"Error: Directory not found: {directory}")
            sys.exit(1)
            
    # Initialize detector
    detector = DuplicateDetector(
        similarity_threshold=args.similarity,
        enable_near_duplicates=not args.no_near_duplicates
    )
    
    print("="*80)
    print("ADVANCED DUPLICATE DETECTION v1.0")
    print("="*80)
    
    if args.dry_run:
        print("[DRY RUN MODE] Preview only - no files will be modified")
        
    if not HAS_PILLOW:
        print("[WARNING] PIL/Pillow not available - near-duplicate detection disabled")
        
    if not HAS_OPENCV:
        print("[INFO] OpenCV not available - advanced image comparison disabled")
        
    print(f"Similarity threshold: {args.similarity}")
    print(f"Directories to scan: {', '.join(args.directories)}")
    print()
    
    # Run detection
    duplicate_groups = detector.detect_all_duplicates(
        args.directories, 
        recursive=not args.no_recursive
    )
    
    # Generate report
    if args.report:
        detector.generate_report(args.report)
    else:
        detector.generate_report()
        
    # Save JSON output if requested
    if args.json_output:
        json_data = {
            'timestamp': datetime.now().isoformat(),
            'statistics': detector.stats,
            'groups': [
                {
                    'method': group.method,
                    'similarity': group.similarity,
                    'files': group.files,
                    'recommended_action': group.recommended_action
                }
                for group in duplicate_groups
            ]
        }
        
        with open(args.json_output, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        print(f"\nJSON output saved to: {args.json_output}")
        
    print(f"\nDetection complete! Found {len(duplicate_groups)} duplicate groups.")
    
    if duplicate_groups and not args.dry_run:
        print("\nNext steps:")
        print("1. Review the report carefully before taking any action")
        print("2. Use the recommended actions as guidance")
        print("3. Consider running with --dry-run first for safety")


if __name__ == "__main__":
    main()