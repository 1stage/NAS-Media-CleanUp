#!/usr/bin/env python3
"""
detect_duplicates_nas.py
Version 1.0 - NAS-Optimized Duplicate Detection

Lightweight duplicate detection optimized for Synology NAS environments.
Focuses on exact duplicates and basic similarity detection without heavy dependencies.

This version is designed to run efficiently on NAS hardware with minimal
memory usage and no external dependencies beyond Python standard library.

Features:
- Exact duplicate detection using file hashing
- Basic image similarity using file metadata
- Efficient processing for large media collections  
- NAS-specific optimizations (handles @eaDir, thumbnails, etc.)
- Minimal memory footprint
- Progressive processing for very large datasets

Usage:
    python3 detect_duplicates_nas.py [options] directory1 [directory2 ...]
"""

import os
import sys
import argparse
import hashlib
import json
import time
import mimetypes
from datetime import datetime
from collections import defaultdict, namedtuple
from pathlib import Path

# File information structure
FileInfo = namedtuple('FileInfo', ['path', 'size', 'mtime', 'hash'])
DuplicateGroup = namedtuple('DuplicateGroup', ['method', 'files', 'total_size', 'recommended_keep'])

class NASDetector:
    """Lightweight duplicate detector optimized for NAS environments."""
    
    def __init__(self, chunk_size=8192, progress_interval=100):
        self.chunk_size = chunk_size
        self.progress_interval = progress_interval
        
        # NAS-specific exclusions
        self.skip_dirs = {'@eaDir', '.DS_Store', 'Thumbs.db', '@Recycle', '@tmp'}
        self.skip_files = {'.DS_Store', 'Thumbs.db', 'desktop.ini', '@eaDir'}
        
        # Supported media types
        self.supported_extensions = {
            # Images
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', 
            '.webp', '.heic', '.heif', '.raw', '.cr2', '.nef', '.arw',
            # Videos  
            '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.m4v', 
            '.3gp', '.webm', '.mpg', '.mpeg', '.m2v', '.asf'
        }
        
        # Statistics
        self.stats = {
            'files_scanned': 0,
            'files_skipped': 0,
            'duplicate_groups': 0,
            'duplicate_files': 0,
            'space_wasted': 0,
            'processing_time': 0,
            'largest_group': 0
        }
        
    def log(self, message, level="INFO"):
        """Simple logging with timestamps."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {message}")
        
    def should_skip_path(self, path):
        """Check if path should be skipped (NAS-specific logic)."""
        path_parts = Path(path).parts
        
        # Skip system directories
        for part in path_parts:
            if part in self.skip_dirs or part.startswith('@'):
                return True
                
        # Skip system files
        filename = os.path.basename(path)
        if filename in self.skip_files or filename.startswith('.'):
            return True
            
        return False
        
    def is_supported_file(self, filepath):
        """Check if file is a supported media type."""
        if self.should_skip_path(filepath):
            return False
            
        ext = Path(filepath).suffix.lower()
        return ext in self.supported_extensions
        
    def calculate_hash(self, filepath, algorithm='md5'):
        """Calculate file hash efficiently."""
        hash_func = hashlib.md5() if algorithm == 'md5' else hashlib.sha256()
        
        try:
            with open(filepath, 'rb') as f:
                while chunk := f.read(self.chunk_size):
                    hash_func.update(chunk)
            return hash_func.hexdigest()
        except (IOError, OSError) as e:
            self.log(f"Error hashing {filepath}: {e}")
            return None
            
    def get_file_info(self, filepath):
        """Get file information including hash."""
        try:
            stat = os.stat(filepath)
            file_hash = self.calculate_hash(filepath)
            
            if file_hash is None:
                return None
                
            return FileInfo(
                path=filepath,
                size=stat.st_size,
                mtime=stat.st_mtime,
                hash=file_hash
            )
        except (IOError, OSError) as e:
            self.log(f"Error getting info for {filepath}: {e}")
            return None
            
    def scan_directory(self, directory, recursive=True):
        """Scan directory for supported media files."""
        self.log(f"Scanning: {directory}")
        files = []
        
        try:
            if recursive:
                for root, dirs, filenames in os.walk(directory):
                    # Filter out system directories in-place
                    dirs[:] = [d for d in dirs if not self.should_skip_path(os.path.join(root, d))]
                    
                    for filename in filenames:
                        filepath = os.path.join(root, filename)
                        
                        if self.is_supported_file(filepath):
                            files.append(filepath)
                            self.stats['files_scanned'] += 1
                            
                            # Progress indication for large scans
                            if self.stats['files_scanned'] % self.progress_interval == 0:
                                self.log(f"Scanned {self.stats['files_scanned']} files...")
                        else:
                            self.stats['files_skipped'] += 1
            else:
                for filename in os.listdir(directory):
                    filepath = os.path.join(directory, filename)
                    if os.path.isfile(filepath) and self.is_supported_file(filepath):
                        files.append(filepath)
                        self.stats['files_scanned'] += 1
                        
        except (IOError, OSError) as e:
            self.log(f"Error scanning {directory}: {e}")
            
        return files
        
    def find_duplicates(self, directories, recursive=True):
        """Find duplicate files using size and hash comparison."""
        start_time = time.time()
        
        # Collect all files
        all_files = []
        for directory in directories:
            if os.path.exists(directory):
                files = self.scan_directory(directory, recursive)
                all_files.extend(files)
            else:
                self.log(f"Directory not found: {directory}")
                
        self.log(f"Found {len(all_files)} media files to process")
        
        if len(all_files) < 2:
            self.log("Not enough files for duplicate detection")
            return []
            
        # Group by size first (fast pre-filter)
        self.log("Grouping files by size...")
        size_groups = defaultdict(list)
        
        for filepath in all_files:
            try:
                size = os.path.getsize(filepath)
                if size > 0:  # Skip empty files
                    size_groups[size].append(filepath)
            except OSError:
                continue
                
        # Find potential duplicates (same size)
        potential_duplicates = []
        for size, files in size_groups.items():
            if len(files) > 1:
                potential_duplicates.extend(files)
                
        self.log(f"Found {len(potential_duplicates)} files with matching sizes")
        
        # Calculate hashes for potential duplicates
        self.log("Calculating hashes for potential duplicates...")
        file_info = {}
        
        for i, filepath in enumerate(potential_duplicates):
            info = self.get_file_info(filepath)
            if info:
                file_info[filepath] = info
                
            # Progress indication
            if (i + 1) % 50 == 0:
                progress = (i + 1) / len(potential_duplicates) * 100
                self.log(f"Hash progress: {progress:.1f}% ({i + 1}/{len(potential_duplicates)})")
                
        # Group by hash to find exact duplicates
        self.log("Identifying duplicate groups...")
        hash_groups = defaultdict(list)
        
        for filepath, info in file_info.items():
            hash_groups[info.hash].append(info)
            
        # Create duplicate groups
        duplicate_groups = []
        for file_hash, file_list in hash_groups.items():
            if len(file_list) > 1:
                # Sort by modification time (newest first) for recommendation
                file_list.sort(key=lambda x: x.mtime, reverse=True)
                
                total_size = sum(info.size for info in file_list)
                wasted_space = total_size - file_list[0].size  # Keep newest, waste others
                
                group = DuplicateGroup(
                    method='exact',
                    files=[info.path for info in file_list],
                    total_size=total_size,
                    recommended_keep=file_list[0].path
                )
                
                duplicate_groups.append(group)
                self.stats['duplicate_groups'] += 1
                self.stats['duplicate_files'] += len(file_list) - 1
                self.stats['space_wasted'] += wasted_space
                self.stats['largest_group'] = max(self.stats['largest_group'], len(file_list))
                
        # Final statistics
        self.stats['processing_time'] = time.time() - start_time
        
        return duplicate_groups
        
    def recommend_action(self, group):
        """Recommend which file to keep based on various criteria."""
        if len(group.files) < 2:
            return group.files[0] if group.files else None
            
        scored_files = []
        
        for filepath in group.files:
            try:
                stat = os.stat(filepath)
                score = 0
                
                # Prefer newer files
                score += stat.st_mtime / 1000000  # Normalize timestamp
                
                # Prefer files in organized directory structures
                path_parts = len(Path(filepath).parts)
                if path_parts > 3:  # Indicates organized structure
                    score += 5
                    
                # Prefer certain naming patterns
                filename = os.path.basename(filepath).lower()
                if any(pattern in filename for pattern in ['img_', 'dsc_', 'p_']):
                    score += 2  # Camera naming patterns
                    
                # Prefer standard locations
                if any(folder in filepath.lower() for folder in ['photos', 'pictures', 'images']):
                    score += 3
                    
                scored_files.append((score, filepath))
                
            except OSError:
                scored_files.append((0, filepath))
                
        # Return highest scored file
        scored_files.sort(reverse=True)
        return scored_files[0][1]
        
    def generate_report(self, duplicate_groups, output_file=None):
        """Generate comprehensive duplicate report."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        report_lines = [
            "="*80,
            f"NAS DUPLICATE DETECTION REPORT - {timestamp}",
            "="*80,
            "",
            "SUMMARY:",
            f"  Files scanned: {self.stats['files_scanned']:,}",
            f"  Files skipped: {self.stats['files_skipped']:,}",
            f"  Duplicate groups found: {self.stats['duplicate_groups']:,}",
            f"  Duplicate files: {self.stats['duplicate_files']:,}",
            f"  Space wasted by duplicates: {self.stats['space_wasted'] / (1024*1024*1024):.2f} GB",
            f"  Largest duplicate group: {self.stats['largest_group']} files",
            f"  Processing time: {self.stats['processing_time']:.1f} seconds",
            "",
            "POTENTIAL SPACE SAVINGS:",
            f"  {self.stats['duplicate_files']} files could be removed",
            f"  {self.stats['space_wasted'] / (1024*1024*1024):.2f} GB could be freed",
            "",
            "DUPLICATE GROUPS:",
            "-" * 50
        ]
        
        for i, group in enumerate(duplicate_groups, 1):
            group_size_mb = group.total_size / (1024*1024)
            wasted_mb = (group.total_size - os.path.getsize(group.recommended_keep)) / (1024*1024)
            
            report_lines.extend([
                f"\nGroup #{i} - {len(group.files)} identical files ({group_size_mb:.1f} MB total)",
                f"Recommended keep: {os.path.basename(group.recommended_keep)}",
                f"Space wasted: {wasted_mb:.1f} MB",
                "Files:"
            ])
            
            for filepath in group.files:
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                    size_mb = os.path.getsize(filepath) / (1024*1024)
                    marker = " [KEEP]" if filepath == group.recommended_keep else " [DELETE]"
                    report_lines.append(f"  {marker} {filepath}")
                    report_lines.append(f"       Size: {size_mb:.1f} MB, Modified: {mtime.strftime('%Y-%m-%d %H:%M:%S')}")
                except OSError:
                    report_lines.append(f"  [ERROR] {filepath} (file not accessible)")
                    
        report_content = "\n".join(report_lines)
        
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report_content)
                self.log(f"Report saved to: {output_file}")
            except IOError as e:
                self.log(f"Error saving report: {e}")
                print(report_content)
        else:
            print(report_content)
            
        return report_content
        
    def save_json_results(self, duplicate_groups, output_file):
        """Save results in JSON format for processing by other tools."""
        results = {
            'timestamp': datetime.now().isoformat(),
            'statistics': self.stats,
            'duplicate_groups': [
                {
                    'method': group.method,
                    'files': group.files,
                    'total_size': group.total_size,
                    'recommended_keep': group.recommended_keep,
                    'space_wasted': group.total_size - os.path.getsize(group.recommended_keep)
                }
                for group in duplicate_groups
            ]
        }
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            self.log(f"JSON results saved to: {output_file}")
        except IOError as e:
            self.log(f"Error saving JSON: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="NAS Duplicate Detection v1.0 - Lightweight duplicate finder for NAS environments",
        epilog="""Examples:
  %(prog)s /volume1/photo --report duplicates.txt        # Scan photos with report
  %(prog)s /volume1/video /volume2/backup --json out.json  # Multiple dirs, JSON output
  %(prog)s /volume1/photo --no-recursive                 # Current directory only
  %(prog)s /shared/media --progress 500                  # Show progress every 500 files
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("directories", nargs="+", 
                       help="Directories to scan for duplicates")
    parser.add_argument("--no-recursive", action="store_true",
                       help="Don't scan subdirectories")
    parser.add_argument("--report", type=str,
                       help="Save detailed report to file")
    parser.add_argument("--json", type=str,
                       help="Save results as JSON file")
    parser.add_argument("--progress", type=int, default=100,
                       help="Show progress every N files (default: 100)")
    parser.add_argument("--chunk-size", type=int, default=8192,
                       help="File reading chunk size in bytes (default: 8192)")
    
    args = parser.parse_args()
    
    # Validate directories
    for directory in args.directories:
        if not os.path.exists(directory):
            print(f"Error: Directory not found: {directory}")
            sys.exit(1)
            
    # Initialize detector
    detector = NASDetector(
        chunk_size=args.chunk_size,
        progress_interval=args.progress
    )
    
    print("="*80)
    print("NAS DUPLICATE DETECTION v1.0")
    print("="*80)
    print(f"Scanning directories: {', '.join(args.directories)}")
    print(f"Recursive: {'No' if args.no_recursive else 'Yes'}")
    print(f"Progress updates: Every {args.progress} files")
    print()
    
    # Run duplicate detection
    duplicate_groups = detector.find_duplicates(
        args.directories,
        recursive=not args.no_recursive
    )
    
    # Generate outputs
    if duplicate_groups:
        detector.log(f"Found {len(duplicate_groups)} duplicate groups")
        
        # Generate report
        if args.report:
            detector.generate_report(duplicate_groups, args.report)
        else:
            detector.generate_report(duplicate_groups)
            
        # Save JSON if requested
        if args.json:
            detector.save_json_results(duplicate_groups, args.json)
            
        print(f"\nSummary:")
        print(f"- {detector.stats['duplicate_files']} duplicate files found")
        print(f"- {detector.stats['space_wasted'] / (1024*1024*1024):.2f} GB wasted space")
        print(f"- Processing completed in {detector.stats['processing_time']:.1f} seconds")
        
    else:
        detector.log("No duplicates found!")
        print("Your media collection appears to be duplicate-free.")


if __name__ == "__main__":
    main()