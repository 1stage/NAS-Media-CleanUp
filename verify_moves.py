#!/usr/bin/env python3
"""
verify_moves.py
Spot-check files from a report to verify they exist in their target directories.
"""

import os
import sys
import random

def parse_report(report_path):
    """Parse the report file to extract moved files."""
    moved_files = []
    
    with open(report_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Find the moved files section (before the summary)
    in_moves_section = False
    for line in lines:
        line = line.strip()
        if line == "Moved Files Report":
            in_moves_section = True
            continue
        elif line.startswith("----"):
            break
        elif in_moves_section and " → " in line:
            # Parse the line: filename → target_path
            parts = line.split(" → ")
            if len(parts) == 2:
                filename = parts[0].strip()
                target_dir = parts[1].strip()
                target_path = os.path.join(target_dir, filename)
                moved_files.append((filename, target_path))
    
    return moved_files

def verify_files(moved_files, sample_size=10):
    """Verify a random sample of moved files exist."""
    if not moved_files:
        print("No moved files found in report.")
        return
    
    # Take a random sample
    sample_size = min(sample_size, len(moved_files))
    sample = random.sample(moved_files, sample_size)
    
    print(f"Verifying {sample_size} random files from {len(moved_files)} total moves...")
    print("=" * 80)
    
    verified = 0
    missing = 0
    
    for filename, target_path in sample:
        if os.path.exists(target_path):
            print(f"✅ FOUND: {filename}")
            print(f"   Path: {target_path}")
            verified += 1
        else:
            print(f"❌ MISSING: {filename}")
            print(f"   Expected: {target_path}")
            missing += 1
        print()
    
    print("=" * 80)
    print(f"VERIFICATION RESULTS:")
    print(f"Files checked: {sample_size}")
    print(f"Found: {verified} ({verified/sample_size*100:.1f}%)")
    print(f"Missing: {missing} ({missing/sample_size*100:.1f}%)")
    
    if missing == 0:
        print("🎉 All sampled files verified successfully!")
    else:
        print(f"⚠️  {missing} files appear to be missing from target locations.")

def main():
    if len(sys.argv) != 2:
        print("Usage: python verify_moves.py <report_file>")
        sys.exit(1)
    
    report_path = sys.argv[1]
    
    if not os.path.exists(report_path):
        print(f"Error: Report file not found: {report_path}")
        sys.exit(1)
    
    print(f"Analyzing report: {report_path}")
    moved_files = parse_report(report_path)
    verify_files(moved_files)

if __name__ == "__main__":
    main()