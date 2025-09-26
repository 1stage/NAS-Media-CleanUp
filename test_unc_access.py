"""
test_unc_access.py
Version 0.3 (Pre-release) — Created by Sean P. Harrington with assistance from Microsoft Copilot  
Date: Friday, 26 September 2025, 08:27 AM PDT

Tests access to upload directories defined in oby.cfg and lists all files found,
excluding year-based subfolders. Useful for verifying UNC path access and folder visibility.

Requirements:
    oby.cfg must be present in the same folder.
"""

import os
import configparser
from datetime import datetime

# === LOAD CONFIG ===
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "oby.cfg"))

UPLOAD_DIRS = [d.strip() for d in config.get("paths", "upload_dirs").split(",")]

# === TIMESTAMPED LOG PATH ===
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
base_dir = os.path.dirname(os.path.abspath(__file__))
log_folder = os.path.join(base_dir, config.get("logging", "log_folder"))
os.makedirs(log_folder, exist_ok=True)
LOG_PATH = os.path.join(log_folder, f"unc_test_log_{timestamp}.txt")

log_entries = []

def log(message):
    print(message)
    log_entries.append(message)

def test_access():
    for upload_dir in UPLOAD_DIRS:
        log(f"\nScanning: {upload_dir}")
        if not os.path.exists(upload_dir):
            log(f"[ERROR] Path not found or inaccessible: {upload_dir}")
            continue

        for root, _, files in os.walk(upload_dir):
            folder_name = os.path.basename(root)
            if folder_name.isdigit() and len(folder_name) == 4:
                continue  # Skip year-based subfolders

            for file in files:
                filepath = os.path.join(root, file)
                log(f"[FOUND] {filepath}")

def write_log():
    with open(LOG_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(log_entries))

if __name__ == "__main__":
    test_access()
    write_log()