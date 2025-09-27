#!/usr/bin/env python3
import sqlite3

# Connect to the database
conn = sqlite3.connect(r'\\NAS-MEDIA\photo\Sorted\photo_duplicates.db')
cursor = conn.cursor()

print("=== 2020 - Photos Duplicate Detection Results ===\n")

# Check total files processed
cursor.execute('SELECT COUNT(*) FROM photo_files')
total_files = cursor.fetchone()[0]
print(f'Total files processed: {total_files}')

# Check duplicates found
cursor.execute('SELECT COUNT(*) FROM photo_files WHERE binary_verified = 1 AND is_original = 0')
total_duplicates = cursor.fetchone()[0]
print(f'Total duplicates found: {total_duplicates}')

# Check unique originals
cursor.execute('SELECT COUNT(*) FROM photo_files WHERE is_original = 1')
total_originals = cursor.fetchone()[0]
print(f'Total originals marked: {total_originals}')

# Check files flagged for removal
cursor.execute('SELECT COUNT(*) FROM photo_files WHERE removal_flagged = 1')
flagged_for_removal = cursor.fetchone()[0]
print(f'Files flagged for removal: {flagged_for_removal}')

# Check duplicate groups
cursor.execute('SELECT COUNT(*) FROM duplicate_groups')
total_groups = cursor.fetchone()[0]
print(f'Total duplicate groups: {total_groups}')

# Calculate space recoverable
cursor.execute('SELECT SUM(file_size) FROM photo_files WHERE removal_flagged = 1')
space_recoverable_bytes = cursor.fetchone()[0] or 0
space_recoverable_mb = space_recoverable_bytes / (1024 * 1024)
print(f'Space recoverable: {space_recoverable_mb:.2f} MB')

# Show some sample duplicates
print('\n=== Sample Duplicate Groups ===')
cursor.execute('''
    SELECT normalized_hash, COUNT(*) as group_size 
    FROM photo_files 
    WHERE binary_verified = 1
    GROUP BY normalized_hash 
    HAVING COUNT(*) > 1
    ORDER BY group_size DESC
    LIMIT 5
''')

for hash_val, group_size in cursor.fetchall():
    print(f'\nGroup {hash_val[:8]}... ({group_size} files):')
    cursor.execute('''
        SELECT relative_path, is_original, file_size 
        FROM photo_files 
        WHERE normalized_hash = ? 
        ORDER BY is_original DESC
    ''', (hash_val,))
    
    for filename, is_original, file_size in cursor.fetchall():
        status_emoji = "🟢" if is_original else "🔴"
        status_text = "ORIGINAL" if is_original else "DUPLICATE"
        size_mb = file_size / (1024 * 1024)
        print(f'  {status_emoji} {filename[:60]} ({size_mb:.2f} MB) - {status_text}')

conn.close()