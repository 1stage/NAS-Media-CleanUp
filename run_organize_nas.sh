#!/bin/bash
# NAS Media Organization v1.0 - Auto-updating Runner Script
# Ensures the latest version is always used for scheduled runs

LOG_FILE="/volume1/share/sph/NAS-Media-CleanUp/logs/auto_run_$(date +%Y-%m-%d_%H-%M-%S).log"

echo "$(date): Starting automated media organization..." >> "$LOG_FILE"

# Change to script directory
cd /volume1/share/sph/NAS-Media-CleanUp

# Update from Git
echo "$(date): Pulling latest updates from Git..." >> "$LOG_FILE"
if git pull origin main >> "$LOG_FILE" 2>&1; then
    echo "$(date): Git pull successful" >> "$LOG_FILE"
else
    echo "$(date): Warning - Git pull failed, continuing with current version" >> "$LOG_FILE"
fi

# Run the organization script
echo "$(date): Running media organization script..." >> "$LOG_FILE"
if python3 organize_by_year_nas.py --delete-duplicates --cleanup-empty-dirs --report >> "$LOG_FILE" 2>&1; then
    echo "$(date): Media organization completed successfully" >> "$LOG_FILE"
else
    echo "$(date): Error - Media organization failed" >> "$LOG_FILE"
    exit 1
fi

echo "$(date): Automated run completed" >> "$LOG_FILE"