#!/bin/bash

# run_duplicate_detection.sh
# Version 1.0 - Scheduled Duplicate Detection for NAS
#
# This script pulls the latest code from Git and runs duplicate detection
# with comprehensive logging. Designed for Synology Task Scheduler.
#
# Usage:
#   ./run_duplicate_detection.sh [scan_directory] [options]
#
# Examples:
#   ./run_duplicate_detection.sh /volume1/photo
#   ./run_duplicate_detection.sh /volume1/video --json /volume1/reports/duplicates.json
#
# Setup for Synology Task Scheduler:
# 1. Control Panel > Task Scheduler > Create > Scheduled Task > User-defined script
# 2. General: Name the task (e.g., "Duplicate Detection")
# 3. Schedule: Set desired frequency (weekly/monthly recommended)
# 4. Task Settings:
#    - User-defined script: bash /volume1/scripts/run_duplicate_detection.sh /volume1/photo
#    - Send run details by email: Yes (optional)

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="detect_duplicates_nas.py"
LOG_DIR="$SCRIPT_DIR/logs"
REPORT_DIR="$SCRIPT_DIR/reports"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_FILE="$LOG_DIR/duplicate_detection_log_$TIMESTAMP.txt"
REPORT_FILE="$REPORT_DIR/duplicate_report_$TIMESTAMP.txt"

# Colors for console output (if supported)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log_message() {
    local level="$1"
    local message="$2"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Enhanced logging with colors for console
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
    log_message "INFO" "$1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
    log_message "WARNING" "$1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    log_message "ERROR" "$1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
    log_message "DEBUG" "$1"
}

# Create directories if they don't exist
create_directories() {
    mkdir -p "$LOG_DIR" "$REPORT_DIR"
    if [[ $? -ne 0 ]]; then
        echo "Failed to create directories" >&2
        exit 1
    fi
}

# Update from Git repository
update_from_git() {
    log_info "Updating from Git repository..."
    cd "$SCRIPT_DIR"
    
    # Check if we're in a git repository
    if [[ ! -d ".git" ]]; then
        log_warning "Not a Git repository - skipping update"
        return 0
    fi
    
    # Stash any local changes
    if git status --porcelain | grep -q .; then
        log_info "Stashing local changes..."
        git stash push -m "Auto-stash before scheduled run $TIMESTAMP" >> "$LOG_FILE" 2>&1
    fi
    
    # Pull latest changes
    log_info "Pulling latest changes from origin..."
    if git pull origin main >> "$LOG_FILE" 2>&1; then
        log_info "Git update successful"
        
        # Log the current commit
        CURRENT_COMMIT=$(git rev-parse --short HEAD)
        COMMIT_MESSAGE=$(git log -1 --pretty=format:"%s")
        log_info "Running with commit: $CURRENT_COMMIT - $COMMIT_MESSAGE"
    else
        log_error "Git pull failed - continuing with current version"
        return 1
    fi
}

# Check script dependencies
check_dependencies() {
    log_info "Checking dependencies..."
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python3 not found"
        return 1
    fi
    
    local python_version=$(python3 --version 2>&1)
    log_info "Found: $python_version"
    
    # Check if duplicate detection script exists
    if [[ ! -f "$SCRIPT_DIR/$SCRIPT_NAME" ]]; then
        log_error "Duplicate detection script not found: $SCRIPT_DIR/$SCRIPT_NAME"
        return 1
    fi
    
    log_info "All dependencies satisfied"
    return 0
}

# Run duplicate detection
run_duplicate_detection() {
    local scan_directory="$1"
    shift  # Remove first argument, rest are options
    local options="$@"
    
    log_info "Starting duplicate detection..."
    log_info "Scan directory: $scan_directory"
    log_info "Options: $options"
    log_info "Report file: $REPORT_FILE"
    
    # Build command
    local cmd="python3 '$SCRIPT_DIR/$SCRIPT_NAME' '$scan_directory' --report '$REPORT_FILE' $options"
    log_debug "Command: $cmd"
    
    # Record start time
    local start_time=$(date +%s)
    
    # Run the detection
    if eval $cmd >> "$LOG_FILE" 2>&1; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        log_info "Duplicate detection completed successfully in ${duration}s"
        
        # Check if report was created
        if [[ -f "$REPORT_FILE" ]]; then
            local report_size=$(wc -l < "$REPORT_FILE")
            log_info "Report generated: $REPORT_FILE ($report_size lines)"
            
            # Extract key statistics from report if possible
            if grep -q "Duplicate groups found:" "$REPORT_FILE"; then
                local duplicate_groups=$(grep "Duplicate groups found:" "$REPORT_FILE" | cut -d':' -f2 | tr -d ' ')
                local space_wasted=$(grep "Space wasted by duplicates:" "$REPORT_FILE" | cut -d':' -f2 | tr -d ' ')
                log_info "Results: $duplicate_groups duplicate groups, $space_wasted wasted space"
            fi
        else
            log_warning "Report file was not created"
        fi
        
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        log_error "Duplicate detection failed after ${duration}s"
        return 1
    fi
}

# Cleanup old logs and reports
cleanup_old_files() {
    log_info "Cleaning up old files..."
    
    # Keep last 10 log files
    cd "$LOG_DIR"
    if ls duplicate_detection_log_*.txt 1> /dev/null 2>&1; then
        local old_logs=$(ls -t duplicate_detection_log_*.txt | tail -n +11)
        if [[ -n "$old_logs" ]]; then
            echo "$old_logs" | xargs rm -f
            log_info "Removed old log files: $(echo "$old_logs" | wc -l) files"
        fi
    fi
    
    # Keep last 5 report files
    cd "$REPORT_DIR"
    if ls duplicate_report_*.txt 1> /dev/null 2>&1; then
        local old_reports=$(ls -t duplicate_report_*.txt | tail -n +6)
        if [[ -n "$old_reports" ]]; then
            echo "$old_reports" | xargs rm -f
            log_info "Removed old report files: $(echo "$old_reports" | wc -l) files"
        fi
    fi
}

# Email notification (if mail is configured)
send_notification() {
    local status="$1"
    local scan_directory="$2"
    
    if command -v mail &> /dev/null; then
        local subject="Duplicate Detection $status - $(hostname)"
        local body="Duplicate detection $status for $scan_directory at $(date)

Log file: $LOG_FILE
Report file: $REPORT_FILE

Check the logs for details."
        
        echo "$body" | mail -s "$subject" root 2>/dev/null || log_warning "Failed to send email notification"
    fi
}

# Main execution
main() {
    echo "================================================================="
    echo "NAS Duplicate Detection Runner v1.0"
    echo "================================================================="
    echo "Started at: $(date)"
    echo "Script directory: $SCRIPT_DIR"
    echo "Log file: $LOG_FILE"
    echo
    
    # Create required directories
    create_directories
    
    # Start logging
    log_info "=== Duplicate Detection Run Started ==="
    log_info "Arguments: $*"
    
    # Parse arguments
    if [[ $# -lt 1 ]]; then
        log_error "Usage: $0 <scan_directory> [options]"
        echo "Usage: $0 <scan_directory> [options]"
        echo
        echo "Examples:"
        echo "  $0 /volume1/photo"
        echo "  $0 /volume1/video --json /tmp/results.json"
        echo "  $0 /shared/media --no-recursive"
        exit 1
    fi
    
    local scan_directory="$1"
    shift
    local options="$*"
    
    # Validate scan directory
    if [[ ! -d "$scan_directory" ]]; then
        log_error "Scan directory does not exist: $scan_directory"
        exit 1
    fi
    
    # Update from Git
    update_from_git
    
    # Check dependencies
    if ! check_dependencies; then
        log_error "Dependency check failed"
        exit 1
    fi
    
    # Run duplicate detection
    if run_duplicate_detection "$scan_directory" $options; then
        log_info "=== Duplicate Detection Run Completed Successfully ==="
        send_notification "SUCCESS" "$scan_directory"
        
        # Cleanup old files
        cleanup_old_files
        
        echo
        echo "================================================================="
        echo "Duplicate detection completed successfully!"
        echo "Report: $REPORT_FILE"
        echo "Log: $LOG_FILE"
        echo "================================================================="
        
        exit 0
    else
        log_error "=== Duplicate Detection Run Failed ==="
        send_notification "FAILED" "$scan_directory"
        
        echo
        echo "================================================================="
        echo "Duplicate detection FAILED!"
        echo "Check log for details: $LOG_FILE"
        echo "================================================================="
        
        exit 1
    fi
}

# Execute main function with all arguments
main "$@"