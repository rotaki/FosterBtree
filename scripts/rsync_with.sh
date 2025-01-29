#!/bin/bash

# Check if user and host are provided as arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <remote_user> <remote_host>"
    exit 1
fi

# Assign the input arguments to variables
REMOTE_USER=$1
REMOTE_HOST=$2
REMOTE_DIR="/mnt/nvme1/rotaki/FosterBtree" # Replace with the path to the remote directory
# REMOTE_DIR="/tank/local/rotaki/FosterBtree" # Replace with the path to the remote directory

# Check if .gitignore file exists
if [ ! -f .gitignore ]; then
    echo ".gitignore file not found!"
    exit 1
fi

# Create an array of rsync exclude parameters from .gitignore
RSYNC_EXCLUDES=()
while IFS= read -r line; do
    # Skip empty lines and comments
    if [[ -n "$line" && "$line" != \#* ]]; then
        RSYNC_EXCLUDES+=(--exclude="$line")
    fi
done < .gitignore

# Run rsync with the constructed exclude parameters
rsync -avz --delete "${RSYNC_EXCLUDES[@]}" . "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}"

# Check if rsync was successful
if [ $? -eq 0 ]; then
    echo "Sync completed successfully."
else
    echo "An error occurred during sync."
fi
