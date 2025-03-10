#!/bin/bash

# Usage: ./run_inserts.sh
# Make sure servers are already running before executing this script.

# Define the ZIP file containing the insert files
insert_zip="insert.zip"

# Directory to extract the files to
extract_dir="insert_data"

# Ensure the output directory exists
mkdir -p "$extract_dir"

# Extract all files from the ZIP archive
echo "Extracting files from $insert_zip..."
unzip -q "$insert_zip" -d "$extract_dir"

START_TIME=$(date +%s%N) # Start time in nanoseconds

# Fire off 10 parallel processes (one for each node/file)
for i in {0..9}; do
  port=$((5000 + i))  # Assign a unique port for each node

  # For each file insert_0{i}.txt, do line-by-line inserts using the CLI
  (
    # Read each song title from the extracted file
    while read -r song_title; do
      # Example CLI usage for inserting a song into the DHT system
      python3 client.py insert --host 127.0.0.1 --port "$port" --key "$song_title" --k 3 --consistency "l"
    done < "$extract_dir/insert_0${i}_part.txt"
  ) &

done

# Wait for all background jobs (insertions) to finish
wait

END_TIME=$(date +%s%N)   # End time in nanoseconds
TOTAL_TIME=$((END_TIME - START_TIME)) # Total time in nanoseconds

# Convert nanoseconds to seconds for easier readability
TOTAL_TIME_SECONDS=$(echo "scale=9; $TOTAL_TIME / 1000000000" | bc)
echo "All inserts completed in $TOTAL_TIME nanoseconds ($TOTAL_TIME_SECONDS seconds)."

# Calculate throughput which is the time per 500 requests (assuming 500 keys per file)
# You can adjust this based on the actual number of songs in each file.
NUM_KEYS=500
THROUGHPUT=$(echo "scale=9; $NUM_KEYS * 10 / $TOTAL_TIME_SECONDS" | bc)
echo "Throughput: $THROUGHPUT requests/second."

