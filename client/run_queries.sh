#!/bin/bash

# Define server host and port
HOST="localhost"
PORT="5000"

# Extract the requests.zip file
unzip -o queries.zip

# Loop through all request files
for file in queries_0{0..9}.txt; do
    echo "Processing $file..."
    while IFS= read -r line; do
        echo "Sending command: $line"
        echo "$line" | python3 cli.py "$HOST" "$PORT"
    done < "$file"
done

echo "All requests processed."

