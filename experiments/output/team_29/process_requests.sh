#!/bin/bash

START_TIME=$(date +%s%N)

# Define hosts (each appears twice for 2 ports)
hosts=("10.0.36.56" "10.0.36.56" "10.0.36.124" "10.0.36.124" "10.0.36.15" "10.0.36.15" "10.0.36.43" "10.0.36.43" "10.0.36.80" "10.0.36.80")

mkdir -p logs  # Create logs directory if not exists
LOG_FILE="logs/ev_all_k_3_new.log"
LOCK_FILE="logs/ev_all_k_3_new.lock"

# Logging function using flock for atomic writes
log_message() {
  local message="$1"
  (
    flock -x 200
    echo -e "$message" >> "$LOG_FILE"
  ) 200>"$LOCK_FILE"
}

for i in {0..9}; do
  port=$((5000 + i))  # Assign ports 5000-5009
  host=${hosts[i]}    # Assign hosts in round-robin
  file="request/requests_0${i}.txt"

  if [[ ! -f "$file" ]]; then
    log_message "[WARNING] File $file not found. Skipping..."
    continue
  fi

  log_message "[INFO] Processing requests from $file on $host:$port..."

  (
    while IFS=, read -r command key value; do
      # Remove leading/trailing spaces (keep spaces inside key)
      command=$(echo "$command")
      key=$(echo "$key" | sed 's/^ *//g' | sed 's/ *$//g')
      value=$(echo "$value")
      
      block=""  # Initialize a block to collect all output for a request
      
      if [[ "$command" == "query" ]]; then
        query_cmd="query \"$key\" $host $port"
        block+="[QUERY] Sending: $query_cmd\n"
        response=$( { echo "$query_cmd"; echo "exit"; } | python3 ~/distributed_systems/server/client_with_ports_insert.py 2>&1 )
        block+="[QUERY RESPONSE] $response\n"
      elif [[ "$command" == "insert" ]]; then
        insert_cmd="insert \"$key\" $host $port \"$value\""
        block+="[INSERT] Executing: $insert_cmd\n"
        response=$( { echo "$insert_cmd"; echo "exit"; } | python3 ~/distributed_systems/server/client_with_ports_insert.py 2>&1 )
        block+="[INSERT RESPONSE] $response\n"
      fi
      
      # Write the entire block in one atomic call
      log_message "$block"
    done < "$file"
  ) &
done

wait  # Wait for all background processes to finish

END_TIME=$(date +%s%N)
TOTAL_TIME=$((END_TIME - START_TIME))
TOTAL_TIME_SECONDS=$(echo "scale=9; $TOTAL_TIME / 1000000000" | bc)

echo "All inserts completed in $TOTAL_TIME nanoseconds ($TOTAL_TIME_SECONDS seconds)."
THROUGHPUT=$(echo "scale=9; 500 / $TOTAL_TIME_SECONDS" | bc)
echo "Throughput: $THROUGHPUT inserts/sec"


