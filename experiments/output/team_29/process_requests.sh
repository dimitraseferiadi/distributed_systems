#!/bin/bash

START_TIME=$(date +%s%N)

# Define hosts (each appears twice for 2 ports)
hosts=("10.0.36.56" "10.0.36.56" "10.0.36.124" "10.0.36.124" "10.0.36.15" "10.0.36.15" "10.0.36.43" "10.0.36.43" "10.0.36.80" "10.0.36.80")

mkdir -p logs  # Create logs directory if not exists

echo "[INFO] Waiting 5 seconds before sending requests..."
  # ✅ Ensure nodes are running

for i in {0..9}; do
  port=$((5000 + i))  # Assign ports 5000-5009
  host=${hosts[i]}    # Assign hosts in round-robin
  file="request/requests_0${i}.txt"

  if [[ ! -f "$file" ]]; then
    echo "[WARNING] File $file not found. Skipping..."
    continue
  fi

  echo "[INFO] Processing requests from $file on $host:$port..."

  (
    while IFS=, read -r command key value; do
      if [[ "$command" == "query" ]]; then
        echo "[QUERY] Sending query \"$key\" to $host:$port..."
        (
          echo "query \"$key\" $host $port"
          echo "exit"
        ) | python3 ~/distributed_systems/server/client_with_ports_insert.py  
      
      elif [[ "$command" == "insert" ]]; then
        echo "[INSERT] Inserting \"$key\" with value \"$value\" to $host:$port..."
        (
          echo "insert \"$key\" $host $port \"$value\""
          echo "exit"
        ) | python3 ~/distributed_systems/server/client_with_ports_insert.py  
      
      else
        echo "[ERROR] Invalid command \"$command\" in $file"
      fi

    done < "$file"
  ) &
done

wait  # Wait for all parallel requests to finish

END_TIME=$(date +%s%N)
TOTAL_TIME=$((END_TIME - START_TIME))
TOTAL_TIME_SECONDS=$(echo "scale=9; $TOTAL_TIME / 1000000000" | bc)

echo "All requests completed in $TOTAL_TIME nanoseconds ($TOTAL_TIME_SECONDS seconds)."
THROUGHPUT=$(echo "scale=9; 500 / $TOTAL_TIME_SECONDS" | bc)
echo "Throughput: $THROUGHPUT requests/sec"

