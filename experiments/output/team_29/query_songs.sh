#!/bin/bash

START_TIME=$(date +%s%N)

# Define hosts (each appears twice for 2 ports)
hosts=("10.0.36.56" "10.0.36.56" "10.0.36.124" "10.0.36.124" "10.0.36.15" "10.0.36.15" "10.0.36.43" "10.0.36.43" "10.0.36.80" "10.0.36.80")

mkdir -p logs  # Create logs directory if not exists

for i in {0..9}; do
  port=$((5000 + i))  # Assign ports 5000-5009
  host=${hosts[i]}    # Assign hosts in round-robin
  file="queries/query_0${i}.txt"

  if [[ ! -f "$file" ]]; then
    echo "[WARNING] File $file not found. Skipping..."
    continue
  fi

  (
    while read -r song_title; do
      key="$song_title"  # ✅ Keep the key as is

      echo "[QUERY] Sending query \"$key\" to $host:$port..."
      (
        echo "query \"$key\" $host $port"  # ✅ Correct format (no quotes around host/port)
        echo "exit"
      ) | python3 ~/distributed_systems/server/client_with_ports_insert.py  

    done < "$file"
  ) &
done

wait  # Wait for all parallel queries to finish

END_TIME=$(date +%s%N)
TOTAL_TIME=$((END_TIME - START_TIME))
TOTAL_TIME_SECONDS=$(echo "scale=9; $TOTAL_TIME / 1000000000" | bc)

echo "All queries completed in $TOTAL_TIME nanoseconds ($TOTAL_TIME_SECONDS seconds)."
THROUGHPUT=$(echo "scale=9; 500 / $TOTAL_TIME_SECONDS" | bc)
echo "Throughput: $THROUGHPUT queries/sec"

