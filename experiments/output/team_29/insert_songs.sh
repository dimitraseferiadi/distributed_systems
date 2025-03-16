#!/bin/bash

START_TIME=$(date +%s%N)

# Define hosts (each appears twice for 2 ports)
hosts=("10.0.36.56" "10.0.36.56" "10.0.36.124" "10.0.36.124" "10.0.36.15" "10.0.36.15" "10.0.36.43" "10.0.36.43" "10.0.36.80" "10.0.36.80")

# Generate unique keys (A-Z, a-z, AA-ZZ)
values=()
for c in {A..Z}; do values+=("$c"); done
for c in {a..z}; do values+=("$c"); done
for c1 in {A..Z}; do
  for c2 in {A..Z}; do
    values+=("$c1$c2")
  done
done

value_index=0  # Global index
mkdir -p logs  # Create logs directory if not exists

for i in {0..9}; do
  port=$((5000 + i))  # Assign ports 5000-5009
  host=${hosts[i]}    # Assign hosts in round-robin

  (
    while read -r song_title; do
      value=${values[value_index]}  # Generate unique key
      key=$song_title  # Convert song title to a valid key (replace spaces)

      # Run Chordify CLI and pass the insert command
      (
        echo "insert \"$key\" \"$host\" \"$port\" \"$value\""
        echo "exit"
      ) | python3 ~/distributed_systems/server/client_with_ports_insert.py >> "logs/insert_$port.log" 2>&1  

      ((value_index++))
    done < "insert/insert_0${i}_part.txt"
  ) &
done

wait  # Wait for all parallel inserts to finish

END_TIME=$(date +%s%N)
TOTAL_TIME=$((END_TIME - START_TIME))
TOTAL_TIME_SECONDS=$(echo "scale=9; $TOTAL_TIME / 1000000000" | bc)

echo "All inserts completed in $TOTAL_TIME nanoseconds ($TOTAL_TIME_SECONDS seconds)."
THROUGHPUT=$(echo "scale=9; 500 / $TOTAL_TIME_SECONDS" | bc)
echo "Throughput: $THROUGHPUT inserts/sec"

