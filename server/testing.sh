#!/bin/bash

# Define node ports
NODE1=6000
NODE2=6001
NODE3=6002

# Insert songs into different nodes
echo "Inserting songs into Chord DHT..."

output=$(python3 client.py insert "Like a Rolling Stone" "node1_location" --host 127.0.0.1 --port $NODE1)
echo "$output"

output=$(python3 client.py insert "Satisfaction" "node1_location" --host 127.0.0.1 --port $NODE1)
echo "$output"

output=$(python3 client.py insert "The Message" "node2_location" --host 127.0.0.1 --port $NODE2)
echo "$output"

output=$(python3 client.py insert "When Doves Cry" "node2_location" --host 127.0.0.1 --port $NODE2)
echo "$output"

output=$(python3 client.py insert "Smells Like Teen Spirit" "node3_location" --host 127.0.0.1 --port $NODE3)
echo "$output"

output=$(python3 client.py insert "Bohemian Rhapsody" "node3_location" --host 127.0.0.1 --port $NODE3)
echo "$output"

output=$(python3 client.py insert "Hotel California" "node1_location" --host 127.0.0.1 --port $NODE1)
echo "$output"

output=$(python3 client.py insert "Billie Jean" "node2_location" --host 127.0.0.1 --port $NODE2)
echo "$output"

output=$(python3 client.py insert "Stairway to Heaven" "node3_location" --host 127.0.0.1 --port $NODE3)
echo "$output"

output=$(python3 client.py insert "Hey Jude" "node1_location" --host 127.0.0.1 --port $NODE1)
echo "$output"

echo "Insertion complete!"

# Query for a few songs
echo "Querying songs..."

output=$(python3 client.py query "Like a Rolling Stone" --host 127.0.0.1 --port $NODE1)
echo "$output"

output=$(python3 client.py query "Satisfaction" --host 127.0.0.1 --port $NODE1)
echo "$output"

output=$(python3 client.py query "The Message" --host 127.0.0.1 --port $NODE2)
echo "$output"

output=$(python3 client.py query "When Doves Cry" --host 127.0.0.1 --port $NODE2)
echo "$output"

output=$(python3 client.py query "Smells Like Teen Spirit" --host 127.0.0.1 --port $NODE3)
echo "$output"

# Delete a song and verify deletion
echo "Deleting a song..."

output=$(python3 client.py delete "Like a Rolling Stone" --host 127.0.0.1 --port $NODE1)
echo "$output"

output=$(python3 client.py query "Like a Rolling Stone" --host 127.0.0.1 --port $NODE1)
echo "$output"

output=$(python3 client.py delete "Bohemian Rhapsody" --host 127.0.0.1 --port $NODE3)
echo "$output"

output=$(python3 client.py query "Bohemian Rhapsody" --host 127.0.0.1 --port $NODE3)
echo "$output"

# Query all songs from different nodes to verify all entries
echo "Querying all songs..."

output=$(python3 client.py query "*" --host 127.0.0.1 --port $NODE1)
echo "$output"

output=$(python3 client.py query "*" --host 127.0.0.1 --port $NODE2)
echo "$output"

output=$(python3 client.py query "*" --host 127.0.0.1 --port $NODE3)
echo "$output"

echo "Test complete!"

