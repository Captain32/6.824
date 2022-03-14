#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT

# Note: because the socketID is based on the current userID,
# ./test-mr.sh cannot be run in parallel
runs=$1
chmod +x test-sc.sh

for i in $(seq 1 $runs); do
    echo 'Trail ' $i
    #go test -run TestSnapshotInstallUnreliable2D > test_1.txt
    go test -race > test.txt
    if grep -q 'FAIL' test.txt; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi
done
echo '***' PASSED ALL $i TESTING TRIALS