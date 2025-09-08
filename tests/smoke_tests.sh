#!/bin/bash
#
# Smoke tests for simple-httpfs FUSE filesystem
# 
# These tests require FUSE to be available and should be run manually.
# They are not part of the automated test suite.
#

set -euxo pipefail

TARGET1="https://raw.githubusercontent.com/octocat/Hello-World/master/README"
TARGET2="s3://pkerp/public/tiny.txt"
MOUNT_POINT="/tmp/cloud"
EOL="..."

echo "Starting filesystem..."
mkdir -p "$MOUNT_POINT"
simple-httpfs -f -v "$MOUNT_POINT" --log /dev/null &
sleep 2

echo "Testing HTTP..."
http_url="$MOUNT_POINT/https:/raw.githubusercontent.com/octocat/Hello-World/master/README"
ls -la $MOUNT_POINT/$TARGET1$EOL
ls -la $MOUNT_POINT/$TARGET2$EOL
head -c 100 $MOUNT_POINT/$TARGET1$EOL
wc -c $MOUNT_POINT/$TARGET1$EOL

echo "Testing concurrent operations..."
head -c 100 $MOUNT_POINT/$TARGET1$EOL &
pid1=$!
head -c 100 $MOUNT_POINT/$TARGET2$EOL &
pid2=$!
wait $pid1 $pid2
echo "Concurrent operations completed successfully";

echo "Stopping filesystem..."
umount "$MOUNT_POINT"