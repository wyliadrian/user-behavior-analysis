#!/bin/bash
CONTAINER='4741fa41e248'
SRC_PATH='/Users/weiyili/Desktop/Projects/user-behavior-analysis'
DEST_PATH='/var/lib/postgresql/data/user-behavior-analysis/raw'

docker cp $SRC_PATH/data/raw/. $CONTAINER:$DEST_PATH
echo "Files have been copied."