#!/bin/bash
#docker ps
#find postgres <container id>
#example: '087bcb8017e7'
CONTAINER=<container id>
SRC_PATH='/Users/weiyili/Desktop/Projects/user-behavior-analysis'
DEST_PATH='/var/lib/postgresql/data/raw'

docker cp $SRC_PATH/data/raw/. $CONTAINER:$DEST_PATH
echo "Files have been copied."