#!/bin/bash
cd /Users/weiyili/Desktop/Projects/user-behavior-analysis/
# mkdir data
# mkdir data/raw

# Run script to download raw files from AWS S3 bucket
./dev/raw_data.py

# Unzip files and move to data folder
unzip data.zip
rm data.zip
mv data/*.csv data/raw