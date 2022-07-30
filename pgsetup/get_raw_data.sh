#!/bin/bash
cd /Users/weiyili/Desktop/Projects/user-behavior-analysis
#mkdir data

#Run script to download raw files from AWS S3 bucket
#chmod +x dev/raw_data.py
./pgsetup/raw_data.py

#Unzip files and move to data folder
unzip data.zip
rm data.zip

mv movie_review.csv  airflow-docker/data