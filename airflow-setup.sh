#!bin/bash
cd /Users/weiyili/Desktop/Projects/user-behavior-analysis
mkdir airflow-docker
cd airflow-docker
#copy docker-compose.yml into this directory
#curl -Lf0 'http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/latest/docker-compose.yaml'
mkdir ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up