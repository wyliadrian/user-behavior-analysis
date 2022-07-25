#!bin/bash
#Basic modification on Postgres container after setting up airflow
docker ps
#get container id <container_id>
docker exec -it -u postgres <container_id> bash
cd /var/lib/postgresql/data/
mkdir raw stage