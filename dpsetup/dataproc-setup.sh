#Run in gcloud shell
gcloud dataproc clusters 
create my-cluster 
--region us-central1 
--zone us-central1-b 
--single-node 
--master-machine-type n1-standard-4 
--master-boot-disk-size 500 
--image-version 2.0-debian10 
--expiration-time Sun Jul 31 2022 00:00:00 GMT-0400 (Eastern Daylight Time) 
--scopes 'https://www.googleapis.com/auth/cloud-platform' 
--project user-behavior-analysis