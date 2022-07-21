docker run \
--name my-postgres \
-p 5432:5432 \
-e POSTGRES_PASSWORD=mysecretpassword \
-d \
postgres